from __future__ import annotations

import json
import os
import sqlite3
import time
from pathlib import Path

import altair as alt
import numpy as np
import pandas as pd
import streamlit as st
from scipy import signal

DB_PATH = Path(os.environ.get("POLAR_DASH_DB", "data/polar_dash.db")).expanduser().resolve()

st.set_page_config(page_title="Polar Dash", layout="wide")
st.title("Polar Dash")
st.caption(
    "Live Polar H10 dashboard. HRV is rolling RMSSD from RR intervals. "
    "Breathing rate is estimated from chest-strap accelerometer motion, so it "
    "works best when movement is limited."
)


def _ns_to_time(values: pd.Series | np.ndarray) -> pd.Series:
    return pd.to_datetime(values, unit="ns", utc=True)


def _latest_numeric(frame: pd.DataFrame, column: str) -> float | None:
    if frame.empty:
        return None
    series = frame[column].dropna()
    if series.empty:
        return None
    return float(series.iloc[-1])


@st.cache_data(ttl=2, show_spinner=False)
def load_snapshot(db_path: str, lookback_minutes: int) -> dict[str, pd.DataFrame | pd.Series | str | None]:
    resolved = Path(db_path).expanduser().resolve()
    if not resolved.exists():
        return {"db_path": str(resolved), "session": None}

    connection = sqlite3.connect(resolved)
    try:
        session = pd.read_sql_query(
            """
            SELECT id, started_at_ns, ended_at_ns, device_name, device_address, battery_percent
            FROM sessions
            ORDER BY started_at_ns DESC
            LIMIT 1
            """,
            connection,
        )
        if session.empty:
            return {"db_path": str(resolved), "session": None}

        session_row = session.iloc[0]
        window_start_ns = max(
            int(session_row["started_at_ns"]),
            time.time_ns() - lookback_minutes * 60 * 1_000_000_000,
        )
        params = (int(session_row["id"]), window_start_ns)

        hr_frames = pd.read_sql_query(
            """
            SELECT recorded_at_ns, average_hr_bpm, rr_intervals_ms_json, energy_kj
            FROM hr_frames
            WHERE session_id = ? AND recorded_at_ns >= ?
            ORDER BY recorded_at_ns
            """,
            connection,
            params=params,
        )
        ecg_frames = pd.read_sql_query(
            """
            SELECT sensor_recorded_at_ns, sample_rate_hz, sample_count, samples_json
            FROM ecg_frames
            WHERE session_id = ? AND sensor_recorded_at_ns >= ?
            ORDER BY sensor_recorded_at_ns
            """,
            connection,
            params=params,
        )
        acc_frames = pd.read_sql_query(
            """
            SELECT sensor_recorded_at_ns, sample_rate_hz, sample_count, samples_json
            FROM acc_frames
            WHERE session_id = ? AND sensor_recorded_at_ns >= ?
            ORDER BY sensor_recorded_at_ns
            """,
            connection,
            params=params,
        )
        events = pd.read_sql_query(
            """
            SELECT recorded_at_ns, level, event_type, details_json
            FROM collector_events
            WHERE session_id = ?
            ORDER BY recorded_at_ns DESC
            LIMIT 10
            """,
            connection,
            params=(int(session_row["id"]),),
        )
    finally:
        connection.close()

    return {
        "db_path": str(resolved),
        "session": session_row,
        "hr_frames": hr_frames,
        "ecg_frames": ecg_frames,
        "acc_frames": acc_frames,
        "events": events,
    }


def build_beats(hr_frames: pd.DataFrame) -> pd.DataFrame:
    if hr_frames.empty:
        return pd.DataFrame(columns=["timestamp_ns", "time", "hr_bpm", "rr_ms", "frame_hr_bpm"])

    records: list[dict[str, float | int | pd.Timestamp]] = []
    for row in hr_frames.itertuples(index=False):
        rr_intervals = json.loads(row.rr_intervals_ms_json)
        if rr_intervals:
            beat_time_ns = int(row.recorded_at_ns - sum(rr_intervals) * 1_000_000)
            for rr_ms in rr_intervals:
                beat_time_ns += int(rr_ms * 1_000_000)
                records.append(
                    {
                        "timestamp_ns": beat_time_ns,
                        "time": _ns_to_time(np.array([beat_time_ns]))[0],
                        "hr_bpm": 60_000.0 / rr_ms,
                        "rr_ms": float(rr_ms),
                        "frame_hr_bpm": float(row.average_hr_bpm),
                    }
                )
        else:
            records.append(
                {
                    "timestamp_ns": int(row.recorded_at_ns),
                    "time": _ns_to_time(np.array([row.recorded_at_ns]))[0],
                    "hr_bpm": float(row.average_hr_bpm),
                    "rr_ms": np.nan,
                    "frame_hr_bpm": float(row.average_hr_bpm),
                }
            )

    return pd.DataFrame.from_records(records)


def compute_rmssd(beats: pd.DataFrame, window_seconds: int = 60) -> pd.DataFrame:
    if beats.empty:
        return pd.DataFrame(columns=["time", "rmssd_ms"])

    timestamps = beats["timestamp_ns"].to_numpy(dtype=np.int64)
    rr_values = beats["rr_ms"].to_numpy(dtype=float)
    window_ns = window_seconds * 1_000_000_000
    rmssd_values = np.full(len(beats), np.nan)
    start_index = 0

    for index in range(len(beats)):
        while timestamps[index] - timestamps[start_index] > window_ns:
            start_index += 1
        window_rr = rr_values[start_index : index + 1]
        window_rr = window_rr[~np.isnan(window_rr)]
        if len(window_rr) < 5:
            continue
        rr_diffs = np.diff(window_rr)
        rmssd_values[index] = float(np.sqrt(np.mean(rr_diffs ** 2)))

    return pd.DataFrame({"time": beats["time"], "rmssd_ms": rmssd_values})


def expand_acc_frames(acc_frames: pd.DataFrame) -> pd.DataFrame:
    if acc_frames.empty:
        return pd.DataFrame(columns=["timestamp_ns", "time", "x_mg", "y_mg", "z_mg"])

    records: list[dict[str, float | int | pd.Timestamp]] = []
    for row in acc_frames.itertuples(index=False):
        samples = json.loads(row.samples_json)
        if not samples:
            continue
        step_ns = int(1_000_000_000 / row.sample_rate_hz)
        start_ns = int(row.sensor_recorded_at_ns - step_ns * (len(samples) - 1))
        for offset, sample in enumerate(samples):
            timestamp_ns = start_ns + offset * step_ns
            records.append(
                {
                    "timestamp_ns": timestamp_ns,
                    "time": _ns_to_time(np.array([timestamp_ns]))[0],
                    "x_mg": float(sample[0]),
                    "y_mg": float(sample[1]),
                    "z_mg": float(sample[2]),
                }
            )
    return pd.DataFrame.from_records(records)


def estimate_breathing_rate(
    acc_samples: pd.DataFrame,
    window_seconds: int = 45,
    step_seconds: int = 5,
) -> pd.DataFrame:
    if acc_samples.empty:
        return pd.DataFrame(columns=["time", "breathing_rate_bpm"])

    timestamps = acc_samples["timestamp_ns"].to_numpy(dtype=np.int64)
    xyz = acc_samples[["x_mg", "y_mg", "z_mg"]].to_numpy(dtype=float)
    if len(timestamps) < 200:
        return pd.DataFrame(columns=["time", "breathing_rate_bpm"])

    sample_rate_hz = round(1_000_000_000 / np.median(np.diff(timestamps)))
    if sample_rate_hz < 20:
        return pd.DataFrame(columns=["time", "breathing_rate_bpm"])

    minimum_samples = int(sample_rate_hz * window_seconds * 0.7)
    window_ns = int(window_seconds * 1_000_000_000)
    step_ns = int(step_seconds * 1_000_000_000)
    sos = signal.butter(2, [0.10, 0.70], btype="bandpass", fs=sample_rate_hz, output="sos")

    estimates: list[dict[str, float | pd.Timestamp]] = []
    start_at = timestamps[0] + window_ns
    end_at = timestamps[-1]

    for window_end_ns in range(start_at, end_at + 1, step_ns):
        left_index = np.searchsorted(timestamps, window_end_ns - window_ns, side="left")
        right_index = np.searchsorted(timestamps, window_end_ns, side="right")
        segment = xyz[left_index:right_index]
        if len(segment) < minimum_samples:
            continue
        centered = segment - segment.mean(axis=0, keepdims=True)
        covariance = np.cov(centered, rowvar=False)
        eigenvalues, eigenvectors = np.linalg.eigh(covariance)
        principal_signal = centered @ eigenvectors[:, np.argmax(eigenvalues)]
        principal_signal = signal.detrend(principal_signal)
        filtered = signal.sosfiltfilt(sos, principal_signal)
        frequencies, power = signal.welch(
            filtered,
            fs=sample_rate_hz,
            nperseg=min(len(filtered), int(sample_rate_hz * window_seconds)),
        )
        band = (frequencies >= 0.10) & (frequencies <= 0.70)
        if not np.any(band):
            continue
        peak_frequency = frequencies[band][int(np.argmax(power[band]))]
        estimates.append(
            {
                "time": _ns_to_time(np.array([window_end_ns]))[0],
                "breathing_rate_bpm": float(peak_frequency * 60.0),
            }
        )

    return pd.DataFrame.from_records(estimates)


def expand_ecg_frames(ecg_frames: pd.DataFrame, tail_seconds: int = 10) -> pd.DataFrame:
    if ecg_frames.empty:
        return pd.DataFrame(columns=["time", "microvolts"])

    cutoff_ns = int(ecg_frames["sensor_recorded_at_ns"].max()) - tail_seconds * 1_000_000_000
    recent_frames = ecg_frames[ecg_frames["sensor_recorded_at_ns"] >= cutoff_ns]
    records: list[dict[str, float | pd.Timestamp]] = []
    for row in recent_frames.itertuples(index=False):
        samples = json.loads(row.samples_json)
        if not samples:
            continue
        step_ns = int(1_000_000_000 / row.sample_rate_hz)
        start_ns = int(row.sensor_recorded_at_ns - step_ns * (len(samples) - 1))
        for offset, sample in enumerate(samples):
            timestamp_ns = start_ns + offset * step_ns
            records.append(
                {
                    "time": _ns_to_time(np.array([timestamp_ns]))[0],
                    "microvolts": float(sample),
                }
            )
    return pd.DataFrame.from_records(records)


def render_line_chart(
    frame: pd.DataFrame,
    *,
    x: str,
    y: str,
    title: str,
    color: str,
    y_title: str,
) -> None:
    if frame.empty:
        st.info(f"No {title.lower()} data yet.")
        return

    chart = (
        alt.Chart(frame)
        .mark_line(color=color, interpolate="monotone")
        .encode(
            x=alt.X(f"{x}:T", title="Time"),
            y=alt.Y(f"{y}:Q", title=y_title),
            tooltip=[
                alt.Tooltip(f"{x}:T", title="Time"),
                alt.Tooltip(f"{y}:Q", title=y_title, format=".2f"),
            ],
        )
        .properties(height=240, title=title)
    )
    st.altair_chart(chart, use_container_width=True)


lookback_minutes = st.sidebar.slider("Lookback window (minutes)", min_value=5, max_value=60, value=15, step=5)
st.sidebar.code(str(DB_PATH), language="text")


@st.fragment(run_every=3)
def live_dashboard() -> None:
    snapshot = load_snapshot(str(DB_PATH), lookback_minutes)
    session = snapshot.get("session")
    if session is None:
        st.warning("No stored sessions yet. Start `polar-dash collect` and this page will populate automatically.")
        return

    beats = build_beats(snapshot["hr_frames"])  # type: ignore[arg-type]
    rmssd = compute_rmssd(beats)
    acc_samples = expand_acc_frames(snapshot["acc_frames"])  # type: ignore[arg-type]
    breathing = estimate_breathing_rate(acc_samples)
    ecg = expand_ecg_frames(snapshot["ecg_frames"])  # type: ignore[arg-type]

    latest_hr = _latest_numeric(beats, "hr_bpm")
    latest_hrv = _latest_numeric(rmssd, "rmssd_ms")
    latest_breathing = _latest_numeric(breathing, "breathing_rate_bpm")
    started_at = pd.to_datetime(int(session["started_at_ns"]), unit="ns", utc=True)
    ended_at_ns = session["ended_at_ns"]
    ended_at = pd.to_datetime(int(ended_at_ns), unit="ns", utc=True) if pd.notna(ended_at_ns) else pd.Timestamp.utcnow(tz="UTC")
    duration_minutes = (ended_at - started_at).total_seconds() / 60.0

    top_row = st.columns(4)
    top_row[0].metric("Latest HR", f"{latest_hr:.0f} bpm" if latest_hr is not None else "n/a")
    top_row[1].metric("Rolling HRV", f"{latest_hrv:.0f} ms" if latest_hrv is not None else "n/a")
    top_row[2].metric(
        "Breathing Rate",
        f"{latest_breathing:.1f} br/min" if latest_breathing is not None else "n/a",
    )
    top_row[3].metric("Session Duration", f"{duration_minutes:.1f} min")

    st.write(
        f"Session `{int(session['id'])}` on `{session['device_name']}` "
        f"({session['device_address']}). Battery: `{session['battery_percent']}`."
    )

    render_line_chart(
        beats,
        x="time",
        y="hr_bpm",
        title="Heart Rate Across Time",
        color="#c9184a",
        y_title="BPM",
    )
    render_line_chart(
        rmssd,
        x="time",
        y="rmssd_ms",
        title="HRV Across Time (Rolling RMSSD)",
        color="#005f73",
        y_title="RMSSD (ms)",
    )
    render_line_chart(
        breathing,
        x="time",
        y="breathing_rate_bpm",
        title="Breathing Rate Across Time",
        color="#4361ee",
        y_title="Breaths / min",
    )
    render_line_chart(
        ecg,
        x="time",
        y="microvolts",
        title="Raw ECG Preview (Last 10 Seconds)",
        color="#ff7b00",
        y_title="Microvolts",
    )

    events = snapshot["events"]  # type: ignore[assignment]
    if isinstance(events, pd.DataFrame) and not events.empty:
        events = events.copy()
        events["time"] = _ns_to_time(events["recorded_at_ns"])
        events["details"] = events["details_json"].apply(json.loads)
        st.subheader("Collector Events")
        st.dataframe(
            events[["time", "level", "event_type", "details"]],
            use_container_width=True,
            hide_index=True,
        )


live_dashboard()
