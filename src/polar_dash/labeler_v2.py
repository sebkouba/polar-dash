from __future__ import annotations

import json
import os
import sqlite3
import subprocess
import time
import tkinter as tk
from dataclasses import dataclass
from pathlib import Path
from tkinter import ttk

import numpy as np
from scipy import signal

from polar_dash.storage import DEFAULT_DB_PATH, Storage

PROTOCOL_NAME = "breathing_phase_keys_v1"
POLL_INTERVAL_MS = 250
GRAPH_WINDOW_SECONDS = 60
SURFACE_BG = "#f6f3ec"
SURFACE_BORDER = "#cfc8bb"
TEXT_FG = "#1f1f1f"
MUTED_FG = "#5d5a53"
ACCENT = "#005f73"

KEY_BINDINGS = {
    "h": ("inhale_end", "Finished inhaling", "#e76f51"),
    "j": ("exhale_start", "Started exhaling", "#264653"),
    "k": ("exhale_end", "Finished exhaling", "#2a9d8f"),
    "l": ("inhale_start", "Started inhaling", "#f4a261"),
}
PHASE_CYCLE = ("j", "k", "l", "h")


@dataclass(slots=True)
class LiveEstimate:
    rate_bpm: float | None
    source: str | None
    age_seconds: float | None


@dataclass(slots=True)
class RespiratoryWaveform:
    timestamps_ns: np.ndarray
    values: np.ndarray
    source: str


@dataclass(slots=True)
class SessionSummary:
    session_id: int
    name: str
    started_at_ns: int
    ended_at_ns: int | None
    linked_sensor_session_id: int | None
    label_count: int
    is_active: bool


@dataclass(slots=True)
class LabelRecord:
    recorded_at_ns: int
    key_name: str
    phase_code: str


@dataclass(slots=True)
class LabelerView:
    mode: str
    draft_name: str
    status_message: str
    displayed_sensor_session_id: int | None
    active_annotation_session_id: int | None
    review_annotation_session_id: int | None
    live_estimate: LiveEstimate
    waveform: RespiratoryWaveform | None
    waveform_reference_ns: int
    waveform_markers: list[LabelRecord]
    recent_labels: list[LabelRecord]
    session_summaries: list[SessionSummary]
    can_start: bool
    can_stop: bool
    can_delete: bool
    session_state_text: str
    annotation_text: str
    viewing_text: str
    sensor_text: str
    last_label_text: str
    label_count_text: str
    saved_summary_text: str


class LabelerController:
    def __init__(self, storage: Storage, name: str | None = None) -> None:
        self.storage = storage
        self.active_annotation_session_id: int | None = None
        self.review_annotation_session_id: int | None = None
        self.draft_name = name or self._default_session_name()
        self.status_message = "Ready. Start a session to begin labeling."

    def close(self) -> None:
        if self.active_annotation_session_id is not None:
            if self.storage.count_breathing_phase_labels(self.active_annotation_session_id) == 0:
                self.storage.delete_annotation_session(self.active_annotation_session_id)
            else:
                self.storage.close_annotation_session(self.active_annotation_session_id)
        self.storage.close()

    def set_draft_name(self, name: str) -> None:
        stripped = name.strip()
        self.draft_name = stripped or self._default_session_name()

    def start_session(self, *, now_ns: int | None = None) -> None:
        if self.active_annotation_session_id is not None:
            self.status_message = "A labeling session is already active."
            return

        target_ns = now_ns or time.time_ns()
        live_sensor = self.storage.find_live_sensor_session_at(target_ns)
        if live_sensor is None:
            self.status_message = (
                "No live sensor stream is available. Start `polar-dash collect` before labeling."
            )
            return

        session_name = self.draft_name.strip() or self._default_session_name()
        self.active_annotation_session_id = self.storage.start_annotation_session(
            session_name,
            protocol_name=PROTOCOL_NAME,
            linked_session_id=int(live_sensor["id"]),
            notes={"key_bindings": KEY_BINDINGS, "phase_cycle": list(PHASE_CYCLE)},
        )
        self.review_annotation_session_id = None
        self.status_message = (
            f"Started annotation session {self.active_annotation_session_id}. "
            "Tag phases with H / J / K / L."
        )

    def stop_session(self) -> None:
        if self.active_annotation_session_id is None:
            self.status_message = "No active labeling session to stop."
            return

        finished_session_id = self.active_annotation_session_id
        if self.storage.count_breathing_phase_labels(finished_session_id) == 0:
            self.storage.delete_annotation_session(finished_session_id)
            self.status_message = f"Discarded empty annotation session {finished_session_id}."
            self.active_annotation_session_id = None
            self.review_annotation_session_id = None
            self.draft_name = self._default_session_name()
            return

        self.storage.close_annotation_session(finished_session_id)
        self.active_annotation_session_id = None
        self.review_annotation_session_id = finished_session_id
        self.status_message = f"Saved annotation session {finished_session_id}."
        self.draft_name = self._default_session_name()

    def delete_selected_saved_session(self) -> None:
        if self.active_annotation_session_id is not None:
            self.status_message = "Stop the active session before deleting saved history."
            return
        if self.review_annotation_session_id is None:
            self.status_message = "Select a saved session to delete."
            return
        if self.storage.delete_annotation_session(self.review_annotation_session_id):
            deleted_id = self.review_annotation_session_id
            self.review_annotation_session_id = None
            self.status_message = f"Deleted annotation session {deleted_id}."
            return
        self.status_message = "The selected saved session was not found."

    def select_saved_session(self, session_id: int) -> None:
        if self.active_annotation_session_id is not None:
            self.status_message = "Stop the active session before browsing saved sessions."
            return

        annotation_session = self.storage.get_annotation_session(session_id)
        if annotation_session is None:
            self.status_message = f"Saved session {session_id} was not found."
            return
        if self.storage.count_breathing_phase_labels(session_id) == 0:
            self.status_message = (
                f"Saved session {session_id} has no labels and is not available for review."
            )
            return

        self.review_annotation_session_id = session_id
        self.status_message = f"Viewing saved annotation session {session_id}."

    def record_phase_key(
        self,
        key_name: str,
        *,
        trigger_key_name: str | None = None,
        now_ns: int | None = None,
    ) -> None:
        if self.active_annotation_session_id is None:
            self.status_message = "Start a session before recording breathing labels."
            return
        if key_name not in KEY_BINDINGS:
            return

        target_ns = now_ns or time.time_ns()
        live_sensor = self._current_recording_sensor_session(target_ns)
        if live_sensor is None:
            self.status_message = (
                "The linked live sensor stream is unavailable. Stop and start a new session."
            )
            return

        sensor_session_id = int(live_sensor["id"])
        phase_code, description, _ = KEY_BINDINGS[key_name]
        estimate = self.storage.find_nearest_breathing_estimate(
            target_ns,
            sensor_session_id=sensor_session_id,
        )

        estimate_rate = None
        estimate_source = None
        estimate_time_ns = None
        estimate_age_ms = None
        if estimate is not None:
            estimate_rate = float(estimate["breaths_per_min"])
            estimate_source = str(estimate["source"])
            estimate_time_ns = int(estimate["estimated_at_ns"])
            estimate_age_ms = abs(target_ns - estimate_time_ns) / 1_000_000

        self.storage.insert_breathing_phase_label(
            self.active_annotation_session_id,
            recorded_at_ns=target_ns,
            phase_code=phase_code,
            key_name=key_name.upper(),
            sensor_session_id=sensor_session_id,
            breathing_estimate_bpm=estimate_rate,
            breathing_estimate_source=estimate_source,
            breathing_estimate_time_ns=estimate_time_ns,
            estimate_age_ms=estimate_age_ms,
        )
        self.storage.insert_event(
            "breathing_phase_label",
            {
                "annotation_session_id": self.active_annotation_session_id,
                "phase_code": phase_code,
                "key_name": key_name.upper(),
                "trigger_key_name": (
                    trigger_key_name.upper() if trigger_key_name is not None else key_name.upper()
                ),
            },
            session_id=sensor_session_id,
            recorded_at_ns=target_ns,
        )

        label_time = time.strftime("%H:%M:%S", time.localtime(target_ns / 1_000_000_000))
        status_prefix = key_name.upper()
        if trigger_key_name is not None:
            status_prefix = f"{trigger_key_name.upper()} -> {key_name.upper()}"
        self.status_message = (
            f"Recorded {status_prefix} -> {description} at "
            f"{label_time}.{target_ns % 1_000_000_000:09d}"
        )

    def record_next_phase(self, *, now_ns: int | None = None) -> None:
        if self.active_annotation_session_id is None:
            self.status_message = "Start a session before recording breathing labels."
            return

        next_key_name = self._next_cycle_key()
        if next_key_name is None:
            self.status_message = "Record one phase manually with H / J / K / L before using G."
            return
        self.record_phase_key(next_key_name, trigger_key_name="g", now_ns=now_ns)

    def undo_last_label(self) -> None:
        if self.active_annotation_session_id is None:
            self.status_message = "Undo is only available while recording."
            return
        if self.storage.delete_last_breathing_phase_label(self.active_annotation_session_id):
            self.status_message = "Removed the most recent label from the active session."
            return
        self.status_message = "No labels are available to undo."

    def snapshot(self, *, now_ns: int | None = None) -> LabelerView:
        target_ns = now_ns or time.time_ns()
        live_sensor = self.storage.find_live_sensor_session_at(target_ns)
        live_sensor_id = int(live_sensor["id"]) if live_sensor is not None else None

        session_summaries = self._session_summaries()
        if self.active_annotation_session_id is None:
            visible_ids = {summary.session_id for summary in session_summaries if summary.label_count > 0}
            if self.review_annotation_session_id not in visible_ids:
                self.review_annotation_session_id = session_summaries[0].session_id if session_summaries else None

        mode = "idle"
        focused_annotation_session_id: int | None = None
        displayed_sensor_session_id: int | None = live_sensor_id
        reference_ns = target_ns

        if self.active_annotation_session_id is not None:
            mode = "recording"
            focused_annotation_session_id = self.active_annotation_session_id
            recording_sensor = self._current_recording_sensor_session(target_ns)
            displayed_sensor_session_id = (
                int(recording_sensor["id"]) if recording_sensor is not None else None
            )
        elif self.review_annotation_session_id is not None:
            mode = "review"
            focused_annotation_session_id = self.review_annotation_session_id
            review_session = self.storage.get_annotation_session(self.review_annotation_session_id)
            if review_session is not None and review_session["linked_session_id"] is not None:
                displayed_sensor_session_id = int(review_session["linked_session_id"])
            else:
                displayed_sensor_session_id = None
            if review_session is not None:
                reference_ns = self._preview_reference_time_ns(
                    displayed_sensor_session_id,
                    preferred_reference_ns=self._annotation_reference_time_ns(review_session),
                )
        else:
            reference_ns = self._preview_reference_time_ns(
                displayed_sensor_session_id,
                preferred_reference_ns=target_ns,
            )

        live_estimate = self._load_live_estimate(reference_ns, displayed_sensor_session_id)
        waveform = self._load_waveform(displayed_sensor_session_id, reference_ns)
        waveform_markers = (
            self._load_waveform_markers(focused_annotation_session_id, reference_ns)
            if focused_annotation_session_id is not None
            else []
        )
        recent_labels = (
            self._load_recent_labels(focused_annotation_session_id)
            if focused_annotation_session_id is not None
            else []
        )

        can_start = self.active_annotation_session_id is None and live_sensor_id is not None
        can_stop = self.active_annotation_session_id is not None
        can_delete = self.active_annotation_session_id is None and self.review_annotation_session_id is not None

        label_count = len(recent_labels)
        if focused_annotation_session_id is not None:
            label_count = self.storage.count_breathing_phase_labels(focused_annotation_session_id)

        last_label_text = "Last label: none"
        if recent_labels:
            last_label = recent_labels[0]
            last_label_text = f"Last label: {last_label.key_name} -> {last_label.phase_code}"

        if self.active_annotation_session_id is not None:
            session_state_text = f"Recorder: ACTIVE on session {self.active_annotation_session_id}"
            annotation_text = f"Annotation session: {self.active_annotation_session_id} (active)"
            viewing_text = "Viewing: active recording"
        elif self.review_annotation_session_id is not None:
            session_state_text = "Recorder: idle"
            annotation_text = f"Annotation session: inactive (selected {self.review_annotation_session_id})"
            viewing_text = f"Viewing: saved session {self.review_annotation_session_id}"
        else:
            session_state_text = "Recorder: idle"
            annotation_text = "Annotation session: inactive"
            viewing_text = "Viewing: live feed"

        sensor_text = (
            f"Sensor session: {displayed_sensor_session_id}"
            if displayed_sensor_session_id is not None
            else "Sensor session: n/a"
        )

        if session_summaries:
            saved_summary_text = (
                f"Annotation sessions: {len(session_summaries)}. "
                f"Latest session: {session_summaries[0].session_id}."
            )
        else:
            saved_summary_text = "Annotation sessions: 0. Start a session and add labels to keep it."

        return LabelerView(
            mode=mode,
            draft_name=self.draft_name,
            status_message=self.status_message,
            displayed_sensor_session_id=displayed_sensor_session_id,
            active_annotation_session_id=self.active_annotation_session_id,
            review_annotation_session_id=self.review_annotation_session_id,
            live_estimate=live_estimate,
            waveform=waveform,
            waveform_reference_ns=reference_ns,
            waveform_markers=waveform_markers,
            recent_labels=recent_labels,
            session_summaries=session_summaries,
            can_start=can_start,
            can_stop=can_stop,
            can_delete=can_delete,
            session_state_text=session_state_text,
            annotation_text=annotation_text,
            viewing_text=viewing_text,
            sensor_text=sensor_text,
            last_label_text=last_label_text,
            label_count_text=f"Labels recorded: {label_count}",
            saved_summary_text=saved_summary_text,
        )

    def _default_session_name(self) -> str:
        return time.strftime("breathing_labels_%Y%m%d_%H%M%S")

    def _session_summaries(self) -> list[SessionSummary]:
        summaries: list[SessionSummary] = []
        for row in self.storage.list_annotation_sessions(include_active=True):
            session_id = int(row["id"])
            label_count = int(row["label_count"])
            if label_count == 0 and session_id != self.active_annotation_session_id:
                continue
            summaries.append(
                SessionSummary(
                    session_id=session_id,
                    name=str(row["name"]),
                    started_at_ns=int(row["started_at_ns"]),
                    ended_at_ns=int(row["ended_at_ns"]) if row["ended_at_ns"] is not None else None,
                    linked_sensor_session_id=(
                        int(row["linked_session_id"]) if row["linked_session_id"] is not None else None
                    ),
                    label_count=label_count,
                    is_active=session_id == self.active_annotation_session_id,
                )
            )
        return summaries

    def _current_recording_sensor_session(self, target_ns: int) -> sqlite3.Row | None:
        if self.active_annotation_session_id is None:
            return None
        annotation_session = self.storage.get_annotation_session(self.active_annotation_session_id)
        if annotation_session is None or annotation_session["linked_session_id"] is None:
            return None
        live_sensor = self.storage.find_live_sensor_session_at(target_ns)
        if live_sensor is None:
            return None
        if int(live_sensor["id"]) != int(annotation_session["linked_session_id"]):
            return None
        return live_sensor

    def _next_cycle_key(self) -> str | None:
        if self.active_annotation_session_id is None:
            return None
        row = self.storage.connection.execute(
            """
            SELECT key_name
            FROM breathing_phase_labels
            WHERE annotation_session_id = ?
            ORDER BY recorded_at_ns DESC
            LIMIT 1
            """,
            (self.active_annotation_session_id,),
        ).fetchone()
        if row is None:
            return None
        last_key_name = str(row["key_name"]).lower()
        if last_key_name not in PHASE_CYCLE:
            return None
        last_index = PHASE_CYCLE.index(last_key_name)
        return PHASE_CYCLE[(last_index + 1) % len(PHASE_CYCLE)]

    def _annotation_reference_time_ns(self, annotation_session: sqlite3.Row) -> int:
        session_id = int(annotation_session["id"])
        latest_label = self.storage.connection.execute(
            """
            SELECT recorded_at_ns
            FROM breathing_phase_labels
            WHERE annotation_session_id = ?
            ORDER BY recorded_at_ns DESC
            LIMIT 1
            """,
            (session_id,),
        ).fetchone()
        if latest_label is not None:
            return int(latest_label["recorded_at_ns"])
        if annotation_session["ended_at_ns"] is not None:
            return int(annotation_session["ended_at_ns"])
        return int(annotation_session["started_at_ns"])

    def _preview_reference_time_ns(
        self,
        sensor_session_id: int | None,
        *,
        preferred_reference_ns: int,
    ) -> int:
        if sensor_session_id is None:
            return preferred_reference_ns
        latest_acc_row = self.storage.connection.execute(
            """
            SELECT MAX(sensor_recorded_at_ns) AS latest_sensor_recorded_at_ns
            FROM acc_frames
            WHERE session_id = ?
            """,
            (sensor_session_id,),
        ).fetchone()
        latest_acc_ns = latest_acc_row["latest_sensor_recorded_at_ns"] if latest_acc_row is not None else None
        if latest_acc_ns is None:
            return preferred_reference_ns
        return min(preferred_reference_ns, int(latest_acc_ns))

    def _load_live_estimate(self, target_ns: int, sensor_session_id: int | None) -> LiveEstimate:
        if sensor_session_id is None:
            return LiveEstimate(None, None, None)
        row = self.storage.find_nearest_breathing_estimate(
            target_ns,
            sensor_session_id=sensor_session_id,
        )
        if row is None:
            return LiveEstimate(None, None, None)
        age_seconds = abs(target_ns - int(row["estimated_at_ns"])) / 1_000_000_000
        return LiveEstimate(
            rate_bpm=float(row["breaths_per_min"]),
            source=str(row["source"]),
            age_seconds=age_seconds,
        )

    def _load_waveform(self, sensor_session_id: int | None, target_ns: int) -> RespiratoryWaveform | None:
        if sensor_session_id is None:
            return None
        rows = self.storage.connection.execute(
            """
            SELECT sensor_recorded_at_ns, sample_rate_hz, samples_json
            FROM acc_frames
            WHERE session_id = ?
              AND sensor_recorded_at_ns >= ?
              AND sensor_recorded_at_ns <= ?
            ORDER BY sensor_recorded_at_ns
            """,
            (
                sensor_session_id,
                target_ns - GRAPH_WINDOW_SECONDS * 1_000_000_000,
                target_ns,
            ),
        ).fetchall()
        if len(rows) < 2:
            return None

        samples: list[tuple[int, float, float, float]] = []
        for row in rows:
            frame_samples = json.loads(row["samples_json"])
            sample_rate_hz = int(row["sample_rate_hz"])
            step_ns = int(1_000_000_000 / sample_rate_hz)
            start_ns = int(row["sensor_recorded_at_ns"]) - step_ns * (len(frame_samples) - 1)
            for index, (x, y, z) in enumerate(frame_samples):
                samples.append((start_ns + index * step_ns, float(x), float(y), float(z)))

        if len(samples) < 200:
            return None

        timestamps = np.array([sample[0] for sample in samples], dtype=np.int64)
        xyz = np.array([sample[1:] for sample in samples], dtype=float)
        sample_rate_hz = round(1_000_000_000 / np.median(np.diff(timestamps)))
        if sample_rate_hz < 20:
            return None

        centered = xyz - xyz.mean(axis=0, keepdims=True)
        covariance = np.cov(centered, rowvar=False)
        eigenvalues, eigenvectors = np.linalg.eigh(covariance)
        principal_signal = centered @ eigenvectors[:, int(np.argmax(eigenvalues))]
        lowpassed = signal.sosfiltfilt(
            signal.butter(2, 1.0, btype="lowpass", fs=sample_rate_hz, output="sos"),
            principal_signal,
        )
        reduced = signal.resample_poly(lowpassed, up=1, down=8)
        reduced_timestamps = np.linspace(
            timestamps[0],
            timestamps[-1],
            num=len(reduced),
            dtype=np.int64,
        )
        respiratory = signal.sosfiltfilt(
            signal.butter(2, [0.08, 0.70], btype="bandpass", fs=sample_rate_hz / 8, output="sos"),
            signal.detrend(reduced),
        )
        return RespiratoryWaveform(
            timestamps_ns=reduced_timestamps,
            values=respiratory,
            source="acc-respiratory-waveform",
        )

    def _load_waveform_markers(
        self,
        annotation_session_id: int,
        reference_ns: int,
    ) -> list[LabelRecord]:
        rows = self.storage.connection.execute(
            """
            SELECT recorded_at_ns, key_name, phase_code
            FROM breathing_phase_labels
            WHERE annotation_session_id = ?
              AND recorded_at_ns >= ?
            ORDER BY recorded_at_ns
            """,
            (annotation_session_id, reference_ns - GRAPH_WINDOW_SECONDS * 1_000_000_000),
        ).fetchall()
        return [
            LabelRecord(
                recorded_at_ns=int(row["recorded_at_ns"]),
                key_name=str(row["key_name"]),
                phase_code=str(row["phase_code"]),
            )
            for row in rows
        ]

    def _load_recent_labels(self, annotation_session_id: int) -> list[LabelRecord]:
        rows = self.storage.connection.execute(
            """
            SELECT recorded_at_ns, key_name, phase_code
            FROM breathing_phase_labels
            WHERE annotation_session_id = ?
            ORDER BY recorded_at_ns DESC
            LIMIT 12
            """,
            (annotation_session_id,),
        ).fetchall()
        return [
            LabelRecord(
                recorded_at_ns=int(row["recorded_at_ns"]),
                key_name=str(row["key_name"]),
                phase_code=str(row["phase_code"]),
            )
            for row in rows
        ]


class BreathingLabelerV2App:
    def __init__(self, db_path: Path | str = DEFAULT_DB_PATH, name: str | None = None) -> None:
        self.controller = LabelerController(Storage(db_path), name=name)

        self.root = tk.Tk()
        self.root.title("Polar Dash Breathing Labeler")
        self.root.geometry("1180x760")
        self.root.minsize(1080, 680)
        self.root.attributes("-topmost", True)
        self.root.configure(padx=14, pady=14)
        self.root.protocol("WM_DELETE_WINDOW", self.close)

        initial_view = self.controller.snapshot()

        self.session_name_var = tk.StringVar(master=self.root, value=initial_view.draft_name)
        self.status_var = tk.StringVar(master=self.root, value=initial_view.status_message)
        self.estimate_var = tk.StringVar(master=self.root, value="--.- br/min")
        self.meta_var = tk.StringVar(master=self.root, value="Waiting for breathing estimate...")
        self.sensor_var = tk.StringVar(master=self.root, value=initial_view.sensor_text)
        self.annotation_var = tk.StringVar(master=self.root, value=initial_view.annotation_text)
        self.last_label_var = tk.StringVar(master=self.root, value=initial_view.last_label_text)
        self.count_var = tk.StringVar(master=self.root, value=initial_view.label_count_text)
        self.saved_session_var = tk.StringVar(master=self.root, value=initial_view.saved_summary_text)
        self.session_state_var = tk.StringVar(master=self.root, value=initial_view.session_state_text)
        self.selection_var = tk.StringVar(master=self.root, value=initial_view.viewing_text)

        self.graph: tk.Canvas
        self.recent_list: tk.Listbox
        self.sessions_list: tk.Listbox
        self._updating_session_list = False

        self._build_ui()
        self._bind_keys()
        self._apply_view(initial_view)
        self.root.after(1, self._post_init_focus)
        self.root.after(POLL_INTERVAL_MS, self._poll)

    def _build_ui(self) -> None:
        header = ttk.Frame(self.root)
        header.pack(fill="x")

        title = ttk.Label(
            header,
            text="Breathing Phase Labeler",
            font=("SF Pro Rounded", 24, "bold"),
        )
        title.pack(anchor="w")

        subtitle = ttk.Label(
            header,
            text=(
                "This graph shows a respiratory waveform derived from the live strap data "
                "rather than only the breathing rate. Press H / J / K / L while a session is active, "
                "or use G to apply the next label in the breathing cycle."
            ),
            wraplength=800,
        )
        subtitle.pack(anchor="w", pady=(4, 10))

        body = ttk.Frame(self.root)
        body.pack(fill="both", expand=True)

        content = ttk.Frame(body)
        content.pack(side="left", fill="both", expand=True)

        sidebar = ttk.Frame(body, width=340)
        sidebar.pack(side="left", fill="y", padx=(12, 0))
        sidebar.pack_propagate(False)

        controls = ttk.Frame(content)
        controls.pack(fill="x", pady=(0, 10))
        ttk.Label(controls, text="Session name").pack(side="left")
        name_entry = ttk.Entry(controls, textvariable=self.session_name_var, width=34)
        name_entry.pack(side="left", padx=(8, 12))
        self.session_name_var.trace_add("write", self._on_name_change)
        self.start_button = ttk.Button(controls, text="Start Session", command=self._start_session)
        self.start_button.pack(side="left")
        self.stop_button = ttk.Button(controls, text="Stop && Save", command=self._stop_session)
        self.stop_button.pack(side="left", padx=(8, 0))
        ttk.Button(controls, text="Refresh", command=self._refresh_view).pack(side="left", padx=(8, 0))

        summary = ttk.Frame(content)
        summary.pack(fill="x", pady=(0, 10))
        ttk.Label(summary, textvariable=self.estimate_var, font=("SF Pro Rounded", 30, "bold")).pack(
            anchor="w"
        )
        ttk.Label(summary, textvariable=self.meta_var).pack(anchor="w", pady=(2, 0))
        ttk.Label(summary, textvariable=self.sensor_var).pack(anchor="w")
        ttk.Label(summary, textvariable=self.selection_var).pack(anchor="w")

        self.graph = tk.Canvas(
            content,
            width=800,
            height=260,
            bg=SURFACE_BG,
            highlightthickness=1,
            highlightbackground=SURFACE_BORDER,
            highlightcolor=SURFACE_BORDER,
            borderwidth=0,
        )
        self.graph.pack(fill="x", pady=(4, 12))

        key_frame = ttk.LabelFrame(content, text="Keys")
        key_frame.pack(fill="x", pady=(0, 12))
        for key_name, (phase_code, description, _) in KEY_BINDINGS.items():
            ttk.Label(key_frame, text=f"{key_name.upper()}: {description} ({phase_code})").pack(
                anchor="w", padx=8, pady=2
            )
        ttk.Label(
            key_frame,
            text="G: next phase in cycle    U: undo last label    Q or Esc: quit",
        ).pack(anchor="w", padx=8, pady=(6, 6))

        feedback = ttk.Frame(content)
        feedback.pack(fill="x")
        ttk.Label(feedback, textvariable=self.last_label_var).pack(anchor="w")
        ttk.Label(feedback, textvariable=self.count_var).pack(anchor="w", pady=(0, 8))

        recent_frame = ttk.LabelFrame(content, text="Recent Labels")
        recent_frame.pack(fill="both", expand=True)
        self.recent_list = tk.Listbox(
            recent_frame,
            height=12,
            activestyle="none",
            bg=SURFACE_BG,
            fg=TEXT_FG,
            selectbackground=ACCENT,
            selectforeground="#ffffff",
            highlightthickness=1,
            highlightbackground=SURFACE_BORDER,
            highlightcolor=SURFACE_BORDER,
            borderwidth=0,
            font=("SF Pro Text", 12),
        )
        self.recent_list.pack(fill="both", expand=True, padx=8, pady=8)

        session_frame = ttk.LabelFrame(sidebar, text="Session Status")
        session_frame.pack(fill="x", pady=(0, 12))
        ttk.Label(
            session_frame,
            textvariable=self.session_state_var,
            font=("SF Pro Rounded", 15, "bold"),
        ).pack(anchor="w", padx=8, pady=(8, 2))
        ttk.Label(session_frame, textvariable=self.annotation_var, wraplength=300).pack(
            anchor="w", padx=8, pady=(0, 2)
        )
        ttk.Label(session_frame, textvariable=self.selection_var, wraplength=300).pack(
            anchor="w", padx=8, pady=(0, 2)
        )
        ttk.Label(session_frame, textvariable=self.sensor_var, wraplength=300).pack(
            anchor="w", padx=8, pady=(0, 8)
        )

        sessions_frame = ttk.LabelFrame(sidebar, text="Session History")
        sessions_frame.pack(fill="both", expand=True)
        ttk.Label(
            sessions_frame,
            text=(
                "Saved labeled sessions appear here. Select one to review it or delete it. "
                "The active session stays in focus while recording."
            ),
            wraplength=300,
        ).pack(anchor="w", padx=8, pady=(8, 6))
        sessions_list_frame = ttk.Frame(sessions_frame)
        sessions_list_frame.pack(fill="both", expand=True, padx=8, pady=(0, 8))
        self.sessions_list = tk.Listbox(
            sessions_list_frame,
            height=12,
            width=42,
            activestyle="none",
            exportselection=False,
            bg=SURFACE_BG,
            fg=TEXT_FG,
            selectbackground=ACCENT,
            selectforeground="#ffffff",
            highlightthickness=1,
            highlightbackground=SURFACE_BORDER,
            highlightcolor=SURFACE_BORDER,
            borderwidth=0,
            font=("SF Pro Text", 12),
        )
        sessions_scrollbar = ttk.Scrollbar(
            sessions_list_frame,
            orient="vertical",
            command=self.sessions_list.yview,
        )
        self.sessions_list.configure(yscrollcommand=sessions_scrollbar.set)
        self.sessions_list.pack(side="left", fill="both", expand=True)
        sessions_scrollbar.pack(side="right", fill="y")
        button_row = ttk.Frame(sessions_frame)
        button_row.pack(fill="x", padx=8, pady=(0, 8))
        self.delete_button = ttk.Button(
            button_row,
            text="Delete Selected",
            command=self._delete_selected_session,
        )
        self.delete_button.pack(side="left")
        ttk.Label(sessions_frame, textvariable=self.saved_session_var, wraplength=300).pack(
            anchor="w", padx=8, pady=(0, 8)
        )

        ttk.Label(
            self.root,
            textvariable=self.status_var,
            wraplength=800,
            foreground="#555555",
        ).pack(anchor="w", pady=(10, 0))

    def _bind_keys(self) -> None:
        for key_name in KEY_BINDINGS:
            self.root.bind(f"<KeyPress-{key_name}>", self._on_phase_key)
            self.root.bind(f"<KeyPress-{key_name.upper()}>", self._on_phase_key)
        self.root.bind("<KeyPress-u>", self._undo_last_label)
        self.root.bind("<KeyPress-U>", self._undo_last_label)
        self.root.bind("<KeyPress-g>", self._record_next_phase)
        self.root.bind("<KeyPress-G>", self._record_next_phase)
        self.root.bind("<KeyPress-q>", self._quit_event)
        self.root.bind("<KeyPress-Q>", self._quit_event)
        self.root.bind("<Escape>", self._quit_event)
        self.sessions_list.bind("<<ListboxSelect>>", self._select_saved_session)

    def _post_init_focus(self) -> None:
        self.root.deiconify()
        self.root.update_idletasks()
        width = max(self.root.winfo_width(), 1180)
        height = max(self.root.winfo_height(), 760)
        screen_width = self.root.winfo_screenwidth()
        screen_height = self.root.winfo_screenheight()
        x = max((screen_width - width) // 2, 0)
        y = max((screen_height - height) // 4, 0)
        self.root.geometry(f"{width}x{height}+{x}+{y}")
        self.root.lift()
        self.root.focus_force()
        self.root.bell()
        self.root.after(300, self._bring_process_to_front)
        self.root.after(1000, lambda: self.root.attributes("-topmost", False))

    def _bring_process_to_front(self) -> None:
        script = (
            'tell application "System Events" to set frontmost of '
            f'(first process whose unix id is {os.getpid()}) to true'
        )
        subprocess.run(["osascript", "-e", script], check=False, capture_output=True)

    def run(self) -> None:
        self.root.mainloop()

    def close(self) -> None:
        self.controller.close()
        self.root.destroy()

    def _quit_event(self, _event: tk.Event[tk.Misc] | None = None) -> str:
        self.close()
        return "break"

    def _on_name_change(self, *_args: str) -> None:
        self.controller.set_draft_name(self.session_name_var.get())

    def _start_session(self) -> None:
        self.controller.set_draft_name(self.session_name_var.get())
        self.controller.start_session()
        self._refresh_view()

    def _stop_session(self) -> None:
        self.controller.stop_session()
        self._refresh_view()

    def _delete_selected_session(self) -> None:
        self.controller.delete_selected_saved_session()
        self._refresh_view()

    def _undo_last_label(self, _event: tk.Event[tk.Misc] | None = None) -> str:
        self.controller.undo_last_label()
        self._refresh_view()
        return "break"

    def _on_phase_key(self, event: tk.Event[tk.Misc]) -> str:
        self.controller.record_phase_key(event.keysym.lower())
        self._refresh_view()
        return "break"

    def _record_next_phase(self, _event: tk.Event[tk.Misc] | None = None) -> str:
        self.controller.record_next_phase()
        self._refresh_view()
        return "break"

    def _poll(self) -> None:
        if self.root.winfo_exists():
            self._refresh_view()
            self.root.after(POLL_INTERVAL_MS, self._poll)

    def _refresh_view(self) -> None:
        self._apply_view(self.controller.snapshot())

    def _apply_view(self, view: LabelerView) -> None:
        if self.session_name_var.get() != view.draft_name:
            self.session_name_var.set(view.draft_name)
        self.status_var.set(view.status_message)

        if view.live_estimate.rate_bpm is None:
            self.estimate_var.set("--.- br/min")
            self.meta_var.set("No recent breathing estimate available.")
        else:
            self.estimate_var.set(f"{view.live_estimate.rate_bpm:0.2f} br/min")
            self.meta_var.set(
                f"Source: {view.live_estimate.source}    Age: {view.live_estimate.age_seconds:0.2f}s"
            )

        self.sensor_var.set(view.sensor_text)
        self.annotation_var.set(view.annotation_text)
        self.last_label_var.set(view.last_label_text)
        self.count_var.set(view.label_count_text)
        self.saved_session_var.set(view.saved_summary_text)
        self.session_state_var.set(view.session_state_text)
        self.selection_var.set(view.viewing_text)

        self.start_button.state(["!disabled"] if view.can_start else ["disabled"])
        self.stop_button.state(["!disabled"] if view.can_stop else ["disabled"])
        self.delete_button.state(["!disabled"] if view.can_delete else ["disabled"])

        self._draw_waveform(view)
        self._load_recent_labels(view)
        self._load_saved_sessions(view)

    def _draw_waveform(self, view: LabelerView) -> None:
        self.graph.delete("all")
        width = max(self.graph.winfo_width(), int(self.graph["width"]))
        height = max(self.graph.winfo_height(), int(self.graph["height"]))
        self.graph.create_rectangle(0, 0, width, height, fill=SURFACE_BG, outline=SURFACE_BORDER)

        waveform = view.waveform
        if waveform is None or len(waveform.values) < 2:
            self.graph.create_text(
                width / 2,
                height / 2,
                text="Waiting for respiratory waveform",
                fill=MUTED_FG,
                font=("SF Pro Rounded", 16, "normal"),
            )
            return

        min_value = float(np.min(waveform.values))
        max_value = float(np.max(waveform.values))
        value_range = max(max_value - min_value, 1e-6)

        padding_left = 40
        padding_right = 14
        padding_top = 18
        padding_bottom = 22
        usable_width = max(width - padding_left - padding_right, 1)
        usable_height = max(height - padding_top - padding_bottom, 1)

        x0 = int(waveform.timestamps_ns[0])
        x1 = int(waveform.timestamps_ns[-1])
        duration_ns = max(x1 - x0, 1)

        points: list[float] = []
        for timestamp_ns, value in zip(waveform.timestamps_ns, waveform.values, strict=False):
            x = padding_left + ((int(timestamp_ns) - x0) / duration_ns) * usable_width
            normalized = (float(value) - min_value) / value_range
            y = height - padding_bottom - normalized * usable_height
            points.extend([x, y])

        midline_y = padding_top + usable_height / 2
        self.graph.create_line(
            padding_left,
            midline_y,
            width - padding_right,
            midline_y,
            fill=SURFACE_BORDER,
            dash=(4, 4),
        )
        self.graph.create_line(*points, fill=ACCENT, width=2, smooth=True)
        self.graph.create_text(
            10,
            padding_top,
            anchor="nw",
            text=f"{max_value:0.2f}",
            fill=MUTED_FG,
            font=("SF Pro Rounded", 10, "normal"),
        )
        self.graph.create_text(
            10,
            height - padding_bottom,
            anchor="sw",
            text=f"{min_value:0.2f}",
            fill=MUTED_FG,
            font=("SF Pro Rounded", 10, "normal"),
        )
        self.graph.create_text(
            padding_left,
            6,
            anchor="nw",
            text=f"Waveform source: {waveform.source}",
            fill=MUTED_FG,
            font=("SF Pro Rounded", 10, "normal"),
        )

        max_marker_lead_ns = 5_000_000_000
        for marker in view.waveform_markers:
            if marker.recorded_at_ns < x0:
                continue
            marker_timestamp_ns = marker.recorded_at_ns
            if marker.recorded_at_ns > x1:
                is_live_active_marker = (
                    view.mode == "recording"
                    and marker.recorded_at_ns - x1 <= max_marker_lead_ns
                )
                if not is_live_active_marker:
                    continue
                marker_timestamp_ns = x1
            color = KEY_BINDINGS[marker.key_name.lower()][2]
            x = padding_left + ((marker_timestamp_ns - x0) / duration_ns) * usable_width
            self.graph.create_line(x, padding_top, x, height - padding_bottom, fill=color, width=2)
            self.graph.create_text(
                x + 4,
                padding_top + 4,
                anchor="nw",
                text=marker.key_name,
                fill=color,
                font=("SF Pro Rounded", 10, "bold"),
            )

    def _load_recent_labels(self, view: LabelerView) -> None:
        self.recent_list.delete(0, tk.END)
        if not view.recent_labels:
            self.recent_list.insert(tk.END, "No labels yet.")
            return
        for label in view.recent_labels:
            label_time = time.strftime(
                "%H:%M:%S",
                time.localtime(label.recorded_at_ns / 1_000_000_000),
            )
            self.recent_list.insert(
                tk.END,
                f"{label_time}.{label.recorded_at_ns % 1_000_000_000:09d}  "
                f"{label.key_name}  {label.phase_code}",
            )

    def _load_saved_sessions(self, view: LabelerView) -> None:
        self._updating_session_list = True
        self.sessions_list.delete(0, tk.END)

        if not view.session_summaries:
            self.sessions_list.insert(tk.END, "No labeled annotation sessions yet.")
            self._updating_session_list = False
            return

        selected_session_id = view.active_annotation_session_id or view.review_annotation_session_id
        restored_index: int | None = None
        for index, summary in enumerate(view.session_summaries):
            started_text = time.strftime(
                "%Y-%m-%d %H:%M:%S",
                time.localtime(summary.started_at_ns / 1_000_000_000),
            )
            status = "active" if summary.is_active else "saved"
            self.sessions_list.insert(
                tk.END,
                f"{summary.session_id}: [{status}] {summary.name}  "
                f"[{summary.label_count} labels]  {started_text}",
            )
            if selected_session_id == summary.session_id:
                restored_index = index

        if restored_index is None:
            restored_index = 0

        self.sessions_list.selection_set(restored_index)
        self.sessions_list.activate(restored_index)
        self.sessions_list.see(restored_index)
        self._updating_session_list = False

    def _selected_saved_session_id(self) -> int | None:
        selection = self.sessions_list.curselection()
        if not selection:
            return None
        value = self.sessions_list.get(selection[0])
        prefix = value.split(":", 1)[0].strip()
        return int(prefix) if prefix.isdigit() else None

    def _select_saved_session(self, _event: tk.Event[tk.Misc] | None = None) -> None:
        if self._updating_session_list:
            return
        session_id = self._selected_saved_session_id()
        if session_id is None:
            return
        self.controller.select_saved_session(session_id)
        self._refresh_view()


def run_labeler(db_path: Path | str = DEFAULT_DB_PATH, name: str | None = None) -> None:
    app = BreathingLabelerV2App(db_path=db_path, name=name)
    app.run()
