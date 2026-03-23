from __future__ import annotations

import json
import math
import os
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

KEY_BINDINGS = {
    "h": ("inhale_end", "Finished inhaling", "#e76f51"),
    "j": ("exhale_start", "Started exhaling", "#264653"),
    "k": ("exhale_end", "Finished exhaling", "#2a9d8f"),
    "l": ("inhale_start", "Started inhaling", "#f4a261"),
}


@dataclass(slots=True)
class LiveEstimate:
    rate_bpm: float | None
    source: str | None
    age_seconds: float | None
    sensor_session_id: int | None


@dataclass(slots=True)
class RespiratoryWaveform:
    timestamps_ns: np.ndarray
    values: np.ndarray
    source: str


class BreathingLabelerApp:
    def __init__(self, db_path: Path | str = DEFAULT_DB_PATH, name: str | None = None) -> None:
        self.storage = Storage(db_path)

        self.root = tk.Tk()
        self.root.title("Polar Dash Breathing Labeler")
        self.root.geometry("1180x760")
        self.root.minsize(1080, 680)
        self.root.attributes("-topmost", True)
        self.root.configure(padx=14, pady=14)
        self.root.protocol("WM_DELETE_WINDOW", self.close)

        self.annotation_session_id: int | None = None
        self.last_annotation_session_id: int | None = None
        self.session_name_var = tk.StringVar(
            master=self.root,
            value=name or time.strftime("breathing_labels_%Y%m%d_%H%M%S"),
        )
        self.status_var = tk.StringVar(
            master=self.root,
            value="Ready. Start a session to begin labeling.",
        )
        self.estimate_var = tk.StringVar(master=self.root, value="--.- br/min")
        self.meta_var = tk.StringVar(
            master=self.root,
            value="Waiting for breathing estimate...",
        )
        self.sensor_var = tk.StringVar(master=self.root, value="Sensor session: n/a")
        self.annotation_var = tk.StringVar(
            master=self.root,
            value="Annotation session: inactive",
        )
        self.last_label_var = tk.StringVar(master=self.root, value="Last label: none")
        self.count_var = tk.StringVar(master=self.root, value="Labels recorded: 0")
        self.saved_session_var = tk.StringVar(master=self.root, value="Saved sessions")
        self.session_state_var = tk.StringVar(master=self.root, value="Recorder: idle")
        self.selection_var = tk.StringVar(master=self.root, value="Viewing: live feed")

        self.graph = tk.Canvas(
            self.root,
            width=800,
            height=260,
            bg="#f7f7f2",
            highlightthickness=1,
            highlightbackground="#d4d4d0",
        )
        self.recent_list = tk.Listbox(self.root, height=12, activestyle="none")
        self.sessions_list = tk.Listbox(
            self.root,
            height=12,
            width=42,
            activestyle="none",
            exportselection=False,
        )

        self._build_ui()
        self._bind_keys()
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
                "rather than only the breathing rate. Press H / J / K / L while a session is active."
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
        ttk.Button(controls, text="Start Session", command=self.start_session).pack(side="left")
        ttk.Button(controls, text="Stop && Save", command=self.stop_session).pack(
            side="left", padx=(8, 0)
        )
        ttk.Button(controls, text="Refresh", command=self._refresh_view).pack(
            side="left", padx=(8, 0)
        )

        summary = ttk.Frame(content)
        summary.pack(fill="x", pady=(0, 10))
        ttk.Label(
            summary,
            textvariable=self.estimate_var,
            font=("SF Pro Rounded", 30, "bold"),
        ).pack(anchor="w")
        ttk.Label(summary, textvariable=self.meta_var).pack(anchor="w", pady=(2, 0))
        ttk.Label(summary, textvariable=self.sensor_var).pack(anchor="w")
        ttk.Label(summary, textvariable=self.selection_var).pack(anchor="w")

        self.graph.pack(in_=content, fill="x", pady=(4, 12))

        key_frame = ttk.LabelFrame(content, text="Keys")
        key_frame.pack(fill="x", pady=(0, 12))
        for key_name, (phase_code, description, _) in KEY_BINDINGS.items():
            ttk.Label(
                key_frame,
                text=f"{key_name.upper()}: {description} ({phase_code})",
            ).pack(anchor="w", padx=8, pady=2)
        ttk.Label(
            key_frame,
            text="U: undo last label    Q or Esc: quit",
        ).pack(anchor="w", padx=8, pady=(6, 6))

        feedback = ttk.Frame(content)
        feedback.pack(fill="x")
        ttk.Label(feedback, textvariable=self.last_label_var).pack(anchor="w")
        ttk.Label(feedback, textvariable=self.count_var).pack(anchor="w", pady=(0, 8))

        recent_frame = ttk.LabelFrame(content, text="Recent Labels")
        recent_frame.pack(fill="both", expand=True)
        self.recent_list.pack(in_=recent_frame, fill="both", expand=True, padx=8, pady=8)

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

        sessions_frame = ttk.LabelFrame(sidebar, text="Saved Sessions")
        sessions_frame.pack(fill="both", expand=True)
        ttk.Label(
            sessions_frame,
            text="Saved sessions appear here after Stop & Save. Select one to inspect it or delete it.",
            wraplength=300,
        ).pack(anchor="w", padx=8, pady=(8, 6))
        sessions_list_frame = ttk.Frame(sessions_frame)
        sessions_list_frame.pack(fill="both", expand=True, padx=8, pady=(0, 8))
        sessions_scrollbar = ttk.Scrollbar(
            sessions_list_frame,
            orient="vertical",
            command=self.sessions_list.yview,
        )
        self.sessions_list.configure(yscrollcommand=sessions_scrollbar.set)
        self.sessions_list.pack(in_=sessions_list_frame, side="left", fill="both", expand=True)
        sessions_scrollbar.pack(side="right", fill="y")
        button_row = ttk.Frame(sessions_frame)
        button_row.pack(fill="x", padx=8, pady=(0, 8))
        ttk.Button(button_row, text="Delete Selected", command=self.delete_selected_session).pack(
            side="left"
        )
        ttk.Label(
            sessions_frame,
            textvariable=self.saved_session_var,
            wraplength=300,
        ).pack(anchor="w", padx=8, pady=(0, 8))

        status = ttk.Label(
            self.root,
            textvariable=self.status_var,
            wraplength=800,
            foreground="#555555",
        )
        status.pack(anchor="w", pady=(10, 0))

    def _bind_keys(self) -> None:
        for key_name in KEY_BINDINGS:
            self.root.bind(f"<KeyPress-{key_name}>", self._on_phase_key)
            self.root.bind(f"<KeyPress-{key_name.upper()}>", self._on_phase_key)
        self.root.bind("<KeyPress-u>", self._undo_last_label)
        self.root.bind("<KeyPress-U>", self._undo_last_label)
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

    def start_session(self) -> None:
        if self.annotation_session_id is not None:
            self.status_var.set("A labeling session is already active.")
            return

        current_sensor_session = self.storage.find_sensor_session_at()
        self.annotation_session_id = self.storage.start_annotation_session(
            name=self.session_name_var.get().strip() or time.strftime("breathing_labels_%Y%m%d_%H%M%S"),
            protocol_name=PROTOCOL_NAME,
            linked_session_id=(
                int(current_sensor_session["id"]) if current_sensor_session is not None else None
            ),
            notes={"key_bindings": KEY_BINDINGS},
        )
        self.last_annotation_session_id = self.annotation_session_id
        self.annotation_var.set(f"Annotation session: {self.annotation_session_id} (active)")
        self.status_var.set("Session started. Tag phases with H / J / K / L.")
        self._refresh_view()

    def stop_session(self) -> None:
        if self.annotation_session_id is None:
            self.status_var.set("No active labeling session to stop.")
            return

        self.storage.close_annotation_session(self.annotation_session_id)
        self.status_var.set(f"Saved annotation session {self.annotation_session_id}.")
        self.last_annotation_session_id = self.annotation_session_id
        self.annotation_session_id = None
        self.annotation_var.set(
            f"Annotation session: inactive (last saved {self.last_annotation_session_id})"
        )
        self.session_name_var.set(time.strftime("breathing_labels_%Y%m%d_%H%M%S"))
        self._refresh_view()

    def delete_selected_session(self) -> None:
        selection = self.sessions_list.curselection()
        if not selection:
            self.status_var.set("Select a saved session to delete.")
            self.root.bell()
            return

        session_id = self._selected_saved_session_id()
        if session_id is None:
            self.status_var.set("Could not resolve the selected saved session.")
            self.root.bell()
            return

        if self.annotation_session_id == session_id:
            self.status_var.set("Stop the active session before deleting it.")
            self.root.bell()
            return

        if self.storage.delete_annotation_session(session_id):
            if self.last_annotation_session_id == session_id:
                self.last_annotation_session_id = None
            self.saved_session_var.set(f"Deleted saved session {session_id}.")
            self.status_var.set(f"Deleted annotation session {session_id}.")
            self._refresh_view()
            return

        self.status_var.set(f"Annotation session {session_id} was not found.")
        self.root.bell()

    def close(self) -> None:
        try:
            if self.annotation_session_id is not None:
                self.storage.close_annotation_session(self.annotation_session_id)
        finally:
            self.storage.close()
            self.root.destroy()

    def _quit_event(self, _event: tk.Event[tk.Misc] | None = None) -> str:
        self.close()
        return "break"

    def _undo_last_label(self, _event: tk.Event[tk.Misc] | None = None) -> str:
        target_session_id = self.annotation_session_id or self.last_annotation_session_id
        if target_session_id is None:
            self.status_var.set("No annotation session available to undo.")
            return "break"
        if self.storage.delete_last_breathing_phase_label(target_session_id):
            self.status_var.set("Removed the most recent label from the current session.")
        else:
            self.status_var.set("No labels available to undo.")
        self._refresh_view()
        return "break"

    def _on_phase_key(self, event: tk.Event[tk.Misc]) -> str:
        if self.annotation_session_id is None:
            self.status_var.set("Start a session before recording breathing labels.")
            self.root.bell()
            return "break"

        key_name = event.keysym.lower()
        if key_name not in KEY_BINDINGS:
            return "break"

        recorded_at_ns = time.time_ns()
        phase_code, description, _ = KEY_BINDINGS[key_name]
        sensor_session = self.storage.find_sensor_session_at(recorded_at_ns)
        sensor_session_id = int(sensor_session["id"]) if sensor_session is not None else None
        estimate = self.storage.find_nearest_breathing_estimate(
            recorded_at_ns,
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
            estimate_age_ms = abs(recorded_at_ns - estimate_time_ns) / 1_000_000

        self.storage.insert_breathing_phase_label(
            self.annotation_session_id,
            recorded_at_ns=recorded_at_ns,
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
                "annotation_session_id": self.annotation_session_id,
                "phase_code": phase_code,
                "key_name": key_name.upper(),
            },
            session_id=sensor_session_id,
            recorded_at_ns=recorded_at_ns,
        )

        label_time = time.strftime("%H:%M:%S", time.localtime(recorded_at_ns / 1_000_000_000))
        self.status_var.set(
            f"Recorded {key_name.upper()} -> {description} at {label_time}.{recorded_at_ns % 1_000_000_000:09d}"
        )
        self.root.bell()
        self._refresh_view()
        return "break"

    def _poll(self) -> None:
        if self.root.winfo_exists():
            self._refresh_view()
            self.root.after(POLL_INTERVAL_MS, self._poll)

    def _refresh_view(self) -> None:
        current_ns = time.time_ns()
        sensor_session = self.storage.find_sensor_session_at(current_ns)
        sensor_session_id = int(sensor_session["id"]) if sensor_session is not None else None

        live_estimate = self._load_live_estimate(current_ns, sensor_session_id)
        self._update_summary(live_estimate, sensor_session_id)
        self._draw_waveform(sensor_session_id, current_ns)
        self._load_recent_labels()
        self._load_saved_sessions()

    def _load_live_estimate(
        self,
        current_ns: int,
        sensor_session_id: int | None,
    ) -> LiveEstimate:
        row = self.storage.find_nearest_breathing_estimate(
            current_ns,
            sensor_session_id=sensor_session_id,
        )
        if row is None:
            return LiveEstimate(None, None, None, sensor_session_id)
        age_seconds = abs(current_ns - int(row["estimated_at_ns"])) / 1_000_000_000
        return LiveEstimate(
            rate_bpm=float(row["breaths_per_min"]),
            source=str(row["source"]),
            age_seconds=age_seconds,
            sensor_session_id=sensor_session_id,
        )

    def _update_summary(
        self,
        live_estimate: LiveEstimate,
        sensor_session_id: int | None,
    ) -> None:
        if live_estimate.rate_bpm is None:
            self.estimate_var.set("--.- br/min")
            self.meta_var.set("No recent breathing estimate available.")
        else:
            self.estimate_var.set(f"{live_estimate.rate_bpm:0.2f} br/min")
            self.meta_var.set(
                f"Source: {live_estimate.source}    Age: {live_estimate.age_seconds:0.2f}s"
            )

        if sensor_session_id is None:
            self.sensor_var.set("Sensor session: n/a")
        else:
            self.sensor_var.set(f"Sensor session: {sensor_session_id}")

        if self.annotation_session_id is not None:
            self.annotation_var.set(f"Annotation session: {self.annotation_session_id} (active)")
            self.session_state_var.set(f"Recorder: ACTIVE on session {self.annotation_session_id}")
            self.selection_var.set("Viewing: active recording")
        elif self.last_annotation_session_id is not None:
            self.annotation_var.set(
                f"Annotation session: inactive (last saved {self.last_annotation_session_id})"
            )
            self.session_state_var.set("Recorder: idle")
            self.selection_var.set(f"Viewing: saved session {self.last_annotation_session_id}")
        else:
            self.annotation_var.set("Annotation session: inactive")
            self.session_state_var.set("Recorder: idle")
            self.selection_var.set("Viewing: live feed")

    def _load_waveform(
        self,
        sensor_session_id: int | None,
        current_ns: int,
    ) -> RespiratoryWaveform | None:
        if sensor_session_id is None:
            return None

        rows = self.storage.connection.execute(
            """
            SELECT sensor_recorded_at_ns, sample_rate_hz, samples_json
            FROM acc_frames
            WHERE session_id = ?
              AND sensor_recorded_at_ns >= ?
            ORDER BY sensor_recorded_at_ns
            """,
            (sensor_session_id, current_ns - GRAPH_WINDOW_SECONDS * 1_000_000_000),
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
                samples.append(
                    (start_ns + index * step_ns, float(x), float(y), float(z))
                )

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

    def _draw_waveform(self, sensor_session_id: int | None, current_ns: int) -> None:
        self.graph.delete("all")
        width = int(self.graph["width"])
        height = int(self.graph["height"])

        waveform = self._load_waveform(sensor_session_id, current_ns)
        if waveform is None or len(waveform.values) < 2:
            self.graph.create_text(
                width / 2,
                height / 2,
                text="Waiting for respiratory waveform",
                fill="#666666",
                font=("SF Pro Rounded", 16, "normal"),
            )
            return

        values = waveform.values
        min_value = float(np.min(values))
        max_value = float(np.max(values))
        value_range = max(max_value - min_value, 1e-6)

        padding_left = 40
        padding_right = 14
        padding_top = 18
        padding_bottom = 22
        usable_width = max(width - padding_left - padding_right, 1)
        usable_height = max(height - padding_top - padding_bottom, 1)

        x0 = waveform.timestamps_ns[0]
        x1 = waveform.timestamps_ns[-1]
        duration_ns = max(int(x1 - x0), 1)

        points: list[float] = []
        for timestamp_ns, value in zip(waveform.timestamps_ns, values, strict=False):
            x = padding_left + ((int(timestamp_ns) - int(x0)) / duration_ns) * usable_width
            normalized = (float(value) - min_value) / value_range
            y = height - padding_bottom - normalized * usable_height
            points.extend([x, y])

        midline_y = padding_top + usable_height / 2
        self.graph.create_line(
            padding_left,
            midline_y,
            width - padding_right,
            midline_y,
            fill="#d0d0ca",
            dash=(4, 4),
        )
        self.graph.create_line(
            *points,
            fill="#005f73",
            width=2,
            smooth=True,
        )

        self.graph.create_text(
            10,
            padding_top,
            anchor="nw",
            text=f"{max_value:0.2f}",
            fill="#555555",
            font=("SF Pro Rounded", 10, "normal"),
        )
        self.graph.create_text(
            10,
            height - padding_bottom,
            anchor="sw",
            text=f"{min_value:0.2f}",
            fill="#555555",
            font=("SF Pro Rounded", 10, "normal"),
        )
        self.graph.create_text(
            padding_left,
            6,
            anchor="nw",
            text=f"Waveform source: {waveform.source}",
            fill="#555555",
            font=("SF Pro Rounded", 10, "normal"),
        )

        target_session_id = self.annotation_session_id or self.last_annotation_session_id
        if target_session_id is None:
            return

        marker_rows = self.storage.connection.execute(
            """
            SELECT recorded_at_ns, key_name, phase_code
            FROM breathing_phase_labels
            WHERE annotation_session_id = ?
              AND recorded_at_ns >= ?
            ORDER BY recorded_at_ns
            """,
            (target_session_id, current_ns - GRAPH_WINDOW_SECONDS * 1_000_000_000),
        ).fetchall()
        for row in marker_rows:
            recorded_at_ns = int(row["recorded_at_ns"])
            if recorded_at_ns < int(x0) or recorded_at_ns > int(x1):
                continue
            color = KEY_BINDINGS[str(row["key_name"]).lower()][2]
            x = padding_left + ((recorded_at_ns - int(x0)) / duration_ns) * usable_width
            self.graph.create_line(x, padding_top, x, height - padding_bottom, fill=color, width=2)
            self.graph.create_text(
                x + 4,
                padding_top + 4,
                anchor="nw",
                text=str(row["key_name"]),
                fill=color,
                font=("SF Pro Rounded", 10, "bold"),
            )

    def _load_recent_labels(self) -> None:
        target_session_id = self.annotation_session_id or self.last_annotation_session_id
        self.recent_list.delete(0, tk.END)
        if target_session_id is None:
            self.recent_list.insert(tk.END, "No annotation session yet.")
            self.last_label_var.set("Last label: none")
            self.count_var.set("Labels recorded: 0")
            return

        rows = self.storage.connection.execute(
            """
            SELECT recorded_at_ns, key_name, phase_code
            FROM breathing_phase_labels
            WHERE annotation_session_id = ?
            ORDER BY recorded_at_ns DESC
            LIMIT 12
            """,
            (target_session_id,),
        ).fetchall()
        if not rows:
            self.recent_list.insert(tk.END, "No labels yet.")
            self.last_label_var.set("Last label: none")
            self.count_var.set("Labels recorded: 0")
            return

        for row in rows:
            timestamp_ns = int(row["recorded_at_ns"])
            label_time = time.strftime(
                "%H:%M:%S",
                time.localtime(timestamp_ns / 1_000_000_000),
            )
            self.recent_list.insert(
                tk.END,
                f"{label_time}.{timestamp_ns % 1_000_000_000:09d}  "
                f"{row['key_name']}  {row['phase_code']}",
            )

        latest = rows[0]
        self.last_label_var.set(
            f"Last label: {latest['key_name']} -> {latest['phase_code']}"
        )
        count = self.storage.connection.execute(
            """
            SELECT COUNT(*)
            FROM breathing_phase_labels
            WHERE annotation_session_id = ?
            """,
            (target_session_id,),
        ).fetchone()[0]
        self.count_var.set(f"Labels recorded: {count}")

    def _load_saved_sessions(self) -> None:
        rows = self.storage.list_annotation_sessions(include_active=False)
        selected_session_id = self._selected_saved_session_id()
        self.sessions_list.delete(0, tk.END)

        if not rows:
            self.sessions_list.insert(tk.END, "No saved sessions yet.")
            self.saved_session_var.set("Saved sessions: 0. Use Stop & Save to finish a labeling run.")
            return

        restored_index: int | None = None
        for index, row in enumerate(rows):
            session_id = int(row["id"])
            started_text = time.strftime(
                "%Y-%m-%d %H:%M:%S",
                time.localtime(int(row["started_at_ns"]) / 1_000_000_000),
            )
            label_count = int(row["label_count"])
            name = str(row["name"])
            self.sessions_list.insert(
                tk.END,
                f"{session_id}: {name}  [{label_count} labels]  {started_text}",
            )
            if selected_session_id == session_id:
                restored_index = index
            elif restored_index is None and self.last_annotation_session_id == session_id:
                restored_index = index

        if restored_index is not None:
            self.sessions_list.selection_set(restored_index)
            self.sessions_list.activate(restored_index)
            self.sessions_list.see(restored_index)

        self.saved_session_var.set(
            f"Saved sessions: {len(rows)}. Latest saved session: {int(rows[0]['id'])}."
        )

    def _selected_saved_session_id(self) -> int | None:
        selection = self.sessions_list.curselection()
        if not selection:
            return None
        value = self.sessions_list.get(selection[0])
        prefix = value.split(":", 1)[0].strip()
        if not prefix.isdigit():
            return None
        return int(prefix)

    def _select_saved_session(self, _event: tk.Event[tk.Misc] | None = None) -> None:
        session_id = self._selected_saved_session_id()
        if session_id is None:
            return
        self.last_annotation_session_id = session_id
        self.selection_var.set(f"Viewing: saved session {session_id}")
        self.status_var.set(f"Viewing saved annotation session {session_id}.")
        self._refresh_view()


def run_labeler(db_path: Path | str = DEFAULT_DB_PATH, name: str | None = None) -> None:
    app = BreathingLabelerApp(db_path=db_path, name=name)
    app.run()
