from __future__ import annotations

import time
import tkinter as tk
from dataclasses import dataclass
from pathlib import Path
from tkinter import ttk

from polar_dash.storage import DEFAULT_DB_PATH, Storage

PROTOCOL_NAME = "breathing_phase_keys_v1"
POLL_INTERVAL_MS = 250
SPARKLINE_WINDOW_SECONDS = 120

KEY_BINDINGS = {
    "h": ("inhale_end", "Finished inhaling"),
    "j": ("exhale_start", "Started exhaling"),
    "k": ("exhale_end", "Finished exhaling"),
    "l": ("inhale_start", "Started inhaling"),
}


@dataclass(slots=True)
class LiveEstimate:
    rate_bpm: float | None
    source: str | None
    age_seconds: float | None
    sensor_session_id: int | None


class BreathingLabelerApp:
    def __init__(self, db_path: Path | str = DEFAULT_DB_PATH, name: str | None = None) -> None:
        self.storage = Storage(db_path)
        current_sensor_session = self.storage.find_sensor_session_at()
        self.annotation_session_id = self.storage.start_annotation_session(
            name=name or time.strftime("breathing_labels_%Y%m%d_%H%M%S"),
            protocol_name=PROTOCOL_NAME,
            linked_session_id=(
                int(current_sensor_session["id"]) if current_sensor_session is not None else None
            ),
            notes={"key_bindings": KEY_BINDINGS},
        )

        self.root = tk.Tk()
        self.root.title("Polar Dash Breathing Labeler")
        self.root.geometry("520x500")
        self.root.minsize(500, 460)
        self.root.attributes("-topmost", True)
        self.root.configure(padx=14, pady=14)
        self.root.protocol("WM_DELETE_WINDOW", self.close)

        self.status_var = tk.StringVar(value="Starting annotation session...")
        self.estimate_var = tk.StringVar(value="--.- br/min")
        self.meta_var = tk.StringVar(value="Waiting for breathing estimate...")
        self.sensor_var = tk.StringVar(value="Sensor session: n/a")
        self.annotation_var = tk.StringVar(
            value=f"Annotation session: {self.annotation_session_id}"
        )
        self.last_label_var = tk.StringVar(value="Last label: none")
        self.count_var = tk.StringVar(value="Labels recorded: 0")

        self.sparkline = tk.Canvas(
            self.root,
            width=480,
            height=110,
            bg="#f7f7f2",
            highlightthickness=1,
            highlightbackground="#d4d4d0",
        )
        self.recent_list = tk.Listbox(self.root, height=10, activestyle="none")

        self._build_ui()
        self._bind_keys()
        self.root.after(1, self._post_init_focus)
        self.root.after(POLL_INTERVAL_MS, self._poll)

    def _build_ui(self) -> None:
        title = ttk.Label(
            self.root,
            text="Breathing Phase Labeler",
            font=("SF Pro Rounded", 22, "bold"),
        )
        title.pack(anchor="w")

        subtitle = ttk.Label(
            self.root,
            text=(
                "Focus this window and press H / J / K / L at the exact breathing phase "
                "transitions. Labels are stored with nanosecond timestamps."
            ),
            wraplength=480,
        )
        subtitle.pack(anchor="w", pady=(4, 10))

        estimate_frame = ttk.Frame(self.root)
        estimate_frame.pack(fill="x", pady=(0, 10))
        ttk.Label(
            estimate_frame,
            textvariable=self.estimate_var,
            font=("SF Pro Rounded", 28, "bold"),
        ).pack(anchor="w")
        ttk.Label(estimate_frame, textvariable=self.meta_var).pack(anchor="w", pady=(2, 0))
        ttk.Label(estimate_frame, textvariable=self.sensor_var).pack(anchor="w")
        ttk.Label(estimate_frame, textvariable=self.annotation_var).pack(anchor="w")

        self.sparkline.pack(fill="x", pady=(4, 12))

        key_frame = ttk.LabelFrame(self.root, text="Keys")
        key_frame.pack(fill="x", pady=(0, 12))
        for key_name, (phase_code, description) in KEY_BINDINGS.items():
            ttk.Label(
                key_frame,
                text=f"{key_name.upper()}: {description} ({phase_code})",
            ).pack(anchor="w", padx=8, pady=2)
        ttk.Label(
            key_frame,
            text="U: undo last label    R: refresh now    Q or Esc: quit",
        ).pack(anchor="w", padx=8, pady=(6, 6))

        ttk.Label(self.root, textvariable=self.last_label_var).pack(anchor="w")
        ttk.Label(self.root, textvariable=self.count_var).pack(anchor="w", pady=(0, 8))

        recent_frame = ttk.LabelFrame(self.root, text="Recent Labels")
        recent_frame.pack(fill="both", expand=True)
        self.recent_list.pack(in_=recent_frame, fill="both", expand=True, padx=8, pady=8)

        status = ttk.Label(
            self.root,
            textvariable=self.status_var,
            wraplength=480,
            foreground="#555555",
        )
        status.pack(anchor="w", pady=(10, 0))

    def _bind_keys(self) -> None:
        for key_name in KEY_BINDINGS:
            self.root.bind(f"<KeyPress-{key_name}>", self._on_phase_key)
            self.root.bind(f"<KeyPress-{key_name.upper()}>", self._on_phase_key)
        self.root.bind("<KeyPress-u>", self._undo_last_label)
        self.root.bind("<KeyPress-U>", self._undo_last_label)
        self.root.bind("<KeyPress-r>", self._refresh_now)
        self.root.bind("<KeyPress-R>", self._refresh_now)
        self.root.bind("<KeyPress-q>", self._quit_event)
        self.root.bind("<KeyPress-Q>", self._quit_event)
        self.root.bind("<Escape>", self._quit_event)

    def _post_init_focus(self) -> None:
        self.root.lift()
        self.root.focus_force()
        self.status_var.set("Labeler ready. Bring the window to front and start tagging.")

    def run(self) -> None:
        self.root.mainloop()

    def close(self) -> None:
        try:
            self.storage.close_annotation_session(self.annotation_session_id)
        finally:
            self.storage.close()
            self.root.destroy()

    def _quit_event(self, _event: tk.Event[tk.Misc] | None = None) -> str:
        self.close()
        return "break"

    def _refresh_now(self, _event: tk.Event[tk.Misc] | None = None) -> str:
        self._refresh_view()
        return "break"

    def _undo_last_label(self, _event: tk.Event[tk.Misc] | None = None) -> str:
        if self.storage.delete_last_breathing_phase_label(self.annotation_session_id):
            self.status_var.set("Removed the most recent label from this annotation session.")
        else:
            self.status_var.set("No labels available to undo.")
        self._refresh_view()
        return "break"

    def _on_phase_key(self, event: tk.Event[tk.Misc]) -> str:
        key_name = event.keysym.lower()
        if key_name not in KEY_BINDINGS:
            return "break"

        recorded_at_ns = time.time_ns()
        phase_code, description = KEY_BINDINGS[key_name]
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
        self._refresh_view()
        return "break"

    def _poll(self) -> None:
        self._refresh_view()
        self.root.after(POLL_INTERVAL_MS, self._poll)

    def _refresh_view(self) -> None:
        current_ns = time.time_ns()
        sensor_session = self.storage.find_sensor_session_at(current_ns)
        sensor_session_id = int(sensor_session["id"]) if sensor_session is not None else None

        live_estimate = self._load_live_estimate(current_ns, sensor_session_id)
        self._update_summary(live_estimate, sensor_session_id)
        self._draw_sparkline(sensor_session_id, current_ns)
        self._load_recent_labels()

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

    def _draw_sparkline(self, sensor_session_id: int | None, current_ns: int) -> None:
        self.sparkline.delete("all")
        if sensor_session_id is None:
            self.sparkline.create_text(
                240,
                55,
                text="No active sensor session",
                fill="#666666",
                font=("SF Pro Rounded", 14, "normal"),
            )
            return

        rows = self.storage.connection.execute(
            """
            SELECT estimated_at_ns, breaths_per_min
            FROM breathing_estimates
            WHERE session_id = ?
              AND estimated_at_ns >= ?
            ORDER BY estimated_at_ns
            """,
            (sensor_session_id, current_ns - SPARKLINE_WINDOW_SECONDS * 1_000_000_000),
        ).fetchall()
        if len(rows) < 2:
            self.sparkline.create_text(
                240,
                55,
                text="Waiting for breathing history",
                fill="#666666",
                font=("SF Pro Rounded", 14, "normal"),
            )
            return

        values = [float(row["breaths_per_min"]) for row in rows]
        min_value = min(values)
        max_value = max(values)
        value_range = max(max_value - min_value, 0.5)

        points: list[float] = []
        width = int(self.sparkline["width"])
        height = int(self.sparkline["height"])
        padding_x = 10
        padding_y = 10
        usable_width = max(width - 2 * padding_x, 1)
        usable_height = max(height - 2 * padding_y, 1)

        for index, value in enumerate(values):
            x = padding_x + (index / max(len(values) - 1, 1)) * usable_width
            normalized = (value - min_value) / value_range
            y = height - padding_y - normalized * usable_height
            points.extend([x, y])

        self.sparkline.create_line(
            *points,
            fill="#005f73",
            width=2,
            smooth=True,
        )
        self.sparkline.create_text(
            12,
            8,
            anchor="nw",
            text=f"{min_value:0.1f}",
            fill="#555555",
            font=("SF Pro Rounded", 10, "normal"),
        )
        self.sparkline.create_text(
            12,
            height - 8,
            anchor="sw",
            text=f"{max_value:0.1f}",
            fill="#555555",
            font=("SF Pro Rounded", 10, "normal"),
        )

    def _load_recent_labels(self) -> None:
        rows = self.storage.connection.execute(
            """
            SELECT recorded_at_ns, key_name, phase_code
            FROM breathing_phase_labels
            WHERE annotation_session_id = ?
            ORDER BY recorded_at_ns DESC
            LIMIT 12
            """,
            (self.annotation_session_id,),
        ).fetchall()

        self.recent_list.delete(0, tk.END)
        if not rows:
            self.recent_list.insert(tk.END, "No labels yet.")
            self.last_label_var.set("Last label: none")
            self.count_var.set("Labels recorded: 0")
            return

        for row in rows:
            label_time = time.strftime(
                "%H:%M:%S",
                time.localtime(int(row["recorded_at_ns"]) / 1_000_000_000),
            )
            self.recent_list.insert(
                tk.END,
                f"{label_time}.{int(row['recorded_at_ns']) % 1_000_000_000:09d}  "
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
            (self.annotation_session_id,),
        ).fetchone()[0]
        self.count_var.set(f"Labels recorded: {count}")


def run_labeler(db_path: Path | str = DEFAULT_DB_PATH, name: str | None = None) -> None:
    app = BreathingLabelerApp(db_path=db_path, name=name)
    app.run()
