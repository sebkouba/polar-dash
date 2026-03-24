from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import queue
import threading
import time
import tkinter as tk
from collections import defaultdict, deque
from dataclasses import dataclass
from pathlib import Path
from tkinter import ttk
from typing import Any

from bleak import BleakClient, BleakScanner
from bleakheart import BatteryLevel, HeartRate, PolarMeasurementData

from polar_dash.breathing import (
    ACC_SAMPLE_RATE_HZ,
    ECG_SAMPLE_RATE_HZ,
    CandidateEstimate,
    FusionCalibration,
    LiveBreathingEngine,
    compute_rmssd_series,
    fit_fusion_calibration,
    load_default_fusion_calibration,
    rebuild_learned_fusion_history,
)
from polar_dash.storage import DEFAULT_DB_PATH, Storage

logger = logging.getLogger(__name__)

PROTOCOL_NAME = "breathing_turnaround_fg_v1"
GRAPH_WINDOW_SECONDS = 30
POLL_INTERVAL_MS = 200
SURFACE_BG = "#f6f3ec"
SURFACE_BORDER = "#cfc8bb"
TEXT_FG = "#1f1f1f"
MUTED_FG = "#5d5a53"
ACCENT = "#005f73"

KEY_BINDINGS = {
    "f": ("exhale_end", "Finished exhaling", "#2a9d8f"),
    "g": ("inhale_end", "Finished inhaling", "#e76f51"),
}
CANDIDATE_COLORS = {
    "acc_pca": "#1d3557",
    "ecg_qrs": "#c1121f",
    "rr_interval": "#6a4c93",
    "learned_fusion": "#2a9d8f",
}
SHORTCUT_HINTS = (
    ("F", "Finished exhaling"),
    ("G", "Finished inhaling"),
    ("Esc", "Close cockpit"),
)


@dataclass(slots=True)
class CockpitConfig:
    device_name_prefix: str = "Polar H10"
    db_path: str = str(DEFAULT_DB_PATH)
    scan_timeout: float = 10.0
    reconnect_delay: float = 3.0
    auto_start: bool = True


@dataclass(slots=True)
class CollectorMessage:
    kind: str
    payload: dict[str, Any]


@dataclass(slots=True)
class CockpitSnapshot:
    status_message: str
    session_text: str
    calibration_text: str
    label_text: str
    estimates: dict[str, CandidateEstimate]
    ecg_points: list[tuple[int, float]]
    acc_points: list[tuple[int, float, float, float]]
    respiratory_points: list[tuple[int, float]]
    beats: list[tuple[int, float]]
    rmssd: list[tuple[int, float]]
    candidate_history: dict[str, list[CandidateEstimate]]
    labels: list[tuple[int, str, str]]
    latest_time_ns: int | None
    active_annotation_session_id: int | None


class AsyncCockpitCollector:
    def __init__(
        self,
        config: CockpitConfig,
        message_queue: queue.Queue[CollectorMessage],
    ) -> None:
        self.config = config
        self.message_queue = message_queue
        self._stop_event = asyncio.Event()
        self._loop: asyncio.AbstractEventLoop | None = None

    def request_stop(self) -> None:
        if self._loop is not None:
            self._loop.call_soon_threadsafe(self._stop_event.set)

    async def run(self) -> None:
        self._loop = asyncio.get_running_loop()
        while not self._stop_event.is_set():
            device = await self._find_device()
            if device is None:
                self._emit(
                    "status",
                    message=f"No BLE device found matching {self.config.device_name_prefix!r}; retrying",
                )
                await self._sleep_or_stop(self.config.reconnect_delay)
                continue
            try:
                await self._collect_session(device)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.warning("Cockpit collection error: %s", exc, exc_info=True)
                self._emit("warning", message=str(exc))
                await self._sleep_or_stop(self.config.reconnect_delay)

    async def _sleep_or_stop(self, delay: float) -> None:
        try:
            await asyncio.wait_for(self._stop_event.wait(), timeout=delay)
        except asyncio.TimeoutError:
            return

    async def _find_device(self) -> Any | None:
        devices = await BleakScanner.discover(timeout=self.config.scan_timeout)
        prefix_lower = self.config.device_name_prefix.lower()
        matches = [device for device in devices if prefix_lower in (device.name or "").lower()]
        matches.sort(key=lambda device: ((device.name or ""), device.address))
        if not matches:
            return None
        return matches[0]

    async def _collect_session(self, device: Any) -> None:
        device_name = device.name or self.config.device_name_prefix
        disconnect_event = asyncio.Event()
        loop = asyncio.get_running_loop()

        def handle_disconnect(_: BleakClient) -> None:
            loop.call_soon_threadsafe(disconnect_event.set)

        self._emit(
            "status",
            message=f"Connecting to {device_name} ({device.address})",
        )
        async with BleakClient(device, disconnected_callback=handle_disconnect) as client:
            self._emit(
                "connected",
                device_name=device_name,
                device_address=device.address,
            )
            try:
                battery_percent = await BatteryLevel(client).read()
            except Exception as exc:
                self._emit("warning", message=f"Battery read failed: {exc}")
            else:
                self._emit("battery", battery_percent=battery_percent)

            hr_queue: asyncio.Queue[tuple[str, int, tuple[float, list[int]], int | None]] = asyncio.Queue()
            ecg_queue: asyncio.Queue[tuple[str, int, list[int]]] = asyncio.Queue()
            acc_queue: asyncio.Queue[tuple[str, int, list[tuple[int, int, int]]]] = asyncio.Queue()

            hr_monitor = HeartRate(client, queue=hr_queue, unpack=False)
            pmd = PolarMeasurementData(
                client,
                ecg_queue=ecg_queue,
                acc_queue=acc_queue,
                callback=lambda _payload: None,
            )
            consumer_tasks = [
                asyncio.create_task(self._consume_hr_frames(hr_queue)),
                asyncio.create_task(self._consume_ecg_frames(ecg_queue)),
                asyncio.create_task(self._consume_acc_frames(acc_queue)),
            ]
            started_streams: list[str] = []

            try:
                await hr_monitor.start_notify()
                self._emit("event", event_type="hr_notify_started", details={})

                available_measurements = await pmd.available_measurements()
                self._emit(
                    "event",
                    event_type="pmd_measurements",
                    details={"available": available_measurements},
                )

                if "ECG" in available_measurements:
                    await self._start_stream(pmd, "ECG", started_streams)
                if "ACC" in available_measurements:
                    await self._start_stream(pmd, "ACC", started_streams)

                await self._wait_for_disconnect(disconnect_event)
            finally:
                for measurement in reversed(started_streams):
                    with contextlib.suppress(Exception):
                        await pmd.stop_streaming(measurement)
                with contextlib.suppress(Exception):
                    await hr_monitor.stop_notify()
                for task in consumer_tasks:
                    task.cancel()
                await asyncio.gather(*consumer_tasks, return_exceptions=True)
                self._emit("disconnected")

    async def _start_stream(
        self,
        pmd: PolarMeasurementData,
        measurement: str,
        started_streams: list[str],
    ) -> None:
        error_code, message, _ = await pmd.start_streaming(measurement)
        if error_code == 0:
            started_streams.append(measurement)
            self._emit("event", event_type="stream_started", details={"measurement": measurement})
            return
        self._emit(
            "warning",
            message=f"Unable to start {measurement} stream: {message}",
        )

    async def _wait_for_disconnect(self, disconnect_event: asyncio.Event) -> None:
        stop_task = asyncio.create_task(self._stop_event.wait())
        disconnect_task = asyncio.create_task(disconnect_event.wait())
        done, pending = await asyncio.wait(
            {stop_task, disconnect_task},
            return_when=asyncio.FIRST_COMPLETED,
        )
        for task in pending:
            task.cancel()
        await asyncio.gather(*pending, return_exceptions=True)

    async def _consume_hr_frames(
        self,
        queue_: asyncio.Queue[tuple[str, int, tuple[float, list[int]], int | None]],
    ) -> None:
        while True:
            _measurement, recorded_at_ns, payload, energy_kj = await queue_.get()
            average_hr_bpm, rr_intervals_ms = payload
            self._emit(
                "hr_frame",
                recorded_at_ns=recorded_at_ns,
                average_hr_bpm=average_hr_bpm,
                rr_intervals_ms=rr_intervals_ms,
                energy_kj=energy_kj,
            )

    async def _consume_ecg_frames(
        self,
        queue_: asyncio.Queue[tuple[str, int, list[int]]],
    ) -> None:
        while True:
            _measurement, sensor_recorded_at_ns, samples = await queue_.get()
            self._emit(
                "ecg_frame",
                sensor_recorded_at_ns=sensor_recorded_at_ns,
                sample_rate_hz=ECG_SAMPLE_RATE_HZ,
                samples=samples,
            )

    async def _consume_acc_frames(
        self,
        queue_: asyncio.Queue[tuple[str, int, list[tuple[int, int, int]]]],
    ) -> None:
        while True:
            _measurement, sensor_recorded_at_ns, samples = await queue_.get()
            self._emit(
                "acc_frame",
                sensor_recorded_at_ns=sensor_recorded_at_ns,
                sample_rate_hz=ACC_SAMPLE_RATE_HZ,
                samples=samples,
            )

    def _emit(self, kind: str, **payload: Any) -> None:
        self.message_queue.put(CollectorMessage(kind=kind, payload=payload))


class CockpitController:
    def __init__(self, storage: Storage, config: CockpitConfig) -> None:
        self.storage = storage
        self.config = config
        self.engine = LiveBreathingEngine()
        self.message_queue: queue.Queue[CollectorMessage] = queue.Queue()
        self.collector_thread: threading.Thread | None = None
        self.collector_worker: AsyncCockpitCollector | None = None
        self.current_session_id: int | None = None
        self.current_device_name: str | None = None
        self.current_device_address: str | None = None
        self.active_annotation_session_id: int | None = None
        self.status_message = "Ready. Collector idle."
        self.recent_labels: deque[tuple[int, str, str]] = deque(maxlen=40)
        self.candidate_history_by_name: dict[str, deque[CandidateEstimate]] = defaultdict(
            lambda: deque(maxlen=600)
        )
        self.current_estimates: dict[str, CandidateEstimate] = {}

        latest_calibration = self.storage.get_latest_breathing_calibration()
        if latest_calibration is not None:
            calibration = FusionCalibration.from_dict(json.loads(latest_calibration["model_json"]))
            calibration.version = int(latest_calibration["id"])
            self.engine.set_calibration(calibration)
        else:
            self.engine.set_calibration(load_default_fusion_calibration())

    def close(self) -> None:
        self.stop_collection()
        if self.active_annotation_session_id is not None:
            if self.storage.count_breathing_phase_labels(self.active_annotation_session_id) == 0:
                self.storage.delete_annotation_session(self.active_annotation_session_id)
            else:
                self.storage.close_annotation_session(self.active_annotation_session_id)
            self.active_annotation_session_id = None
        if self.current_session_id is not None:
            self.storage.close_session(self.current_session_id)
            self.current_session_id = None
        self.storage.close()

    def start_collection(self) -> None:
        if self.collector_thread is not None and self.collector_thread.is_alive():
            self.status_message = "Collector already running."
            return
        self.collector_worker = AsyncCockpitCollector(self.config, self.message_queue)
        self.collector_thread = threading.Thread(
            target=self._run_collector_worker,
            name="polar-dash-cockpit",
            daemon=True,
        )
        self.collector_thread.start()
        self.status_message = "Starting collector..."

    def stop_collection(self) -> None:
        if self.collector_worker is not None:
            self.collector_worker.request_stop()
        if self.collector_thread is not None:
            self.collector_thread.join(timeout=2.0)
        self.collector_thread = None
        self.collector_worker = None
        self.status_message = "Collector stopped."

    def drain_messages(self) -> None:
        while True:
            try:
                message = self.message_queue.get_nowait()
            except queue.Empty:
                break
            self._handle_message(message)

    def begin_live_session(
        self,
        device_name: str,
        device_address: str,
    ) -> int:
        if self.current_session_id is not None:
            self.storage.close_session(self.current_session_id)
        session_id = self.storage.start_session(device_name, device_address)
        self.current_session_id = session_id
        self.current_device_name = device_name
        self.current_device_address = device_address
        self.storage.insert_event(
            "collector_connected",
            {"device_name": device_name, "device_address": device_address},
            session_id=session_id,
        )
        self.status_message = f"Live session {session_id} started."
        return session_id

    def end_live_session(self) -> None:
        if self.current_session_id is None:
            return
        self.storage.insert_event("collector_disconnected", {}, session_id=self.current_session_id)
        self.storage.close_session(self.current_session_id)
        self.status_message = f"Live session {self.current_session_id} closed."
        self.current_session_id = None

    def ingest_hr_frame(
        self,
        *,
        recorded_at_ns: int,
        average_hr_bpm: float,
        rr_intervals_ms: list[int],
        energy_kj: int | None = None,
    ) -> None:
        if self.current_session_id is None:
            return
        self.storage.insert_hr_frame(
            self.current_session_id,
            recorded_at_ns=recorded_at_ns,
            average_hr_bpm=average_hr_bpm,
            rr_intervals_ms=rr_intervals_ms,
            energy_kj=energy_kj,
        )
        estimates = self.engine.add_hr_frame(
            recorded_at_ns=recorded_at_ns,
            average_hr_bpm=average_hr_bpm,
            rr_intervals_ms=rr_intervals_ms,
        )
        self._process_estimates(estimates)

    def ingest_ecg_frame(
        self,
        *,
        sensor_recorded_at_ns: int,
        sample_rate_hz: int,
        samples: list[int],
    ) -> None:
        if self.current_session_id is None:
            return
        self.storage.insert_ecg_frame(
            self.current_session_id,
            sensor_recorded_at_ns=sensor_recorded_at_ns,
            sample_rate_hz=sample_rate_hz,
            samples=samples,
        )
        estimates = self.engine.add_ecg_frame(
            sensor_recorded_at_ns=sensor_recorded_at_ns,
            sample_rate_hz=sample_rate_hz,
            samples=samples,
        )
        self._process_estimates(estimates)

    def ingest_acc_frame(
        self,
        *,
        sensor_recorded_at_ns: int,
        sample_rate_hz: int,
        samples: list[tuple[int, int, int]],
    ) -> None:
        if self.current_session_id is None:
            return
        self.storage.insert_acc_frame(
            self.current_session_id,
            sensor_recorded_at_ns=sensor_recorded_at_ns,
            sample_rate_hz=sample_rate_hz,
            samples=samples,
        )
        estimates = self.engine.add_acc_frame(
            sensor_recorded_at_ns=sensor_recorded_at_ns,
            sample_rate_hz=sample_rate_hz,
            samples=samples,
        )
        self._process_estimates(estimates)

    def start_label_session(self, name: str | None = None) -> None:
        if self.current_session_id is None:
            self.status_message = "No live collector session is active."
            return
        if self.active_annotation_session_id is not None:
            self.status_message = "A label session is already active."
            return
        session_name = name or time.strftime("breathing_turnarounds_%Y%m%d_%H%M%S")
        self.active_annotation_session_id = self.storage.start_annotation_session(
            session_name,
            protocol_name=PROTOCOL_NAME,
            linked_session_id=self.current_session_id,
            notes={"key_bindings": KEY_BINDINGS},
        )
        self.status_message = f"Label session {self.active_annotation_session_id} started."

    def stop_label_session(self, *, apply_recalibration: bool = False) -> None:
        if self.active_annotation_session_id is None:
            self.status_message = "No active label session."
            return
        annotation_session_id = self.active_annotation_session_id
        label_count = self.storage.count_breathing_phase_labels(annotation_session_id)
        if label_count == 0:
            self.storage.delete_annotation_session(annotation_session_id)
            self.active_annotation_session_id = None
            self.status_message = f"Discarded empty label session {annotation_session_id}."
            return

        self.storage.close_annotation_session(annotation_session_id)
        self.active_annotation_session_id = None
        if apply_recalibration:
            self._apply_recalibration(annotation_session_id)
        else:
            self.status_message = f"Saved label session {annotation_session_id}."

    def reset_calibration(self) -> None:
        calibration = load_default_fusion_calibration()
        self.engine.set_calibration(calibration)
        self.candidate_history_by_name["learned_fusion"].clear()
        rebuilt = rebuild_learned_fusion_history(self._base_candidate_history(), calibration)
        self.candidate_history_by_name["learned_fusion"].extend(rebuilt)
        if rebuilt:
            self.current_estimates["learned_fusion"] = rebuilt[-1]
        else:
            self.current_estimates.pop("learned_fusion", None)
        if calibration.protocol_name != "default":
            self.status_message = f"Calibration reset to repo default ({calibration.protocol_name})."
        else:
            self.status_message = "Calibration reset to default fusion."

    def handle_keypress(self, key_name: str, *, now_ns: int | None = None) -> None:
        key = key_name.lower()
        if key not in KEY_BINDINGS:
            return
        if self.active_annotation_session_id is None:
            self.status_message = "Start a label session before recording turnarounds."
            return
        target_ns = now_ns or self.engine.latest_time_ns() or time.time_ns()
        phase_code, description, _ = KEY_BINDINGS[key]
        live_estimate = self.current_estimates.get("learned_fusion")
        self.storage.insert_breathing_phase_label(
            self.active_annotation_session_id,
            recorded_at_ns=target_ns,
            phase_code=phase_code,
            key_name=key.upper(),
            sensor_session_id=self.current_session_id,
            breathing_estimate_bpm=live_estimate.rate_bpm if live_estimate is not None else None,
            breathing_estimate_source=live_estimate.source if live_estimate is not None else None,
            breathing_estimate_time_ns=live_estimate.estimated_at_ns if live_estimate is not None else None,
            estimate_age_ms=(
                abs(target_ns - live_estimate.estimated_at_ns) / 1_000_000
                if live_estimate is not None
                else None
            ),
        )
        self.recent_labels.append((target_ns, key.upper(), phase_code))
        self.status_message = f"Recorded {key.upper()} -> {description}."

    def snapshot(self) -> CockpitSnapshot:
        rmssd = compute_rmssd_series(self.engine.recent_beats(lookback_seconds=GRAPH_WINDOW_SECONDS))
        candidate_history = {
            name: list(history)
            for name, history in self.candidate_history_by_name.items()
        }
        calibration = self.engine.calibration
        if calibration.version is not None:
            calibration_text = f"Calibration: v{calibration.version} ({calibration.protocol_name})"
        elif calibration.protocol_name != "default":
            calibration_text = f"Calibration: repo default ({calibration.protocol_name})"
        else:
            calibration_text = "Calibration: default fusion"
        session_text = (
            f"Session {self.current_session_id} on {self.current_device_name or 'unknown'}"
            if self.current_session_id is not None
            else "Session: idle"
        )
        label_text = (
            f"Label session {self.active_annotation_session_id}"
            if self.active_annotation_session_id is not None
            else "Labels: inactive"
        )
        return CockpitSnapshot(
            status_message=self.status_message,
            session_text=session_text,
            calibration_text=calibration_text,
            label_text=label_text,
            estimates=dict(self.current_estimates),
            ecg_points=self.engine.recent_ecg(lookback_seconds=GRAPH_WINDOW_SECONDS),
            acc_points=self.engine.recent_acc(lookback_seconds=GRAPH_WINDOW_SECONDS),
            respiratory_points=self.engine.respiratory_waveform(lookback_seconds=GRAPH_WINDOW_SECONDS),
            beats=self.engine.recent_beats(lookback_seconds=GRAPH_WINDOW_SECONDS),
            rmssd=rmssd,
            candidate_history=candidate_history,
            labels=list(self.recent_labels),
            latest_time_ns=self.engine.latest_time_ns(),
            active_annotation_session_id=self.active_annotation_session_id,
        )

    def _run_collector_worker(self) -> None:
        assert self.collector_worker is not None
        asyncio.run(self.collector_worker.run())

    def _handle_message(self, message: CollectorMessage) -> None:
        if message.kind == "connected":
            self.begin_live_session(
                message.payload["device_name"],
                message.payload["device_address"],
            )
            return
        if message.kind == "battery":
            if self.current_session_id is not None:
                self.storage.update_session_battery(
                    self.current_session_id,
                    int(message.payload["battery_percent"]),
                )
            return
        if message.kind == "event":
            if self.current_session_id is not None:
                self.storage.insert_event(
                    message.payload["event_type"],
                    message.payload["details"],
                    session_id=self.current_session_id,
                )
            return
        if message.kind == "warning":
            self.status_message = str(message.payload["message"])
            if self.current_session_id is not None:
                self.storage.insert_event(
                    "collector_warning",
                    {"message": self.status_message},
                    level="WARNING",
                    session_id=self.current_session_id,
                )
            return
        if message.kind == "status":
            self.status_message = str(message.payload["message"])
            return
        if message.kind == "disconnected":
            self.end_live_session()
            return
        if message.kind == "hr_frame":
            self.ingest_hr_frame(**message.payload)
            return
        if message.kind == "ecg_frame":
            self.ingest_ecg_frame(**message.payload)
            return
        if message.kind == "acc_frame":
            self.ingest_acc_frame(**message.payload)

    def _process_estimates(self, estimates: list[CandidateEstimate]) -> None:
        if self.current_session_id is None:
            return
        for estimate in estimates:
            self.storage.insert_breathing_candidate_estimate(
                self.current_session_id,
                estimated_at_ns=estimate.estimated_at_ns,
                candidate_name=estimate.source,
                breaths_per_min=estimate.rate_bpm,
                quality=estimate.quality,
                calibration_version=estimate.calibration_version,
            )
            self.candidate_history_by_name[estimate.source].append(estimate)
            self.current_estimates[estimate.source] = estimate
            if estimate.source == "learned_fusion":
                self.storage.insert_breathing_estimate(
                    self.current_session_id,
                    estimated_at_ns=estimate.estimated_at_ns,
                    breaths_per_min=estimate.rate_bpm,
                    window_seconds=self.engine.window_seconds,
                    source=estimate.source,
                )

    def _apply_recalibration(self, annotation_session_id: int) -> None:
        rows = list(
            self.storage.connection.execute(
                """
                SELECT recorded_at_ns, phase_code
                FROM breathing_phase_labels
                WHERE annotation_session_id = ?
                ORDER BY recorded_at_ns ASC
                """,
                (annotation_session_id,),
            ).fetchall()
        )
        calibration = fit_fusion_calibration(
            self._base_candidate_history(),
            rows,
            protocol_name=PROTOCOL_NAME,
            annotation_session_id=annotation_session_id,
            version=None,
            now_ns=time.time_ns(),
        )
        if not calibration.reliability_by_candidate:
            self.status_message = (
                f"Saved label session {annotation_session_id}, but there were not enough aligned "
                "points to recalibrate fusion."
            )
            return
        calibration_id = self.storage.insert_breathing_calibration(
            annotation_session_id=annotation_session_id,
            protocol_name=PROTOCOL_NAME,
            model=calibration.to_dict(),
        )
        calibration.version = calibration_id
        self.engine.set_calibration(calibration)
        rebuilt = rebuild_learned_fusion_history(self._base_candidate_history(), calibration)
        self.candidate_history_by_name["learned_fusion"].clear()
        self.candidate_history_by_name["learned_fusion"].extend(rebuilt)
        if rebuilt:
            self.current_estimates["learned_fusion"] = rebuilt[-1]
        self.status_message = (
            f"Saved label session {annotation_session_id} and applied calibration v{calibration_id}."
        )

    def _base_candidate_history(self) -> dict[str, list[CandidateEstimate]]:
        return {
            name: list(history)
            for name, history in self.candidate_history_by_name.items()
            if name != "learned_fusion"
        }


class BreathingCockpitApp:
    def __init__(self, db_path: Path | str = DEFAULT_DB_PATH, config: CockpitConfig | None = None) -> None:
        self.config = config or CockpitConfig(db_path=str(db_path))
        self.controller = CockpitController(Storage(self.config.db_path), self.config)

        self.root = tk.Tk()
        self.root.title("Polar Dash Breathing Cockpit")
        self.root.geometry("1320x860")
        self.root.minsize(1180, 760)
        self.root.configure(padx=14, pady=14)
        self.root.protocol("WM_DELETE_WINDOW", self.close)

        self.status_var = tk.StringVar(master=self.root, value="Starting...")
        self.session_var = tk.StringVar(master=self.root, value="Session: idle")
        self.calibration_var = tk.StringVar(master=self.root, value="Calibration: default fusion")
        self.label_var = tk.StringVar(master=self.root, value="Labels: inactive")
        self.estimate_vars = {
            name: tk.StringVar(master=self.root, value="--.-")
            for name in ("acc_pca", "ecg_qrs", "rr_interval", "learned_fusion")
        }
        self.quality_vars = {
            name: tk.StringVar(master=self.root, value="q --")
            for name in ("acc_pca", "ecg_qrs", "rr_interval", "learned_fusion")
        }

        self.graph: tk.Canvas
        self.recent_list: tk.Listbox
        self._build_ui()
        self._bind_keys()

        if self.config.auto_start:
            self.root.after(1, self.controller.start_collection)
        self.root.after(POLL_INTERVAL_MS, self._poll)

    def run(self) -> None:
        self.root.mainloop()

    def close(self) -> None:
        self.controller.close()
        self.root.destroy()

    def _build_ui(self) -> None:
        header = ttk.Frame(self.root)
        header.pack(fill="x")

        ttk.Label(header, text="Breathing Cockpit", font=("SF Pro Rounded", 26, "bold")).pack(
            anchor="w"
        )
        ttk.Label(
            header,
            text="Live Polar H10 cockpit with candidate breathing estimators, F/G turnaround labels, and batch recalibration.",
            foreground=MUTED_FG,
        ).pack(anchor="w", pady=(4, 10))

        controls = ttk.Frame(self.root)
        controls.pack(fill="x", pady=(0, 12))
        ttk.Button(controls, text="Start Collector", command=self.controller.start_collection).pack(side="left")
        ttk.Button(controls, text="Stop Collector", command=self.controller.stop_collection).pack(side="left", padx=(8, 0))
        ttk.Button(controls, text="Start Labels", command=self.controller.start_label_session).pack(side="left", padx=(16, 0))
        ttk.Button(
            controls,
            text="Stop && Recalibrate",
            command=lambda: self.controller.stop_label_session(apply_recalibration=True),
        ).pack(side="left", padx=(8, 0))
        ttk.Button(controls, text="Reset Calibration", command=self.controller.reset_calibration).pack(side="left", padx=(8, 0))

        meta = ttk.Frame(self.root)
        meta.pack(fill="x", pady=(0, 12))
        ttk.Label(meta, textvariable=self.session_var).pack(anchor="w")
        ttk.Label(meta, textvariable=self.calibration_var).pack(anchor="w")
        ttk.Label(meta, textvariable=self.label_var).pack(anchor="w")
        ttk.Label(meta, textvariable=self.status_var).pack(anchor="w", pady=(4, 0))

        estimates_frame = ttk.LabelFrame(self.root, text="Current Breathing Rates")
        estimates_frame.pack(fill="x", pady=(0, 12))
        for name in ("acc_pca", "ecg_qrs", "rr_interval", "learned_fusion"):
            card = ttk.Frame(estimates_frame)
            card.pack(side="left", fill="x", expand=True, padx=6, pady=6)
            ttk.Label(card, text=name.replace("_", " ").title()).pack(anchor="w")
            ttk.Label(card, textvariable=self.estimate_vars[name], font=("SF Pro Rounded", 22, "bold")).pack(anchor="w")
            ttk.Label(card, textvariable=self.quality_vars[name], foreground=MUTED_FG).pack(anchor="w")

        body = ttk.Frame(self.root)
        body.pack(fill="both", expand=True)
        content = ttk.Frame(body)
        content.pack(side="left", fill="both", expand=True)
        sidebar = ttk.Frame(body, width=280)
        sidebar.pack(side="left", fill="y", padx=(12, 0))
        sidebar.pack_propagate(False)

        self.graph = tk.Canvas(
            content,
            bg=SURFACE_BG,
            highlightthickness=1,
            highlightbackground=SURFACE_BORDER,
            width=980,
            height=680,
        )
        self.graph.pack(fill="both", expand=True)

        shortcuts_frame = ttk.LabelFrame(sidebar, text="Keyboard Shortcuts")
        shortcuts_frame.pack(fill="x", pady=(0, 12))
        ttk.Label(
            shortcuts_frame,
            text="F/G are active after you start a label session.",
            foreground=MUTED_FG,
        ).pack(anchor="w", padx=8, pady=(8, 6))
        for key_name, description in SHORTCUT_HINTS:
            row = ttk.Frame(shortcuts_frame)
            row.pack(fill="x", padx=8, pady=2)
            ttk.Label(
                row,
                text=key_name,
                width=5,
                font=("SF Pro Text", 11, "bold"),
            ).pack(side="left")
            ttk.Label(row, text=description, foreground=MUTED_FG).pack(side="left")

        recent_frame = ttk.LabelFrame(sidebar, text="Recent Labels")
        recent_frame.pack(fill="both", expand=True)
        self.recent_list = tk.Listbox(
            recent_frame,
            activestyle="none",
            bg=SURFACE_BG,
            fg=TEXT_FG,
            selectbackground=ACCENT,
            selectforeground="#ffffff",
            highlightthickness=1,
            highlightbackground=SURFACE_BORDER,
            borderwidth=0,
            font=("SF Pro Text", 12),
        )
        self.recent_list.pack(fill="both", expand=True, padx=8, pady=8)

    def _bind_keys(self) -> None:
        self.root.bind("<KeyPress-f>", lambda _event: self.controller.handle_keypress("f"))
        self.root.bind("<KeyPress-g>", lambda _event: self.controller.handle_keypress("g"))
        self.root.bind("<Escape>", lambda _event: self.close())

    def _poll(self) -> None:
        self.controller.drain_messages()
        self._apply_snapshot(self.controller.snapshot())
        self.root.after(POLL_INTERVAL_MS, self._poll)

    def _apply_snapshot(self, snapshot: CockpitSnapshot) -> None:
        self.status_var.set(snapshot.status_message)
        self.session_var.set(snapshot.session_text)
        self.calibration_var.set(snapshot.calibration_text)
        self.label_var.set(snapshot.label_text)

        for name in ("acc_pca", "ecg_qrs", "rr_interval", "learned_fusion"):
            estimate = snapshot.estimates.get(name)
            if estimate is None:
                self.estimate_vars[name].set("--.- br/min")
                self.quality_vars[name].set("q --")
            else:
                self.estimate_vars[name].set(f"{estimate.rate_bpm:0.1f} br/min")
                self.quality_vars[name].set(f"q {estimate.quality:0.2f}")

        self.recent_list.delete(0, tk.END)
        for timestamp_ns, key_name, phase_code in reversed(snapshot.labels[-20:]):
            self.recent_list.insert(
                tk.END,
                f"{time.strftime('%H:%M:%S', time.localtime(timestamp_ns / 1_000_000_000))}  {key_name}  {phase_code}",
            )

        self._render_graph(snapshot)

    def _render_graph(self, snapshot: CockpitSnapshot) -> None:
        self.graph.delete("all")
        width = max(int(self.graph.winfo_width()), 960)
        height = max(int(self.graph.winfo_height()), 640)
        lanes = [
            ("ECG", 0.06, 0.24),
            ("ACC / Resp", 0.28, 0.48),
            ("HR / RR / RMSSD", 0.52, 0.72),
            ("Breathing Candidates", 0.76, 0.96),
        ]
        latest_ns = snapshot.latest_time_ns or time.time_ns()
        x_start_ns = latest_ns - GRAPH_WINDOW_SECONDS * 1_000_000_000

        for title, top_fraction, bottom_fraction in lanes:
            top = int(height * top_fraction)
            bottom = int(height * bottom_fraction)
            self.graph.create_rectangle(48, top, width - 16, bottom, outline=SURFACE_BORDER)
            self.graph.create_text(58, top + 10, anchor="w", text=title, fill=TEXT_FG)

        self._draw_simple_series(snapshot.ecg_points, width, height, 0.06, 0.24, x_start_ns, latest_ns, color="#c1121f")
        self._draw_xyz_series(snapshot.acc_points, width, height, 0.28, 0.48, x_start_ns, latest_ns)
        self._draw_scalar_series(snapshot.respiratory_points, width, height, 0.28, 0.48, x_start_ns, latest_ns, color="#005f73")
        hr_points = [(timestamp_ns, 60_000.0 / rr_ms) for timestamp_ns, rr_ms in snapshot.beats if rr_ms > 0]
        rr_points = [(timestamp_ns, rr_ms) for timestamp_ns, rr_ms in snapshot.beats]
        self._draw_scalar_series(hr_points, width, height, 0.52, 0.72, x_start_ns, latest_ns, color="#c1121f")
        self._draw_scalar_series(rr_points, width, height, 0.52, 0.72, x_start_ns, latest_ns, color="#1d3557")
        self._draw_scalar_series(snapshot.rmssd, width, height, 0.52, 0.72, x_start_ns, latest_ns, color="#2a9d8f")

        for name, history in snapshot.candidate_history.items():
            points = [(estimate.estimated_at_ns, estimate.rate_bpm) for estimate in history]
            self._draw_scalar_series(
                points,
                width,
                height,
                0.76,
                0.96,
                x_start_ns,
                latest_ns,
                color=CANDIDATE_COLORS.get(name, ACCENT),
            )
        for timestamp_ns, key_name, _phase_code in snapshot.labels:
            if timestamp_ns < x_start_ns:
                continue
            x = self._scale_time(timestamp_ns, x_start_ns, latest_ns, width)
            self.graph.create_line(x, int(height * 0.06), x, int(height * 0.96), fill="#7c7c7c", dash=(3, 5))
            self.graph.create_text(x + 4, int(height * 0.76) + 14, anchor="w", text=key_name, fill="#333333")

    def _draw_simple_series(
        self,
        points: list[tuple[int, float]],
        width: int,
        height: int,
        top_fraction: float,
        bottom_fraction: float,
        x_start_ns: int,
        x_end_ns: int,
        *,
        color: str,
    ) -> None:
        self._draw_scalar_series(points, width, height, top_fraction, bottom_fraction, x_start_ns, x_end_ns, color=color)

    def _draw_xyz_series(
        self,
        points: list[tuple[int, float, float, float]],
        width: int,
        height: int,
        top_fraction: float,
        bottom_fraction: float,
        x_start_ns: int,
        x_end_ns: int,
    ) -> None:
        for index, color in enumerate(("#999999", "#bbbbbb", "#dddddd")):
            series = [(timestamp_ns, values[index]) for timestamp_ns, *values in points]
            self._draw_scalar_series(
                series,
                width,
                height,
                top_fraction,
                bottom_fraction,
                x_start_ns,
                x_end_ns,
                color=color,
            )

    def _draw_scalar_series(
        self,
        points: list[tuple[int, float]],
        width: int,
        height: int,
        top_fraction: float,
        bottom_fraction: float,
        x_start_ns: int,
        x_end_ns: int,
        *,
        color: str,
    ) -> None:
        visible = [(timestamp_ns, value) for timestamp_ns, value in points if timestamp_ns >= x_start_ns]
        if len(visible) < 2:
            return
        lane_top = int(height * top_fraction) + 22
        lane_bottom = int(height * bottom_fraction) - 10
        values = [value for _, value in visible]
        min_value = min(values)
        max_value = max(values)
        value_range = max(max_value - min_value, 1e-6)

        scaled_points: list[float] = []
        for timestamp_ns, value in visible:
            x = self._scale_time(timestamp_ns, x_start_ns, x_end_ns, width)
            normalized = (value - min_value) / value_range
            y = lane_bottom - normalized * (lane_bottom - lane_top)
            scaled_points.extend((x, y))
        self.graph.create_line(*scaled_points, fill=color, width=1.6, smooth=True)

    def _scale_time(
        self,
        timestamp_ns: int,
        x_start_ns: int,
        x_end_ns: int,
        width: int,
    ) -> float:
        usable_width = width - 64
        duration_ns = max(x_end_ns - x_start_ns, 1)
        return 48 + ((timestamp_ns - x_start_ns) / duration_ns) * usable_width


def run_cockpit(
    db_path: str | Path = DEFAULT_DB_PATH,
    *,
    prefix: str = "Polar H10",
    scan_timeout: float = 10.0,
    reconnect_delay: float = 3.0,
) -> None:
    app = BreathingCockpitApp(
        db_path=db_path,
        config=CockpitConfig(
            device_name_prefix=prefix,
            db_path=str(Path(db_path).expanduser().resolve()),
            scan_timeout=scan_timeout,
            reconnect_delay=reconnect_delay,
            auto_start=True,
        ),
    )
    app.run()
