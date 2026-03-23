from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import sqlite3
from collections import deque
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import numpy as np
from bleak import BleakClient, BleakScanner
from bleakheart import BatteryLevel, HeartRate, PolarMeasurementData
from scipy import signal

from polar_dash.storage import DEFAULT_DB_PATH, Storage

logger = logging.getLogger(__name__)

ECG_SAMPLE_RATE_HZ = 130
ACC_SAMPLE_RATE_HZ = 200


class RollingBreathingEstimator:
    def __init__(self, window_seconds: int = 45, step_seconds: int = 5) -> None:
        self.window_seconds = window_seconds
        self.window_ns = window_seconds * 1_000_000_000
        self.step_ns = step_seconds * 1_000_000_000
        self.samples: deque[tuple[int, float, float, float]] = deque()
        self.last_estimate_at_ns: int | None = None

    def add_frame(
        self,
        sensor_recorded_at_ns: int,
        sample_rate_hz: int,
        samples: list[tuple[int, int, int]],
    ) -> tuple[int, float] | None:
        step_ns = int(1_000_000_000 / sample_rate_hz)
        start_ns = sensor_recorded_at_ns - step_ns * (len(samples) - 1)
        for offset, sample in enumerate(samples):
            timestamp_ns = start_ns + offset * step_ns
            self.samples.append(
                (timestamp_ns, float(sample[0]), float(sample[1]), float(sample[2]))
            )

        cutoff_ns = sensor_recorded_at_ns - self.window_ns - self.step_ns
        while self.samples and self.samples[0][0] < cutoff_ns:
            self.samples.popleft()

        if not self.samples or sensor_recorded_at_ns - self.samples[0][0] < self.window_ns:
            return None
        if (
            self.last_estimate_at_ns is not None
            and sensor_recorded_at_ns - self.last_estimate_at_ns < self.step_ns
        ):
            return None

        estimate = self._estimate_from_samples(list(self.samples))
        if estimate is None:
            return None
        self.last_estimate_at_ns = sensor_recorded_at_ns
        return sensor_recorded_at_ns, estimate

    def _estimate_from_samples(
        self,
        samples: list[tuple[int, float, float, float]],
    ) -> float | None:
        if len(samples) < 200:
            return None

        timestamps = np.array([sample[0] for sample in samples], dtype=np.int64)
        xyz = np.array([sample[1:] for sample in samples], dtype=float)
        sample_spacing = np.diff(timestamps)
        if len(sample_spacing) == 0:
            return None

        sample_rate_hz = round(1_000_000_000 / np.median(sample_spacing))
        if sample_rate_hz < 20:
            return None

        centered = xyz - xyz.mean(axis=0, keepdims=True)
        covariance = np.cov(centered, rowvar=False)
        eigenvalues, eigenvectors = np.linalg.eigh(covariance)
        principal_signal = centered @ eigenvectors[:, int(np.argmax(eigenvalues))]
        principal_signal = signal.detrend(principal_signal)

        try:
            sos = signal.butter(
                2,
                [0.10, 0.70],
                btype="bandpass",
                fs=sample_rate_hz,
                output="sos",
            )
            filtered = signal.sosfiltfilt(sos, principal_signal)
        except ValueError:
            return None

        frequencies, power = signal.welch(
            filtered,
            fs=sample_rate_hz,
            nperseg=min(len(filtered), int(sample_rate_hz * self.window_seconds)),
        )
        band = (frequencies >= 0.10) & (frequencies <= 0.70)
        if not np.any(band):
            return None
        peak_frequency = frequencies[band][int(np.argmax(power[band]))]
        return float(peak_frequency * 60.0)


@dataclass(slots=True)
class CollectorConfig:
    device_name_prefix: str = "Polar H10"
    db_path: str = str(DEFAULT_DB_PATH)
    scan_timeout: float = 10.0
    reconnect_delay: float = 3.0
    capture_ecg: bool = True
    capture_acc: bool = True
    once: bool = False


async def scan_for_devices(prefix: str, timeout: float) -> list[dict[str, Any]]:
    devices = await _discover_matching_devices(prefix, timeout)
    matches: list[dict[str, Any]] = []
    for device in devices:
        matches.append(
            {
                "name": device.name or "",
                "address": device.address,
                "rssi": getattr(device, "rssi", None),
            }
        )
    matches.sort(key=lambda item: (item["name"], item["address"]))
    return matches


async def _discover_matching_devices(prefix: str, timeout: float) -> list[Any]:
    devices = await BleakScanner.discover(timeout=timeout)
    prefix_lower = prefix.lower()
    matches: list[Any] = []
    for device in devices:
        name = device.name or ""
        if prefix_lower in name.lower():
            matches.append(device)
    matches.sort(key=lambda item: ((item.name or ""), item.address))
    return matches


class PolarCollector:
    def __init__(self, config: CollectorConfig) -> None:
        self.config = config
        self.storage = Storage(config.db_path)
        self._stop_event = asyncio.Event()

    def request_stop(self) -> None:
        self._stop_event.set()

    async def run(self) -> None:
        try:
            while not self._stop_event.is_set():
                device = await self._find_device()
                if device is None:
                    message = (
                        f"No BLE device found matching "
                        f"{self.config.device_name_prefix!r}"
                    )
                    if self.config.once:
                        raise RuntimeError(message)
                    logger.info("%s; retrying in %.1fs", message, self.config.reconnect_delay)
                    await self._sleep_or_stop(self.config.reconnect_delay)
                    continue
                try:
                    await self._collect_session(device)
                    if self.config.once:
                        return
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    if self.config.once:
                        raise
                    logger.warning("Collection error: %s", exc, exc_info=True)
                    await self._sleep_or_stop(self.config.reconnect_delay)
        finally:
            self.storage.close()

    async def _sleep_or_stop(self, delay: float) -> None:
        try:
            await asyncio.wait_for(self._stop_event.wait(), timeout=delay)
        except asyncio.TimeoutError:
            return

    async def _find_device(self) -> Any | None:
        matches = await _discover_matching_devices(
            self.config.device_name_prefix,
            self.config.scan_timeout,
        )
        if not matches:
            return None
        logger.info("Using %s (%s)", matches[0].name or "", matches[0].address)
        return matches[0]

    async def _collect_session(self, device: Any) -> None:
        device_name = device.name or self.config.device_name_prefix
        disconnect_event = asyncio.Event()
        loop = asyncio.get_running_loop()

        def handle_disconnect(_: BleakClient) -> None:
            loop.call_soon_threadsafe(disconnect_event.set)

        logger.info("Connecting to %s (%s)", device_name, device.address)
        async with BleakClient(device, disconnected_callback=handle_disconnect) as client:
            session_id = self.storage.start_session(device_name, device.address)
            self.storage.insert_event(
                "collector_connected",
                {"device_name": device_name, "device_address": device.address},
                session_id=session_id,
            )

            try:
                battery_percent = await BatteryLevel(client).read()
            except Exception as exc:
                logger.warning("Battery read failed: %s", exc)
                self.storage.insert_event(
                    "battery_read_failed",
                    {"error": str(exc)},
                    level="WARNING",
                    session_id=session_id,
                )
            else:
                logger.info("Battery %s%%", battery_percent)
                self.storage.update_session_battery(session_id, battery_percent)

            hr_queue: asyncio.Queue[tuple[str, int, tuple[float, list[int]], int | None]] = asyncio.Queue()
            ecg_queue: asyncio.Queue[tuple[str, int, list[int]]] = asyncio.Queue()
            acc_queue: asyncio.Queue[tuple[str, int, list[tuple[int, int, int]]]] = asyncio.Queue()

            hr_monitor = HeartRate(client, queue=hr_queue, unpack=False)
            pmd = PolarMeasurementData(
                client,
                ecg_queue=ecg_queue if self.config.capture_ecg else None,
                acc_queue=acc_queue if self.config.capture_acc else None,
                callback=lambda _payload: None,
            )
            consumer_tasks = [
                asyncio.create_task(self._consume_hr_frames(session_id, hr_queue)),
                asyncio.create_task(self._consume_ecg_frames(session_id, ecg_queue)),
                asyncio.create_task(self._consume_acc_frames(session_id, acc_queue)),
            ]
            started_streams: list[str] = []

            try:
                await hr_monitor.start_notify()
                self.storage.insert_event("hr_notify_started", {}, session_id=session_id)

                available_measurements = await pmd.available_measurements()
                self.storage.insert_event(
                    "pmd_measurements",
                    {"available": available_measurements},
                    session_id=session_id,
                )

                if self.config.capture_ecg and "ECG" in available_measurements:
                    await self._start_stream(pmd, session_id, "ECG", started_streams)
                if self.config.capture_acc and "ACC" in available_measurements:
                    await self._start_stream(pmd, session_id, "ACC", started_streams)

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
                self.storage.insert_event("collector_disconnected", {}, session_id=session_id)
                self.storage.close_session(session_id)
                logger.info("Session %s closed", session_id)

    async def _start_stream(
        self,
        pmd: PolarMeasurementData,
        session_id: int,
        measurement: str,
        started_streams: list[str],
    ) -> None:
        error_code, message, _ = await pmd.start_streaming(measurement)
        if error_code == 0:
            started_streams.append(measurement)
            self.storage.insert_event(
                "stream_started",
                {"measurement": measurement},
                session_id=session_id,
            )
            logger.info("%s streaming started", measurement)
            return
        self.storage.insert_event(
            "stream_start_failed",
            {"measurement": measurement, "error_code": error_code, "message": message},
            level="WARNING",
            session_id=session_id,
        )
        logger.warning("Unable to start %s stream: %s", measurement, message)

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
        if disconnect_task in done and disconnect_event.is_set():
            logger.warning("BLE connection lost")

    async def _consume_hr_frames(
        self,
        session_id: int,
        queue: asyncio.Queue[tuple[str, int, tuple[float, list[int]], int | None]],
    ) -> None:
        while True:
            _measurement, recorded_at_ns, payload, energy_kj = await queue.get()
            average_hr_bpm, rr_intervals_ms = payload
            self.storage.insert_hr_frame(
                session_id=session_id,
                recorded_at_ns=recorded_at_ns,
                average_hr_bpm=average_hr_bpm,
                rr_intervals_ms=rr_intervals_ms,
                energy_kj=energy_kj,
            )

    async def _consume_ecg_frames(
        self,
        session_id: int,
        queue: asyncio.Queue[tuple[str, int, list[int]]],
    ) -> None:
        while True:
            _measurement, sensor_recorded_at_ns, samples = await queue.get()
            self.storage.insert_ecg_frame(
                session_id=session_id,
                sensor_recorded_at_ns=sensor_recorded_at_ns,
                sample_rate_hz=ECG_SAMPLE_RATE_HZ,
                samples=samples,
            )

    async def _consume_acc_frames(
        self,
        session_id: int,
        queue: asyncio.Queue[tuple[str, int, list[tuple[int, int, int]]]],
    ) -> None:
        breathing_estimator = RollingBreathingEstimator()
        while True:
            _measurement, sensor_recorded_at_ns, samples = await queue.get()
            self.storage.insert_acc_frame(
                session_id=session_id,
                sensor_recorded_at_ns=sensor_recorded_at_ns,
                sample_rate_hz=ACC_SAMPLE_RATE_HZ,
                samples=samples,
            )
            estimate = breathing_estimator.add_frame(
                sensor_recorded_at_ns=sensor_recorded_at_ns,
                sample_rate_hz=ACC_SAMPLE_RATE_HZ,
                samples=samples,
            )
            if estimate is not None:
                estimated_at_ns, breaths_per_min = estimate
                self.storage.insert_breathing_estimate(
                    session_id=session_id,
                    estimated_at_ns=estimated_at_ns,
                    breaths_per_min=breaths_per_min,
                    window_seconds=breathing_estimator.window_seconds,
                )


async def run_collection(config: CollectorConfig) -> None:
    collector = PolarCollector(config)
    await collector.run()


def backfill_breathing_estimates(db_path: str | Path = DEFAULT_DB_PATH) -> int:
    storage = Storage(db_path)
    estimator = RollingBreathingEstimator()
    current_session: int | None = None
    inserted = 0

    rows = storage.connection.execute(
        """
        SELECT session_id, sensor_recorded_at_ns, sample_rate_hz, samples_json
        FROM acc_frames
        ORDER BY session_id, sensor_recorded_at_ns
        """
    ).fetchall()

    for row in rows:
        session_id = int(row["session_id"])
        if current_session != session_id:
            estimator = RollingBreathingEstimator()
            current_session = session_id

        samples = [tuple(sample) for sample in json.loads(row["samples_json"])]
        estimate = estimator.add_frame(
            sensor_recorded_at_ns=int(row["sensor_recorded_at_ns"]),
            sample_rate_hz=int(row["sample_rate_hz"]),
            samples=samples,
        )
        if estimate is None:
            continue
        estimated_at_ns, breaths_per_min = estimate
        storage.insert_breathing_estimate(
            session_id=session_id,
            estimated_at_ns=estimated_at_ns,
            breaths_per_min=breaths_per_min,
            window_seconds=estimator.window_seconds,
            source="acc-backfill",
        )
        inserted += 1

    storage.close()
    return inserted
