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


@dataclass(slots=True)
class BreathingCandidate:
    rate_bpm: float
    quality: float
    source: str


class RollingBreathingEstimator:
    def __init__(self, window_seconds: int = 45, step_seconds: int = 5) -> None:
        self.window_seconds = window_seconds
        self.window_ns = window_seconds * 1_000_000_000
        self.step_ns = step_seconds * 1_000_000_000
        self.acc_samples: deque[tuple[int, float, float, float]] = deque()
        self.ecg_samples: deque[tuple[int, float]] = deque()
        self.last_estimate_at_ns: int | None = None
        self.previous_rate_bpm: float | None = None

    def add_acc_frame(
        self,
        sensor_recorded_at_ns: int,
        sample_rate_hz: int,
        samples: list[tuple[int, int, int]],
    ) -> tuple[int, float, str] | None:
        step_ns = int(1_000_000_000 / sample_rate_hz)
        start_ns = sensor_recorded_at_ns - step_ns * (len(samples) - 1)
        for offset, sample in enumerate(samples):
            timestamp_ns = start_ns + offset * step_ns
            self.acc_samples.append(
                (timestamp_ns, float(sample[0]), float(sample[1]), float(sample[2]))
            )
        return self._maybe_estimate(sensor_recorded_at_ns)

    def add_ecg_frame(
        self,
        sensor_recorded_at_ns: int,
        sample_rate_hz: int,
        samples: list[int],
    ) -> tuple[int, float, str] | None:
        step_ns = int(1_000_000_000 / sample_rate_hz)
        start_ns = sensor_recorded_at_ns - step_ns * (len(samples) - 1)
        for offset, sample in enumerate(samples):
            timestamp_ns = start_ns + offset * step_ns
            self.ecg_samples.append((timestamp_ns, float(sample)))
        return self._maybe_estimate(sensor_recorded_at_ns)

    def _maybe_estimate(self, sensor_recorded_at_ns: int) -> tuple[int, float, str] | None:
        cutoff_ns = sensor_recorded_at_ns - self.window_ns - self.step_ns
        while self.acc_samples and self.acc_samples[0][0] < cutoff_ns:
            self.acc_samples.popleft()
        while self.ecg_samples and self.ecg_samples[0][0] < cutoff_ns:
            self.ecg_samples.popleft()

        enough_acc = (
            bool(self.acc_samples)
            and sensor_recorded_at_ns - self.acc_samples[0][0] >= self.window_ns
        )
        enough_ecg = (
            bool(self.ecg_samples)
            and sensor_recorded_at_ns - self.ecg_samples[0][0] >= self.window_ns
        )
        if not enough_acc and not enough_ecg:
            return None
        if (
            self.last_estimate_at_ns is not None
            and sensor_recorded_at_ns - self.last_estimate_at_ns < self.step_ns
        ):
            return None

        estimate = self._fused_estimate(sensor_recorded_at_ns)
        if estimate is None or estimate.quality <= 0:
            return None
        self.last_estimate_at_ns = sensor_recorded_at_ns
        self.previous_rate_bpm = estimate.rate_bpm
        return sensor_recorded_at_ns, estimate.rate_bpm, estimate.source

    def _fused_estimate(self, end_ns: int) -> BreathingCandidate | None:
        candidates = [
            candidate
            for candidate in (
                self._estimate_acc_candidate(end_ns),
                self._estimate_ecg_candidate(end_ns),
            )
            if candidate is not None
        ]
        if not candidates:
            return None

        if len(candidates) == 1:
            return self._smoothed_candidate(candidates[0])

        rates = np.array([candidate.rate_bpm for candidate in candidates], dtype=float)
        qualities = np.array([candidate.quality for candidate in candidates], dtype=float)

        if np.std(rates) <= 3.0:
            fused_rate = float(np.average(rates, weights=qualities))
            return self._smoothed_candidate(
                BreathingCandidate(
                    rate_bpm=fused_rate,
                    quality=float(np.mean(qualities)),
                    source="fusion",
                )
            )

        if self.previous_rate_bpm is None:
            best = candidates[int(np.argmax(qualities))]
            return self._smoothed_candidate(best)

        scored = [
            (
                candidate.quality / (1.0 + abs(candidate.rate_bpm - self.previous_rate_bpm) / 4.0),
                candidate,
            )
            for candidate in candidates
        ]
        best = max(scored, key=lambda item: item[0])[1]
        return self._smoothed_candidate(best)

    def _smoothed_candidate(self, candidate: BreathingCandidate) -> BreathingCandidate:
        if self.previous_rate_bpm is None:
            return candidate

        alpha = 0.45 if candidate.source == "fusion" else 0.30
        smoothed_rate = alpha * candidate.rate_bpm + (1.0 - alpha) * self.previous_rate_bpm
        return BreathingCandidate(
            rate_bpm=float(smoothed_rate),
            quality=candidate.quality,
            source=candidate.source,
        )

    def _estimate_acc_candidate(self, end_ns: int) -> BreathingCandidate | None:
        samples = [
            sample
            for sample in self.acc_samples
            if sample[0] >= end_ns - self.window_ns
        ]
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
        lowpassed = signal.sosfiltfilt(
            signal.butter(2, 1.0, btype="lowpass", fs=sample_rate_hz, output="sos"),
            principal_signal,
        )
        reduced = signal.resample_poly(lowpassed, up=1, down=8)
        return self._estimate_waveform_candidate(
            reduced,
            sample_rate_hz / 8,
            source="acc-pca",
        )

    def _estimate_ecg_candidate(self, end_ns: int) -> BreathingCandidate | None:
        samples = [
            sample
            for sample in self.ecg_samples
            if sample[0] >= end_ns - self.window_ns
        ]
        if len(samples) < ECG_SAMPLE_RATE_HZ * 20:
            return None

        timestamps = np.array([sample[0] for sample in samples], dtype=np.int64)
        ecg = np.array([sample[1] for sample in samples], dtype=float)
        sample_spacing = np.diff(timestamps)
        if len(sample_spacing) == 0:
            return None

        sample_rate_hz = round(1_000_000_000 / np.median(sample_spacing))
        if sample_rate_hz < 60:
            return None

        qrs_band = signal.sosfiltfilt(
            signal.butter(2, [5.0, 20.0], btype="bandpass", fs=sample_rate_hz, output="sos"),
            ecg,
        )
        derivative = np.gradient(qrs_band)
        envelope = np.abs(derivative)
        peaks, _ = signal.find_peaks(
            envelope,
            distance=int(sample_rate_hz * 0.35),
            prominence=max(np.std(envelope) * 0.5, np.percentile(envelope, 75) * 0.1),
        )
        if len(peaks) < 10:
            return None

        features: list[float] = []
        feature_times: list[int] = []
        half_window = int(sample_rate_hz * 0.05)
        for peak in peaks:
            start = max(0, peak - half_window)
            end = min(len(derivative), peak + half_window)
            segment = derivative[start:end]
            if len(segment) == 0:
                continue
            features.append(float(segment.max() - segment.min()))
            feature_times.append(int(timestamps[peak]))

        if len(features) < 10:
            return None

        feature_times_np = np.array(feature_times, dtype=np.int64)
        feature_values = np.array(features, dtype=float)
        uniform_times = np.arange(
            feature_times_np[0],
            feature_times_np[-1],
            int(1_000_000_000 / 4),
        )
        if len(uniform_times) < 20:
            return None

        interpolated = np.interp(uniform_times, feature_times_np, feature_values)
        return self._estimate_waveform_candidate(
            interpolated,
            4.0,
            source="ecg-qrs-slope",
        )

    def _estimate_waveform_candidate(
        self,
        waveform: np.ndarray,
        sample_rate_hz: float,
        *,
        source: str,
    ) -> BreathingCandidate | None:
        if len(waveform) < sample_rate_hz * 10:
            return None

        try:
            filtered = signal.sosfiltfilt(
                signal.butter(2, [0.08, 0.70], btype="bandpass", fs=sample_rate_hz, output="sos"),
                signal.detrend(waveform),
            )
        except ValueError:
            return None

        frequencies, power = signal.welch(
            filtered,
            fs=sample_rate_hz,
            nperseg=min(len(filtered), int(sample_rate_hz * self.window_seconds)),
        )
        band = (frequencies >= 0.08) & (frequencies <= 0.70)
        if not np.any(band):
            return None

        band_frequencies = frequencies[band]
        band_power = power[band]
        peak_index = int(np.argmax(band_power))
        spectral_rate_bpm = float(band_frequencies[peak_index] * 60.0)
        spectral_quality = float(np.log1p(band_power[peak_index] / np.median(band_power)))

        autocorr = np.correlate(filtered, filtered, mode="full")[len(filtered) - 1 :]
        if autocorr[0] == 0:
            return None
        autocorr = autocorr / autocorr[0]
        min_lag = max(1, int(sample_rate_hz * 60 / 30))
        max_lag = max(min_lag + 1, int(sample_rate_hz * 60 / 6))
        if max_lag >= len(autocorr):
            return None

        autocorr_segment = autocorr[min_lag : max_lag + 1]
        autocorr_peak = int(np.argmax(autocorr_segment)) + min_lag
        autocorr_rate_bpm = float(60.0 / (autocorr_peak / sample_rate_hz))
        autocorr_quality = float(max(autocorr[autocorr_peak], 0.0))

        rate_gap = abs(spectral_rate_bpm - autocorr_rate_bpm)
        if rate_gap <= 3.0:
            rate_bpm = float(
                np.average(
                    [spectral_rate_bpm, autocorr_rate_bpm],
                    weights=[max(spectral_quality, 0.1), max(autocorr_quality, 0.1)],
                )
            )
            quality = spectral_quality * (0.5 + autocorr_quality)
        else:
            rate_bpm = spectral_rate_bpm
            quality = spectral_quality * 0.35

        return BreathingCandidate(rate_bpm=rate_bpm, quality=float(quality), source=source)


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
            breathing_estimator = RollingBreathingEstimator()
            pmd = PolarMeasurementData(
                client,
                ecg_queue=ecg_queue if self.config.capture_ecg else None,
                acc_queue=acc_queue if self.config.capture_acc else None,
                callback=lambda _payload: None,
            )
            consumer_tasks = [
                asyncio.create_task(self._consume_hr_frames(session_id, hr_queue)),
                asyncio.create_task(
                    self._consume_ecg_frames(session_id, ecg_queue, breathing_estimator)
                ),
                asyncio.create_task(
                    self._consume_acc_frames(session_id, acc_queue, breathing_estimator)
                ),
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
        breathing_estimator: RollingBreathingEstimator,
    ) -> None:
        while True:
            _measurement, sensor_recorded_at_ns, samples = await queue.get()
            self.storage.insert_ecg_frame(
                session_id=session_id,
                sensor_recorded_at_ns=sensor_recorded_at_ns,
                sample_rate_hz=ECG_SAMPLE_RATE_HZ,
                samples=samples,
            )
            estimate = breathing_estimator.add_ecg_frame(
                sensor_recorded_at_ns=sensor_recorded_at_ns,
                sample_rate_hz=ECG_SAMPLE_RATE_HZ,
                samples=samples,
            )
            if estimate is not None:
                estimated_at_ns, breaths_per_min, source = estimate
                self.storage.insert_breathing_estimate(
                    session_id=session_id,
                    estimated_at_ns=estimated_at_ns,
                    breaths_per_min=breaths_per_min,
                    window_seconds=breathing_estimator.window_seconds,
                    source=source,
                )

    async def _consume_acc_frames(
        self,
        session_id: int,
        queue: asyncio.Queue[tuple[str, int, list[tuple[int, int, int]]]],
        breathing_estimator: RollingBreathingEstimator,
    ) -> None:
        while True:
            _measurement, sensor_recorded_at_ns, samples = await queue.get()
            self.storage.insert_acc_frame(
                session_id=session_id,
                sensor_recorded_at_ns=sensor_recorded_at_ns,
                sample_rate_hz=ACC_SAMPLE_RATE_HZ,
                samples=samples,
            )
            estimate = breathing_estimator.add_acc_frame(
                sensor_recorded_at_ns=sensor_recorded_at_ns,
                sample_rate_hz=ACC_SAMPLE_RATE_HZ,
                samples=samples,
            )
            if estimate is not None:
                estimated_at_ns, breaths_per_min, source = estimate
                self.storage.insert_breathing_estimate(
                    session_id=session_id,
                    estimated_at_ns=estimated_at_ns,
                    breaths_per_min=breaths_per_min,
                    window_seconds=breathing_estimator.window_seconds,
                    source=source,
                )


async def run_collection(config: CollectorConfig) -> None:
    collector = PolarCollector(config)
    await collector.run()


def backfill_breathing_estimates(db_path: str | Path = DEFAULT_DB_PATH) -> int:
    storage = Storage(db_path)
    estimator = RollingBreathingEstimator()
    current_session: int | None = None
    inserted = 0

    storage.connection.execute("DELETE FROM breathing_estimates")
    storage.connection.commit()

    rows = storage.connection.execute(
        """
        SELECT session_id, sensor_recorded_at_ns, sample_rate_hz, samples_json, 'ACC' AS kind
        FROM acc_frames
        UNION ALL
        SELECT session_id, sensor_recorded_at_ns, sample_rate_hz, samples_json, 'ECG' AS kind
        FROM ecg_frames
        ORDER BY session_id, sensor_recorded_at_ns
        """
    ).fetchall()

    for row in rows:
        session_id = int(row["session_id"])
        if current_session != session_id:
            estimator = RollingBreathingEstimator()
            current_session = session_id

        if row["kind"] == "ACC":
            samples = [tuple(sample) for sample in json.loads(row["samples_json"])]
            estimate = estimator.add_acc_frame(
                sensor_recorded_at_ns=int(row["sensor_recorded_at_ns"]),
                sample_rate_hz=int(row["sample_rate_hz"]),
                samples=samples,
            )
        else:
            samples = [int(sample) for sample in json.loads(row["samples_json"])]
            estimate = estimator.add_ecg_frame(
                sensor_recorded_at_ns=int(row["sensor_recorded_at_ns"]),
                sample_rate_hz=int(row["sample_rate_hz"]),
                samples=samples,
            )
        if estimate is None:
            continue
        estimated_at_ns, breaths_per_min, source = estimate
        storage.insert_breathing_estimate(
            session_id=session_id,
            estimated_at_ns=estimated_at_ns,
            breaths_per_min=breaths_per_min,
            window_seconds=estimator.window_seconds,
            source=source,
        )
        inserted += 1

    storage.close()
    return inserted
