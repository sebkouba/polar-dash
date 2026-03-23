from __future__ import annotations

import asyncio
import contextlib
import logging
from dataclasses import dataclass
from typing import Any

from bleak import BleakClient, BleakScanner
from bleakheart import BatteryLevel, HeartRate, PolarMeasurementData

from polar_dash.storage import DEFAULT_DB_PATH, Storage

logger = logging.getLogger(__name__)

ECG_SAMPLE_RATE_HZ = 130
ACC_SAMPLE_RATE_HZ = 200


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
        while True:
            _measurement, sensor_recorded_at_ns, samples = await queue.get()
            self.storage.insert_acc_frame(
                session_id=session_id,
                sensor_recorded_at_ns=sensor_recorded_at_ns,
                sample_rate_hz=ACC_SAMPLE_RATE_HZ,
                samples=samples,
            )


async def run_collection(config: CollectorConfig) -> None:
    collector = PolarCollector(config)
    await collector.run()
