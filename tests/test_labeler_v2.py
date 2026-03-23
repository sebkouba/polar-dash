from __future__ import annotations

import math
import tempfile
import time
import unittest
from pathlib import Path

from polar_dash.labeler_v2 import LabelerController
from polar_dash.storage import Storage


class LabelerControllerTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory()
        self.db_path = Path(self.tempdir.name) / "labeler_v2.db"

    def tearDown(self) -> None:
        self.tempdir.cleanup()

    def test_start_requires_recent_live_acc_frames(self) -> None:
        storage = Storage(self.db_path)
        now_ns = time.time_ns()
        self._insert_sensor_session(
            storage,
            started_at_ns=now_ns - 120_000_000_000,
            frame_end_ns=now_ns - 40_000_000_000,
            ended_at_ns=None,
        )

        controller = LabelerController(storage)
        controller.start_session(now_ns=now_ns)

        self.assertIsNone(controller.active_annotation_session_id)
        self.assertIn("No live sensor stream", controller.status_message)
        controller.close()

    def test_live_recording_flow_and_saved_delete(self) -> None:
        storage = Storage(self.db_path)
        now_ns = time.time_ns()
        sensor_session_id = self._insert_sensor_session(
            storage,
            started_at_ns=now_ns - 25_000_000_000,
            frame_end_ns=now_ns,
            ended_at_ns=None,
        )
        storage.insert_breathing_estimate(
            sensor_session_id,
            estimated_at_ns=now_ns - 1_000_000_000,
            breaths_per_min=9.2,
            window_seconds=30,
            source="fusion",
        )
        old_saved_id = storage.start_annotation_session(
            "old-bad-session",
            protocol_name="breathing_phase_keys_v1",
            linked_session_id=sensor_session_id,
        )
        storage.connection.execute(
            """
            UPDATE annotation_sessions
            SET started_at_ns = ?, ended_at_ns = ?
            WHERE id = ?
            """,
            (now_ns - 20_000_000_000, now_ns - 10_000_000_000, old_saved_id),
        )
        storage.insert_breathing_phase_label(
            old_saved_id,
            recorded_at_ns=now_ns - 12_000_000_000,
            phase_code="exhale_start",
            key_name="J",
            sensor_session_id=sensor_session_id,
            breathing_estimate_bpm=9.2,
            breathing_estimate_source="fusion",
            breathing_estimate_time_ns=now_ns - 12_000_000_000,
            estimate_age_ms=0.0,
        )

        controller = LabelerController(storage, name="focused-live-run")
        controller.start_session(now_ns=now_ns)
        controller.record_phase_key("h", now_ns=now_ns - 500_000_000)
        controller.record_phase_key("j", now_ns=now_ns - 100_000_000)
        view = controller.snapshot(now_ns=now_ns)

        self.assertEqual(view.mode, "recording")
        self.assertEqual(view.displayed_sensor_session_id, sensor_session_id)
        self.assertEqual([marker.key_name for marker in view.waveform_markers[-2:]], ["H", "J"])
        self.assertEqual([label.key_name for label in view.recent_labels[:2]], ["J", "H"])

        controller.stop_session()
        new_saved_id = controller.review_annotation_session_id
        self.assertIsNotNone(new_saved_id)

        controller.select_saved_session(old_saved_id)
        controller.delete_selected_saved_session()
        remaining_ids = [
            int(row["id"])
            for row in storage.connection.execute(
                "SELECT id FROM annotation_sessions ORDER BY id"
            ).fetchall()
        ]
        self.assertEqual(remaining_ids, [int(new_saved_id)])
        controller.close()

    def test_empty_session_is_discarded_on_stop(self) -> None:
        storage = Storage(self.db_path)
        now_ns = time.time_ns()
        self._insert_sensor_session(
            storage,
            started_at_ns=now_ns - 20_000_000_000,
            frame_end_ns=now_ns,
            ended_at_ns=None,
        )

        controller = LabelerController(storage)
        controller.start_session(now_ns=now_ns)
        controller.stop_session()
        view = controller.snapshot(now_ns=now_ns)

        self.assertEqual(view.session_summaries, [])
        self.assertIn("Discarded empty annotation session", view.status_message)
        controller.close()

    def _insert_sensor_session(
        self,
        storage: Storage,
        *,
        started_at_ns: int,
        frame_end_ns: int,
        ended_at_ns: int | None,
    ) -> int:
        session_id = storage.start_session("Polar H10 Test", "AA:BB:CC")
        storage.connection.execute(
            """
            UPDATE sessions
            SET started_at_ns = ?, ended_at_ns = ?
            WHERE id = ?
            """,
            (started_at_ns, ended_at_ns, session_id),
        )

        sample_rate_hz = 40
        frame_size = 40
        step_ns = int(1_000_000_000 / sample_rate_hz)
        total_frames = 25
        first_frame_end_ns = frame_end_ns - (total_frames - 1) * frame_size * step_ns
        for frame_index in range(total_frames):
            samples: list[tuple[int, int, int]] = []
            for sample_index in range(frame_size):
                absolute_index = frame_index * frame_size + sample_index
                t = absolute_index / sample_rate_hz
                x = int(90 * math.sin(2 * math.pi * 0.22 * t))
                y = int(50 * math.cos(2 * math.pi * 0.22 * t))
                z = int(900 + 180 * math.sin(2 * math.pi * 0.22 * t + 0.35))
                samples.append((x, y, z))
            current_frame_end_ns = first_frame_end_ns + frame_index * frame_size * step_ns
            storage.insert_acc_frame(
                session_id,
                sensor_recorded_at_ns=current_frame_end_ns,
                sample_rate_hz=sample_rate_hz,
                samples=samples,
            )
        storage.connection.commit()
        return session_id


if __name__ == "__main__":
    unittest.main()
