from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from polar_dash.breathing import CandidateEstimate
from polar_dash.cockpit import CockpitConfig, CockpitController
from polar_dash.storage import Storage


class CockpitControllerTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory()
        self.addCleanup(self.tempdir.cleanup)
        self.db_path = Path(self.tempdir.name) / "cockpit.db"
        self.storage = Storage(self.db_path)
        self.addCleanup(self.storage.close)
        self.controller = CockpitController(
            self.storage,
            CockpitConfig(db_path=str(self.db_path), auto_start=False),
        )

    def test_controller_persists_learned_fusion_for_swift_compatibility(self) -> None:
        self.controller.begin_live_session("Polar H10 Test", "AA:BB:CC")

        self.controller._process_estimates(
            [
                CandidateEstimate(estimated_at_ns=1, rate_bpm=11.5, quality=1.2, source="learned_fusion"),
            ]
        )

        row = self.storage.connection.execute(
            """
            SELECT breaths_per_min, source
            FROM breathing_estimates
            ORDER BY id DESC
            LIMIT 1
            """
        ).fetchone()
        self.assertIsNotNone(row)
        self.assertAlmostEqual(float(row["breaths_per_min"]), 11.5)
        self.assertEqual(str(row["source"]), "learned_fusion")

    def test_handle_keypress_requires_active_label_session(self) -> None:
        self.controller.begin_live_session("Polar H10 Test", "AA:BB:CC")

        self.controller.handle_keypress("f", now_ns=1_000_000_000)

        self.assertIn("Start a label session", self.controller.status_message)

    def test_label_session_and_recalibration_flow(self) -> None:
        self.controller.begin_live_session("Polar H10 Test", "AA:BB:CC")
        self.controller._process_estimates(
            [
                CandidateEstimate(3_000_000_000, 13.0, 1.0, "acc_pca"),
                CandidateEstimate(3_000_000_000, 11.0, 1.0, "ecg_qrs"),
                CandidateEstimate(3_000_000_000, 12.5, 1.0, "rr_interval"),
                CandidateEstimate(8_000_000_000, 13.0, 1.0, "acc_pca"),
                CandidateEstimate(8_000_000_000, 11.0, 1.0, "ecg_qrs"),
                CandidateEstimate(8_000_000_000, 12.5, 1.0, "rr_interval"),
                CandidateEstimate(11_000_000_000, 13.0, 1.0, "acc_pca"),
                CandidateEstimate(11_000_000_000, 11.0, 1.0, "ecg_qrs"),
                CandidateEstimate(11_000_000_000, 12.5, 1.0, "rr_interval"),
            ]
        )

        self.controller.start_label_session(name="live-fg")
        self.controller.handle_keypress("f", now_ns=0)
        self.controller.handle_keypress("g", now_ns=2_500_000_000)
        self.controller.handle_keypress("f", now_ns=5_000_000_000)
        self.controller.handle_keypress("g", now_ns=7_500_000_000)
        self.controller.handle_keypress("f", now_ns=10_000_000_000)
        self.controller.handle_keypress("g", now_ns=12_500_000_000)
        self.controller.stop_label_session(apply_recalibration=True)

        calibration_row = self.storage.get_latest_breathing_calibration()
        self.assertIsNotNone(calibration_row)
        self.assertIsNotNone(self.controller.engine.calibration.version)
        self.assertEqual(self.controller.active_annotation_session_id, None)
        self.assertIn("applied calibration", self.controller.status_message)


if __name__ == "__main__":
    unittest.main()
