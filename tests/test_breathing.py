from __future__ import annotations

import math
import tempfile
import time
import unittest
from pathlib import Path

from polar_dash.breathing import (
    CandidateEstimate,
    FusionCalibration,
    LiveBreathingEngine,
    fit_fusion_calibration,
)
from polar_dash.storage import Storage


class LiveBreathingEngineTests(unittest.TestCase):
    def test_rr_interval_candidate_tracks_synthetic_respiratory_modulation(self) -> None:
        engine = LiveBreathingEngine(window_seconds=20, step_seconds=2)

        rr_intervals_ms: list[int] = []
        elapsed_seconds = 0.0
        while elapsed_seconds < 30.0:
            rr_ms = 1_000.0 + 120.0 * math.sin(2 * math.pi * 0.20 * elapsed_seconds)
            rr_intervals_ms.append(int(rr_ms))
            elapsed_seconds += rr_ms / 1_000.0

        recorded_at_ns = int(sum(rr_intervals_ms) * 1_000_000)
        estimates = engine.add_hr_frame(
            recorded_at_ns=recorded_at_ns,
            average_hr_bpm=60.0,
            rr_intervals_ms=rr_intervals_ms,
        )

        rr_candidate = next(
            estimate for estimate in estimates if estimate.source == "rr_interval"
        )
        self.assertAlmostEqual(rr_candidate.rate_bpm, 12.0, delta=2.0)

    def test_fit_fusion_calibration_learns_bias_and_reliability(self) -> None:
        labels = [
            {"recorded_at_ns": 0, "phase_code": "exhale_end"},
            {"recorded_at_ns": 5_000_000_000, "phase_code": "exhale_end"},
            {"recorded_at_ns": 10_000_000_000, "phase_code": "exhale_end"},
            {"recorded_at_ns": 2_500_000_000, "phase_code": "inhale_end"},
            {"recorded_at_ns": 7_500_000_000, "phase_code": "inhale_end"},
            {"recorded_at_ns": 12_500_000_000, "phase_code": "inhale_end"},
        ]
        candidate_history = {
            "acc_pca": [
                CandidateEstimate(estimated_at_ns=3_000_000_000, rate_bpm=13.0, quality=1.0, source="acc_pca"),
                CandidateEstimate(estimated_at_ns=8_000_000_000, rate_bpm=13.0, quality=1.0, source="acc_pca"),
            ],
            "ecg_qrs": [
                CandidateEstimate(estimated_at_ns=3_000_000_000, rate_bpm=11.0, quality=1.0, source="ecg_qrs"),
                CandidateEstimate(estimated_at_ns=8_000_000_000, rate_bpm=11.0, quality=1.0, source="ecg_qrs"),
            ],
        }

        calibration = fit_fusion_calibration(
            candidate_history,
            labels,
            protocol_name="breathing_turnaround_fg_v1",
            annotation_session_id=12,
            version=3,
            now_ns=99,
            minimum_points_per_candidate=2,
        )

        self.assertEqual(calibration.version, 3)
        self.assertAlmostEqual(calibration.bias_by_candidate["acc_pca"], 1.0)
        self.assertAlmostEqual(calibration.bias_by_candidate["ecg_qrs"], -1.0)
        self.assertGreater(
            calibration.reliability_by_candidate["acc_pca"],
            0.0,
        )

    def test_learned_fusion_falls_back_without_calibration(self) -> None:
        engine = LiveBreathingEngine()
        engine.set_calibration(FusionCalibration.default())
        fusion = engine._estimate_learned_fusion(
            10,
            [
                CandidateEstimate(10, 12.0, 1.0, "acc_pca"),
                CandidateEstimate(10, 10.0, 0.5, "ecg_qrs"),
            ],
        )

        self.assertIsNotNone(fusion)
        self.assertEqual(fusion.source, "learned_fusion")
        self.assertGreater(float(fusion.rate_bpm), 10.0)
        self.assertLess(float(fusion.rate_bpm), 12.0)


class StorageCalibrationTests(unittest.TestCase):
    def test_storage_persists_candidate_estimates_and_calibration(self) -> None:
        tempdir = tempfile.TemporaryDirectory()
        self.addCleanup(tempdir.cleanup)
        db_path = Path(tempdir.name) / "cockpit.db"
        storage = Storage(db_path)
        self.addCleanup(storage.close)

        session_id = storage.start_session("Polar H10 Test", "AA:BB:CC")
        storage.insert_breathing_candidate_estimate(
            session_id,
            estimated_at_ns=1,
            candidate_name="rr_interval",
            breaths_per_min=12.3,
            quality=0.9,
            calibration_version=4,
        )
        calibration_id = storage.insert_breathing_calibration(
            annotation_session_id=None,
            protocol_name="breathing_turnaround_fg_v1",
            model={"version": 4},
        )

        rows = storage.list_breathing_candidate_estimates(session_id)
        latest = storage.get_latest_breathing_calibration()
        self.assertEqual(len(rows), 1)
        self.assertEqual(int(rows[0]["calibration_version"]), 4)
        self.assertEqual(int(latest["id"]), calibration_id)


if __name__ == "__main__":
    unittest.main()
