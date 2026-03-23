from __future__ import annotations

import contextlib
import io
import tempfile
import unittest
from pathlib import Path

from polar_dash import main
from polar_dash.evaluate import evaluate_annotation_session
from polar_dash.storage import Storage


class BreathingEvaluationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory()
        self.db_path = Path(self.tempdir.name) / "evaluation.db"

    def tearDown(self) -> None:
        self.tempdir.cleanup()

    def test_evaluate_annotation_session_builds_reference_rates_and_metrics(self) -> None:
        storage = Storage(self.db_path)
        sensor_session_id = storage.start_session("Polar H10 Test", "AA:BB:CC")
        annotation_session_id = storage.start_annotation_session(
            "steady-12bpm",
            protocol_name="breathing_phase_keys_v1",
            linked_session_id=sensor_session_id,
        )

        phase_times = {
            "inhale_start": [0, 5, 10],
            "inhale_end": [2, 7, 12],
        }
        base_ns = 1_000_000_000_000
        for phase_code, offsets_seconds in phase_times.items():
            for offset_seconds in offsets_seconds:
                storage.insert_breathing_phase_label(
                    annotation_session_id,
                    recorded_at_ns=base_ns + offset_seconds * 1_000_000_000,
                    phase_code=phase_code,
                    key_name=phase_code[:1].upper(),
                    sensor_session_id=sensor_session_id,
                    breathing_estimate_bpm=None,
                    breathing_estimate_source=None,
                    breathing_estimate_time_ns=None,
                    estimate_age_ms=None,
                )

        estimates = [
            (3, 11.5, "fusion"),
            (6, 12.5, "acc-pca"),
            (8, 14.0, "fusion"),
        ]
        for offset_seconds, rate_bpm, source in estimates:
            storage.insert_breathing_estimate(
                sensor_session_id,
                estimated_at_ns=base_ns + offset_seconds * 1_000_000_000,
                breaths_per_min=rate_bpm,
                window_seconds=45,
                source=source,
            )

        storage.connection.execute(
            """
            UPDATE annotation_sessions
            SET started_at_ns = ?, ended_at_ns = ?
            WHERE id = ?
            """,
            (base_ns, base_ns + 12_000_000_000, annotation_session_id),
        )
        storage.connection.commit()

        evaluation = evaluate_annotation_session(storage, annotation_session_id)

        self.assertEqual(evaluation.annotation_name, "steady-12bpm")
        self.assertEqual(evaluation.linked_sensor_session_id, sensor_session_id)
        self.assertEqual(evaluation.label_count, 6)
        self.assertEqual(evaluation.reference_interval_count, 4)
        self.assertEqual(len(evaluation.comparisons), 3)

        overall_metrics = {metric.source: metric for metric in evaluation.source_metrics}
        self.assertEqual(set(overall_metrics), {"acc-pca", "fusion"})
        self.assertAlmostEqual(overall_metrics["fusion"].mean_absolute_error_bpm or 0.0, 1.25)
        self.assertAlmostEqual(overall_metrics["fusion"].root_mean_squared_error_bpm or 0.0, 1.4577379737)
        self.assertAlmostEqual(overall_metrics["fusion"].mean_error_bpm or 0.0, 0.75)
        self.assertAlmostEqual(overall_metrics["fusion"].max_absolute_error_bpm or 0.0, 2.0)
        self.assertAlmostEqual(overall_metrics["acc-pca"].mean_absolute_error_bpm or 0.0, 0.5)
        self.assertEqual(overall_metrics["acc-pca"].coverage_ratio, 1.0)

        storage.close()

    def test_cli_evaluate_breathing_prints_summary_and_writes_json(self) -> None:
        storage = Storage(self.db_path)
        sensor_session_id = storage.start_session("Polar H10 Test", "AA:BB:CC")
        annotation_session_id = storage.start_annotation_session(
            "cli-run",
            protocol_name="breathing_phase_keys_v1",
            linked_session_id=sensor_session_id,
        )
        base_ns = 2_000_000_000_000
        for offset_seconds in (0, 5, 10):
            storage.insert_breathing_phase_label(
                annotation_session_id,
                recorded_at_ns=base_ns + offset_seconds * 1_000_000_000,
                phase_code="inhale_start",
                key_name="L",
                sensor_session_id=sensor_session_id,
                breathing_estimate_bpm=None,
                breathing_estimate_source=None,
                breathing_estimate_time_ns=None,
                estimate_age_ms=None,
            )
        storage.insert_breathing_estimate(
            sensor_session_id,
            estimated_at_ns=base_ns + 6_000_000_000,
            breaths_per_min=12.0,
            window_seconds=45,
            source="fusion",
        )
        storage.connection.execute(
            """
            UPDATE annotation_sessions
            SET started_at_ns = ?, ended_at_ns = ?
            WHERE id = ?
            """,
            (base_ns, base_ns + 10_000_000_000, annotation_session_id),
        )
        storage.connection.commit()
        storage.close()

        json_path = Path(self.tempdir.name) / "evaluation.json"
        stdout = io.StringIO()
        with contextlib.redirect_stdout(stdout):
            exit_code = main(
                [
                    "evaluate-breathing",
                    "--db",
                    str(self.db_path),
                    "--annotation-session-id",
                    str(annotation_session_id),
                    "--json-out",
                    str(json_path),
                ]
            )

        output = stdout.getvalue()
        self.assertEqual(exit_code, 0)
        self.assertIn(f"Annotation session {annotation_session_id} (cli-run)", output)
        self.assertIn("fusion", output)
        self.assertIn("Wrote JSON report", output)
        self.assertTrue(json_path.exists())


if __name__ == "__main__":
    unittest.main()
