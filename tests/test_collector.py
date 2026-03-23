from __future__ import annotations

import unittest

from polar_dash.collector import (
    DEFAULT_BREATHING_STEP_SECONDS,
    DEFAULT_BREATHING_WINDOW_SECONDS,
    FUSION_SMOOTHING_ALPHA,
    SINGLE_SOURCE_SMOOTHING_ALPHA,
    BreathingCandidate,
    RollingBreathingEstimator,
)


class RollingBreathingEstimatorTests(unittest.TestCase):
    def test_defaults_are_tuned_for_faster_refresh(self) -> None:
        estimator = RollingBreathingEstimator()

        self.assertEqual(estimator.window_seconds, DEFAULT_BREATHING_WINDOW_SECONDS)
        self.assertEqual(estimator.step_ns, DEFAULT_BREATHING_STEP_SECONDS * 1_000_000_000)

    def test_fusion_smoothing_uses_retuned_alpha(self) -> None:
        estimator = RollingBreathingEstimator()
        estimator.previous_rate_bpm = 10.0

        candidate = estimator._smoothed_candidate(
            BreathingCandidate(rate_bpm=14.0, quality=1.0, source="fusion")
        )

        expected = FUSION_SMOOTHING_ALPHA * 14.0 + (1.0 - FUSION_SMOOTHING_ALPHA) * 10.0
        self.assertAlmostEqual(candidate.rate_bpm, expected)

    def test_single_source_smoothing_uses_retuned_alpha(self) -> None:
        estimator = RollingBreathingEstimator()
        estimator.previous_rate_bpm = 10.0

        candidate = estimator._smoothed_candidate(
            BreathingCandidate(rate_bpm=14.0, quality=1.0, source="acc-pca")
        )

        expected = (
            SINGLE_SOURCE_SMOOTHING_ALPHA * 14.0
            + (1.0 - SINGLE_SOURCE_SMOOTHING_ALPHA) * 10.0
        )
        self.assertAlmostEqual(candidate.rate_bpm, expected)


if __name__ == "__main__":
    unittest.main()
