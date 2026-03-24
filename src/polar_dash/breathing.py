from __future__ import annotations

import json
import math
from collections import deque
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Sequence

import numpy as np
from scipy import signal

ECG_SAMPLE_RATE_HZ = 130
ACC_SAMPLE_RATE_HZ = 200
RESPIRATORY_BAND_HZ = (0.08, 0.70)
DEFAULT_BREATHING_WINDOW_SECONDS = 20
DEFAULT_BREATHING_STEP_SECONDS = 2
DEFAULT_FUSION_SMOOTHING_ALPHA = 0.75
DEFAULT_SINGLE_SOURCE_SMOOTHING_ALPHA = 0.60
DEFAULT_CALIBRATION_EPSILON = 0.25
DEFAULT_MIN_CALIBRATION_POINTS = 3
DEFAULT_REPO_CALIBRATION_PATH = (
    Path(__file__).resolve().parents[2]
    / "macos"
    / "BreathingBar"
    / "Sources"
    / "BreathingBarCore"
    / "Resources"
    / "default_breathing_calibration.json"
)


@dataclass(slots=True)
class CandidateEstimate:
    estimated_at_ns: int
    rate_bpm: float
    quality: float
    source: str
    calibration_version: int | None = None


@dataclass(slots=True)
class ReferenceInterval:
    phase_code: str
    start_ns: int
    end_ns: int
    rate_bpm: float


@dataclass(slots=True)
class FusionCalibration:
    version: int | None
    protocol_name: str
    annotation_session_id: int | None
    bias_by_candidate: dict[str, float]
    reliability_by_candidate: dict[str, float]
    minimum_points_per_candidate: int
    epsilon: float
    trained_point_count: int
    trained_at_ns: int | None

    @classmethod
    def default(cls) -> FusionCalibration:
        return cls(
            version=None,
            protocol_name="default",
            annotation_session_id=None,
            bias_by_candidate={},
            reliability_by_candidate={},
            minimum_points_per_candidate=DEFAULT_MIN_CALIBRATION_POINTS,
            epsilon=DEFAULT_CALIBRATION_EPSILON,
            trained_point_count=0,
            trained_at_ns=None,
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "version": self.version,
            "protocol_name": self.protocol_name,
            "annotation_session_id": self.annotation_session_id,
            "bias_by_candidate": self.bias_by_candidate,
            "reliability_by_candidate": self.reliability_by_candidate,
            "minimum_points_per_candidate": self.minimum_points_per_candidate,
            "epsilon": self.epsilon,
            "trained_point_count": self.trained_point_count,
            "trained_at_ns": self.trained_at_ns,
        }

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> FusionCalibration:
        return cls(
            version=payload.get("version"),
            protocol_name=str(payload.get("protocol_name", "default")),
            annotation_session_id=payload.get("annotation_session_id"),
            bias_by_candidate={
                str(key): float(value)
                for key, value in dict(payload.get("bias_by_candidate", {})).items()
            },
            reliability_by_candidate={
                str(key): float(value)
                for key, value in dict(payload.get("reliability_by_candidate", {})).items()
            },
            minimum_points_per_candidate=int(
                payload.get("minimum_points_per_candidate", DEFAULT_MIN_CALIBRATION_POINTS)
            ),
            epsilon=float(payload.get("epsilon", DEFAULT_CALIBRATION_EPSILON)),
            trained_point_count=int(payload.get("trained_point_count", 0)),
            trained_at_ns=payload.get("trained_at_ns"),
        )


def load_default_fusion_calibration(
    calibration_path: Path | str = DEFAULT_REPO_CALIBRATION_PATH,
) -> FusionCalibration:
    try:
        payload = json.loads(Path(calibration_path).read_text(encoding="utf-8"))
    except (FileNotFoundError, OSError, json.JSONDecodeError):
        return FusionCalibration.default()
    return FusionCalibration.from_dict(payload)


def build_reference_intervals_from_labels(
    labels: Sequence[dict[str, Any] | Any],
    *,
    min_cycle_seconds: float = 1.5,
    max_cycle_seconds: float = 15.0,
) -> list[ReferenceInterval]:
    phase_timestamps: dict[str, list[int]] = {}
    for label in labels:
        phase_code = str(label["phase_code"])
        phase_timestamps.setdefault(phase_code, []).append(int(label["recorded_at_ns"]))

    minimum_ns = int(min_cycle_seconds * 1_000_000_000)
    maximum_ns = int(max_cycle_seconds * 1_000_000_000)
    intervals: list[ReferenceInterval] = []
    for phase_code, timestamps in phase_timestamps.items():
        ordered = sorted(timestamps)
        for previous_ns, current_ns in zip(ordered, ordered[1:], strict=False):
            duration_ns = current_ns - previous_ns
            if duration_ns < minimum_ns or duration_ns > maximum_ns:
                continue
            intervals.append(
                ReferenceInterval(
                    phase_code=phase_code,
                    start_ns=previous_ns,
                    end_ns=current_ns,
                    rate_bpm=60_000_000_000 / duration_ns,
                )
            )
    intervals.sort(key=lambda item: (item.start_ns, item.end_ns, item.phase_code))
    return intervals


def reference_rate_at(
    timestamp_ns: int,
    intervals: Sequence[ReferenceInterval],
) -> float | None:
    rates = [
        interval.rate_bpm
        for interval in intervals
        if interval.start_ns <= timestamp_ns <= interval.end_ns
    ]
    if not rates:
        return None
    return float(sum(rates) / len(rates))


def fit_fusion_calibration(
    candidate_history_by_name: dict[str, Sequence[CandidateEstimate]],
    labels: Sequence[dict[str, Any] | Any],
    *,
    protocol_name: str,
    annotation_session_id: int | None,
    version: int | None,
    now_ns: int | None = None,
    epsilon: float = DEFAULT_CALIBRATION_EPSILON,
    minimum_points_per_candidate: int = DEFAULT_MIN_CALIBRATION_POINTS,
) -> FusionCalibration:
    intervals = build_reference_intervals_from_labels(labels)
    if not intervals:
        return FusionCalibration.default()

    bias_by_candidate: dict[str, float] = {}
    reliability_by_candidate: dict[str, float] = {}
    trained_point_count = 0
    for candidate_name, estimates in candidate_history_by_name.items():
        errors = [
            estimate.rate_bpm - reference_rate
            for estimate in estimates
            if (reference_rate := reference_rate_at(estimate.estimated_at_ns, intervals)) is not None
        ]
        if len(errors) < minimum_points_per_candidate:
            continue
        trained_point_count += len(errors)
        bias = float(sum(errors) / len(errors))
        mae = float(sum(abs(error) for error in errors) / len(errors))
        bias_by_candidate[candidate_name] = bias
        reliability_by_candidate[candidate_name] = 1.0 / max(mae + epsilon, epsilon)

    return FusionCalibration(
        version=version,
        protocol_name=protocol_name,
        annotation_session_id=annotation_session_id,
        bias_by_candidate=bias_by_candidate,
        reliability_by_candidate=reliability_by_candidate,
        minimum_points_per_candidate=minimum_points_per_candidate,
        epsilon=epsilon,
        trained_point_count=trained_point_count,
        trained_at_ns=now_ns,
    )


def compute_rmssd_series(
    beats: Sequence[tuple[int, float]],
    *,
    window_seconds: int = 60,
) -> list[tuple[int, float]]:
    if not beats:
        return []

    timestamps = np.array([beat[0] for beat in beats], dtype=np.int64)
    rr_values = np.array([beat[1] for beat in beats], dtype=float)
    rmssd_values: list[tuple[int, float]] = []
    window_ns = window_seconds * 1_000_000_000
    start_index = 0

    for index in range(len(beats)):
        while timestamps[index] - timestamps[start_index] > window_ns:
            start_index += 1
        window_rr = rr_values[start_index : index + 1]
        if len(window_rr) < 5:
            continue
        diffs = np.diff(window_rr)
        rmssd_values.append((int(timestamps[index]), float(np.sqrt(np.mean(diffs ** 2)))))

    return rmssd_values


def rebuild_learned_fusion_history(
    candidate_history_by_name: dict[str, Sequence[CandidateEstimate]],
    calibration: FusionCalibration,
    *,
    smoothing_alpha: float = DEFAULT_FUSION_SMOOTHING_ALPHA,
) -> list[CandidateEstimate]:
    grouped: dict[int, list[CandidateEstimate]] = {}
    for candidate_name, estimates in candidate_history_by_name.items():
        if candidate_name == "learned_fusion":
            continue
        for estimate in estimates:
            grouped.setdefault(estimate.estimated_at_ns, []).append(estimate)

    history: list[CandidateEstimate] = []
    previous_rate: float | None = None
    for estimated_at_ns in sorted(grouped):
        weighted_terms: list[tuple[float, float]] = []
        for candidate in grouped[estimated_at_ns]:
            bias = calibration.bias_by_candidate.get(candidate.source, 0.0)
            reliability = calibration.reliability_by_candidate.get(candidate.source, 1.0)
            corrected_rate = candidate.rate_bpm - bias
            weight = max(candidate.quality, 0.05) * reliability
            weighted_terms.append((corrected_rate, weight))
        if not weighted_terms:
            continue
        total_weight = sum(weight for _, weight in weighted_terms)
        if total_weight <= 0:
            continue
        rate_bpm = sum(rate * weight for rate, weight in weighted_terms) / total_weight
        if previous_rate is not None:
            rate_bpm = smoothing_alpha * rate_bpm + (1.0 - smoothing_alpha) * previous_rate
        previous_rate = rate_bpm
        history.append(
            CandidateEstimate(
                estimated_at_ns=estimated_at_ns,
                rate_bpm=float(rate_bpm),
                quality=float(total_weight / len(weighted_terms)),
                source="learned_fusion",
                calibration_version=calibration.version,
            )
        )
    return history


class LiveBreathingEngine:
    def __init__(
        self,
        *,
        window_seconds: int = DEFAULT_BREATHING_WINDOW_SECONDS,
        step_seconds: int = DEFAULT_BREATHING_STEP_SECONDS,
        fusion_smoothing_alpha: float = DEFAULT_FUSION_SMOOTHING_ALPHA,
        single_source_smoothing_alpha: float = DEFAULT_SINGLE_SOURCE_SMOOTHING_ALPHA,
    ) -> None:
        self.window_seconds = window_seconds
        self.window_ns = window_seconds * 1_000_000_000
        self.step_ns = step_seconds * 1_000_000_000
        self.fusion_smoothing_alpha = fusion_smoothing_alpha
        self.single_source_smoothing_alpha = single_source_smoothing_alpha
        self.calibration = FusionCalibration.default()
        self.acc_samples: deque[tuple[int, float, float, float]] = deque()
        self.ecg_samples: deque[tuple[int, float]] = deque()
        self.beats: deque[tuple[int, float]] = deque()
        self.last_estimate_at_ns: int | None = None
        self.previous_rate_by_source: dict[str, float] = {}

    def set_calibration(self, calibration: FusionCalibration) -> None:
        self.calibration = calibration

    def add_acc_frame(
        self,
        sensor_recorded_at_ns: int,
        sample_rate_hz: int,
        samples: Sequence[tuple[int, int, int]],
    ) -> list[CandidateEstimate]:
        step_ns = int(1_000_000_000 / sample_rate_hz)
        start_ns = sensor_recorded_at_ns - step_ns * (len(samples) - 1)
        for index, sample in enumerate(samples):
            timestamp_ns = start_ns + index * step_ns
            self.acc_samples.append(
                (timestamp_ns, float(sample[0]), float(sample[1]), float(sample[2]))
            )
        return self._maybe_estimate(sensor_recorded_at_ns)

    def add_ecg_frame(
        self,
        sensor_recorded_at_ns: int,
        sample_rate_hz: int,
        samples: Sequence[int],
    ) -> list[CandidateEstimate]:
        step_ns = int(1_000_000_000 / sample_rate_hz)
        start_ns = sensor_recorded_at_ns - step_ns * (len(samples) - 1)
        for index, sample in enumerate(samples):
            self.ecg_samples.append((start_ns + index * step_ns, float(sample)))
        return self._maybe_estimate(sensor_recorded_at_ns)

    def add_hr_frame(
        self,
        recorded_at_ns: int,
        average_hr_bpm: float,
        rr_intervals_ms: Sequence[int],
    ) -> list[CandidateEstimate]:
        if rr_intervals_ms:
            beat_time_ns = int(recorded_at_ns - sum(rr_intervals_ms) * 1_000_000)
            for rr_ms in rr_intervals_ms:
                beat_time_ns += int(rr_ms * 1_000_000)
                self.beats.append((beat_time_ns, float(rr_ms)))
        else:
            rr_ms = 60_000.0 / max(average_hr_bpm, 1.0)
            self.beats.append((int(recorded_at_ns), float(rr_ms)))
        return self._maybe_estimate(recorded_at_ns)

    def recent_ecg(self, *, lookback_seconds: int = 12) -> list[tuple[int, float]]:
        cutoff_ns = self._latest_time_ns() - lookback_seconds * 1_000_000_000
        return [(timestamp_ns, value) for timestamp_ns, value in self.ecg_samples if timestamp_ns >= cutoff_ns]

    def recent_acc(self, *, lookback_seconds: int = 30) -> list[tuple[int, float, float, float]]:
        cutoff_ns = self._latest_time_ns() - lookback_seconds * 1_000_000_000
        return [
            (timestamp_ns, x, y, z)
            for timestamp_ns, x, y, z in self.acc_samples
            if timestamp_ns >= cutoff_ns
        ]

    def recent_beats(self, *, lookback_seconds: int = 30) -> list[tuple[int, float]]:
        cutoff_ns = self._latest_time_ns() - lookback_seconds * 1_000_000_000
        return [(timestamp_ns, rr_ms) for timestamp_ns, rr_ms in self.beats if timestamp_ns >= cutoff_ns]

    def respiratory_waveform(
        self,
        *,
        lookback_seconds: int = 30,
    ) -> list[tuple[int, float]]:
        samples = self.recent_acc(lookback_seconds=lookback_seconds)
        if len(samples) < 200:
            return []
        timestamps = np.array([sample[0] for sample in samples], dtype=np.int64)
        xyz = np.array([sample[1:] for sample in samples], dtype=float)
        sample_spacing = np.diff(timestamps)
        if len(sample_spacing) == 0:
            return []

        sample_rate_hz = round(1_000_000_000 / np.median(sample_spacing))
        if sample_rate_hz < 20:
            return []

        centered = xyz - xyz.mean(axis=0, keepdims=True)
        covariance = np.cov(centered, rowvar=False)
        eigenvalues, eigenvectors = np.linalg.eigh(covariance)
        principal_signal = centered @ eigenvectors[:, int(np.argmax(eigenvalues))]
        lowpassed = signal.sosfiltfilt(
            signal.butter(2, 1.0, btype="lowpass", fs=sample_rate_hz, output="sos"),
            principal_signal,
        )
        reduced = signal.resample_poly(lowpassed, up=1, down=8)
        reduced_timestamps = np.linspace(
            timestamps[0],
            timestamps[-1],
            num=len(reduced),
            dtype=np.int64,
        )
        respiratory = signal.sosfiltfilt(
            signal.butter(2, list(RESPIRATORY_BAND_HZ), btype="bandpass", fs=sample_rate_hz / 8, output="sos"),
            signal.detrend(reduced),
        )
        return list(zip(reduced_timestamps.tolist(), respiratory.tolist(), strict=False))

    def _maybe_estimate(self, timestamp_ns: int) -> list[CandidateEstimate]:
        cutoff_ns = timestamp_ns - self.window_ns - self.step_ns
        while self.acc_samples and self.acc_samples[0][0] < cutoff_ns:
            self.acc_samples.popleft()
        while self.ecg_samples and self.ecg_samples[0][0] < cutoff_ns:
            self.ecg_samples.popleft()
        while self.beats and self.beats[0][0] < cutoff_ns:
            self.beats.popleft()

        if (
            self.last_estimate_at_ns is not None
            and timestamp_ns - self.last_estimate_at_ns < self.step_ns
        ):
            return []

        candidates = [
            candidate
            for candidate in (
                self._estimate_acc_candidate(timestamp_ns),
                self._estimate_ecg_candidate(timestamp_ns),
                self._estimate_rr_candidate(timestamp_ns),
            )
            if candidate is not None
        ]
        if not candidates:
            return []

        fusion = self._estimate_learned_fusion(timestamp_ns, candidates)
        estimates = candidates + ([fusion] if fusion is not None else [])
        self.last_estimate_at_ns = timestamp_ns
        for estimate in estimates:
            self.previous_rate_by_source[estimate.source] = estimate.rate_bpm
        return estimates

    def _estimate_acc_candidate(self, end_ns: int) -> CandidateEstimate | None:
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
            end_ns,
            reduced,
            sample_rate_hz / 8,
            source="acc_pca",
        )

    def _estimate_ecg_candidate(self, end_ns: int) -> CandidateEstimate | None:
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
            end_ns,
            interpolated,
            4.0,
            source="ecg_qrs",
        )

    def _estimate_rr_candidate(self, end_ns: int) -> CandidateEstimate | None:
        beats = [
            beat
            for beat in self.beats
            if beat[0] >= end_ns - self.window_ns
        ]
        if len(beats) < 8:
            return None

        beat_times = np.array([beat[0] for beat in beats], dtype=np.int64)
        rr_values = np.array([beat[1] for beat in beats], dtype=float)
        if beat_times[-1] - beat_times[0] < 10_000_000_000:
            return None

        uniform_times = np.arange(
            beat_times[0],
            beat_times[-1],
            int(1_000_000_000 / 4),
        )
        if len(uniform_times) < 20:
            return None

        interpolated = np.interp(uniform_times, beat_times, rr_values)
        return self._estimate_waveform_candidate(
            end_ns,
            interpolated,
            4.0,
            source="rr_interval",
        )

    def _estimate_waveform_candidate(
        self,
        end_ns: int,
        waveform: np.ndarray,
        sample_rate_hz: float,
        *,
        source: str,
    ) -> CandidateEstimate | None:
        if len(waveform) < sample_rate_hz * 10:
            return None

        try:
            filtered = signal.sosfiltfilt(
                signal.butter(2, list(RESPIRATORY_BAND_HZ), btype="bandpass", fs=sample_rate_hz, output="sos"),
                signal.detrend(waveform),
            )
        except ValueError:
            return None

        frequencies, power = signal.welch(
            filtered,
            fs=sample_rate_hz,
            nperseg=min(len(filtered), int(sample_rate_hz * self.window_seconds)),
        )
        band = (frequencies >= RESPIRATORY_BAND_HZ[0]) & (frequencies <= RESPIRATORY_BAND_HZ[1])
        if not np.any(band):
            return None

        band_frequencies = frequencies[band]
        band_power = power[band]
        median_power = max(float(np.median(band_power)), 1e-9)
        peak_index = int(np.argmax(band_power))
        spectral_rate_bpm = float(band_frequencies[peak_index] * 60.0)
        spectral_quality = float(np.log1p(float(band_power[peak_index]) / median_power))

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

        if abs(spectral_rate_bpm - autocorr_rate_bpm) <= 3.0:
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

        return self._smoothed_estimate(
            CandidateEstimate(
                estimated_at_ns=end_ns,
                rate_bpm=rate_bpm,
                quality=float(quality),
                source=source,
            )
        )

    def _estimate_learned_fusion(
        self,
        end_ns: int,
        candidates: Sequence[CandidateEstimate],
    ) -> CandidateEstimate | None:
        weighted_terms: list[tuple[float, float]] = []
        for candidate in candidates:
            bias = self.calibration.bias_by_candidate.get(candidate.source, 0.0)
            reliability = self.calibration.reliability_by_candidate.get(candidate.source, 1.0)
            corrected_rate = candidate.rate_bpm - bias
            weight = max(candidate.quality, 0.05) * reliability
            weighted_terms.append((corrected_rate, weight))

        if not weighted_terms:
            return None

        total_weight = sum(weight for _, weight in weighted_terms)
        if total_weight <= 0:
            return None

        rate_bpm = sum(rate * weight for rate, weight in weighted_terms) / total_weight
        quality = total_weight / len(weighted_terms)
        return self._smoothed_estimate(
            CandidateEstimate(
                estimated_at_ns=end_ns,
                rate_bpm=float(rate_bpm),
                quality=float(quality),
                source="learned_fusion",
                calibration_version=self.calibration.version,
            )
        )

    def _smoothed_estimate(self, estimate: CandidateEstimate) -> CandidateEstimate:
        previous_rate = self.previous_rate_by_source.get(estimate.source)
        if previous_rate is None:
            return estimate

        alpha = (
            self.fusion_smoothing_alpha
            if estimate.source == "learned_fusion"
            else self.single_source_smoothing_alpha
        )
        smoothed_rate = alpha * estimate.rate_bpm + (1.0 - alpha) * previous_rate
        return CandidateEstimate(
            estimated_at_ns=estimate.estimated_at_ns,
            rate_bpm=float(smoothed_rate),
            quality=estimate.quality,
            source=estimate.source,
            calibration_version=estimate.calibration_version,
        )

    def _latest_time_ns(self) -> int:
        latest_values = [
            self.acc_samples[-1][0] if self.acc_samples else 0,
            self.ecg_samples[-1][0] if self.ecg_samples else 0,
            self.beats[-1][0] if self.beats else 0,
        ]
        return max(latest_values)

    def latest_time_ns(self) -> int | None:
        latest = self._latest_time_ns()
        return latest if latest > 0 else None
