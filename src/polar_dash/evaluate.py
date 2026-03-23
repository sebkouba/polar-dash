from __future__ import annotations

import json
import math
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from polar_dash.storage import DEFAULT_DB_PATH, Storage


@dataclass(slots=True)
class LabelInterval:
    phase_code: str
    start_ns: int
    end_ns: int
    rate_bpm: float

    @property
    def duration_seconds(self) -> float:
        return (self.end_ns - self.start_ns) / 1_000_000_000

    def to_dict(self) -> dict[str, Any]:
        return {
            "phase_code": self.phase_code,
            "start_ns": self.start_ns,
            "end_ns": self.end_ns,
            "duration_seconds": self.duration_seconds,
            "rate_bpm": self.rate_bpm,
        }


@dataclass(slots=True)
class EstimateComparison:
    source: str
    estimated_at_ns: int
    estimated_rate_bpm: float
    reference_rate_bpm: float
    error_bpm: float

    def to_dict(self) -> dict[str, Any]:
        return {
            "source": self.source,
            "estimated_at_ns": self.estimated_at_ns,
            "estimated_rate_bpm": self.estimated_rate_bpm,
            "reference_rate_bpm": self.reference_rate_bpm,
            "error_bpm": self.error_bpm,
        }


@dataclass(slots=True)
class SourceMetrics:
    source: str
    total_points: int
    compared_points: int
    coverage_ratio: float
    mean_absolute_error_bpm: float | None
    root_mean_squared_error_bpm: float | None
    mean_error_bpm: float | None
    max_absolute_error_bpm: float | None

    def to_dict(self) -> dict[str, Any]:
        return {
            "source": self.source,
            "total_points": self.total_points,
            "compared_points": self.compared_points,
            "coverage_ratio": self.coverage_ratio,
            "mean_absolute_error_bpm": self.mean_absolute_error_bpm,
            "root_mean_squared_error_bpm": self.root_mean_squared_error_bpm,
            "mean_error_bpm": self.mean_error_bpm,
            "max_absolute_error_bpm": self.max_absolute_error_bpm,
        }


@dataclass(slots=True)
class SessionEvaluation:
    annotation_session_id: int
    annotation_name: str
    linked_sensor_session_id: int | None
    label_count: int
    label_window_start_ns: int
    label_window_end_ns: int
    reference_interval_count: int
    source_metrics: list[SourceMetrics]
    comparisons: list[EstimateComparison]
    reference_intervals: list[LabelInterval]

    def to_dict(self) -> dict[str, Any]:
        return {
            "annotation_session_id": self.annotation_session_id,
            "annotation_name": self.annotation_name,
            "linked_sensor_session_id": self.linked_sensor_session_id,
            "label_count": self.label_count,
            "label_window_start_ns": self.label_window_start_ns,
            "label_window_end_ns": self.label_window_end_ns,
            "reference_interval_count": self.reference_interval_count,
            "source_metrics": [metric.to_dict() for metric in self.source_metrics],
            "comparisons": [comparison.to_dict() for comparison in self.comparisons],
            "reference_intervals": [interval.to_dict() for interval in self.reference_intervals],
        }


def build_reference_intervals(
    label_rows: list[sqlite3.Row],
    *,
    min_cycle_seconds: float = 1.5,
    max_cycle_seconds: float = 15.0,
) -> list[LabelInterval]:
    phase_timestamps: dict[str, list[int]] = {}
    for row in label_rows:
        phase_code = str(row["phase_code"])
        phase_timestamps.setdefault(phase_code, []).append(int(row["recorded_at_ns"]))

    minimum_ns = int(min_cycle_seconds * 1_000_000_000)
    maximum_ns = int(max_cycle_seconds * 1_000_000_000)
    intervals: list[LabelInterval] = []
    for phase_code, timestamps in phase_timestamps.items():
        ordered = sorted(timestamps)
        for previous_ns, current_ns in zip(ordered, ordered[1:], strict=False):
            duration_ns = current_ns - previous_ns
            if duration_ns < minimum_ns or duration_ns > maximum_ns:
                continue
            intervals.append(
                LabelInterval(
                    phase_code=phase_code,
                    start_ns=previous_ns,
                    end_ns=current_ns,
                    rate_bpm=60_000_000_000 / duration_ns,
                )
            )

    intervals.sort(key=lambda interval: (interval.start_ns, interval.end_ns, interval.phase_code))
    return intervals


def evaluate_annotation_session(
    storage: Storage,
    annotation_session_id: int,
    *,
    min_cycle_seconds: float = 1.5,
    max_cycle_seconds: float = 15.0,
) -> SessionEvaluation:
    annotation_session = storage.get_annotation_session(annotation_session_id)
    if annotation_session is None:
        raise ValueError(f"Annotation session {annotation_session_id} was not found.")

    label_rows = list(
        storage.connection.execute(
            """
            SELECT recorded_at_ns, phase_code, sensor_session_id
            FROM breathing_phase_labels
            WHERE annotation_session_id = ?
            ORDER BY recorded_at_ns ASC
            """,
            (annotation_session_id,),
        ).fetchall()
    )
    if not label_rows:
        raise ValueError(f"Annotation session {annotation_session_id} has no labels.")

    reference_intervals = build_reference_intervals(
        label_rows,
        min_cycle_seconds=min_cycle_seconds,
        max_cycle_seconds=max_cycle_seconds,
    )
    if not reference_intervals:
        raise ValueError(
            f"Annotation session {annotation_session_id} does not contain enough repeated phase labels "
            "to reconstruct breathing-rate intervals."
        )

    label_window_start_ns = int(label_rows[0]["recorded_at_ns"])
    label_window_end_ns = int(label_rows[-1]["recorded_at_ns"])
    linked_sensor_session_id = _resolve_sensor_session_id(annotation_session, label_rows)

    query = """
        SELECT estimated_at_ns, breaths_per_min, source
        FROM breathing_estimates
        WHERE estimated_at_ns BETWEEN ? AND ?
    """
    params: list[Any] = [label_window_start_ns, label_window_end_ns]
    if linked_sensor_session_id is not None:
        query += " AND session_id = ?"
        params.append(linked_sensor_session_id)
    query += " ORDER BY estimated_at_ns ASC"

    estimate_rows = list(storage.connection.execute(query, params).fetchall())
    comparisons: list[EstimateComparison] = []
    total_points_by_source: dict[str, int] = {}
    for row in estimate_rows:
        source = str(row["source"])
        total_points_by_source[source] = total_points_by_source.get(source, 0) + 1
        estimated_at_ns = int(row["estimated_at_ns"])
        reference_rate_bpm = _reference_rate_at(estimated_at_ns, reference_intervals)
        if reference_rate_bpm is None:
            continue
        estimated_rate_bpm = float(row["breaths_per_min"])
        comparisons.append(
            EstimateComparison(
                source=source,
                estimated_at_ns=estimated_at_ns,
                estimated_rate_bpm=estimated_rate_bpm,
                reference_rate_bpm=reference_rate_bpm,
                error_bpm=estimated_rate_bpm - reference_rate_bpm,
            )
        )

    source_metrics = _build_source_metrics(comparisons, total_points_by_source)
    return SessionEvaluation(
        annotation_session_id=annotation_session_id,
        annotation_name=str(annotation_session["name"]),
        linked_sensor_session_id=linked_sensor_session_id,
        label_count=len(label_rows),
        label_window_start_ns=label_window_start_ns,
        label_window_end_ns=label_window_end_ns,
        reference_interval_count=len(reference_intervals),
        source_metrics=source_metrics,
        comparisons=comparisons,
        reference_intervals=reference_intervals,
    )


def evaluate_breathing_labels(
    db_path: str | Path = DEFAULT_DB_PATH,
    *,
    annotation_session_ids: list[int] | None = None,
    min_cycle_seconds: float = 1.5,
    max_cycle_seconds: float = 15.0,
) -> list[SessionEvaluation]:
    storage = Storage(db_path)
    try:
        session_ids = annotation_session_ids or [
            int(row["id"])
            for row in storage.list_annotation_sessions(include_active=False)
            if int(row["label_count"]) > 0
        ]
        return [
            evaluate_annotation_session(
                storage,
                session_id,
                min_cycle_seconds=min_cycle_seconds,
                max_cycle_seconds=max_cycle_seconds,
            )
            for session_id in session_ids
        ]
    finally:
        storage.close()


def write_evaluation_json(path: str | Path, evaluations: list[SessionEvaluation]) -> None:
    output_path = Path(path).expanduser().resolve()
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        json.dumps(
            {"sessions": [evaluation.to_dict() for evaluation in evaluations]},
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )


def format_evaluation_report(evaluations: list[SessionEvaluation]) -> str:
    if not evaluations:
        return "No completed annotation sessions with labels were found."

    sections = [format_session_report(evaluation) for evaluation in evaluations]
    return "\n\n".join(sections)


def format_session_report(evaluation: SessionEvaluation) -> str:
    header = (
        f"Annotation session {evaluation.annotation_session_id} "
        f"({evaluation.annotation_name})"
    )
    lines = [
        header,
        f"  linked sensor session: {evaluation.linked_sensor_session_id or 'n/a'}",
        f"  labels: {evaluation.label_count}",
        f"  label window: {evaluation.label_window_start_ns} -> {evaluation.label_window_end_ns}",
        f"  reference intervals: {evaluation.reference_interval_count}",
    ]
    if not evaluation.source_metrics:
        lines.append("  compared points: 0")
        lines.append("  no estimator points overlapped the labeled reference intervals")
        return "\n".join(lines)

    lines.append("  metrics:")
    lines.append("    source        points  cover   mae   rmse  bias  maxabs")
    for metric in evaluation.source_metrics:
        mae_text = _format_metric(metric.mean_absolute_error_bpm)
        rmse_text = _format_metric(metric.root_mean_squared_error_bpm)
        bias_text = _format_metric(metric.mean_error_bpm)
        maxabs_text = _format_metric(metric.max_absolute_error_bpm)
        lines.append(
            "    "
            f"{metric.source:<12}  "
            f"{metric.compared_points:>5}/{metric.total_points:<5} "
            f"{metric.coverage_ratio:>5.0%}  "
            f"{mae_text:>4}  "
            f"{rmse_text:>4}  "
            f"{bias_text:>4}  "
            f"{maxabs_text:>6}"
        )
    return "\n".join(lines)


def _resolve_sensor_session_id(
    annotation_session: sqlite3.Row,
    label_rows: list[sqlite3.Row],
) -> int | None:
    if annotation_session["linked_session_id"] is not None:
        return int(annotation_session["linked_session_id"])

    sensor_ids = {
        int(row["sensor_session_id"])
        for row in label_rows
        if row["sensor_session_id"] is not None
    }
    if len(sensor_ids) == 1:
        return next(iter(sensor_ids))
    return None


def _reference_rate_at(timestamp_ns: int, intervals: list[LabelInterval]) -> float | None:
    rates = [
        interval.rate_bpm
        for interval in intervals
        if interval.start_ns <= timestamp_ns <= interval.end_ns
    ]
    if not rates:
        return None
    return float(sum(rates) / len(rates))


def _build_source_metrics(
    comparisons: list[EstimateComparison],
    total_points_by_source: dict[str, int],
) -> list[SourceMetrics]:
    grouped_errors: dict[str, list[float]] = {}
    for comparison in comparisons:
        grouped_errors.setdefault(comparison.source, []).append(comparison.error_bpm)

    sources = sorted(set(total_points_by_source) | set(grouped_errors))
    metrics: list[SourceMetrics] = []
    for source in sources:
        errors = grouped_errors.get(source, [])
        total_points = total_points_by_source.get(source, 0)
        compared_points = len(errors)
        metrics.append(
            SourceMetrics(
                source=source,
                total_points=total_points,
                compared_points=compared_points,
                coverage_ratio=(compared_points / total_points) if total_points else 0.0,
                mean_absolute_error_bpm=_mean_absolute_error(errors),
                root_mean_squared_error_bpm=_root_mean_squared_error(errors),
                mean_error_bpm=(sum(errors) / compared_points) if compared_points else None,
                max_absolute_error_bpm=max((abs(error) for error in errors), default=None),
            )
        )
    return metrics


def _mean_absolute_error(errors: list[float]) -> float | None:
    if not errors:
        return None
    return sum(abs(error) for error in errors) / len(errors)


def _root_mean_squared_error(errors: list[float]) -> float | None:
    if not errors:
        return None
    return math.sqrt(sum(error * error for error in errors) / len(errors))


def _format_metric(value: float | None) -> str:
    if value is None:
        return "n/a"
    return f"{value:0.2f}"
