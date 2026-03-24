import Foundation
import BreathingBarCore

enum HistoryGraphMetric: String, CaseIterable, Identifiable {
    case breathingRate
    case heartRate
    case heartRateVariability

    var id: String { rawValue }

    func value(from sample: MetricHistorySample) -> Double? {
        switch self {
        case .breathingRate:
            return sample.breathingRate
        case .heartRate:
            return sample.heartRate
        case .heartRateVariability:
            return sample.hrvRMSSD
        }
    }
}

struct HistoryGraphPoint: Equatable, Identifiable {
    let sampledAt: Date
    let value: Double

    var id: Date { sampledAt }
}

struct HistoryGraphSegment: Equatable, Identifiable {
    let id: String
    let points: [HistoryGraphPoint]
}

struct HistoryGraphScale: Equatable {
    let defaultDomain: ClosedRange<Double>
    let hardLower: Double
    let hardUpper: Double
    let minimumSpan: Double

    func domain(for values: [Double]) -> ClosedRange<Double> {
        guard let minimum = values.min(), let maximum = values.max() else {
            return normalizedDomain(defaultDomain)
        }

        let span = maximum - minimum
        let padding = max(span * 0.18, minimumSpan * 0.35)
        let candidateDomain: ClosedRange<Double>

        if span < 0.001 {
            candidateDomain = (minimum - minimumSpan / 2.0)...(maximum + minimumSpan / 2.0)
        } else {
            candidateDomain = (minimum - padding)...(maximum + padding)
        }

        return normalizedDomain(candidateDomain)
    }

    private func normalizedDomain(_ domain: ClosedRange<Double>) -> ClosedRange<Double> {
        var lower = max(hardLower, min(hardUpper, domain.lowerBound))
        var upper = max(hardLower, min(hardUpper, domain.upperBound))

        if upper - lower < minimumSpan {
            let halfSpan = minimumSpan / 2.0
            var center = (lower + upper) / 2.0
            center = min(max(center, hardLower + halfSpan), hardUpper - halfSpan)
            lower = center - halfSpan
            upper = center + halfSpan
        }

        if lower < hardLower {
            upper += hardLower - lower
            lower = hardLower
        }
        if upper > hardUpper {
            lower -= upper - hardUpper
            upper = hardUpper
        }

        if upper <= lower {
            lower = hardLower
            upper = min(hardUpper, hardLower + minimumSpan)
        }

        return lower...upper
    }
}

struct HistoryGraphSeries: Equatable, Identifiable {
    let metric: HistoryGraphMetric
    let segments: [HistoryGraphSegment]
    let domain: ClosedRange<Double>

    var id: String { metric.rawValue }

    var latestValue: Double? {
        segments.last?.points.last?.value
    }
}

enum HistoryGraphBuilder {
    static let defaultGapThreshold: TimeInterval = 90

    static func makeSeries(
        samples: [MetricHistorySample],
        metric: HistoryGraphMetric,
        scale: HistoryGraphScale,
        gapThreshold: TimeInterval = defaultGapThreshold
    ) -> HistoryGraphSeries {
        let points = samples
            .compactMap { sample -> HistoryGraphPoint? in
                guard let value = metric.value(from: sample) else {
                    return nil
                }
                return HistoryGraphPoint(sampledAt: sample.sampledAt, value: value)
            }
            .sorted { $0.sampledAt < $1.sampledAt }

        return HistoryGraphSeries(
            metric: metric,
            segments: segments(from: points, gapThreshold: gapThreshold),
            domain: scale.domain(for: points.map(\.value))
        )
    }

    static func segments(
        from points: [HistoryGraphPoint],
        gapThreshold: TimeInterval = defaultGapThreshold
    ) -> [HistoryGraphSegment] {
        guard let firstPoint = points.first else {
            return []
        }

        var currentPoints: [HistoryGraphPoint] = [firstPoint]
        var segments: [HistoryGraphSegment] = []

        for point in points.dropFirst() {
            let gap = point.sampledAt.timeIntervalSince(currentPoints.last?.sampledAt ?? point.sampledAt)
            if gap > gapThreshold {
                segments.append(makeSegment(points: currentPoints, index: segments.count))
                currentPoints = [point]
            } else {
                currentPoints.append(point)
            }
        }

        segments.append(makeSegment(points: currentPoints, index: segments.count))
        return segments
    }

    private static func makeSegment(points: [HistoryGraphPoint], index: Int) -> HistoryGraphSegment {
        let origin = points.first?.sampledAt.timeIntervalSince1970 ?? 0
        return HistoryGraphSegment(
            id: "segment-\(index)-\(origin)",
            points: points
        )
    }
}
