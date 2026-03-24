import Foundation
import XCTest
@testable import BreathingBar
@testable import BreathingBarCore

final class HistoryGraphSupportTests: XCTestCase {
    func testSegmentsSplitAcrossLargeSamplingGaps() {
        let base = Date(timeIntervalSince1970: 1_000)
        let points = [
            HistoryGraphPoint(sampledAt: base, value: 10.0),
            HistoryGraphPoint(sampledAt: base.addingTimeInterval(30), value: 11.0),
            HistoryGraphPoint(sampledAt: base.addingTimeInterval(180), value: 12.0),
        ]

        let segments = HistoryGraphBuilder.segments(from: points, gapThreshold: 90)

        XCTAssertEqual(segments.count, 2)
        XCTAssertEqual(segments[0].points.count, 2)
        XCTAssertEqual(segments[1].points.count, 1)
    }

    func testSeriesUsesMetricSpecificValuesAndDynamicDomain() {
        let samples = [
            MetricHistorySample(
                sampledAt: Date(timeIntervalSince1970: 100),
                breathingRate: 10.0,
                heartRate: 65.0,
                hrvRMSSD: 22.0
            ),
            MetricHistorySample(
                sampledAt: Date(timeIntervalSince1970: 130),
                breathingRate: 14.0,
                heartRate: 68.0,
                hrvRMSSD: 28.0
            ),
        ]

        let series = HistoryGraphBuilder.makeSeries(
            samples: samples,
            metric: .breathingRate,
            scale: HistoryGraphScale(
                defaultDomain: 6.0...18.0,
                hardLower: 0.0,
                hardUpper: 40.0,
                minimumSpan: 8.0
            )
        )

        XCTAssertEqual(series.segments.count, 1)
        XCTAssertEqual(series.latestValue, 14.0)
        XCTAssertLessThanOrEqual(series.domain.lowerBound, 10.0)
        XCTAssertGreaterThanOrEqual(series.domain.upperBound, 14.0)
    }
}
