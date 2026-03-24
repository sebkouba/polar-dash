import Foundation
import XCTest
@testable import BreathingBarCore

final class MetricHistoryStoreTests: XCTestCase {
    private var temporaryDirectory: URL!

    override func setUpWithError() throws {
        temporaryDirectory = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        try FileManager.default.createDirectory(at: temporaryDirectory, withIntermediateDirectories: true)
    }

    override func tearDownWithError() throws {
        try? FileManager.default.removeItem(at: temporaryDirectory)
    }

    func testRecordAndFetchSamplesInTimeOrder() throws {
        let store = try MetricHistoryStore(databaseURL: temporaryDirectory.appendingPathComponent("history.sqlite"))
        defer { store.close() }

        try store.recordSample(MetricHistorySample(sampledAt: Date(timeIntervalSince1970: 200), breathingRate: 12.0, heartRate: 67.0, hrvRMSSD: 25.0))
        try store.recordSample(MetricHistorySample(sampledAt: Date(timeIntervalSince1970: 100), breathingRate: 11.0, heartRate: 64.0, hrvRMSSD: 22.0))

        let samples = try store.fetchSamples(
            from: Date(timeIntervalSince1970: 0),
            to: Date(timeIntervalSince1970: 300)
        )

        XCTAssertEqual(samples.count, 2)
        XCTAssertEqual(samples.map(\.heartRate), [64.0, 67.0])
    }

    func testPurgeSamplesOlderThanCutoff() throws {
        let store = try MetricHistoryStore(databaseURL: temporaryDirectory.appendingPathComponent("history.sqlite"))
        defer { store.close() }

        try store.recordSample(MetricHistorySample(sampledAt: Date(timeIntervalSince1970: 100), breathingRate: 11.0, heartRate: 64.0, hrvRMSSD: 22.0))
        try store.recordSample(MetricHistorySample(sampledAt: Date(timeIntervalSince1970: 200), breathingRate: 12.0, heartRate: 67.0, hrvRMSSD: 25.0))

        try store.purgeSamples(olderThan: Date(timeIntervalSince1970: 150))

        let samples = try store.fetchSamples(
            from: Date(timeIntervalSince1970: 0),
            to: Date(timeIntervalSince1970: 300)
        )
        XCTAssertEqual(samples.count, 1)
        XCTAssertEqual(samples.first?.heartRate, 67.0)
    }

    func testPurgeAllRemovesStoredHistory() throws {
        let store = try MetricHistoryStore(databaseURL: temporaryDirectory.appendingPathComponent("history.sqlite"))
        defer { store.close() }

        try store.recordSample(MetricHistorySample(sampledAt: Date(timeIntervalSince1970: 100), breathingRate: 11.0, heartRate: 64.0, hrvRMSSD: 22.0))
        try store.purgeAllSamples()

        XCTAssertEqual(try store.countSamples(), 0)
    }
}
