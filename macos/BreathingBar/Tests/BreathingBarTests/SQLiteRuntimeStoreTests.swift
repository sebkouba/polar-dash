import Foundation
import SQLite3
import XCTest
@testable import BreathingBarCore

final class SQLiteRuntimeStoreTests: XCTestCase {
    private var temporaryDirectory: URL!

    override func setUpWithError() throws {
        temporaryDirectory = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        try FileManager.default.createDirectory(at: temporaryDirectory, withIntermediateDirectories: true)
    }

    override func tearDownWithError() throws {
        try? FileManager.default.removeItem(at: temporaryDirectory)
    }

    func testBootstrapCreatesRuntimeTables() throws {
        let store = try SQLiteRuntimeStore(databaseURL: temporaryDirectory.appendingPathComponent("polar_dash.db"))
        defer { store.close() }

        XCTAssertEqual(try store.countRows(in: "sessions"), 0)
        XCTAssertEqual(try store.countRows(in: "breathing_estimates"), 0)
        XCTAssertEqual(try store.countRows(in: "breathing_calibrations"), 0)
    }

    func testLatestCalibrationLoadsExistingRecordAndUsesRowIDAsVersion() throws {
        let databaseURL = temporaryDirectory.appendingPathComponent("existing.db")
        let store = try SQLiteRuntimeStore(databaseURL: databaseURL)
        defer { store.close() }

        let db = try openRaw(databaseURL)
        defer { sqlite3_close(db) }
        let json = """
        {
          "version": null,
          "protocol_name": "breathing_turnaround_fg_v1",
          "annotation_session_id": 12,
          "bias_by_candidate": {"acc_pca": 1.25},
          "reliability_by_candidate": {"acc_pca": 2.5},
          "minimum_points_per_candidate": 3,
          "epsilon": 0.25,
          "trained_point_count": 6,
          "trained_at_ns": 99
        }
        """
        try execute(
            """
            INSERT INTO breathing_calibrations (annotation_session_id, protocol_name, model_json)
            VALUES (12, 'breathing_turnaround_fg_v1', '\(json.replacingOccurrences(of: "'", with: "''"))')
            """,
            db: db
        )

        let calibration = try XCTUnwrap(store.latestCalibrationRecord())
        XCTAssertEqual(calibration.id, 1)
        XCTAssertEqual(calibration.protocolName, "breathing_turnaround_fg_v1")
        XCTAssertEqual(calibration.calibration.version, 1)
        XCTAssertEqual(calibration.calibration.biasByCandidate["acc_pca"], 1.25)
    }

    func testWritesSessionFramesAndLearnedFusionEstimateWithCompatibleColumns() throws {
        let databaseURL = temporaryDirectory.appendingPathComponent("runtime.db")
        let store = try SQLiteRuntimeStore(databaseURL: databaseURL)
        defer { store.close() }

        let sessionID = try store.startSession(deviceName: "Polar H10 Test", deviceAddress: "AA:BB:CC")
        try store.updateSessionBattery(sessionID: sessionID, batteryPercent: 87)
        try store.insertHeartRateFrame(
            sessionID: sessionID,
            recordedAtNs: 1_000,
            averageHeartRateBpm: 61.0,
            rrIntervalsMs: [980, 1_010],
            energyKJ: nil
        )
        try store.insertECGFrame(
            sessionID: sessionID,
            sensorRecordedAtNs: 2_000,
            sampleRateHz: ECGSampleRateHz,
            samples: [1, 2, 3]
        )
        try store.insertACCFrame(
            sessionID: sessionID,
            sensorRecordedAtNs: 3_000,
            sampleRateHz: ACCSampleRateHz,
            samples: [(1, 2, 3), (4, 5, 6)]
        )
        try store.insertBreathingCandidateEstimate(
            sessionID: sessionID,
            estimate: CandidateEstimate(
                estimatedAtNs: 4_000,
                rateBpm: 12.4,
                quality: 1.3,
                source: "learned_fusion",
                calibrationVersion: 7
            )
        )
        try store.insertBreathingEstimate(
            sessionID: sessionID,
            estimate: CandidateEstimate(
                estimatedAtNs: 4_000,
                rateBpm: 12.4,
                quality: 1.3,
                source: "learned_fusion",
                calibrationVersion: 7
            ),
            windowSeconds: defaultBreathingWindowSeconds
        )
        try store.insertEvent(
            eventType: "collector_connected",
            details: ["device_name": "Polar H10 Test"],
            sessionID: sessionID
        )
        try store.closeSession(id: sessionID)

        let db = try openRaw(databaseURL)
        defer { sqlite3_close(db) }
        XCTAssertEqual(try scalar("SELECT COUNT(*) FROM sessions", db: db), 1)
        XCTAssertEqual(try scalar("SELECT COUNT(*) FROM hr_frames", db: db), 1)
        XCTAssertEqual(try scalar("SELECT COUNT(*) FROM ecg_frames", db: db), 1)
        XCTAssertEqual(try scalar("SELECT COUNT(*) FROM acc_frames", db: db), 1)
        XCTAssertEqual(try scalar("SELECT COUNT(*) FROM collector_events", db: db), 1)
        XCTAssertEqual(try scalar("SELECT COUNT(*) FROM breathing_candidate_estimates", db: db), 1)
        XCTAssertEqual(try scalar("SELECT COUNT(*) FROM breathing_estimates WHERE source = 'learned_fusion'", db: db), 1)
        XCTAssertEqual(try scalar("SELECT battery_percent FROM sessions LIMIT 1", db: db), 87)
    }

    private func openRaw(_ url: URL) throws -> OpaquePointer {
        var db: OpaquePointer?
        guard sqlite3_open_v2(url.path, &db, SQLITE_OPEN_READWRITE, nil) == SQLITE_OK, let db else {
            throw SQLiteRuntimeStoreError.openDatabase("Unable to open raw test database")
        }
        return db
    }

    private func execute(_ sql: String, db: OpaquePointer) throws {
        guard sqlite3_exec(db, sql, nil, nil, nil) == SQLITE_OK else {
            throw SQLiteRuntimeStoreError.execute(String(cString: sqlite3_errmsg(db)))
        }
    }

    private func scalar(_ sql: String, db: OpaquePointer) throws -> Int {
        var statement: OpaquePointer?
        guard sqlite3_prepare_v2(db, sql, -1, &statement, nil) == SQLITE_OK, let statement else {
            throw SQLiteRuntimeStoreError.prepare(String(cString: sqlite3_errmsg(db)))
        }
        defer { sqlite3_finalize(statement) }
        guard sqlite3_step(statement) == SQLITE_ROW else {
            throw SQLiteRuntimeStoreError.step(String(cString: sqlite3_errmsg(db)))
        }
        return Int(sqlite3_column_int64(statement, 0))
    }
}
