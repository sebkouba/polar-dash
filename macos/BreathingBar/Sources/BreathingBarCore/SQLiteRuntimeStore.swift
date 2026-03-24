import Foundation
import SQLite3

enum SQLiteRuntimeStoreError: Error {
    case openDatabase(String)
    case execute(String)
    case prepare(String)
    case step(String)
    case bind(String)
    case decodeCalibration(String)
}

struct CalibrationRecord {
    let id: Int
    let protocolName: String
    let calibration: FusionCalibration
}

final class SQLiteRuntimeStore {
    private(set) var databaseURL: URL
    private var handle: OpaquePointer?

    init(databaseURL: URL) throws {
        self.databaseURL = databaseURL.standardizedFileURL
        try FileManager.default.createDirectory(
            at: self.databaseURL.deletingLastPathComponent(),
            withIntermediateDirectories: true
        )
        try open()
        try configure()
        try initializeSchema()
    }

    deinit {
        close()
    }

    func close() {
        if let handle {
            sqlite3_close(handle)
            self.handle = nil
        }
    }

    @discardableResult
    func startSession(deviceName: String, deviceAddress: String) throws -> Int {
        let startedAtNs = currentEpochNanoseconds()
        let statement = try prepare(
            """
            INSERT INTO sessions (started_at_ns, device_name, device_address)
            VALUES (?, ?, ?)
            """
        )
        defer { sqlite3_finalize(statement) }
        try bindInt64(startedAtNs, at: 1, in: statement)
        try bindText(deviceName, at: 2, in: statement)
        try bindText(deviceAddress, at: 3, in: statement)
        try step(statement)
        return Int(sqlite3_last_insert_rowid(handle))
    }

    func closeSession(id: Int) throws {
        let statement = try prepare("UPDATE sessions SET ended_at_ns = ? WHERE id = ?")
        defer { sqlite3_finalize(statement) }
        try bindInt64(currentEpochNanoseconds(), at: 1, in: statement)
        try bindInt64(Int64(id), at: 2, in: statement)
        try step(statement)
    }

    func updateSessionBattery(sessionID: Int, batteryPercent: Int) throws {
        let statement = try prepare("UPDATE sessions SET battery_percent = ? WHERE id = ?")
        defer { sqlite3_finalize(statement) }
        try bindInt64(Int64(batteryPercent), at: 1, in: statement)
        try bindInt64(Int64(sessionID), at: 2, in: statement)
        try step(statement)
    }

    func insertHeartRateFrame(
        sessionID: Int,
        recordedAtNs: Int64,
        averageHeartRateBpm: Double,
        rrIntervalsMs: [Int],
        energyKJ: Int?
    ) throws {
        let statement = try prepare(
            """
            INSERT INTO hr_frames (
                session_id,
                recorded_at_ns,
                average_hr_bpm,
                rr_intervals_ms_json,
                energy_kj
            ) VALUES (?, ?, ?, ?, ?)
            """
        )
        defer { sqlite3_finalize(statement) }
        try bindInt64(Int64(sessionID), at: 1, in: statement)
        try bindInt64(recordedAtNs, at: 2, in: statement)
        try bindDouble(averageHeartRateBpm, at: 3, in: statement)
        try bindJSON(rrIntervalsMs, at: 4, in: statement)
        if let energyKJ {
            try bindInt64(Int64(energyKJ), at: 5, in: statement)
        } else {
            sqlite3_bind_null(statement, 5)
        }
        try step(statement)
    }

    func insertECGFrame(
        sessionID: Int,
        sensorRecordedAtNs: Int64,
        sampleRateHz: Int,
        samples: [Int]
    ) throws {
        let statement = try prepare(
            """
            INSERT INTO ecg_frames (
                session_id,
                sensor_recorded_at_ns,
                sample_rate_hz,
                sample_count,
                samples_json
            ) VALUES (?, ?, ?, ?, ?)
            """
        )
        defer { sqlite3_finalize(statement) }
        try bindInt64(Int64(sessionID), at: 1, in: statement)
        try bindInt64(sensorRecordedAtNs, at: 2, in: statement)
        try bindInt64(Int64(sampleRateHz), at: 3, in: statement)
        try bindInt64(Int64(samples.count), at: 4, in: statement)
        try bindJSON(samples, at: 5, in: statement)
        try step(statement)
    }

    func insertACCFrame(
        sessionID: Int,
        sensorRecordedAtNs: Int64,
        sampleRateHz: Int,
        samples: [(Int, Int, Int)]
    ) throws {
        let statement = try prepare(
            """
            INSERT INTO acc_frames (
                session_id,
                sensor_recorded_at_ns,
                sample_rate_hz,
                sample_count,
                samples_json
            ) VALUES (?, ?, ?, ?, ?)
            """
        )
        defer { sqlite3_finalize(statement) }
        try bindInt64(Int64(sessionID), at: 1, in: statement)
        try bindInt64(sensorRecordedAtNs, at: 2, in: statement)
        try bindInt64(Int64(sampleRateHz), at: 3, in: statement)
        try bindInt64(Int64(samples.count), at: 4, in: statement)
        let payload = samples.map { [$0.0, $0.1, $0.2] }
        try bindJSON(payload, at: 5, in: statement)
        try step(statement)
    }

    func insertEvent(
        eventType: String,
        details: [String: Any],
        level: String = "INFO",
        sessionID: Int? = nil,
        recordedAtNs: Int64? = nil
    ) throws {
        let statement = try prepare(
            """
            INSERT INTO collector_events (
                session_id,
                recorded_at_ns,
                level,
                event_type,
                details_json
            ) VALUES (?, ?, ?, ?, ?)
            """
        )
        defer { sqlite3_finalize(statement) }
        if let sessionID {
            try bindInt64(Int64(sessionID), at: 1, in: statement)
        } else {
            sqlite3_bind_null(statement, 1)
        }
        try bindInt64(recordedAtNs ?? currentEpochNanoseconds(), at: 2, in: statement)
        try bindText(level, at: 3, in: statement)
        try bindText(eventType, at: 4, in: statement)
        try bindJSONObject(details, at: 5, in: statement)
        try step(statement)
    }

    func insertBreathingEstimate(
        sessionID: Int,
        estimate: CandidateEstimate,
        windowSeconds: Int
    ) throws {
        let statement = try prepare(
            """
            INSERT OR REPLACE INTO breathing_estimates (
                session_id,
                estimated_at_ns,
                breaths_per_min,
                window_seconds,
                source
            ) VALUES (?, ?, ?, ?, ?)
            """
        )
        defer { sqlite3_finalize(statement) }
        try bindInt64(Int64(sessionID), at: 1, in: statement)
        try bindInt64(estimate.estimatedAtNs, at: 2, in: statement)
        try bindDouble(estimate.rateBpm, at: 3, in: statement)
        try bindInt64(Int64(windowSeconds), at: 4, in: statement)
        try bindText(estimate.source, at: 5, in: statement)
        try step(statement)
    }

    func insertBreathingCandidateEstimate(
        sessionID: Int,
        estimate: CandidateEstimate
    ) throws {
        let statement = try prepare(
            """
            INSERT OR REPLACE INTO breathing_candidate_estimates (
                session_id,
                estimated_at_ns,
                candidate_name,
                breaths_per_min,
                quality,
                calibration_version
            ) VALUES (?, ?, ?, ?, ?, ?)
            """
        )
        defer { sqlite3_finalize(statement) }
        try bindInt64(Int64(sessionID), at: 1, in: statement)
        try bindInt64(estimate.estimatedAtNs, at: 2, in: statement)
        try bindText(estimate.source, at: 3, in: statement)
        try bindDouble(estimate.rateBpm, at: 4, in: statement)
        try bindDouble(estimate.quality, at: 5, in: statement)
        if let calibrationVersion = estimate.calibrationVersion {
            try bindInt64(Int64(calibrationVersion), at: 6, in: statement)
        } else {
            sqlite3_bind_null(statement, 6)
        }
        try step(statement)
    }

    func latestCalibrationRecord() throws -> CalibrationRecord? {
        let statement = try prepare(
            """
            SELECT id, protocol_name, model_json
            FROM breathing_calibrations
            ORDER BY id DESC
            LIMIT 1
            """
        )
        defer { sqlite3_finalize(statement) }

        guard sqlite3_step(statement) == SQLITE_ROW else {
            return nil
        }

        let id = Int(sqlite3_column_int64(statement, 0))
        let protocolName = String(cString: sqlite3_column_text(statement, 1))
        guard let modelPointer = sqlite3_column_text(statement, 2) else {
            throw SQLiteRuntimeStoreError.decodeCalibration("Missing model_json")
        }
        let model = String(cString: modelPointer)
        let data = Data(model.utf8)
        do {
            var calibration = try JSONDecoder().decode(FusionCalibration.self, from: data)
            calibration.version = id
            return CalibrationRecord(id: id, protocolName: protocolName, calibration: calibration)
        } catch {
            throw SQLiteRuntimeStoreError.decodeCalibration("Invalid calibration JSON: \(error)")
        }
    }

    func countRows(in table: String) throws -> Int {
        let statement = try prepare("SELECT COUNT(*) FROM \(table)")
        defer { sqlite3_finalize(statement) }
        guard sqlite3_step(statement) == SQLITE_ROW else {
            throw SQLiteRuntimeStoreError.step(lastErrorMessage())
        }
        return Int(sqlite3_column_int64(statement, 0))
    }

    private func open() throws {
        var database: OpaquePointer?
        if sqlite3_open_v2(databaseURL.path, &database, SQLITE_OPEN_CREATE | SQLITE_OPEN_READWRITE, nil) != SQLITE_OK {
            defer {
                if let database {
                    sqlite3_close(database)
                }
            }
            throw SQLiteRuntimeStoreError.openDatabase(lastErrorMessage(database))
        }
        handle = database
    }

    private func configure() throws {
        try execute("PRAGMA foreign_keys = ON;")
        try execute("PRAGMA journal_mode = WAL;")
        try execute("PRAGMA synchronous = NORMAL;")
    }

    private func initializeSchema() throws {
        try execute(
            """
            CREATE TABLE IF NOT EXISTS sessions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                started_at_ns INTEGER NOT NULL,
                ended_at_ns INTEGER,
                device_name TEXT NOT NULL,
                device_address TEXT NOT NULL,
                battery_percent INTEGER,
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            );

            CREATE TABLE IF NOT EXISTS hr_frames (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                session_id INTEGER NOT NULL REFERENCES sessions(id) ON DELETE CASCADE,
                recorded_at_ns INTEGER NOT NULL,
                average_hr_bpm REAL NOT NULL,
                rr_intervals_ms_json TEXT NOT NULL,
                energy_kj INTEGER,
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            );

            CREATE TABLE IF NOT EXISTS ecg_frames (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                session_id INTEGER NOT NULL REFERENCES sessions(id) ON DELETE CASCADE,
                sensor_recorded_at_ns INTEGER NOT NULL,
                sample_rate_hz INTEGER NOT NULL,
                sample_count INTEGER NOT NULL,
                samples_json TEXT NOT NULL,
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            );

            CREATE TABLE IF NOT EXISTS acc_frames (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                session_id INTEGER NOT NULL REFERENCES sessions(id) ON DELETE CASCADE,
                sensor_recorded_at_ns INTEGER NOT NULL,
                sample_rate_hz INTEGER NOT NULL,
                sample_count INTEGER NOT NULL,
                samples_json TEXT NOT NULL,
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            );

            CREATE TABLE IF NOT EXISTS collector_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                session_id INTEGER REFERENCES sessions(id) ON DELETE CASCADE,
                recorded_at_ns INTEGER NOT NULL,
                level TEXT NOT NULL,
                event_type TEXT NOT NULL,
                details_json TEXT NOT NULL,
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            );

            CREATE TABLE IF NOT EXISTS breathing_estimates (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                session_id INTEGER NOT NULL REFERENCES sessions(id) ON DELETE CASCADE,
                estimated_at_ns INTEGER NOT NULL,
                breaths_per_min REAL NOT NULL,
                window_seconds INTEGER NOT NULL,
                source TEXT NOT NULL DEFAULT 'acc',
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(session_id, estimated_at_ns, source)
            );

            CREATE TABLE IF NOT EXISTS breathing_candidate_estimates (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                session_id INTEGER NOT NULL REFERENCES sessions(id) ON DELETE CASCADE,
                estimated_at_ns INTEGER NOT NULL,
                candidate_name TEXT NOT NULL,
                breaths_per_min REAL NOT NULL,
                quality REAL NOT NULL,
                calibration_version INTEGER,
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(session_id, estimated_at_ns, candidate_name)
            );

            CREATE TABLE IF NOT EXISTS annotation_sessions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                started_at_ns INTEGER NOT NULL,
                ended_at_ns INTEGER,
                name TEXT NOT NULL,
                protocol_name TEXT NOT NULL,
                linked_session_id INTEGER REFERENCES sessions(id) ON DELETE SET NULL,
                notes_json TEXT NOT NULL DEFAULT '{}',
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            );

            CREATE TABLE IF NOT EXISTS breathing_phase_labels (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                annotation_session_id INTEGER NOT NULL REFERENCES annotation_sessions(id) ON DELETE CASCADE,
                sensor_session_id INTEGER REFERENCES sessions(id) ON DELETE SET NULL,
                recorded_at_ns INTEGER NOT NULL,
                phase_code TEXT NOT NULL,
                key_name TEXT NOT NULL,
                breathing_estimate_bpm REAL,
                breathing_estimate_source TEXT,
                breathing_estimate_time_ns INTEGER,
                estimate_age_ms REAL,
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            );

            CREATE TABLE IF NOT EXISTS breathing_calibrations (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                annotation_session_id INTEGER REFERENCES annotation_sessions(id) ON DELETE SET NULL,
                protocol_name TEXT NOT NULL,
                model_json TEXT NOT NULL,
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            );

            CREATE INDEX IF NOT EXISTS idx_sessions_started_at
                ON sessions(started_at_ns DESC);
            CREATE INDEX IF NOT EXISTS idx_hr_frames_session_time
                ON hr_frames(session_id, recorded_at_ns);
            CREATE INDEX IF NOT EXISTS idx_ecg_frames_session_time
                ON ecg_frames(session_id, sensor_recorded_at_ns);
            CREATE INDEX IF NOT EXISTS idx_acc_frames_session_time
                ON acc_frames(session_id, sensor_recorded_at_ns);
            CREATE INDEX IF NOT EXISTS idx_collector_events_session_time
                ON collector_events(session_id, recorded_at_ns);
            CREATE INDEX IF NOT EXISTS idx_breathing_estimates_session_time
                ON breathing_estimates(session_id, estimated_at_ns);
            CREATE INDEX IF NOT EXISTS idx_breathing_candidate_estimates_session_time
                ON breathing_candidate_estimates(session_id, estimated_at_ns);
            CREATE INDEX IF NOT EXISTS idx_annotation_sessions_started_at
                ON annotation_sessions(started_at_ns DESC);
            CREATE INDEX IF NOT EXISTS idx_breathing_phase_labels_session_time
                ON breathing_phase_labels(annotation_session_id, recorded_at_ns);
            CREATE INDEX IF NOT EXISTS idx_breathing_phase_labels_sensor_time
                ON breathing_phase_labels(sensor_session_id, recorded_at_ns);
            CREATE INDEX IF NOT EXISTS idx_breathing_calibrations_created_at
                ON breathing_calibrations(created_at DESC);
            """
        )
    }

    private func execute(_ sql: String) throws {
        guard sqlite3_exec(handle, sql, nil, nil, nil) == SQLITE_OK else {
            throw SQLiteRuntimeStoreError.execute(lastErrorMessage())
        }
    }

    private func prepare(_ sql: String) throws -> OpaquePointer {
        var statement: OpaquePointer?
        guard sqlite3_prepare_v2(handle, sql, -1, &statement, nil) == SQLITE_OK, let statement else {
            throw SQLiteRuntimeStoreError.prepare(lastErrorMessage())
        }
        return statement
    }

    private func bindInt64(_ value: UInt64, at index: Int32, in statement: OpaquePointer) throws {
        try bindInt64(Int64(bitPattern: value), at: index, in: statement)
    }

    private func bindInt64(_ value: Int64, at index: Int32, in statement: OpaquePointer) throws {
        guard sqlite3_bind_int64(statement, index, value) == SQLITE_OK else {
            throw SQLiteRuntimeStoreError.bind(lastErrorMessage())
        }
    }

    private func bindDouble(_ value: Double, at index: Int32, in statement: OpaquePointer) throws {
        guard sqlite3_bind_double(statement, index, value) == SQLITE_OK else {
            throw SQLiteRuntimeStoreError.bind(lastErrorMessage())
        }
    }

    private func bindText(_ value: String, at index: Int32, in statement: OpaquePointer) throws {
        let transient = unsafeBitCast(-1, to: sqlite3_destructor_type.self)
        guard sqlite3_bind_text(statement, index, value, -1, transient) == SQLITE_OK else {
            throw SQLiteRuntimeStoreError.bind(lastErrorMessage())
        }
    }

    private func bindJSON<T: Encodable>(_ value: T, at index: Int32, in statement: OpaquePointer) throws {
        let data = try JSONEncoder().encode(value)
        guard let json = String(data: data, encoding: .utf8) else {
            throw SQLiteRuntimeStoreError.bind("Unable to encode JSON string")
        }
        try bindText(json, at: index, in: statement)
    }

    private func bindJSONObject(_ value: [String: Any], at index: Int32, in statement: OpaquePointer) throws {
        let data = try JSONSerialization.data(withJSONObject: value, options: [.sortedKeys])
        guard let json = String(data: data, encoding: .utf8) else {
            throw SQLiteRuntimeStoreError.bind("Unable to encode JSON object")
        }
        try bindText(json, at: index, in: statement)
    }

    private func step(_ statement: OpaquePointer) throws {
        guard sqlite3_step(statement) == SQLITE_DONE else {
            throw SQLiteRuntimeStoreError.step(lastErrorMessage())
        }
    }

    private func lastErrorMessage(_ handle: OpaquePointer? = nil) -> String {
        let handle = handle ?? self.handle
        guard let handle, let message = sqlite3_errmsg(handle) else {
            return "Unknown SQLite error"
        }
        return String(cString: message)
    }
}

private func currentEpochNanoseconds() -> Int64 {
    Int64((Date().timeIntervalSince1970 * 1_000_000_000.0).rounded())
}
