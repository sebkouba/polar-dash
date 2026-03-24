import Foundation
import SQLite3

public struct MetricHistorySample: Equatable {
    public let sampledAt: Date
    public let breathingRate: Double?
    public let heartRate: Double?
    public let hrvRMSSD: Double?

    public init(sampledAt: Date, breathingRate: Double?, heartRate: Double?, hrvRMSSD: Double?) {
        self.sampledAt = sampledAt
        self.breathingRate = breathingRate
        self.heartRate = heartRate
        self.hrvRMSSD = hrvRMSSD
    }
}

public enum MetricHistoryStoreError: Error {
    case openDatabase(String)
    case execute(String)
    case prepare(String)
    case step(String)
    case bind(String)
}

public final class MetricHistoryStore {
    private let databaseURL: URL
    private var handle: OpaquePointer?
    private let calendar: Calendar

    public init(
        databaseURL: URL,
        calendar: Calendar = .current
    ) throws {
        self.databaseURL = databaseURL.standardizedFileURL
        self.calendar = calendar
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

    public func close() {
        if let handle {
            sqlite3_close(handle)
            self.handle = nil
        }
    }

    public func recordSample(_ sample: MetricHistorySample) throws {
        let statement = try prepare(
            """
            INSERT INTO metric_history_samples (
                sampled_at_ns,
                breathing_rate,
                heart_rate,
                hrv_rmssd
            ) VALUES (?, ?, ?, ?)
            """
        )
        defer { sqlite3_finalize(statement) }
        try bindInt64(sample.sampledAt.nanosecondsSince1970, at: 1, in: statement)
        try bindDouble(sample.breathingRate, at: 2, in: statement)
        try bindDouble(sample.heartRate, at: 3, in: statement)
        try bindDouble(sample.hrvRMSSD, at: 4, in: statement)
        try step(statement)
    }

    public func fetchSamples(
        from startDate: Date,
        to endDate: Date
    ) throws -> [MetricHistorySample] {
        let statement = try prepare(
            """
            SELECT sampled_at_ns, breathing_rate, heart_rate, hrv_rmssd
            FROM metric_history_samples
            WHERE sampled_at_ns >= ? AND sampled_at_ns <= ?
            ORDER BY sampled_at_ns ASC
            """
        )
        defer { sqlite3_finalize(statement) }
        try bindInt64(startDate.nanosecondsSince1970, at: 1, in: statement)
        try bindInt64(endDate.nanosecondsSince1970, at: 2, in: statement)

        var samples: [MetricHistorySample] = []
        while sqlite3_step(statement) == SQLITE_ROW {
            let sampledAtNs = sqlite3_column_int64(statement, 0)
            let breathingRate = sqlite3_column_type(statement, 1) == SQLITE_NULL ? nil : sqlite3_column_double(statement, 1)
            let heartRate = sqlite3_column_type(statement, 2) == SQLITE_NULL ? nil : sqlite3_column_double(statement, 2)
            let hrvRMSSD = sqlite3_column_type(statement, 3) == SQLITE_NULL ? nil : sqlite3_column_double(statement, 3)
            samples.append(
                MetricHistorySample(
                    sampledAt: Date(nanosecondsSince1970: sampledAtNs),
                    breathingRate: breathingRate,
                    heartRate: heartRate,
                    hrvRMSSD: hrvRMSSD
                )
            )
        }
        let result = sqlite3_errcode(handle)
        guard result == SQLITE_DONE else {
            throw MetricHistoryStoreError.step(lastErrorMessage())
        }
        return samples
    }

    public func purgeSamples(olderThan cutoffDate: Date) throws {
        let statement = try prepare(
            "DELETE FROM metric_history_samples WHERE sampled_at_ns < ?"
        )
        defer { sqlite3_finalize(statement) }
        try bindInt64(cutoffDate.nanosecondsSince1970, at: 1, in: statement)
        try step(statement)
    }

    public func purgeAllSamples() throws {
        try execute("DELETE FROM metric_history_samples;")
    }

    public func countSamples() throws -> Int {
        let statement = try prepare("SELECT COUNT(*) FROM metric_history_samples")
        defer { sqlite3_finalize(statement) }
        guard sqlite3_step(statement) == SQLITE_ROW else {
            throw MetricHistoryStoreError.step(lastErrorMessage())
        }
        return Int(sqlite3_column_int64(statement, 0))
    }

    public static func defaultDatabaseURL(fileManager: FileManager = .default) -> URL {
        let applicationSupportRoot = fileManager.urls(for: .applicationSupportDirectory, in: .userDomainMask).first
            ?? fileManager.homeDirectoryForCurrentUser.appendingPathComponent("Library/Application Support", isDirectory: true)
        let appDirectory = applicationSupportRoot.appendingPathComponent("PolarDash", isDirectory: true)
        return appDirectory.appendingPathComponent("metric_history.sqlite")
    }

    private func open() throws {
        var database: OpaquePointer?
        if sqlite3_open_v2(databaseURL.path, &database, SQLITE_OPEN_CREATE | SQLITE_OPEN_READWRITE, nil) != SQLITE_OK {
            defer {
                if let database {
                    sqlite3_close(database)
                }
            }
            throw MetricHistoryStoreError.openDatabase(lastErrorMessage(database))
        }
        handle = database
    }

    private func configure() throws {
        try execute("PRAGMA journal_mode = WAL;")
        try execute("PRAGMA synchronous = NORMAL;")
    }

    private func initializeSchema() throws {
        try execute(
            """
            CREATE TABLE IF NOT EXISTS metric_history_samples (
                sampled_at_ns INTEGER PRIMARY KEY,
                breathing_rate REAL,
                heart_rate REAL,
                hrv_rmssd REAL,
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            );

            CREATE INDEX IF NOT EXISTS idx_metric_history_samples_sampled_at
                ON metric_history_samples(sampled_at_ns);
            """
        )
    }

    private func execute(_ sql: String) throws {
        guard sqlite3_exec(handle, sql, nil, nil, nil) == SQLITE_OK else {
            throw MetricHistoryStoreError.execute(lastErrorMessage())
        }
    }

    private func prepare(_ sql: String) throws -> OpaquePointer {
        var statement: OpaquePointer?
        guard sqlite3_prepare_v2(handle, sql, -1, &statement, nil) == SQLITE_OK, let statement else {
            throw MetricHistoryStoreError.prepare(lastErrorMessage())
        }
        return statement
    }

    private func bindInt64(_ value: Int64, at index: Int32, in statement: OpaquePointer) throws {
        guard sqlite3_bind_int64(statement, index, value) == SQLITE_OK else {
            throw MetricHistoryStoreError.bind(lastErrorMessage())
        }
    }

    private func bindDouble(_ value: Double?, at index: Int32, in statement: OpaquePointer) throws {
        if let value {
            guard sqlite3_bind_double(statement, index, value) == SQLITE_OK else {
                throw MetricHistoryStoreError.bind(lastErrorMessage())
            }
        } else {
            sqlite3_bind_null(statement, index)
        }
    }

    private func step(_ statement: OpaquePointer) throws {
        guard sqlite3_step(statement) == SQLITE_DONE else {
            throw MetricHistoryStoreError.step(lastErrorMessage())
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

private extension Date {
    var nanosecondsSince1970: Int64 {
        Int64((timeIntervalSince1970 * 1_000_000_000.0).rounded())
    }

    init(nanosecondsSince1970 value: Int64) {
        self = Date(timeIntervalSince1970: Double(value) / 1_000_000_000.0)
    }
}
