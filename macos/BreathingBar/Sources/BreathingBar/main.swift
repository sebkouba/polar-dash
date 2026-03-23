import AppKit
import Foundation
import SQLite3
import SwiftUI

private enum DefaultsKey {
    static let lowThreshold = "lowThreshold"
    static let highThreshold = "highThreshold"
}

private struct BreathingPoint: Identifiable {
    let timestampNs: Int64
    let rate: Double

    var id: Int64 { timestampNs }
}

private struct BreathingSnapshot {
    let databasePath: String?
    let points: [BreathingPoint]
}

private final class SQLiteBreathingStore {
    func loadSnapshot(limit: Int = 40) -> BreathingSnapshot {
        guard let databaseURL = resolveDatabaseURL() else {
            return BreathingSnapshot(databasePath: nil, points: [])
        }

        var handle: OpaquePointer?
        guard sqlite3_open_v2(databaseURL.path, &handle, SQLITE_OPEN_READONLY, nil) == SQLITE_OK else {
            if let handle {
                sqlite3_close(handle)
            }
            return BreathingSnapshot(databasePath: databaseURL.path, points: [])
        }
        defer { sqlite3_close(handle) }

        let query = """
        SELECT estimated_at_ns, breaths_per_min
        FROM breathing_estimates
        WHERE session_id = (
            SELECT id FROM sessions
            ORDER BY started_at_ns DESC
            LIMIT 1
        )
        ORDER BY estimated_at_ns DESC
        LIMIT ?;
        """

        var statement: OpaquePointer?
        guard sqlite3_prepare_v2(handle, query, -1, &statement, nil) == SQLITE_OK else {
            if let statement {
                sqlite3_finalize(statement)
            }
            return BreathingSnapshot(databasePath: databaseURL.path, points: [])
        }
        defer { sqlite3_finalize(statement) }

        sqlite3_bind_int(statement, 1, Int32(limit))

        var points: [BreathingPoint] = []
        while sqlite3_step(statement) == SQLITE_ROW {
            let timestampNs = sqlite3_column_int64(statement, 0)
            let rate = sqlite3_column_double(statement, 1)
            points.append(BreathingPoint(timestampNs: timestampNs, rate: rate))
        }

        return BreathingSnapshot(
            databasePath: databaseURL.path,
            points: points.reversed()
        )
    }

    private func resolveDatabaseURL() -> URL? {
        if let explicitPath = ProcessInfo.processInfo.environment["POLAR_DASH_DB"] {
            let url = URL(fileURLWithPath: explicitPath).standardizedFileURL
            if FileManager.default.fileExists(atPath: url.path) {
                return url
            }
        }

        let fileManager = FileManager.default
        let roots = [
            URL(fileURLWithPath: fileManager.currentDirectoryPath),
            URL(fileURLWithPath: CommandLine.arguments[0]).deletingLastPathComponent(),
        ]

        for root in roots {
            var current = root.standardizedFileURL
            while true {
                let candidate = current.appendingPathComponent("data/polar_dash.db")
                if fileManager.fileExists(atPath: candidate.path) {
                    return candidate
                }

                let parent = current.deletingLastPathComponent()
                if parent.path == current.path {
                    break
                }
                current = parent
            }
        }

        return nil
    }
}

@MainActor
private final class BreathingBarModel: ObservableObject {
    @Published var currentRate: Double?
    @Published var history: [Double] = []
    @Published var databasePath: String = "Database not found"
    @Published var lowerThreshold: Double
    @Published var upperThreshold: Double
    @Published var flashOn = false

    private let store = SQLiteBreathingStore()
    private var refreshTask: Task<Void, Never>?
    private var flashTask: Task<Void, Never>?

    init() {
        let defaults = UserDefaults.standard
        let storedLow = defaults.object(forKey: DefaultsKey.lowThreshold) as? Double
        let storedHigh = defaults.object(forKey: DefaultsKey.highThreshold) as? Double
        lowerThreshold = storedLow ?? 8.0
        upperThreshold = storedHigh ?? 24.0

        startRefreshing()
        startFlashing()
    }

    deinit {
        refreshTask?.cancel()
        flashTask?.cancel()
    }

    var isAlerting: Bool {
        guard let currentRate else {
            return false
        }
        return currentRate < lowerThreshold || currentRate > upperThreshold
    }

    var alertColor: Color {
        guard currentRate != nil else {
            return .clear
        }
        if isAlerting && flashOn {
            return Color.red.opacity(0.95)
        }
        return .clear
    }

    func setLowerThreshold(_ value: Double) {
        lowerThreshold = value
        UserDefaults.standard.set(value, forKey: DefaultsKey.lowThreshold)
    }

    func setUpperThreshold(_ value: Double) {
        upperThreshold = value
        UserDefaults.standard.set(value, forKey: DefaultsKey.highThreshold)
    }

    func refreshNow() {
        apply(snapshot: store.loadSnapshot())
    }

    private func startRefreshing() {
        refreshNow()
        refreshTask = Task {
            while !Task.isCancelled {
                apply(snapshot: store.loadSnapshot())
                try? await Task.sleep(for: .seconds(1))
            }
        }
    }

    private func startFlashing() {
        flashTask = Task {
            while !Task.isCancelled {
                if isAlerting {
                    flashOn.toggle()
                } else if flashOn {
                    flashOn = false
                }
                try? await Task.sleep(for: .milliseconds(450))
            }
        }
    }

    private func apply(snapshot: BreathingSnapshot) {
        databasePath = snapshot.databasePath ?? "Database not found"
        history = snapshot.points.map(\.rate)
        currentRate = snapshot.points.last?.rate
    }
}

private struct SparklineView: View {
    let values: [Double]
    let strokeColor: Color

    var body: some View {
        Canvas { context, size in
            guard values.count > 1 else {
                return
            }

            let minValue = values.min() ?? 0
            let maxValue = values.max() ?? 1
            let range = max(maxValue - minValue, 0.5)
            let stepX = size.width / CGFloat(values.count - 1)

            var path = Path()
            for (index, value) in values.enumerated() {
                let x = CGFloat(index) * stepX
                let normalized = (value - minValue) / range
                let y = size.height - (CGFloat(normalized) * size.height)
                if index == 0 {
                    path.move(to: CGPoint(x: x, y: y))
                } else {
                    path.addLine(to: CGPoint(x: x, y: y))
                }
            }

            context.stroke(
                path,
                with: .color(strokeColor),
                style: StrokeStyle(lineWidth: 1.6, lineCap: .round, lineJoin: .round)
            )
        }
        .frame(height: 16)
    }
}

private struct StatusBarView: View {
    @ObservedObject var model: BreathingBarModel

    var body: some View {
        HStack(spacing: 6) {
            Text(rateText)
                .font(.system(size: 12, weight: .semibold, design: .rounded))
                .monospacedDigit()
            SparklineView(values: model.history, strokeColor: sparklineColor)
                .frame(width: 40, height: 14)
        }
        .padding(.horizontal, 8)
        .padding(.vertical, 3)
        .background(
            RoundedRectangle(cornerRadius: 6, style: .continuous)
                .fill(model.alertColor)
        )
        .overlay(
            RoundedRectangle(cornerRadius: 6, style: .continuous)
                .stroke(model.isAlerting ? Color.red.opacity(0.9) : .clear, lineWidth: 1)
        )
        .foregroundStyle(model.isAlerting ? Color.white : Color.primary)
        .id(model.isAlerting ? "alert-\(model.flashOn)" : "steady")
    }

    private var rateText: String {
        guard let currentRate = model.currentRate else {
            return "--.-"
        }
        return String(format: "%.1f", currentRate)
    }

    private var sparklineColor: Color {
        model.isAlerting ? .white : .accentColor
    }
}

private struct MenuContentView: View {
    @ObservedObject var model: BreathingBarModel

    var body: some View {
        VStack(alignment: .leading, spacing: 14) {
            VStack(alignment: .leading, spacing: 4) {
                Text("Breathing Rate")
                    .font(.headline)
                Text(currentRateText)
                    .font(.system(size: 32, weight: .bold, design: .rounded))
                    .monospacedDigit()
            }

            SparklineView(values: model.history, strokeColor: .accentColor)
                .frame(height: 42)

            VStack(alignment: .leading, spacing: 8) {
                Stepper(
                    value: Binding(
                        get: { model.lowerThreshold },
                        set: { model.setLowerThreshold($0) }
                    ),
                    in: 4.0...20.0,
                    step: 0.5
                ) {
                    Text("Low flash threshold: \(model.lowerThreshold, specifier: "%.1f") br/min")
                }

                Stepper(
                    value: Binding(
                        get: { model.upperThreshold },
                        set: { model.setUpperThreshold($0) }
                    ),
                    in: 10.0...40.0,
                    step: 0.5
                ) {
                    Text("High flash threshold: \(model.upperThreshold, specifier: "%.1f") br/min")
                }
            }

            Text(model.databasePath)
                .font(.caption2)
                .foregroundStyle(.secondary)
                .textSelection(.enabled)

            HStack {
                Button("Refresh") {
                    model.refreshNow()
                }
                Spacer()
                Button("Quit") {
                    NSApplication.shared.terminate(nil)
                }
            }
        }
        .padding(14)
        .frame(width: 340)
    }

    private var currentRateText: String {
        guard let currentRate = model.currentRate else {
            return "--.- br/min"
        }
        return "\(String(format: "%.1f", currentRate)) br/min"
    }
}

private final class AppDelegate: NSObject, NSApplicationDelegate {
    func applicationDidFinishLaunching(_ notification: Notification) {
        NSApplication.shared.setActivationPolicy(.accessory)
    }
}

@main
private struct BreathingBarApp: App {
    @NSApplicationDelegateAdaptor(AppDelegate.self) private var appDelegate
    @StateObject private var model = BreathingBarModel()

    var body: some Scene {
        MenuBarExtra {
            MenuContentView(model: model)
        } label: {
            StatusBarView(model: model)
        }
        .menuBarExtraStyle(.window)
    }
}
