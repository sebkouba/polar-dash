import AppKit
import Combine
import Foundation
import SQLite3
import SwiftUI

private enum DefaultsKey {
    static let lowThreshold = "lowThreshold"
    static let highThreshold = "highThreshold"
}

private struct HealthSnapshot {
    let databasePath: String?
    let breathingRate: Double?
    let heartRate: Double?
    let heartRateVariabilityMs: Double?
}

private final class SQLiteBreathingStore {
    struct HRFrame {
        let recordedAtNs: Int64
        let averageHeartRateBpm: Double
        let rrIntervalsMs: [Double]
    }

    func loadSnapshot(limitHRFrames: Int = 24) -> HealthSnapshot {
        guard let databaseURL = resolveDatabaseURL() else {
            return HealthSnapshot(
                databasePath: nil,
                breathingRate: nil,
                heartRate: nil,
                heartRateVariabilityMs: nil,
            )
        }

        var handle: OpaquePointer?
        guard sqlite3_open_v2(databaseURL.path, &handle, SQLITE_OPEN_READONLY, nil) == SQLITE_OK else {
            if let handle {
                sqlite3_close(handle)
            }
            return HealthSnapshot(
                databasePath: databaseURL.path,
                breathingRate: nil,
                heartRate: nil,
                heartRateVariabilityMs: nil,
            )
        }
        defer { sqlite3_close(handle) }

        guard let sessionId = resolveActiveSessionId(handle) else {
            return HealthSnapshot(
                databasePath: databaseURL.path,
                breathingRate: nil,
                heartRate: nil,
                heartRateVariabilityMs: nil,
            )
        }

        let breathingRateQuery = """
        SELECT breaths_per_min
        FROM breathing_estimates
        WHERE session_id = ?
        ORDER BY estimated_at_ns DESC
        LIMIT 1;
        """

        var breathingStatement: OpaquePointer?
        guard sqlite3_prepare_v2(handle, breathingRateQuery, -1, &breathingStatement, nil) == SQLITE_OK else {
            if let breathingStatement {
                sqlite3_finalize(breathingStatement)
            }
            return HealthSnapshot(
                databasePath: databaseURL.path,
                breathingRate: nil,
                heartRate: nil,
                heartRateVariabilityMs: nil,
            )
        }
        defer { sqlite3_finalize(breathingStatement) }

        sqlite3_bind_int64(breathingStatement, 1, sessionId)
        var breathingRate: Double?
        if sqlite3_step(breathingStatement) == SQLITE_ROW {
            breathingRate = sqlite3_column_double(breathingStatement, 0)
        }

        let hrQuery = """
        SELECT recorded_at_ns, average_hr_bpm, rr_intervals_ms_json
        FROM hr_frames
        WHERE session_id = ?
        ORDER BY recorded_at_ns DESC
        LIMIT ?;
        """

        var hrStatement: OpaquePointer?
        guard sqlite3_prepare_v2(handle, hrQuery, -1, &hrStatement, nil) == SQLITE_OK else {
            return HealthSnapshot(
                databasePath: databaseURL.path,
                breathingRate: breathingRate,
                heartRate: nil,
                heartRateVariabilityMs: nil,
            )
        }
        defer { sqlite3_finalize(hrStatement) }

        sqlite3_bind_int64(hrStatement, 1, sessionId)
        sqlite3_bind_int(hrStatement, 2, Int32(limitHRFrames))

        var heartRate: Double?
        var hrFrames: [HRFrame] = []

        while sqlite3_step(hrStatement) == SQLITE_ROW {
            let recordedAtNs = sqlite3_column_int64(hrStatement, 0)
            let averageHrBpm = sqlite3_column_double(hrStatement, 1)
            let rrIntervals = parseRRIntervals(from: hrStatement!, column: 2)
            if heartRate == nil {
                heartRate = averageHrBpm
            }
            hrFrames.append(
                HRFrame(
                    recordedAtNs: recordedAtNs,
                    averageHeartRateBpm: averageHrBpm,
                    rrIntervalsMs: rrIntervals,
                )
            )
        }

        return HealthSnapshot(
            databasePath: databaseURL.path,
            breathingRate: breathingRate,
            heartRate: heartRate,
            heartRateVariabilityMs: computeHeartRateVariabilityMs(from: hrFrames),
        )
    }

    private func resolveActiveSessionId(_ handle: OpaquePointer?) -> Int64? {
        let query = """
        SELECT id FROM sessions
        ORDER BY started_at_ns DESC
        LIMIT 1;
        """

        var statement: OpaquePointer?
        guard sqlite3_prepare_v2(handle, query, -1, &statement, nil) == SQLITE_OK else {
            if let statement {
                sqlite3_finalize(statement)
            }
            return nil
        }
        defer { sqlite3_finalize(statement) }

        if sqlite3_step(statement) == SQLITE_ROW {
            return sqlite3_column_int64(statement, 0)
        }

        return nil
    }

    private func parseRRIntervals(from statement: OpaquePointer, column: Int32) -> [Double] {
        guard let rawText = sqlite3_column_text(statement, column) else {
            return []
        }
        let rawLength = Int(sqlite3_column_bytes(statement, column))
        guard rawLength > 0 else {
            return []
        }
        let rawData = Data(bytes: rawText, count: rawLength)
        guard let rawString = String(data: rawData, encoding: .utf8) else {
            return []
        }

        guard let data = rawString.data(using: String.Encoding.utf8),
              let parsed = try? JSONSerialization.jsonObject(with: data) else {
            return []
        }

        guard let values = parsed as? [Any] else {
            return []
        }

        return values.compactMap { value -> Double? in
            switch value {
            case let value as Double:
                return value
            case let value as Int:
                return Double(value)
            case let value as NSNumber:
                return value.doubleValue
            default:
                return nil
            }
        }
    }

    private func computeHeartRateVariabilityMs(from frames: [HRFrame]) -> Double? {
        guard !frames.isEmpty else {
            return nil
        }

        var rrValuesMs: [Double] = []
        var beatTimesNs: [Int64] = []

        for frame in frames.reversed() {
            let intervals = frame.rrIntervalsMs.filter { $0 > 0 }
            if intervals.isEmpty {
                continue
            }

            let frameTotalNs = Int64(intervals.reduce(0.0, +) * 1_000_000)
            var currentBeatNs = frame.recordedAtNs - frameTotalNs
            for intervalMs in intervals {
                currentBeatNs += Int64(intervalMs * 1_000_000)
                rrValuesMs.append(intervalMs)
                beatTimesNs.append(currentBeatNs)
            }
        }

        guard let latestBeatNs = beatTimesNs.last else {
            return nil
        }
        guard rrValuesMs.count >= 5 else {
            return nil
        }

        let windowNs: Int64 = 60_000_000_000
        var startIndex = 0
        while latestBeatNs - beatTimesNs[startIndex] > windowNs, startIndex + 1 < beatTimesNs.count {
            startIndex += 1
        }
        let windowRR = Array(rrValuesMs[startIndex...])
        guard windowRR.count >= 5 else {
            return nil
        }

        var sumSquares: Double = 0
        for index in 1..<windowRR.count {
            let delta = windowRR[index] - windowRR[index - 1]
            sumSquares += delta * delta
        }
        let sampleCount = Double(windowRR.count - 1)
        guard sampleCount > 0 else {
            return nil
        }
        return sqrt(sumSquares / sampleCount)
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
    @Published var currentBreathingRate: Double?
    @Published var currentHeartRate: Double?
    @Published var currentHeartRateVariability: Double?
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
        guard let currentBreathingRate else {
            return false
        }
        return currentBreathingRate < lowerThreshold || currentBreathingRate > upperThreshold
    }

    var alertColor: Color {
        guard currentBreathingRate != nil else {
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

    private func apply(snapshot: HealthSnapshot) {
        databasePath = snapshot.databasePath ?? "Database not found"
        currentBreathingRate = snapshot.breathingRate
        currentHeartRate = snapshot.heartRate
        currentHeartRateVariability = snapshot.heartRateVariabilityMs
    }
}

private final class PassthroughHostingView<Content: View>: NSHostingView<Content> {
    override func hitTest(_ point: NSPoint) -> NSView? {
        nil
    }
}

private struct StatusBarView: View {
    @ObservedObject var model: BreathingBarModel

    var body: some View {
        HStack(spacing: 8) {
            Text(breathingRateText)
                .font(.system(size: 12, weight: .semibold, design: .rounded))
                .monospacedDigit()
                .foregroundStyle(isActivelyFlashing ? Color.white : Color.blue)
                .lineLimit(1)
            Text(heartRateText)
                .font(.system(size: 12, weight: .semibold, design: .rounded))
                .monospacedDigit()
                .foregroundStyle(isActivelyFlashing ? Color.white : Color.red)
                .lineLimit(1)
            Text(hrvText)
                .font(.system(size: 11, weight: .regular, design: .rounded))
                .foregroundStyle(isActivelyFlashing ? Color.white : Color.green)
                .lineLimit(1)
        }
        .padding(.horizontal, 8)
        .padding(.vertical, 3)
        .foregroundStyle(isActivelyFlashing ? Color.white : Color.primary)
    }

    private var breathingRateText: String {
        guard let currentBreathingRate = model.currentBreathingRate else {
            return "--.- br/min"
        }
        return "\(String(format: "%.1f", currentBreathingRate)) br/min"
    }

    private var heartRateText: String {
        guard let currentHeartRate = model.currentHeartRate else {
            return "-- bpm"
        }
        return "\(String(format: "%.0f", currentHeartRate)) bpm"
    }

    private var hrvText: String {
        guard let hrv = model.currentHeartRateVariability else {
            return "-- ms"
        }
        return "\(String(format: "%.0f", hrv)) ms"
    }

    private var isActivelyFlashing: Bool {
        model.isAlerting && model.flashOn
    }
}

private struct MenuContentView: View {
    @ObservedObject var model: BreathingBarModel

    var body: some View {
        VStack(alignment: .leading, spacing: 14) {
            VStack(alignment: .leading, spacing: 4) {
                HStack(alignment: .firstTextBaseline, spacing: 18) {
                    metricView(
                        title: "Breathing Rate",
                        text: currentBreathingRateText,
                        color: flashingColor(for: .blue)
                    )
                    metricView(
                        title: "Heart Rate",
                        text: currentHeartRateText,
                        color: flashingColor(for: .red)
                    )
                    metricView(
                        title: "HRV (RMSSD)",
                        text: currentHeartRateVariabilityText,
                        color: flashingColor(for: .green)
                    )
                }
                Divider()
                Text("Breathing flash threshold uses most recent breathing-rate estimate.")
                    .font(.caption)
                    .foregroundStyle(.secondary)
            }

            VStack(alignment: .leading, spacing: 6) {
                Stepper(
                    value: Binding(
                        get: { model.lowerThreshold },
                        set: { value in
                            model.setLowerThreshold(value)
                        }
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

    @ViewBuilder
    private func metricView(title: String, text: String, color: Color) -> some View {
        VStack(alignment: .leading, spacing: 2) {
            Text(title)
                .font(.subheadline.weight(.semibold))
            Text(text)
                .font(.system(size: 32, weight: .bold, design: .rounded))
                .monospacedDigit()
                .foregroundStyle(color)
        }
    }

    private var currentHeartRateText: String {
        guard let currentHeartRate = model.currentHeartRate else {
            return "--.- bpm"
        }
        return "\(String(format: "%.1f", currentHeartRate)) bpm"
    }

    private var currentHeartRateVariabilityText: String {
        guard let currentHeartRateVariability = model.currentHeartRateVariability else {
            return "--.- ms"
        }
        return "\(String(format: "%.1f", currentHeartRateVariability)) ms"
    }

    private var currentBreathingRateText: String {
        guard let currentBreathingRate = model.currentBreathingRate else {
            return "--.- br/min"
        }
        return "\(String(format: "%.1f", currentBreathingRate)) br/min"
    }

    private func flashingColor(for color: Color) -> Color {
        return model.isAlerting && model.flashOn ? .white : color
    }
}

@MainActor
private final class StatusItemController: NSObject {
    private let model: BreathingBarModel
    private let statusItem: NSStatusItem
    private let popover = NSPopover()
    private let hostingView: PassthroughHostingView<StatusBarView>
    private var cancellables: Set<AnyCancellable> = []

    init(model: BreathingBarModel) {
        self.model = model
        statusItem = NSStatusBar.system.statusItem(withLength: NSStatusItem.variableLength)
        hostingView = PassthroughHostingView(rootView: StatusBarView(model: model))
        super.init()
        configureStatusItem()
        configurePopover()
        subscribe()
        render()
    }

    private func configureStatusItem() {
        guard let button = statusItem.button else {
            return
        }

        button.title = ""
        button.image = nil
        button.action = #selector(togglePopover(_:))
        button.target = self
        button.sendAction(on: [.leftMouseUp])
        button.wantsLayer = true
        button.layer?.cornerRadius = 6
        button.layer?.masksToBounds = true

        hostingView.translatesAutoresizingMaskIntoConstraints = false
        hostingView.setFrameSize(hostingView.fittingSize)
        button.addSubview(hostingView)

        NSLayoutConstraint.activate([
            hostingView.leadingAnchor.constraint(equalTo: button.leadingAnchor, constant: 2),
            hostingView.trailingAnchor.constraint(equalTo: button.trailingAnchor, constant: -2),
            hostingView.topAnchor.constraint(equalTo: button.topAnchor, constant: 1),
            hostingView.bottomAnchor.constraint(equalTo: button.bottomAnchor, constant: -1),
        ])
    }

    private func configurePopover() {
        popover.behavior = .transient
        popover.contentSize = NSSize(width: 340, height: 240)
        popover.contentViewController = NSHostingController(rootView: MenuContentView(model: model))
    }

    private func subscribe() {
        model.objectWillChange
            .sink { [weak self] _ in
                DispatchQueue.main.async {
                    self?.render()
                }
            }
            .store(in: &cancellables)
    }

    private func render() {
        hostingView.rootView = StatusBarView(model: model)
        let fitting = hostingView.fittingSize
        statusItem.length = max(64, fitting.width + 6)

        guard let button = statusItem.button else {
            return
        }

        if model.isAlerting && model.flashOn {
            button.layer?.backgroundColor = NSColor.systemRed.withAlphaComponent(0.95).cgColor
        } else {
            button.layer?.backgroundColor = NSColor.clear.cgColor
        }
        button.needsDisplay = true
    }

    @objc
    private func togglePopover(_ sender: AnyObject?) {
        guard let button = statusItem.button else {
            return
        }

        if popover.isShown {
            popover.performClose(sender)
            return
        }

        popover.contentViewController = NSHostingController(rootView: MenuContentView(model: model))
        popover.show(relativeTo: button.bounds, of: button, preferredEdge: .minY)
        popover.contentViewController?.view.window?.becomeKey()
    }
}

private final class AppDelegate: NSObject, NSApplicationDelegate {
    private var statusController: StatusItemController?

    func applicationDidFinishLaunching(_ notification: Notification) {
        NSApplication.shared.setActivationPolicy(.accessory)
        statusController = StatusItemController(model: BreathingBarModel())
    }
}

@main
private struct BreathingBarApp: App {
    @NSApplicationDelegateAdaptor(AppDelegate.self) private var appDelegate

    var body: some Scene {
        Settings {
            EmptyView()
        }
    }
}
