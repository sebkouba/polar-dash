import AppKit
import Charts
import Combine
import SwiftUI

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

private enum HistoryMetric: String, CaseIterable, Identifiable {
    case breathingRate
    case heartRate
    case heartRateVariability

    var id: String { rawValue }

    var title: String {
        switch self {
        case .breathingRate:
            return "Breathing Rate"
        case .heartRate:
            return "Heart Rate"
        case .heartRateVariability:
            return "HRV (RMSSD)"
        }
    }

    var color: Color {
        switch self {
        case .breathingRate:
            return .blue
        case .heartRate:
            return .red
        case .heartRateVariability:
            return .green
        }
    }

    var storageKey: String {
        switch self {
        case .breathingRate:
            return "historyShowsBreathingRate"
        case .heartRate:
            return "historyShowsHeartRate"
        case .heartRateVariability:
            return "historyShowsHeartRateVariability"
        }
    }
}

private struct BreathingAxisTick: Identifiable {
    let actualValue: Double
    let projectedValue: Double

    var id: Double { projectedValue }
}

private struct MenuContentView: View {
    private enum Page {
        case dashboard
        case settings
    }

    @ObservedObject var model: BreathingBarModel
    @State private var page: Page = .dashboard
    @AppStorage(HistoryMetric.breathingRate.storageKey) private var showsBreathingRate = true
    @AppStorage(HistoryMetric.heartRate.storageKey) private var showsHeartRate = true
    @AppStorage(HistoryMetric.heartRateVariability.storageKey) private var showsHeartRateVariability = true

    var body: some View {
        VStack(alignment: .leading, spacing: 16) {
            if page == .dashboard {
                dashboardPage
            } else {
                settingsPage
            }
        }
        .padding(14)
        .frame(width: 720)
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
        model.isAlerting && model.flashOn ? .white : color
    }

    private var isAnyMetricVisible: Bool {
        showsBreathingRate || showsHeartRate || showsHeartRateVariability
    }

    private var hasVisibleLeftMetrics: Bool {
        showsHeartRate || showsHeartRateVariability
    }

    private var breathingAxisDefaultDomain: ClosedRange<Double> {
        let lower = max(0.0, model.lowerThreshold - 2.0)
        let upper = min(40.0, max(lower + 8.0, model.upperThreshold + 2.0))
        return lower...upper
    }

    private var leftAxisValues: [Double] {
        model.historySamples.reduce(into: []) { values, sample in
            if showsHeartRate, let heartRate = sample.heartRate {
                values.append(heartRate)
            }
            if showsHeartRateVariability, let hrvRMSSD = sample.hrvRMSSD {
                values.append(hrvRMSSD)
            }
        }
    }

    private var breathingAxisValues: [Double] {
        guard showsBreathingRate else {
            return []
        }
        return model.historySamples.compactMap(\.breathingRate)
    }

    private var leftAxisDomain: ClosedRange<Double> {
        metricDomain(
            for: leftAxisValues,
            default: 0.0...200.0,
            hardLower: 0.0,
            hardUpper: 200.0,
            minimumSpan: 20.0
        )
    }

    private var breathingAxisDomain: ClosedRange<Double> {
        metricDomain(
            for: breathingAxisValues,
            default: breathingAxisDefaultDomain,
            hardLower: 0.0,
            hardUpper: 40.0,
            minimumSpan: 8.0
        )
    }

    private var breathingAxisTicks: [BreathingAxisTick] {
        guard showsBreathingRate else {
            return []
        }

        let domain = breathingAxisDomain
        let step = niceAxisStep(for: domain.upperBound - domain.lowerBound)
        let firstTick = ceil(domain.lowerBound / step) * step
        let epsilon = step * 0.25
        var ticks: [BreathingAxisTick] = []
        var value = firstTick

        while value <= domain.upperBound + epsilon {
            ticks.append(
                BreathingAxisTick(
                    actualValue: value,
                    projectedValue: projectedBreathingValue(for: value)
                )
            )
            value += step
        }

        if ticks.isEmpty {
            let midpoint = (domain.lowerBound + domain.upperBound) / 2.0
            return [
                BreathingAxisTick(
                    actualValue: midpoint,
                    projectedValue: projectedBreathingValue(for: midpoint)
                )
            ]
        }

        return ticks
    }

    private var dashboardPage: some View {
        VStack(alignment: .leading, spacing: 14) {
            HStack(alignment: .top) {
                VStack(alignment: .leading, spacing: 4) {
                    HStack(alignment: .firstTextBaseline, spacing: 18) {
                        metricView(title: "Breathing Rate", text: currentBreathingRateText, color: flashingColor(for: .blue))
                        metricView(title: "Heart Rate", text: currentHeartRateText, color: flashingColor(for: .red))
                        metricView(title: "HRV (RMSSD)", text: currentHeartRateVariabilityText, color: flashingColor(for: .green))
                    }
                    Divider()
                    Text(model.runtimeStatus)
                        .font(.caption)
                        .foregroundStyle(.secondary)
                    Text(model.connectionDescription)
                        .font(.caption)
                        .foregroundStyle(.secondary)
                    Text(model.calibrationDescription)
                        .font(.caption)
                        .foregroundStyle(.secondary)
                }
                Spacer()
                Button {
                    page = .settings
                } label: {
                    Image(systemName: "gearshape")
                }
                .buttonStyle(.borderless)
            }

            historyPanel

            HStack {
                Button("Reconnect") {
                    model.reconnect()
                }
                Spacer()
                Button("Quit") {
                    NSApplication.shared.terminate(nil)
                }
            }
        }
    }

    private var historyPanel: some View {
        VStack(alignment: .leading, spacing: 10) {
            HStack {
                Text("History")
                    .font(.headline)
                Spacer()
                Button {
                    model.selectPreviousDay()
                } label: {
                    Image(systemName: "chevron.left")
                }
                .buttonStyle(.bordered)
                Text(model.selectedDayTitle)
                    .font(.subheadline.weight(.semibold))
                    .frame(minWidth: 120)
                Button {
                    model.selectNextDay()
                } label: {
                    Image(systemName: "chevron.right")
                }
                .buttonStyle(.bordered)
                .disabled(!model.canSelectNextDay)
            }

            HStack {
                Stepper(
                    value: Binding(
                        get: { model.historyWindowHours },
                        set: { model.setHistoryWindowHours($0) }
                    ),
                    in: 1...24,
                    step: 1
                ) {
                    Text("Window: \(model.historyWindowLabel)")
                }
                .disabled(model.historyShowsAllDay)

                Button(model.historyShowsAllDay ? "Use Hours" : "All Day") {
                    model.setHistoryShowsAllDay(!model.historyShowsAllDay)
                }
                .buttonStyle(.bordered)

                Spacer()

                Text(model.historyStorageDescription)
                    .font(.caption)
                    .foregroundStyle(.secondary)
            }

            if model.historySamples.isEmpty {
                VStack(alignment: .leading, spacing: 6) {
                    Text("No graph data for this period.")
                        .font(.subheadline.weight(.semibold))
                    Text(model.historyRetentionDays == 0 ? "Set retention above 0 days in Settings to save derived graph history." : "Keep the app running on the selected day to populate history.")
                        .font(.caption)
                        .foregroundStyle(.secondary)
                }
                .frame(maxWidth: .infinity, minHeight: 220, alignment: .leading)
                .padding(14)
                .background(.quaternary.opacity(0.15), in: RoundedRectangle(cornerRadius: 14, style: .continuous))
            } else {
                VStack(alignment: .leading, spacing: 12) {
                    if isAnyMetricVisible {
                        Chart {
                            ForEach(model.historySamples, id: \.sampledAt) { sample in
                                if showsBreathingRate, let breathingRate = sample.breathingRate {
                                    LineMark(
                                        x: .value("Time", sample.sampledAt),
                                        y: .value("Breathing Projection", projectedBreathingValue(for: breathingRate))
                                    )
                                    .foregroundStyle(HistoryMetric.breathingRate.color)
                                    .interpolationMethod(.catmullRom)
                                }
                                if showsHeartRate, let heartRate = sample.heartRate {
                                    LineMark(
                                        x: .value("Time", sample.sampledAt),
                                        y: .value("Heart Rate", heartRate)
                                    )
                                    .foregroundStyle(HistoryMetric.heartRate.color)
                                    .interpolationMethod(.catmullRom)
                                }
                                if showsHeartRateVariability, let hrvRMSSD = sample.hrvRMSSD {
                                    LineMark(
                                        x: .value("Time", sample.sampledAt),
                                        y: .value("HRV (RMSSD)", hrvRMSSD)
                                    )
                                    .foregroundStyle(HistoryMetric.heartRateVariability.color)
                                    .interpolationMethod(.catmullRom)
                                }
                            }
                        }
                        .chartXScale(domain: model.historyGraphRange)
                        .chartYScale(domain: leftAxisDomain)
                        .chartPlotStyle { plotArea in
                            plotArea.clipped()
                        }
                        .chartYAxis {
                            if hasVisibleLeftMetrics {
                                AxisMarks(position: .leading)
                            }
                            if showsBreathingRate {
                                AxisMarks(position: .trailing, values: breathingAxisTicks.map(\.projectedValue)) { value in
                                    if
                                        let projectedValue = value.as(Double.self),
                                        let tick = breathingAxisTick(for: projectedValue)
                                    {
                                        AxisTick()
                                        AxisValueLabel {
                                            Text(axisLabel(for: tick.actualValue))
                                        }
                                    }
                                }
                            }
                        }
                        .frame(height: 260)
                    } else {
                        VStack(alignment: .leading, spacing: 6) {
                            Text("All graph lines are hidden.")
                                .font(.subheadline.weight(.semibold))
                            Text("Click a legend item below to show a series again.")
                                .font(.caption)
                                .foregroundStyle(.secondary)
                        }
                        .frame(maxWidth: .infinity, minHeight: 260, alignment: .leading)
                    }

                    HStack(spacing: 14) {
                        ForEach(HistoryMetric.allCases) { metric in
                            Button {
                                toggleHistoryMetric(metric)
                            } label: {
                                HStack(spacing: 8) {
                                    Circle()
                                        .fill(metric.color)
                                        .frame(width: 10, height: 10)
                                    Text(metric.title)
                                        .strikethrough(!isMetricVisible(metric), color: .secondary)
                                }
                                .foregroundStyle(isMetricVisible(metric) ? Color.primary : Color.secondary)
                                .opacity(isMetricVisible(metric) ? 1.0 : 0.45)
                            }
                            .buttonStyle(.plain)
                        }
                    }
                }
                .padding(10)
                .background(.quaternary.opacity(0.15), in: RoundedRectangle(cornerRadius: 14, style: .continuous))
            }
        }
    }

    private func isMetricVisible(_ metric: HistoryMetric) -> Bool {
        switch metric {
        case .breathingRate:
            return showsBreathingRate
        case .heartRate:
            return showsHeartRate
        case .heartRateVariability:
            return showsHeartRateVariability
        }
    }

    private func toggleHistoryMetric(_ metric: HistoryMetric) {
        switch metric {
        case .breathingRate:
            showsBreathingRate.toggle()
        case .heartRate:
            showsHeartRate.toggle()
        case .heartRateVariability:
            showsHeartRateVariability.toggle()
        }
    }

    private func projectedBreathingValue(for breathingRate: Double) -> Double {
        project(value: breathingRate, from: breathingAxisDomain, to: leftAxisDomain)
    }

    private func breathingAxisTick(for projectedValue: Double) -> BreathingAxisTick? {
        breathingAxisTicks.first { abs($0.projectedValue - projectedValue) < 0.001 }
    }

    private func axisLabel(for value: Double) -> String {
        value.formatted(.number.precision(.fractionLength(0)))
    }

    private func metricDomain(
        for values: [Double],
        default defaultDomain: ClosedRange<Double>,
        hardLower: Double,
        hardUpper: Double,
        minimumSpan: Double
    ) -> ClosedRange<Double> {
        guard let minimum = values.min(), let maximum = values.max() else {
            return normalizedDomain(defaultDomain, hardLower: hardLower, hardUpper: hardUpper, minimumSpan: minimumSpan)
        }

        let span = maximum - minimum
        let padding = max(span * 0.18, minimumSpan * 0.35)
        let candidateDomain: ClosedRange<Double>

        if span < 0.001 {
            candidateDomain = (minimum - minimumSpan / 2.0)...(maximum + minimumSpan / 2.0)
        } else {
            candidateDomain = (minimum - padding)...(maximum + padding)
        }

        return normalizedDomain(candidateDomain, hardLower: hardLower, hardUpper: hardUpper, minimumSpan: minimumSpan)
    }

    private func normalizedDomain(
        _ domain: ClosedRange<Double>,
        hardLower: Double,
        hardUpper: Double,
        minimumSpan: Double
    ) -> ClosedRange<Double> {
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

    private func project(
        value: Double,
        from sourceDomain: ClosedRange<Double>,
        to targetDomain: ClosedRange<Double>
    ) -> Double {
        let clampedValue = min(max(value, sourceDomain.lowerBound), sourceDomain.upperBound)
        let sourceSpan = sourceDomain.upperBound - sourceDomain.lowerBound
        guard sourceSpan > 0 else {
            return targetDomain.lowerBound
        }
        let progress = (clampedValue - sourceDomain.lowerBound) / sourceSpan
        return targetDomain.lowerBound + progress * (targetDomain.upperBound - targetDomain.lowerBound)
    }

    private func niceAxisStep(for span: Double) -> Double {
        let roughStep = max(span / 4.0, 1.0)
        let magnitude = pow(10.0, floor(log10(roughStep)))
        let normalized = roughStep / magnitude

        let stepMultiplier: Double
        switch normalized {
        case ..<1.5:
            stepMultiplier = 1.0
        case ..<3.5:
            stepMultiplier = 2.0
        case ..<7.5:
            stepMultiplier = 5.0
        default:
            stepMultiplier = 10.0
        }

        return stepMultiplier * magnitude
    }

    private var settingsPage: some View {
        VStack(alignment: .leading, spacing: 14) {
            HStack {
                Button {
                    page = .dashboard
                } label: {
                    Label("Back", systemImage: "chevron.left")
                }
                .buttonStyle(.borderless)
                Spacer()
                Text("Settings")
                    .font(.headline)
                Spacer()
                Color.clear.frame(width: 44, height: 1)
            }

            VStack(alignment: .leading, spacing: 8) {
                Stepper(
                    value: Binding(get: { model.lowerThreshold }, set: { model.setLowerThreshold($0) }),
                    in: 4.0...20.0,
                    step: 0.5
                ) {
                    Text("Low flash threshold: \(model.lowerThreshold, specifier: "%.1f") br/min")
                }

                Stepper(
                    value: Binding(get: { model.upperThreshold }, set: { model.setUpperThreshold($0) }),
                    in: 10.0...40.0,
                    step: 0.5
                ) {
                    Text("High flash threshold: \(model.upperThreshold, specifier: "%.1f") br/min")
                }
            }
            .padding(14)
            .background(.quaternary.opacity(0.15), in: RoundedRectangle(cornerRadius: 14, style: .continuous))

            VStack(alignment: .leading, spacing: 8) {
                Stepper(
                    value: Binding(
                        get: { model.historyRetentionDays },
                        set: { model.setHistoryRetentionDays($0) }
                    ),
                    in: 0...1000,
                    step: 1
                ) {
                    Text("History retention: \(model.historyRetentionLabel)")
                }

                Text("Set to 0 to disable graph history storage. Only derived breathing, heart-rate, and HRV points are saved.")
                    .font(.caption)
                    .foregroundStyle(.secondary)
            }
            .padding(14)
            .background(.quaternary.opacity(0.15), in: RoundedRectangle(cornerRadius: 14, style: .continuous))

            Spacer(minLength: 0)

            HStack {
                Button("Reconnect") {
                    model.reconnect()
                }
                Spacer()
                Button("Quit") {
                    NSApplication.shared.terminate(nil)
                }
            }
        }
    }
}

@MainActor
private final class StatusItemController: NSObject, NSPopoverDelegate {
    private let model: BreathingBarModel
    private let statusItem: NSStatusItem
    private let popover = NSPopover()
    private let hostingView: PassthroughHostingView<StatusBarView>
    private var globalMouseMonitor: Any?
    private var localMouseMonitor: Any?
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
        popover.delegate = self
        popover.contentSize = NSSize(width: 720, height: 560)
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
            closePopover(sender)
            return
        }
        popover.contentViewController = NSHostingController(rootView: MenuContentView(model: model))
        popover.show(relativeTo: button.bounds, of: button, preferredEdge: .minY)
        popover.contentViewController?.view.window?.becomeKey()
        startPopoverEventMonitors()
    }

    func popoverDidClose(_ notification: Notification) {
        stopPopoverEventMonitors()
    }

    private func closePopover(_ sender: AnyObject? = nil) {
        guard popover.isShown else {
            return
        }
        popover.performClose(sender)
    }

    private func startPopoverEventMonitors() {
        guard localMouseMonitor == nil, globalMouseMonitor == nil else {
            return
        }
        globalMouseMonitor = NSEvent.addGlobalMonitorForEvents(matching: [.leftMouseDown, .rightMouseDown]) { [weak self] _ in
            Task { @MainActor [weak self] in
                self?.closePopover()
            }
        }
        localMouseMonitor = NSEvent.addLocalMonitorForEvents(matching: [.leftMouseDown, .rightMouseDown]) { [weak self] event in
            guard let self else {
                return event
            }
            guard self.popover.isShown else {
                return event
            }
            let popoverWindow = self.popover.contentViewController?.view.window
            let statusWindow = self.statusItem.button?.window
            if event.window !== popoverWindow && event.window !== statusWindow {
                self.closePopover()
            }
            return event
        }
    }

    private func stopPopoverEventMonitors() {
        if let globalMouseMonitor {
            NSEvent.removeMonitor(globalMouseMonitor)
            self.globalMouseMonitor = nil
        }
        guard let localMouseMonitor else {
            return
        }
        NSEvent.removeMonitor(localMouseMonitor)
        self.localMouseMonitor = nil
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
