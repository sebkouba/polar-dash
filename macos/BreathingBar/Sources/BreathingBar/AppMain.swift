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

private struct MenuContentView: View {
    private enum Page {
        case dashboard
        case settings
    }

    @ObservedObject var model: BreathingBarModel
    @State private var page: Page = .dashboard

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
                Chart {
                    ForEach(model.historySamples, id: \.sampledAt) { sample in
                        if let breathingRate = sample.breathingRate {
                            LineMark(
                                x: .value("Time", sample.sampledAt),
                                y: .value("Value", breathingRate)
                            )
                            .foregroundStyle(by: .value("Metric", "Breathing Rate"))
                            .interpolationMethod(.catmullRom)
                        }
                        if let heartRate = sample.heartRate {
                            LineMark(
                                x: .value("Time", sample.sampledAt),
                                y: .value("Value", heartRate)
                            )
                            .foregroundStyle(by: .value("Metric", "Heart Rate"))
                            .interpolationMethod(.catmullRom)
                        }
                        if let hrvRMSSD = sample.hrvRMSSD {
                            LineMark(
                                x: .value("Time", sample.sampledAt),
                                y: .value("Value", hrvRMSSD)
                            )
                            .foregroundStyle(by: .value("Metric", "HRV (RMSSD)"))
                            .interpolationMethod(.catmullRom)
                        }
                    }
                }
                .chartForegroundStyleScale([
                    "Breathing Rate": Color.blue,
                    "Heart Rate": Color.red,
                    "HRV (RMSSD)": Color.green,
                ])
                .chartLegend(position: .bottom, alignment: .leading)
                .chartXScale(domain: model.historyGraphRange)
                .chartYScale(domain: 0.0...200.0)
                .chartYAxis {
                    AxisMarks(position: .leading)
                }
                .frame(height: 260)
                .padding(10)
                .background(.quaternary.opacity(0.15), in: RoundedRectangle(cornerRadius: 14, style: .continuous))
            }
        }
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
