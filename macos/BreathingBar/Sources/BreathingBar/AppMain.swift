import AppKit
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
    @ObservedObject var model: BreathingBarModel

    var body: some View {
        VStack(alignment: .leading, spacing: 14) {
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

            VStack(alignment: .leading, spacing: 6) {
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
        .padding(14)
        .frame(width: 390)
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
        popover.contentSize = NSSize(width: 390, height: 240)
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
