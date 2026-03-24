import Foundation
import SwiftUI
import BreathingBarCore

private enum DefaultsKey {
    static let lowThreshold = "lowThreshold"
    static let highThreshold = "highThreshold"
    static let historyRetentionDays = "historyRetentionDays"
    static let historyWindowHours = "historyWindowHours"
    static let historyShowsAllDay = "historyShowsAllDay"
}

@MainActor
final class BreathingBarModel: ObservableObject {
    private static let defaultHistoryRetentionDays = 30
    private static let defaultHistoryWindowHours = 2
    private static let historySampleIntervalSeconds: TimeInterval = 30

    @Published var currentBreathingRate: Double?
    @Published var currentHeartRate: Double?
    @Published var currentHeartRateVariability: Double?
    @Published var lowerThreshold: Double
    @Published var upperThreshold: Double
    @Published var historyRetentionDays: Int
    @Published var historyWindowHours: Int
    @Published var historyShowsAllDay: Bool
    @Published private(set) var historySamples: [MetricHistorySample] = []
    @Published private(set) var selectedDayOffset = 0
    @Published var flashOn = false
    @Published var runtimeStatus = "Starting collector..."
    @Published var calibrationDescription = "Calibration: default fusion"
    @Published var connectionDescription = "Connection: idle"

    private let engine = LiveBreathingEngine()
    private let collector: PolarBluetoothCollector
    private let calendar = Calendar.current
    private let historyStore: MetricHistoryStore?
    private var flashTask: Task<Void, Never>?
    private var historySamplingTask: Task<Void, Never>?
    private var lastHistoryPurgeAt: Date?

    init() {
        let defaults = UserDefaults.standard
        lowerThreshold = defaults.object(forKey: DefaultsKey.lowThreshold) as? Double ?? 8.0
        upperThreshold = defaults.object(forKey: DefaultsKey.highThreshold) as? Double ?? 24.0
        historyRetentionDays = defaults.object(forKey: DefaultsKey.historyRetentionDays) as? Int ?? Self.defaultHistoryRetentionDays
        historyWindowHours = defaults.object(forKey: DefaultsKey.historyWindowHours) as? Int ?? Self.defaultHistoryWindowHours
        historyShowsAllDay = defaults.object(forKey: DefaultsKey.historyShowsAllDay) as? Bool ?? false

        collector = PolarBluetoothCollector()
        historyStore = try? MetricHistoryStore(databaseURL: MetricHistoryStore.defaultDatabaseURL())

        reloadCalibration()
        applyHistoryRetentionPolicy()
        refreshHistorySamples()

        configureCollector()
        startFlashing()
        startHistorySampling()
        collector.start()
    }

    deinit {
        collector.stop()
        flashTask?.cancel()
        historySamplingTask?.cancel()
    }

    var isAlerting: Bool {
        guard let currentBreathingRate else {
            return false
        }
        return currentBreathingRate < lowerThreshold || currentBreathingRate > upperThreshold
    }

    func setLowerThreshold(_ value: Double) {
        lowerThreshold = value
        UserDefaults.standard.set(value, forKey: DefaultsKey.lowThreshold)
    }

    func setUpperThreshold(_ value: Double) {
        upperThreshold = value
        UserDefaults.standard.set(value, forKey: DefaultsKey.highThreshold)
    }

    func setHistoryRetentionDays(_ value: Int) {
        let clamped = min(max(value, 0), 1000)
        historyRetentionDays = clamped
        UserDefaults.standard.set(clamped, forKey: DefaultsKey.historyRetentionDays)
        applyHistoryRetentionPolicy()
        if clamped > 0 {
            persistCurrentHistorySample(force: true)
        }
        refreshHistorySamples()
    }

    func setHistoryWindowHours(_ value: Int) {
        let clamped = min(max(value, 1), 24)
        historyWindowHours = clamped
        UserDefaults.standard.set(clamped, forKey: DefaultsKey.historyWindowHours)
        refreshHistorySamples()
    }

    func setHistoryShowsAllDay(_ value: Bool) {
        historyShowsAllDay = value
        UserDefaults.standard.set(value, forKey: DefaultsKey.historyShowsAllDay)
        refreshHistorySamples()
    }

    func selectPreviousDay() {
        selectedDayOffset += 1
        refreshHistorySamples()
    }

    func selectNextDay() {
        guard selectedDayOffset > 0 else {
            return
        }
        selectedDayOffset -= 1
        refreshHistorySamples()
    }

    func reloadCalibration() {
        if let bundledCalibration = DefaultCalibrationStore.loadBundledCalibration() {
            engine.setCalibration(bundledCalibration)
            calibrationDescription = Self.describeCalibration(bundledCalibration)
            return
        }
        engine.setCalibration(.default())
        calibrationDescription = "Calibration: default fusion"
        runtimeStatus = "Bundled calibration missing; using uncalibrated fusion."
    }

    func reconnect() {
        persistCurrentHistorySample(force: true)
        collector.stop()
        connectionDescription = "Connection: idle"
        collector.start()
        runtimeStatus = "Restarting collector..."
    }

    var canSelectNextDay: Bool {
        selectedDayOffset > 0
    }

    var selectedDayTitle: String {
        if selectedDayOffset == 0 {
            return "Today"
        }
        return Self.dayFormatter.string(from: selectedDayDate)
    }

    var historyWindowLabel: String {
        historyShowsAllDay ? "All Day" : "\(historyWindowHours)h"
    }

    var historyRetentionLabel: String {
        historyRetentionDays == 0 ? "Disabled" : "\(historyRetentionDays) days"
    }

    var historyStorageDescription: String {
        guard historyStore != nil else {
            return "History storage unavailable."
        }
        if historyRetentionDays == 0 {
            return "History storage disabled."
        }
        return "Keeping derived graph data for \(historyRetentionDays) days."
    }

    var historyGraphRange: ClosedRange<Date> {
        historyRange.lowerBound...historyRange.upperBound
    }

    private func configureCollector() {
        collector.onStatus = { [weak self] message in
            self?.runtimeStatus = message
        }
        collector.onWarning = { [weak self] message in
            self?.runtimeStatus = message
        }
        collector.onConnected = { [weak self] deviceName, deviceAddress in
            self?.connectionDescription = "Connection: \(deviceName) (\(deviceAddress))"
            self?.runtimeStatus = "Connected to \(deviceName)."
            self?.persistCurrentHistorySample(force: true)
        }
        collector.onDisconnected = { [weak self] in
            self?.persistCurrentHistorySample(force: true)
            self?.runtimeStatus = "Collector disconnected."
            self?.connectionDescription = "Connection: idle"
        }
        collector.onBatteryLevel = { _ in }
        collector.onHeartRateFrame = { [weak self] frame in
            self?.handleHeartRateFrame(frame)
        }
        collector.onPMDFrame = { [weak self] frame in
            self?.handlePMDFrame(frame)
        }
    }

    private func handleHeartRateFrame(_ frame: PolarHeartRateFrame) {
        currentHeartRate = frame.averageHeartRateBpm
        let estimates = engine.addHrFrame(
            recordedAtNs: frame.recordedAtNs,
            averageHeartRateBpm: frame.averageHeartRateBpm,
            rrIntervalsMs: frame.rrIntervalsMs
        )
        processEstimates(estimates)
        currentHeartRateVariability = computeRMSSDSeries(beats: engine.recentBeats(lookbackSeconds: 60)).last?.1
    }

    private func handlePMDFrame(_ frame: PolarPMDFrame) {
        let estimates: [CandidateEstimate]
        switch frame {
        case let .ecg(sensorRecordedAtNs, sampleRateHz, samples):
            estimates = engine.addEcgFrame(
                sensorRecordedAtNs: sensorRecordedAtNs,
                sampleRateHz: sampleRateHz,
                samples: samples
            )
        case let .acc(sensorRecordedAtNs, sampleRateHz, samples):
            estimates = engine.addAccFrame(
                sensorRecordedAtNs: sensorRecordedAtNs,
                sampleRateHz: sampleRateHz,
                samples: samples
            )
        }

        processEstimates(estimates)
    }

    private func processEstimates(_ estimates: [CandidateEstimate]) {
        for estimate in estimates {
            if estimate.source == "learned_fusion" {
                currentBreathingRate = estimate.rateBpm
            }
        }
    }

    private func startHistorySampling() {
        historySamplingTask = Task { @MainActor [weak self] in
            guard let self else {
                return
            }
            while !Task.isCancelled {
                try? await Task.sleep(for: .seconds(Self.historySampleIntervalSeconds))
                persistCurrentHistorySample(force: false)
            }
        }
    }

    private func startFlashing() {
        flashTask = Task { @MainActor [weak self] in
            guard let self else {
                return
            }
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

    private func persistCurrentHistorySample(force: Bool) {
        guard historyRetentionDays > 0, let historyStore else {
            return
        }
        guard currentBreathingRate != nil || currentHeartRate != nil || currentHeartRateVariability != nil else {
            return
        }

        do {
            try historyStore.recordSample(
                MetricHistorySample(
                    sampledAt: Date(),
                    breathingRate: currentBreathingRate,
                    heartRate: currentHeartRate,
                    hrvRMSSD: currentHeartRateVariability
                )
            )
            if shouldPurgeHistory(at: Date()) {
                applyHistoryRetentionPolicy()
            }
            if selectedDayOffset == 0 {
                refreshHistorySamples()
            }
        } catch {
            if force {
                runtimeStatus = "Unable to save graph history snapshot."
            }
        }
    }

    private func shouldPurgeHistory(at now: Date) -> Bool {
        guard let lastHistoryPurgeAt else {
            return true
        }
        return now.timeIntervalSince(lastHistoryPurgeAt) >= 3600
    }

    private func applyHistoryRetentionPolicy() {
        guard let historyStore else {
            historySamples = []
            return
        }
        do {
            if historyRetentionDays == 0 {
                try historyStore.purgeAllSamples()
            } else {
                let cutoffDate = historyCutoffDate()
                try historyStore.purgeSamples(olderThan: cutoffDate)
            }
            lastHistoryPurgeAt = Date()
        } catch {
            runtimeStatus = "Unable to update graph history retention."
        }
    }

    private func refreshHistorySamples() {
        guard historyRetentionDays > 0, let historyStore else {
            historySamples = []
            return
        }
        do {
            historySamples = try historyStore.fetchSamples(
                from: historyRange.lowerBound,
                to: historyRange.upperBound
            )
        } catch {
            historySamples = []
            runtimeStatus = "Unable to load graph history."
        }
    }

    private func historyCutoffDate() -> Date {
        let todayStart = calendar.startOfDay(for: Date())
        return calendar.date(byAdding: .day, value: -(historyRetentionDays - 1), to: todayStart) ?? todayStart
    }

    private var selectedDayDate: Date {
        let todayStart = calendar.startOfDay(for: Date())
        return calendar.date(byAdding: .day, value: -selectedDayOffset, to: todayStart) ?? todayStart
    }

    private var historyRange: Range<Date> {
        let dayStart = selectedDayDate
        let nextDayStart = calendar.date(byAdding: .day, value: 1, to: dayStart) ?? dayStart.addingTimeInterval(86400)
        let currentDayEnd = min(Date(), nextDayStart)
        let rangeEnd = selectedDayOffset == 0 ? currentDayEnd : nextDayStart
        if historyShowsAllDay {
            return dayStart..<rangeEnd
        }
        let windowStart = calendar.date(byAdding: .hour, value: -historyWindowHours, to: rangeEnd) ?? dayStart
        return max(dayStart, windowStart)..<rangeEnd
    }

    private static func describeCalibration(_ calibration: FusionCalibration) -> String {
        if calibration.protocolName != "default" {
            return "Calibration: bundled (\(calibration.protocolName))"
        }
        return "Calibration: default fusion"
    }

    private static let dayFormatter: DateFormatter = {
        let formatter = DateFormatter()
        formatter.dateStyle = .medium
        formatter.timeStyle = .none
        return formatter
    }()
}
