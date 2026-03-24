import Foundation
import SwiftUI
import BreathingBarCore

private enum DefaultsKey {
    static let lowThreshold = "lowThreshold"
    static let highThreshold = "highThreshold"
}

@MainActor
final class BreathingBarModel: ObservableObject {
    @Published var currentBreathingRate: Double?
    @Published var currentHeartRate: Double?
    @Published var currentHeartRateVariability: Double?
    @Published var databasePath: String
    @Published var lowerThreshold: Double
    @Published var upperThreshold: Double
    @Published var flashOn = false
    @Published var runtimeStatus = "Starting collector..."
    @Published var calibrationDescription = "Calibration: default fusion"
    @Published var connectionDescription = "Session: idle"

    private let engine = LiveBreathingEngine()
    private let collector: PolarBluetoothCollector
    private let store: SQLiteRuntimeStore?
    private let databaseURL: URL
    private var currentSessionID: Int?
    private var currentDeviceName: String?
    private var flashTask: Task<Void, Never>?

    init() {
        let defaults = UserDefaults.standard
        lowerThreshold = defaults.object(forKey: DefaultsKey.lowThreshold) as? Double ?? 8.0
        upperThreshold = defaults.object(forKey: DefaultsKey.highThreshold) as? Double ?? 24.0

        databaseURL = Self.resolveDatabaseURL()
        databasePath = databaseURL.path
        store = try? SQLiteRuntimeStore(databaseURL: databaseURL)
        collector = PolarBluetoothCollector()

        if let calibrationRecord = try? store?.latestCalibrationRecord() {
            engine.setCalibration(calibrationRecord.calibration)
            calibrationDescription = "Calibration: v\(calibrationRecord.id) (\(calibrationRecord.protocolName))"
        }

        configureCollector()
        startFlashing()
        collector.start()
    }

    deinit {
        collector.stop()
        flashTask?.cancel()
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

    func reloadCalibration() {
        guard let calibrationRecord = try? store?.latestCalibrationRecord() else {
            engine.setCalibration(.default())
            calibrationDescription = "Calibration: default fusion"
            runtimeStatus = "Using default fusion calibration."
            return
        }
        engine.setCalibration(calibrationRecord.calibration)
        calibrationDescription = "Calibration: v\(calibrationRecord.id) (\(calibrationRecord.protocolName))"
        runtimeStatus = "Reloaded calibration v\(calibrationRecord.id)."
    }

    func reconnect() {
        collector.stop()
        closeCurrentSession()
        collector.start()
        runtimeStatus = "Restarting collector..."
    }

    private func configureCollector() {
        collector.onStatus = { [weak self] message in
            self?.runtimeStatus = message
        }
        collector.onWarning = { [weak self] message in
            guard let self else {
                return
            }
            self.runtimeStatus = message
            try? self.store?.insertEvent(
                eventType: "collector_warning",
                details: ["message": message],
                level: "WARNING",
                sessionID: self.currentSessionID
            )
        }
        collector.onEvent = { [weak self] eventType, details in
            guard let self else {
                return
            }
            try? self.store?.insertEvent(
                eventType: eventType,
                details: details,
                sessionID: self.currentSessionID
            )
        }
        collector.onConnected = { [weak self] deviceName, deviceAddress in
            self?.beginSession(deviceName: deviceName, deviceAddress: deviceAddress)
        }
        collector.onDisconnected = { [weak self] in
            self?.runtimeStatus = "Collector disconnected."
            self?.closeCurrentSession()
        }
        collector.onBatteryLevel = { [weak self] level in
            guard let self else {
                return
            }
            if let currentSessionID {
                try? store?.updateSessionBattery(sessionID: currentSessionID, batteryPercent: level)
            }
        }
        collector.onHeartRateFrame = { [weak self] frame in
            self?.handleHeartRateFrame(frame)
        }
        collector.onPMDFrame = { [weak self] frame in
            self?.handlePMDFrame(frame)
        }
    }

    private func beginSession(deviceName: String, deviceAddress: String) {
        closeCurrentSession()
        currentDeviceName = deviceName
        currentSessionID = try? store?.startSession(deviceName: deviceName, deviceAddress: deviceAddress)
        connectionDescription = "Session \(currentSessionID ?? 0) on \(deviceName)"
        runtimeStatus = "Connected to \(deviceName)."
        if let currentSessionID {
            try? store?.insertEvent(
                eventType: "collector_connected",
                details: ["device_name": deviceName, "device_address": deviceAddress],
                sessionID: currentSessionID
            )
        }
    }

    private func closeCurrentSession() {
        guard let currentSessionID else {
            connectionDescription = "Session: idle"
            return
        }
        try? store?.insertEvent(eventType: "collector_disconnected", details: [:], sessionID: currentSessionID)
        try? store?.closeSession(id: currentSessionID)
        self.currentSessionID = nil
        connectionDescription = "Session: idle"
    }

    private func handleHeartRateFrame(_ frame: PolarHeartRateFrame) {
        guard let currentSessionID else {
            return
        }
        try? store?.insertHeartRateFrame(
            sessionID: currentSessionID,
            recordedAtNs: frame.recordedAtNs,
            averageHeartRateBpm: frame.averageHeartRateBpm,
            rrIntervalsMs: frame.rrIntervalsMs,
            energyKJ: frame.energyKJ
        )
        currentHeartRate = frame.averageHeartRateBpm
        let estimates = engine.addHrFrame(
            recordedAtNs: frame.recordedAtNs,
            averageHeartRateBpm: frame.averageHeartRateBpm,
            rrIntervalsMs: frame.rrIntervalsMs
        )
        processEstimates(estimates, sessionID: currentSessionID)
        currentHeartRateVariability = computeRMSSDSeries(beats: engine.recentBeats(lookbackSeconds: 60)).last?.1
    }

    private func handlePMDFrame(_ frame: PolarPMDFrame) {
        guard let currentSessionID else {
            return
        }

        let estimates: [CandidateEstimate]
        switch frame {
        case let .ecg(sensorRecordedAtNs, sampleRateHz, samples):
            try? store?.insertECGFrame(
                sessionID: currentSessionID,
                sensorRecordedAtNs: sensorRecordedAtNs,
                sampleRateHz: sampleRateHz,
                samples: samples
            )
            estimates = engine.addEcgFrame(
                sensorRecordedAtNs: sensorRecordedAtNs,
                sampleRateHz: sampleRateHz,
                samples: samples
            )
        case let .acc(sensorRecordedAtNs, sampleRateHz, samples):
            try? store?.insertACCFrame(
                sessionID: currentSessionID,
                sensorRecordedAtNs: sensorRecordedAtNs,
                sampleRateHz: sampleRateHz,
                samples: samples
            )
            estimates = engine.addAccFrame(
                sensorRecordedAtNs: sensorRecordedAtNs,
                sampleRateHz: sampleRateHz,
                samples: samples
            )
        }

        processEstimates(estimates, sessionID: currentSessionID)
    }

    private func processEstimates(_ estimates: [CandidateEstimate], sessionID: Int) {
        for estimate in estimates {
            try? store?.insertBreathingCandidateEstimate(sessionID: sessionID, estimate: estimate)
            if estimate.source == "learned_fusion" {
                try? store?.insertBreathingEstimate(
                    sessionID: sessionID,
                    estimate: estimate,
                    windowSeconds: engine.windowSeconds
                )
                currentBreathingRate = estimate.rateBpm
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

    private static func resolveDatabaseURL() -> URL {
        if let explicitPath = ProcessInfo.processInfo.environment["POLAR_DASH_DB"], !explicitPath.isEmpty {
            return URL(fileURLWithPath: explicitPath).standardizedFileURL
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

        return URL(fileURLWithPath: fileManager.currentDirectoryPath)
            .appendingPathComponent("data/polar_dash.db")
            .standardizedFileURL
    }
}
