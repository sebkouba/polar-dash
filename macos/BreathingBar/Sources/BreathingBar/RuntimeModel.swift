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
    @Published var lowerThreshold: Double
    @Published var upperThreshold: Double
    @Published var flashOn = false
    @Published var runtimeStatus = "Starting collector..."
    @Published var calibrationDescription = "Calibration: default fusion"
    @Published var connectionDescription = "Connection: idle"

    private let engine = LiveBreathingEngine()
    private let collector: PolarBluetoothCollector
    private var flashTask: Task<Void, Never>?

    init() {
        let defaults = UserDefaults.standard
        lowerThreshold = defaults.object(forKey: DefaultsKey.lowThreshold) as? Double ?? 8.0
        upperThreshold = defaults.object(forKey: DefaultsKey.highThreshold) as? Double ?? 24.0

        collector = PolarBluetoothCollector()

        reloadCalibration()

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
        collector.stop()
        connectionDescription = "Connection: idle"
        collector.start()
        runtimeStatus = "Restarting collector..."
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
        }
        collector.onDisconnected = { [weak self] in
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

    private static func describeCalibration(_ calibration: FusionCalibration) -> String {
        if calibration.protocolName != "default" {
            return "Calibration: bundled (\(calibration.protocolName))"
        }
        return "Calibration: default fusion"
    }
}
