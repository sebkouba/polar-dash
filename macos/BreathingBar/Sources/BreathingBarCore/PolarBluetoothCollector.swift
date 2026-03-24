@preconcurrency import CoreBluetooth
import Foundation

public final class PolarBluetoothCollector: NSObject, @unchecked Sendable {
    private struct EventPayload: @unchecked Sendable {
        let value: [String: Any]
    }

    private struct HeartRatePayload: @unchecked Sendable {
        let value: PolarHeartRateFrame
    }

    private struct PMDPayload: @unchecked Sendable {
        let value: PolarPMDFrame
    }

    public var onStatus: ((String) -> Void)?
    public var onWarning: ((String) -> Void)?
    public var onConnected: ((String, String) -> Void)?
    public var onDisconnected: (() -> Void)?
    public var onBatteryLevel: ((Int) -> Void)?
    public var onHeartRateFrame: ((PolarHeartRateFrame) -> Void)?
    public var onPMDFrame: ((PolarPMDFrame) -> Void)?
    public var onEvent: ((String, [String: Any]) -> Void)?

    private let deviceNamePrefix: String
    private let reconnectDelay: TimeInterval
    private let queue = DispatchQueue(label: "polar-dash.bluetooth")

    private lazy var centralManager = CBCentralManager(delegate: self, queue: queue)
    private var targetPeripheral: CBPeripheral?
    private var batteryCharacteristic: CBCharacteristic?
    private var heartRateCharacteristic: CBCharacteristic?
    private var pmdControlCharacteristic: CBCharacteristic?
    private var pmdDataCharacteristic: CBCharacteristic?
    private var requestedControlRead = false
    private var pmdControlNotifyEnabled = false
    private var pmdDataNotifyEnabled = false
    private var activeStartMeasurement: PolarPMDMeasurement?
    private var pendingStartMeasurements: [PolarPMDMeasurement] = []
    private var pmdTimebase = PolarPMDTimebase()
    private var reconnectWorkItem: DispatchWorkItem?
    private var shouldRun = false

    public init(deviceNamePrefix: String = "Polar H10", reconnectDelay: TimeInterval = 3.0) {
        self.deviceNamePrefix = deviceNamePrefix
        self.reconnectDelay = reconnectDelay
        super.init()
    }

    private var batteryCharacteristicID: CBUUID { CBUUID(string: "2A19") }
    private var heartRateCharacteristicID: CBUUID { CBUUID(string: "2A37") }
    private var pmdControlCharacteristicID: CBUUID { CBUUID(string: "FB005C81-02E7-F387-1CAD-8ACD2D8DF0C8") }
    private var pmdDataCharacteristicID: CBUUID { CBUUID(string: "FB005C82-02E7-F387-1CAD-8ACD2D8DF0C8") }

    public func start() {
        queue.async {
            self.shouldRun = true
            self.reconnectWorkItem?.cancel()
            self.reconnectWorkItem = nil
            self.startScanningIfPossible()
        }
    }

    public func stop() {
        queue.async {
            self.shouldRun = false
            self.reconnectWorkItem?.cancel()
            self.reconnectWorkItem = nil
            self.centralManager.stopScan()
            if let targetPeripheral = self.targetPeripheral {
                self.centralManager.cancelPeripheralConnection(targetPeripheral)
            }
            self.resetConnectionState()
        }
    }

    private func startScanningIfPossible() {
        guard shouldRun else {
            return
        }
        guard centralManager.state == .poweredOn else {
            emitStatus("Bluetooth unavailable: \(describe(centralManager.state))")
            return
        }
        guard targetPeripheral == nil else {
            return
        }
        emitStatus("Scanning for \(deviceNamePrefix)...")
        centralManager.scanForPeripherals(withServices: nil, options: [
            CBCentralManagerScanOptionAllowDuplicatesKey: false
        ])
    }

    private func connect(to peripheral: CBPeripheral) {
        centralManager.stopScan()
        reconnectWorkItem?.cancel()
        reconnectWorkItem = nil
        targetPeripheral = peripheral
        resetDiscoveredCharacteristics()
        pmdTimebase = PolarPMDTimebase()
        requestedControlRead = false
        activeStartMeasurement = nil
        pendingStartMeasurements = []
        emitStatus("Connecting to \(peripheral.name ?? deviceNamePrefix)...")
        centralManager.connect(peripheral)
    }

    private func scheduleReconnect(reason: String?) {
        guard shouldRun else {
            return
        }
        let workItem = DispatchWorkItem { [weak self] in
            guard let self else {
                return
            }
            self.resetConnectionState()
            self.startScanningIfPossible()
        }
        reconnectWorkItem?.cancel()
        reconnectWorkItem = workItem
        if let reason {
            emitStatus(reason)
        }
        queue.asyncAfter(deadline: .now() + reconnectDelay, execute: workItem)
    }

    private func resetConnectionState() {
        resetDiscoveredCharacteristics()
        targetPeripheral = nil
        pmdTimebase = PolarPMDTimebase()
        requestedControlRead = false
        activeStartMeasurement = nil
        pendingStartMeasurements = []
    }

    private func resetDiscoveredCharacteristics() {
        batteryCharacteristic = nil
        heartRateCharacteristic = nil
        pmdControlCharacteristic = nil
        pmdDataCharacteristic = nil
        pmdControlNotifyEnabled = false
        pmdDataNotifyEnabled = false
    }

    private func maybeRequestAvailableMeasurements() {
        guard requestedControlRead == false,
              pmdControlNotifyEnabled,
              pmdDataNotifyEnabled,
              let controlCharacteristic = pmdControlCharacteristic,
              let peripheral = targetPeripheral
        else {
            return
        }
        requestedControlRead = true
        peripheral.readValue(for: controlCharacteristic)
    }

    private func startNextMeasurementIfNeeded() {
        guard activeStartMeasurement == nil,
              let next = pendingStartMeasurements.first,
              let peripheral = targetPeripheral,
              let controlCharacteristic = pmdControlCharacteristic
        else {
            return
        }
        activeStartMeasurement = next
        peripheral.writeValue(makePolarPMDStartRequest(for: next), for: controlCharacteristic, type: .withResponse)
    }

    private func emitStatus(_ message: String) {
        DispatchQueue.main.async {
            self.onStatus?(message)
        }
    }

    private func emitWarning(_ message: String) {
        DispatchQueue.main.async {
            self.onWarning?(message)
        }
    }

    private func emitEvent(_ eventType: String, details: [String: Any]) {
        let payload = EventPayload(value: details)
        DispatchQueue.main.async {
            self.onEvent?(eventType, payload.value)
        }
    }

    private func emitConnected(name: String, address: String) {
        DispatchQueue.main.async {
            self.onConnected?(name, address)
        }
    }

    private func emitDisconnected() {
        DispatchQueue.main.async {
            self.onDisconnected?()
        }
    }

    private func emitBattery(level: Int) {
        DispatchQueue.main.async {
            self.onBatteryLevel?(level)
        }
    }

    private func emitHeartRateFrame(_ frame: PolarHeartRateFrame) {
        let payload = HeartRatePayload(value: frame)
        DispatchQueue.main.async {
            self.onHeartRateFrame?(payload.value)
        }
    }

    private func emitPMDFrame(_ frame: PolarPMDFrame) {
        let payload = PMDPayload(value: frame)
        DispatchQueue.main.async {
            self.onPMDFrame?(payload.value)
        }
    }

    private func describe(_ state: CBManagerState) -> String {
        switch state {
        case .unknown: return "unknown"
        case .resetting: return "resetting"
        case .unsupported: return "unsupported"
        case .unauthorized: return "unauthorized"
        case .poweredOff: return "powered off"
        case .poweredOn: return "powered on"
        @unknown default: return "unexpected"
        }
    }
}

extension PolarBluetoothCollector: CBCentralManagerDelegate {
    public func centralManagerDidUpdateState(_ central: CBCentralManager) {
        if central.state == .poweredOn {
            emitStatus("Bluetooth ready.")
            startScanningIfPossible()
        } else {
            emitStatus("Bluetooth unavailable: \(describe(central.state))")
        }
    }

    public func centralManager(_ central: CBCentralManager, didDiscover peripheral: CBPeripheral, advertisementData: [String: Any], rssi RSSI: NSNumber) {
        let advertisedName = (advertisementData[CBAdvertisementDataLocalNameKey] as? String) ?? peripheral.name ?? ""
        guard advertisedName.lowercased().contains(deviceNamePrefix.lowercased()) else {
            return
        }
        connect(to: peripheral)
    }

    public func centralManager(_ central: CBCentralManager, didConnect peripheral: CBPeripheral) {
        peripheral.delegate = self
        emitConnected(name: peripheral.name ?? deviceNamePrefix, address: peripheral.identifier.uuidString)
        peripheral.discoverServices(nil)
    }

    public func centralManager(_ central: CBCentralManager, didFailToConnect peripheral: CBPeripheral, error: Error?) {
        emitWarning("Failed to connect: \(error?.localizedDescription ?? "unknown error")")
        scheduleReconnect(reason: "Retrying Bluetooth connection...")
    }

    public func centralManager(_ central: CBCentralManager, didDisconnectPeripheral peripheral: CBPeripheral, error: Error?) {
        if let error {
            emitWarning("Bluetooth disconnected: \(error.localizedDescription)")
        } else {
            emitStatus("Bluetooth disconnected.")
        }
        emitDisconnected()
        scheduleReconnect(reason: "Reconnecting to \(deviceNamePrefix)...")
    }
}

extension PolarBluetoothCollector: CBPeripheralDelegate {
    public func peripheral(_ peripheral: CBPeripheral, didDiscoverServices error: Error?) {
        if let error {
            emitWarning("Service discovery failed: \(error.localizedDescription)")
            return
        }
        peripheral.services?.forEach { peripheral.discoverCharacteristics(nil, for: $0) }
    }

    public func peripheral(_ peripheral: CBPeripheral, didDiscoverCharacteristicsFor service: CBService, error: Error?) {
        if let error {
            emitWarning("Characteristic discovery failed: \(error.localizedDescription)")
            return
        }

        for characteristic in service.characteristics ?? [] {
            switch characteristic.uuid {
            case batteryCharacteristicID:
                batteryCharacteristic = characteristic
                peripheral.readValue(for: characteristic)
            case heartRateCharacteristicID:
                heartRateCharacteristic = characteristic
                peripheral.setNotifyValue(true, for: characteristic)
                emitEvent("hr_notify_started", details: [:])
            case pmdControlCharacteristicID:
                pmdControlCharacteristic = characteristic
                peripheral.setNotifyValue(true, for: characteristic)
            case pmdDataCharacteristicID:
                pmdDataCharacteristic = characteristic
                peripheral.setNotifyValue(true, for: characteristic)
            default:
                continue
            }
        }
    }

    public func peripheral(_ peripheral: CBPeripheral, didUpdateNotificationStateFor characteristic: CBCharacteristic, error: Error?) {
        if let error {
            emitWarning("Notification setup failed: \(error.localizedDescription)")
            return
        }

        switch characteristic.uuid {
        case pmdControlCharacteristicID:
            pmdControlNotifyEnabled = characteristic.isNotifying
            maybeRequestAvailableMeasurements()
        case pmdDataCharacteristicID:
            pmdDataNotifyEnabled = characteristic.isNotifying
            maybeRequestAvailableMeasurements()
        default:
            break
        }
    }

    public func peripheral(_ peripheral: CBPeripheral, didWriteValueFor characteristic: CBCharacteristic, error: Error?) {
        if let error {
            emitWarning("Bluetooth write failed: \(error.localizedDescription)")
        }
    }

    public func peripheral(_ peripheral: CBPeripheral, didUpdateValueFor characteristic: CBCharacteristic, error: Error?) {
        if let error {
            emitWarning("Bluetooth read failed: \(error.localizedDescription)")
            return
        }

        guard let value = characteristic.value else {
            return
        }

        switch characteristic.uuid {
        case batteryCharacteristicID:
            if let level = value.first {
                emitBattery(level: Int(level))
            }
        case heartRateCharacteristicID:
            do {
                let frame = try parsePolarHeartRateMeasurement(value, recordedAtNs: currentEpochNanoseconds())
                emitHeartRateFrame(frame)
            } catch {
                emitWarning("Heart-rate decode failed: \(error)")
            }
        case pmdControlCharacteristicID:
            do {
                let message = try parsePolarPMDControlMessage(value)
                switch message {
                case let .availableMeasurements(measurements):
                    emitEvent("pmd_measurements", details: ["available": measurements.map(\.rawValue)])
                    pendingStartMeasurements = measurements.filter { $0 == .ecg || $0 == .acc }
                    startNextMeasurementIfNeeded()
                case let .acknowledgement(opcode, measurement, errorCode):
                    if errorCode != 0 {
                        emitWarning("PMD \(opcode) for \(measurement) failed with code \(errorCode)")
                    } else if opcode == .start {
                        emitEvent("stream_started", details: ["measurement": measurementName(measurement)])
                        if activeStartMeasurement == measurement {
                            pendingStartMeasurements.removeAll { $0 == measurement }
                            activeStartMeasurement = nil
                            startNextMeasurementIfNeeded()
                        }
                    }
                }
            } catch {
                emitWarning("PMD control decode failed: \(error)")
            }
        case pmdDataCharacteristicID:
            do {
                let frame = try parsePolarPMDDataFrame(value, timebase: &pmdTimebase, nowNs: currentEpochNanoseconds())
                emitPMDFrame(frame)
            } catch PolarProtocolError.unsupportedFrame {
                break
            } catch {
                emitWarning("PMD frame decode failed: \(error)")
            }
        default:
            break
        }
    }

    private func measurementName(_ measurement: PolarPMDMeasurement) -> String {
        switch measurement {
        case .ecg: return "ECG"
        case .acc: return "ACC"
        case .ppg: return "PPG"
        case .ppi: return "PPI"
        case .gyro: return "GYRO"
        case .mag: return "MAG"
        case .sdk: return "SDK"
        }
    }
}

private func currentEpochNanoseconds() -> Int64 {
    Int64((Date().timeIntervalSince1970 * 1_000_000_000.0).rounded())
}
