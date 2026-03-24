import Foundation

public enum PolarProtocolError: Error {
    case invalidControlResponse
    case invalidHeartRateFrame
    case invalidPMDFrame
    case unsupportedFrame
}

public enum PolarPMDMeasurement: UInt8, CaseIterable {
    case ecg = 0x00
    case ppg = 0x01
    case acc = 0x02
    case ppi = 0x03
    case gyro = 0x05
    case mag = 0x06
    case sdk = 0x09
}

enum PolarPMDSetting: UInt8 {
    case sampleRate = 0x00
    case resolution = 0x01
    case range = 0x02
    case channels = 0x04
}

enum PolarPMDControlOpcode: UInt8 {
    case get = 0x01
    case start = 0x02
    case stop = 0x03
    case readResponse = 0x0F
    case controlResponse = 0xF0
}

public struct PolarHeartRateFrame: Equatable {
    public let recordedAtNs: Int64
    public let averageHeartRateBpm: Double
    public let rrIntervalsMs: [Int]
    public let energyKJ: Int?
}

public enum PolarPMDFrame: Equatable {
    case ecg(sensorRecordedAtNs: Int64, sampleRateHz: Int, samples: [Int])
    case acc(sensorRecordedAtNs: Int64, sampleRateHz: Int, samples: [(Int, Int, Int)])

    public static func == (lhs: PolarPMDFrame, rhs: PolarPMDFrame) -> Bool {
        switch (lhs, rhs) {
        case let (.ecg(lhsTime, lhsRate, lhsSamples), .ecg(rhsTime, rhsRate, rhsSamples)):
            return lhsTime == rhsTime && lhsRate == rhsRate && lhsSamples == rhsSamples
        case let (.acc(lhsTime, lhsRate, lhsSamples), .acc(rhsTime, rhsRate, rhsSamples)):
            guard lhsTime == rhsTime, lhsRate == rhsRate, lhsSamples.count == rhsSamples.count else {
                return false
            }
            return zip(lhsSamples, rhsSamples).allSatisfy { lhsSample, rhsSample in
                lhsSample.0 == rhsSample.0
                    && lhsSample.1 == rhsSample.1
                    && lhsSample.2 == rhsSample.2
            }
        default:
            return false
        }
    }
}

enum PolarPMDControlMessage: Equatable {
    case availableMeasurements([PolarPMDMeasurement])
    case acknowledgement(opcode: PolarPMDControlOpcode, measurement: PolarPMDMeasurement, errorCode: UInt8)
}

struct PolarPMDTimebase {
    var timeOffsetNs: Int64?

    mutating func resolve(sensorTimestamp: UInt64, nowNs: Int64) -> Int64 {
        let sensorTimestampNs = Int64(sensorTimestamp)
        if timeOffsetNs == nil {
            timeOffsetNs = nowNs - sensorTimestampNs
        }
        return sensorTimestampNs + (timeOffsetNs ?? 0)
    }
}

public func parsePolarHeartRateMeasurement(_ data: Data, recordedAtNs: Int64) throws -> PolarHeartRateFrame {
    guard data.count >= 2 else {
        throw PolarProtocolError.invalidHeartRateFrame
    }

    let flags = data[0]
    let heartRateIsUInt16 = (flags & 0x01) != 0
    let energyPresent = (flags & 0x08) != 0
    let rrPresent = (flags & 0x10) != 0

    var offset = 1
    let averageHeartRateBpm: Double
    if heartRateIsUInt16 {
        guard data.count >= offset + 2 else {
            throw PolarProtocolError.invalidHeartRateFrame
        }
        averageHeartRateBpm = Double(UInt16(littleEndianBytes: data[offset..<(offset + 2)]))
        offset += 2
    } else {
        averageHeartRateBpm = Double(data[offset])
        offset += 1
    }

    var energyKJ: Int?
    if energyPresent {
        guard data.count >= offset + 2 else {
            throw PolarProtocolError.invalidHeartRateFrame
        }
        energyKJ = Int(UInt16(littleEndianBytes: data[offset..<(offset + 2)]))
        offset += 2
    }

    var rrIntervalsMs: [Int] = []
    if rrPresent {
        while offset + 1 < data.count {
            let raw = UInt16(littleEndianBytes: data[offset..<(offset + 2)])
            rrIntervalsMs.append(Int((Double(raw) * 1000.0 / 1024.0).rounded()))
            offset += 2
        }
    }

    return PolarHeartRateFrame(
        recordedAtNs: recordedAtNs,
        averageHeartRateBpm: averageHeartRateBpm,
        rrIntervalsMs: rrIntervalsMs,
        energyKJ: energyKJ
    )
}

func parsePolarPMDControlMessage(_ data: Data) throws -> PolarPMDControlMessage {
    guard let opcode = PolarPMDControlOpcode(rawValue: data.first ?? 0x00) else {
        throw PolarProtocolError.invalidControlResponse
    }

    switch opcode {
    case .readResponse:
        guard data.count >= 2 else {
            throw PolarProtocolError.invalidControlResponse
        }
        let flags = data[1]
        let measurements = PolarPMDMeasurement.allCases.filter { flags & (1 << $0.rawValue) != 0 }
        return .availableMeasurements(measurements)
    case .controlResponse:
        guard data.count >= 4,
              let responseOpcode = PolarPMDControlOpcode(rawValue: data[1]),
              let measurement = PolarPMDMeasurement(rawValue: data[2])
        else {
            throw PolarProtocolError.invalidControlResponse
        }
        return .acknowledgement(opcode: responseOpcode, measurement: measurement, errorCode: data[3])
    default:
        throw PolarProtocolError.invalidControlResponse
    }
}

func makePolarPMDStartRequest(for measurement: PolarPMDMeasurement) -> Data {
    var data = Data([PolarPMDControlOpcode.start.rawValue, measurement.rawValue])
    switch measurement {
    case .ecg:
        data.append(contentsOf: [
            PolarPMDSetting.sampleRate.rawValue, 0x01, 130, 0x00,
            PolarPMDSetting.resolution.rawValue, 0x01, 14, 0x00,
        ])
    case .acc:
        data.append(contentsOf: [
            PolarPMDSetting.sampleRate.rawValue, 0x01, 200, 0x00,
            PolarPMDSetting.resolution.rawValue, 0x01, 16, 0x00,
            PolarPMDSetting.range.rawValue, 0x01, 2, 0x00,
        ])
    default:
        break
    }
    return data
}

func makePolarPMDStopRequest(for measurement: PolarPMDMeasurement) -> Data {
    Data([PolarPMDControlOpcode.stop.rawValue, measurement.rawValue])
}

func parsePolarPMDDataFrame(
    _ data: Data,
    timebase: inout PolarPMDTimebase,
    nowNs: Int64
) throws -> PolarPMDFrame {
    guard data.count >= 10,
          let measurement = PolarPMDMeasurement(rawValue: data[0])
    else {
        throw PolarProtocolError.invalidPMDFrame
    }

    let sensorTimestamp = UInt64(littleEndianBytes: data[1..<9])
    let sensorRecordedAtNs = timebase.resolve(sensorTimestamp: sensorTimestamp, nowNs: nowNs)
    let frameType = data[9]
    let payload = data[10...]

    switch measurement {
    case .ecg:
        guard frameType == 0x00, payload.count.isMultiple(of: 3) else {
            throw PolarProtocolError.invalidPMDFrame
        }
        var samples: [Int] = []
        samples.reserveCapacity(payload.count / 3)
        var offset = payload.startIndex
        while offset < payload.endIndex {
            let next = payload.index(offset, offsetBy: 3)
            samples.append(Int(Int32(signed24BitLittleEndian: payload[offset..<next])))
            offset = next
        }
        return .ecg(sensorRecordedAtNs: sensorRecordedAtNs, sampleRateHz: ECGSampleRateHz, samples: samples)
    case .acc:
        guard frameType == 0x01, payload.count.isMultiple(of: 6) else {
            throw PolarProtocolError.invalidPMDFrame
        }
        var samples: [(Int, Int, Int)] = []
        samples.reserveCapacity(payload.count / 6)
        var offset = payload.startIndex
        while offset < payload.endIndex {
            let x = Int(Int16(littleEndianBytes: payload[offset..<payload.index(offset, offsetBy: 2)]))
            let yStart = payload.index(offset, offsetBy: 2)
            let zStart = payload.index(offset, offsetBy: 4)
            let y = Int(Int16(littleEndianBytes: payload[yStart..<payload.index(yStart, offsetBy: 2)]))
            let z = Int(Int16(littleEndianBytes: payload[zStart..<payload.index(zStart, offsetBy: 2)]))
            samples.append((x, y, z))
            offset = payload.index(offset, offsetBy: 6)
        }
        return .acc(sensorRecordedAtNs: sensorRecordedAtNs, sampleRateHz: ACCSampleRateHz, samples: samples)
    default:
        throw PolarProtocolError.unsupportedFrame
    }
}

private extension UInt16 {
    init<S: DataProtocol>(littleEndianBytes bytes: S) {
        self = bytes.reversed().reduce(0) { partial, byte in
            (partial << 8) | UInt16(byte)
        }
    }
}

private extension Int16 {
    init<S: DataProtocol>(littleEndianBytes bytes: S) {
        let value = UInt16(littleEndianBytes: bytes)
        self = Int16(bitPattern: value)
    }
}

private extension UInt64 {
    init<S: DataProtocol>(littleEndianBytes bytes: S) {
        self = bytes.reversed().reduce(0) { partial, byte in
            (partial << 8) | UInt64(byte)
        }
    }
}

private extension Int32 {
    init<S: DataProtocol>(signed24BitLittleEndian bytes: S) {
        var raw = bytes.reversed().reduce(0) { partial, byte in
            (partial << 8) | UInt32(byte)
        }
        if (raw & 0x80_0000) != 0 {
            raw |= 0xFF00_0000
        }
        self = Int32(bitPattern: raw)
    }
}
