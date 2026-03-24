import XCTest
@testable import BreathingBarCore

final class PolarProtocolTests: XCTestCase {
    func testHeartRateFrameDecodesAverageAndRRIntervals() throws {
        let frame = try parsePolarHeartRateMeasurement(
            Data([0x10, 60, 0x00, 0x04, 0x00, 0x04]),
            recordedAtNs: 123
        )

        XCTAssertEqual(frame.recordedAtNs, 123)
        XCTAssertEqual(frame.averageHeartRateBpm, 60.0)
        XCTAssertEqual(frame.rrIntervalsMs, [1000, 1000])
    }

    func testAvailableMeasurementControlResponseDecodesECGAndACC() throws {
        let message = try parsePolarPMDControlMessage(Data([0x0F, 0b0000_0101]))
        XCTAssertEqual(message, .availableMeasurements([.ecg, .acc]))
    }

    func testECGStartRequestMatchesPolarDefaults() {
        XCTAssertEqual(
            makePolarPMDStartRequest(for: .ecg),
            Data([0x02, 0x00, 0x00, 0x01, 130, 0x00, 0x01, 0x01, 14, 0x00])
        )
    }

    func testACCStartRequestMatchesPolarDefaults() {
        XCTAssertEqual(
            makePolarPMDStartRequest(for: .acc),
            Data([0x02, 0x02, 0x00, 0x01, 200, 0x00, 0x01, 0x01, 16, 0x00, 0x02, 0x01, 2, 0x00])
        )
    }

    func testECGDataFrameDecodesSigned24BitSamples() throws {
        var timebase = PolarPMDTimebase()
        let data = Data([
            0x00,
            0xE8, 0x03, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00,
            0xE8, 0x03, 0x00,
            0x18, 0xFC, 0xFF,
        ])

        let frame = try parsePolarPMDDataFrame(data, timebase: &timebase, nowNs: 10_000)
        XCTAssertEqual(frame, .ecg(sensorRecordedAtNs: 10_000, sampleRateHz: ECGSampleRateHz, samples: [1000, -1000]))
    }

    func testACCDataFrameDecodesSigned16BitTriples() throws {
        var timebase = PolarPMDTimebase()
        let data = Data([
            0x02,
            0xD0, 0x07, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x01,
            0x01, 0x00, 0xFF, 0xFF, 0x10, 0x00,
            0xFE, 0xFF, 0x02, 0x00, 0xF0, 0xFF,
        ])

        let frame = try parsePolarPMDDataFrame(data, timebase: &timebase, nowNs: 20_000)
        XCTAssertEqual(
            frame,
            .acc(
                sensorRecordedAtNs: 20_000,
                sampleRateHz: ACCSampleRateHz,
                samples: [(1, -1, 16), (-2, 2, -16)]
            )
        )
    }
}
