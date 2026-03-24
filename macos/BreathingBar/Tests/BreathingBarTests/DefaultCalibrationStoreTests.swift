import Foundation
import XCTest
@testable import BreathingBarCore

final class DefaultCalibrationStoreTests: XCTestCase {
    func testLoadDecodesSeededCalibrationJSON() throws {
        let temporaryDirectory = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        try FileManager.default.createDirectory(at: temporaryDirectory, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: temporaryDirectory) }

        let calibrationURL = temporaryDirectory.appendingPathComponent("default_breathing_calibration.json")
        try """
        {
          "version": null,
          "protocol_name": "breathing_turnaround_fg_v1",
          "annotation_session_id": null,
          "bias_by_candidate": {"acc_pca": 1.5},
          "reliability_by_candidate": {"acc_pca": 2.0},
          "minimum_points_per_candidate": 3,
          "epsilon": 0.25,
          "trained_point_count": 8,
          "trained_at_ns": 1234
        }
        """.write(to: calibrationURL, atomically: true, encoding: .utf8)

        let calibration = try XCTUnwrap(DefaultCalibrationStore.load(from: calibrationURL))
        XCTAssertNil(calibration.version)
        XCTAssertEqual(calibration.protocolName, "breathing_turnaround_fg_v1")
        XCTAssertEqual(calibration.biasByCandidate["acc_pca"], 1.5)
    }

    func testLoadBundledCalibrationDecodesSeededResource() throws {
        let calibration = try XCTUnwrap(DefaultCalibrationStore.loadBundledCalibration())
        XCTAssertEqual(calibration.protocolName, "breathing_turnaround_fg_v1")
        XCTAssertEqual(try XCTUnwrap(calibration.biasByCandidate["acc_pca"]), 3.911209097905669, accuracy: 0.0000001)
        XCTAssertEqual(
            try XCTUnwrap(calibration.reliabilityByCandidate["rr_interval"]),
            0.41232667845099424,
            accuracy: 0.0000001
        )
    }
}
