import XCTest
@testable import BreathingBarCore

final class BreathingEngineTests: XCTestCase {
    func testRRIntervalCandidateTracksSyntheticRespiratoryModulation() throws {
        let engine = LiveBreathingEngine(windowSeconds: 20, stepSeconds: 2)

        var rrIntervalsMs: [Int] = []
        var elapsedSeconds = 0.0
        while elapsedSeconds < 30.0 {
            let rrMs = 1_000.0 + 120.0 * sin(2.0 * .pi * 0.20 * elapsedSeconds)
            rrIntervalsMs.append(Int(rrMs))
            elapsedSeconds += rrMs / 1_000.0
        }

        let recordedAtNs = Int64(rrIntervalsMs.reduce(0, +)) * 1_000_000
        let estimates = engine.addHrFrame(
            recordedAtNs: recordedAtNs,
            averageHeartRateBpm: 60.0,
            rrIntervalsMs: rrIntervalsMs
        )

        let rrCandidate = try XCTUnwrap(estimates.first(where: { $0.source == "rr_interval" }))
        XCTAssertEqual(rrCandidate.rateBpm, 12.0, accuracy: 2.0)
    }

    func testFitFusionCalibrationLearnsBiasAndReliability() {
        let labels = [
            PhaseLabel(recordedAtNs: 0, phaseCode: "exhale_end"),
            PhaseLabel(recordedAtNs: 5_000_000_000, phaseCode: "exhale_end"),
            PhaseLabel(recordedAtNs: 10_000_000_000, phaseCode: "exhale_end"),
            PhaseLabel(recordedAtNs: 2_500_000_000, phaseCode: "inhale_end"),
            PhaseLabel(recordedAtNs: 7_500_000_000, phaseCode: "inhale_end"),
            PhaseLabel(recordedAtNs: 12_500_000_000, phaseCode: "inhale_end"),
        ]
        let candidateHistory = [
            "acc_pca": [
                CandidateEstimate(estimatedAtNs: 3_000_000_000, rateBpm: 13.0, quality: 1.0, source: "acc_pca", calibrationVersion: nil),
                CandidateEstimate(estimatedAtNs: 8_000_000_000, rateBpm: 13.0, quality: 1.0, source: "acc_pca", calibrationVersion: nil),
            ],
            "ecg_qrs": [
                CandidateEstimate(estimatedAtNs: 3_000_000_000, rateBpm: 11.0, quality: 1.0, source: "ecg_qrs", calibrationVersion: nil),
                CandidateEstimate(estimatedAtNs: 8_000_000_000, rateBpm: 11.0, quality: 1.0, source: "ecg_qrs", calibrationVersion: nil),
            ],
        ]

        let calibration = fitFusionCalibration(
            candidateHistoryByName: candidateHistory,
            labels: labels,
            protocolName: "breathing_turnaround_fg_v1",
            annotationSessionID: 12,
            version: 3,
            nowNs: 99,
            minimumPointsPerCandidate: 2
        )

        XCTAssertEqual(calibration.version, 3)
        XCTAssertEqual(calibration.biasByCandidate["acc_pca"] ?? 0.0, 1.0, accuracy: 0.0001)
        XCTAssertEqual(calibration.biasByCandidate["ecg_qrs"] ?? 0.0, -1.0, accuracy: 0.0001)
        XCTAssertGreaterThan(calibration.reliabilityByCandidate["acc_pca"] ?? 0.0, 0.0)
    }

    func testLearnedFusionFallsBackWithoutCalibration() {
        let engine = LiveBreathingEngine()
        engine.setCalibration(.default())

        let fusion = engine.addHrFrame(recordedAtNs: 1, averageHeartRateBpm: 60.0, rrIntervalsMs: Array(repeating: 1_000, count: 30))
        XCTAssertFalse(fusion.isEmpty)

        let estimate = engine.addAccFrame(
            sensorRecordedAtNs: 20_000_000_000,
            sampleRateHz: ACCSampleRateHz,
            samples: (0..<(ACCSampleRateHz * 20)).map { index in
                let angle = 2.0 * .pi * 0.2 * Double(index) / Double(ACCSampleRateHz)
                let sample = Int((sin(angle) * 500.0).rounded())
                return (sample, 0, 0)
            }
        ).first(where: { $0.source == "learned_fusion" })

        XCTAssertNotNil(estimate)
    }

    func testCalibrationJSONDecodesSnakeCasePayload() throws {
        let json = """
        {
          "version": 4,
          "protocol_name": "breathing_turnaround_fg_v1",
          "annotation_session_id": 11,
          "bias_by_candidate": {"acc_pca": 0.75},
          "reliability_by_candidate": {"acc_pca": 1.6},
          "minimum_points_per_candidate": 5,
          "epsilon": 0.2,
          "trained_point_count": 42,
          "trained_at_ns": 1234
        }
        """.data(using: .utf8)!

        let calibration = try JSONDecoder().decode(FusionCalibration.self, from: json)
        XCTAssertEqual(calibration.version, 4)
        XCTAssertEqual(calibration.protocolName, "breathing_turnaround_fg_v1")
        XCTAssertEqual(calibration.annotationSessionID, 11)
        XCTAssertEqual(calibration.biasByCandidate["acc_pca"], 0.75)
        XCTAssertEqual(calibration.reliabilityByCandidate["acc_pca"], 1.6)
    }

    func testComputeRMSSDSeriesUsesSlidingWindow() {
        let beats: [(Int64, Double)] = [
            (1_000_000_000, 1_000),
            (2_000_000_000, 1_050),
            (3_000_000_000, 950),
            (4_000_000_000, 1_020),
            (5_000_000_000, 980),
        ]

        let series = computeRMSSDSeries(beats: beats, windowSeconds: 60)
        let last = try? XCTUnwrap(series.last)
        XCTAssertNotNil(last)
        XCTAssertGreaterThan(last?.1 ?? 0.0, 0.0)
    }
}
