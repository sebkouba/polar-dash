import Foundation

public let ECGSampleRateHz = 130
public let ACCSampleRateHz = 200
let respiratoryBandHz = (low: 0.08, high: 0.70)
public let defaultBreathingWindowSeconds = 20
public let defaultBreathingStepSeconds = 2
public let defaultFusionSmoothingAlpha = 0.75
public let defaultSingleSourceSmoothingAlpha = 0.60
let defaultCalibrationEpsilon = 0.25
let defaultMinimumCalibrationPoints = 3

public struct CandidateEstimate: Equatable {
    public let estimatedAtNs: Int64
    public let rateBpm: Double
    public let quality: Double
    public let source: String
    public let calibrationVersion: Int?

    public init(estimatedAtNs: Int64, rateBpm: Double, quality: Double, source: String, calibrationVersion: Int?) {
        self.estimatedAtNs = estimatedAtNs
        self.rateBpm = rateBpm
        self.quality = quality
        self.source = source
        self.calibrationVersion = calibrationVersion
    }
}

struct ReferenceInterval: Equatable {
    let phaseCode: String
    let startNs: Int64
    let endNs: Int64
    let rateBpm: Double
}

public struct FusionCalibration: Codable, Equatable {
    public var version: Int?
    public var protocolName: String
    public var annotationSessionID: Int?
    public var biasByCandidate: [String: Double]
    public var reliabilityByCandidate: [String: Double]
    public var minimumPointsPerCandidate: Int
    public var epsilon: Double
    public var trainedPointCount: Int
    public var trainedAtNs: Int64?

    enum CodingKeys: String, CodingKey {
        case version
        case protocolName = "protocol_name"
        case annotationSessionID = "annotation_session_id"
        case biasByCandidate = "bias_by_candidate"
        case reliabilityByCandidate = "reliability_by_candidate"
        case minimumPointsPerCandidate = "minimum_points_per_candidate"
        case epsilon
        case trainedPointCount = "trained_point_count"
        case trainedAtNs = "trained_at_ns"
    }

    public static func `default`() -> FusionCalibration {
        FusionCalibration(
            version: nil,
            protocolName: "default",
            annotationSessionID: nil,
            biasByCandidate: [:],
            reliabilityByCandidate: [:],
            minimumPointsPerCandidate: defaultMinimumCalibrationPoints,
            epsilon: defaultCalibrationEpsilon,
            trainedPointCount: 0,
            trainedAtNs: nil
        )
    }
}

public struct PhaseLabel {
    public let recordedAtNs: Int64
    public let phaseCode: String

    public init(recordedAtNs: Int64, phaseCode: String) {
        self.recordedAtNs = recordedAtNs
        self.phaseCode = phaseCode
    }
}

public final class LiveBreathingEngine {
    private struct AccSample {
        let timestampNs: Int64
        let x: Double
        let y: Double
        let z: Double
    }

    private struct ECGSample {
        let timestampNs: Int64
        let value: Double
    }

    private struct BeatSample {
        let timestampNs: Int64
        let rrMs: Double
    }

    public let windowSeconds: Int
    let stepSeconds: Int
    let fusionSmoothingAlpha: Double
    let singleSourceSmoothingAlpha: Double

    private let windowNs: Int64
    private let stepNs: Int64
    private var accSamples: [AccSample] = []
    private var ecgSamples: [ECGSample] = []
    private var beats: [BeatSample] = []
    private var previousRateBySource: [String: Double] = [:]
    private var lastEstimateAtNs: Int64?

    public var calibration = FusionCalibration.default()

    public init(
        windowSeconds: Int = defaultBreathingWindowSeconds,
        stepSeconds: Int = defaultBreathingStepSeconds,
        fusionSmoothingAlpha: Double = defaultFusionSmoothingAlpha,
        singleSourceSmoothingAlpha: Double = defaultSingleSourceSmoothingAlpha
    ) {
        self.windowSeconds = windowSeconds
        self.stepSeconds = stepSeconds
        self.fusionSmoothingAlpha = fusionSmoothingAlpha
        self.singleSourceSmoothingAlpha = singleSourceSmoothingAlpha
        windowNs = Int64(windowSeconds) * 1_000_000_000
        stepNs = Int64(stepSeconds) * 1_000_000_000
    }

    public func setCalibration(_ calibration: FusionCalibration) {
        self.calibration = calibration
    }

    public func addAccFrame(
        sensorRecordedAtNs: Int64,
        sampleRateHz: Int,
        samples: [(Int, Int, Int)]
    ) -> [CandidateEstimate] {
        let stepNs = Int64(1_000_000_000 / max(sampleRateHz, 1))
        let startNs = sensorRecordedAtNs - stepNs * Int64(max(samples.count - 1, 0))
        for (index, sample) in samples.enumerated() {
            let timestampNs = startNs + Int64(index) * stepNs
            accSamples.append(
                AccSample(
                    timestampNs: timestampNs,
                    x: Double(sample.0),
                    y: Double(sample.1),
                    z: Double(sample.2)
                )
            )
        }
        return maybeEstimate(at: sensorRecordedAtNs)
    }

    public func addEcgFrame(
        sensorRecordedAtNs: Int64,
        sampleRateHz: Int,
        samples: [Int]
    ) -> [CandidateEstimate] {
        let stepNs = Int64(1_000_000_000 / max(sampleRateHz, 1))
        let startNs = sensorRecordedAtNs - stepNs * Int64(max(samples.count - 1, 0))
        for (index, sample) in samples.enumerated() {
            ecgSamples.append(
                ECGSample(
                    timestampNs: startNs + Int64(index) * stepNs,
                    value: Double(sample)
                )
            )
        }
        return maybeEstimate(at: sensorRecordedAtNs)
    }

    public func addHrFrame(
        recordedAtNs: Int64,
        averageHeartRateBpm: Double,
        rrIntervalsMs: [Int]
    ) -> [CandidateEstimate] {
        if rrIntervalsMs.isEmpty {
            let rrMs = 60_000.0 / max(averageHeartRateBpm, 1.0)
            beats.append(BeatSample(timestampNs: recordedAtNs, rrMs: rrMs))
        } else {
            var beatTimeNs = recordedAtNs - Int64(rrIntervalsMs.reduce(0, +)) * 1_000_000
            for rrMs in rrIntervalsMs {
                beatTimeNs += Int64(rrMs) * 1_000_000
                beats.append(BeatSample(timestampNs: beatTimeNs, rrMs: Double(rrMs)))
            }
        }
        return maybeEstimate(at: recordedAtNs)
    }

    func recentECG(lookbackSeconds: Int = 12) -> [(Int64, Double)] {
        let cutoffNs = latestTimeNsValue() - Int64(lookbackSeconds) * 1_000_000_000
        return ecgSamples
            .filter { $0.timestampNs >= cutoffNs }
            .map { ($0.timestampNs, $0.value) }
    }

    func recentACC(lookbackSeconds: Int = 30) -> [(Int64, Double, Double, Double)] {
        let cutoffNs = latestTimeNsValue() - Int64(lookbackSeconds) * 1_000_000_000
        return accSamples
            .filter { $0.timestampNs >= cutoffNs }
            .map { ($0.timestampNs, $0.x, $0.y, $0.z) }
    }

    public func recentBeats(lookbackSeconds: Int = 30) -> [(Int64, Double)] {
        let cutoffNs = latestTimeNsValue() - Int64(lookbackSeconds) * 1_000_000_000
        return beats
            .filter { $0.timestampNs >= cutoffNs }
            .map { ($0.timestampNs, $0.rrMs) }
    }

    func respiratoryWaveform(lookbackSeconds: Int = 30) -> [(Int64, Double)] {
        let samples = recentACC(lookbackSeconds: lookbackSeconds)
        guard samples.count >= 200 else {
            return []
        }

        let timestamps = samples.map(\.0)
        let xyz = samples.map { [$0.1, $0.2, $0.3] }
        let spacing = differences(timestamps.map(Double.init))
        guard !spacing.isEmpty else {
            return []
        }

        let sampleRateHz = Int((1_000_000_000.0 / median(spacing)).rounded())
        guard sampleRateHz >= 20 else {
            return []
        }

        let principal = principalComponentSignal(from: xyz)
        let lowpassed = zeroPhaseBiquadFilter(
            principal,
            coefficients: BiquadCoefficients.lowPass(
                sampleRateHz: Double(sampleRateHz),
                cutoffHz: 1.0
            )
        )
        let reduced = strideDownsample(lowpassed, factor: 8)
        guard reduced.count >= 8 else {
            return []
        }

        let reducedTimestamps = linspace(
            start: Double(timestamps.first ?? 0),
            end: Double(timestamps.last ?? 0),
            count: reduced.count
        ).map { Int64($0.rounded()) }
        let respiratory = bandpassedRespiratoryWaveform(
            reduced,
            sampleRateHz: Double(sampleRateHz) / 8.0
        )
        return zip(reducedTimestamps, respiratory).map { ($0.0, $0.1) }
    }

    func latestTimeNs() -> Int64? {
        let latest = latestTimeNsValue()
        return latest > 0 ? latest : nil
    }

    private func maybeEstimate(at timestampNs: Int64) -> [CandidateEstimate] {
        let cutoffNs = timestampNs - windowNs - stepNs
        accSamples.removeAll { $0.timestampNs < cutoffNs }
        ecgSamples.removeAll { $0.timestampNs < cutoffNs }
        beats.removeAll { $0.timestampNs < cutoffNs }

        if let lastEstimateAtNs, timestampNs - lastEstimateAtNs < stepNs {
            return []
        }

        var candidates: [CandidateEstimate] = []
        if let accCandidate = estimateAccCandidate(endNs: timestampNs) {
            candidates.append(accCandidate)
        }
        if let ecgCandidate = estimateEcgCandidate(endNs: timestampNs) {
            candidates.append(ecgCandidate)
        }
        if let rrCandidate = estimateRRCandidate(endNs: timestampNs) {
            candidates.append(rrCandidate)
        }
        guard !candidates.isEmpty else {
            return []
        }

        let fusion = estimateLearnedFusion(endNs: timestampNs, candidates: candidates)
        let estimates = candidates + (fusion.map { [$0] } ?? [])
        lastEstimateAtNs = timestampNs
        for estimate in estimates {
            previousRateBySource[estimate.source] = estimate.rateBpm
        }
        return estimates
    }

    private func estimateAccCandidate(endNs: Int64) -> CandidateEstimate? {
        let window = accSamples.filter { $0.timestampNs >= endNs - windowNs }
        guard window.count >= 200 else {
            return nil
        }
        let timestamps = window.map(\.timestampNs).map(Double.init)
        let spacing = differences(timestamps)
        guard !spacing.isEmpty else {
            return nil
        }

        let sampleRateHz = (1_000_000_000.0 / median(spacing)).rounded()
        guard sampleRateHz >= 20 else {
            return nil
        }

        let xyz = window.map { [$0.x, $0.y, $0.z] }
        let principal = principalComponentSignal(from: xyz)
        let lowpassed = zeroPhaseBiquadFilter(
            principal,
            coefficients: BiquadCoefficients.lowPass(sampleRateHz: sampleRateHz, cutoffHz: 1.0)
        )
        let reduced = strideDownsample(lowpassed, factor: 8)
        return estimateWaveformCandidate(
            endNs: endNs,
            waveform: reduced,
            sampleRateHz: sampleRateHz / 8.0,
            source: "acc_pca"
        )
    }

    private func estimateEcgCandidate(endNs: Int64) -> CandidateEstimate? {
        let window = ecgSamples.filter { $0.timestampNs >= endNs - windowNs }
        guard window.count >= ECGSampleRateHz * 20 else {
            return nil
        }

        let timestamps = window.map(\.timestampNs).map(Double.init)
        let spacing = differences(timestamps)
        guard !spacing.isEmpty else {
            return nil
        }

        let sampleRateHz = (1_000_000_000.0 / median(spacing)).rounded()
        guard sampleRateHz >= 60 else {
            return nil
        }

        let ecg = window.map(\.value)
        let qrsBand = zeroPhaseBiquadFilter(
            ecg,
            coefficients: BiquadCoefficients.bandPass(
                sampleRateHz: sampleRateHz,
                centerHz: 10.0,
                q: 10.0 / 15.0
            )
        )
        let derivative = gradient(qrsBand)
        let envelope = derivative.map(abs)
        let peaks = findPeaks(
            in: envelope,
            minimumDistance: max(Int(sampleRateHz * 0.35), 1),
            prominence: max(standardDeviation(envelope) * 0.5, percentile(envelope, 0.75) * 0.1)
        )
        guard peaks.count >= 10 else {
            return nil
        }

        let halfWindow = max(Int(sampleRateHz * 0.05), 1)
        var features: [Double] = []
        var featureTimes: [Double] = []
        for peak in peaks {
            let start = max(0, peak - halfWindow)
            let end = min(derivative.count, peak + halfWindow)
            guard start < end else {
                continue
            }
            let segment = Array(derivative[start..<end])
            guard let maximum = segment.max(), let minimum = segment.min() else {
                continue
            }
            features.append(maximum - minimum)
            featureTimes.append(Double(window[peak].timestampNs))
        }
        guard features.count >= 10 else {
            return nil
        }

        let stepNs = 1_000_000_000.0 / 4.0
        let uniformTimes = uniformRange(
            start: featureTimes.first ?? 0,
            end: featureTimes.last ?? 0,
            step: stepNs
        )
        guard uniformTimes.count >= 20 else {
            return nil
        }
        let interpolated = interpolateLinear(x: featureTimes, y: features, xNew: uniformTimes)
        return estimateWaveformCandidate(
            endNs: endNs,
            waveform: interpolated,
            sampleRateHz: 4.0,
            source: "ecg_qrs"
        )
    }

    private func estimateRRCandidate(endNs: Int64) -> CandidateEstimate? {
        let window = beats.filter { $0.timestampNs >= endNs - windowNs }
        guard window.count >= 8 else {
            return nil
        }
        guard let first = window.first, let last = window.last, last.timestampNs - first.timestampNs >= 10_000_000_000 else {
            return nil
        }

        let times = window.map(\.timestampNs).map(Double.init)
        let rrValues = window.map(\.rrMs)
        let uniformTimes = uniformRange(
            start: times.first ?? 0,
            end: times.last ?? 0,
            step: 1_000_000_000.0 / 4.0
        )
        guard uniformTimes.count >= 20 else {
            return nil
        }
        let interpolated = interpolateLinear(x: times, y: rrValues, xNew: uniformTimes)
        return estimateWaveformCandidate(
            endNs: endNs,
            waveform: interpolated,
            sampleRateHz: 4.0,
            source: "rr_interval"
        )
    }

    private func estimateWaveformCandidate(
        endNs: Int64,
        waveform: [Double],
        sampleRateHz: Double,
        source: String
    ) -> CandidateEstimate? {
        guard waveform.count >= Int(sampleRateHz * 10.0) else {
            return nil
        }

        let respiratory = bandpassedRespiratoryWaveform(waveform, sampleRateHz: sampleRateHz)
        guard respiratory.count >= 8 else {
            return nil
        }

        let spectrum = powerSpectrum(signal: respiratory, sampleRateHz: sampleRateHz)
        let bandSpectrum = spectrum.filter { respiratoryBandHz.low ... respiratoryBandHz.high ~= $0.frequencyHz }
        guard !bandSpectrum.isEmpty else {
            return nil
        }
        guard let peak = bandSpectrum.max(by: { $0.power < $1.power }) else {
            return nil
        }
        let medianPower = max(median(bandSpectrum.map(\.power)), 1e-9)
        let spectralRateBpm = peak.frequencyHz * 60.0
        let spectralQuality = log1p(peak.power / medianPower)

        let autocorrelation = normalizedAutocorrelation(respiratory)
        let minLag = max(1, Int(sampleRateHz * 60.0 / 30.0))
        let maxLag = max(minLag + 1, Int(sampleRateHz * 60.0 / 6.0))
        guard maxLag < autocorrelation.count else {
            return nil
        }
        let segment = Array(autocorrelation[minLag...maxLag])
        guard let segmentPeak = segment.enumerated().max(by: { $0.element < $1.element }) else {
            return nil
        }
        let autocorrLag = segmentPeak.offset + minLag
        let autocorrRateBpm = 60.0 / (Double(autocorrLag) / sampleRateHz)
        let autocorrQuality = max(autocorrelation[autocorrLag], 0.0)

        let rateBpm: Double
        let quality: Double
        if abs(spectralRateBpm - autocorrRateBpm) <= 3.0 {
            let spectralWeight = max(spectralQuality, 0.1)
            let autocorrWeight = max(autocorrQuality, 0.1)
            rateBpm = (
                spectralRateBpm * spectralWeight
                + autocorrRateBpm * autocorrWeight
            ) / (spectralWeight + autocorrWeight)
            quality = spectralQuality * (0.5 + autocorrQuality)
        } else {
            rateBpm = spectralRateBpm
            quality = spectralQuality * 0.35
        }

        return smoothedEstimate(
            CandidateEstimate(
                estimatedAtNs: endNs,
                rateBpm: rateBpm,
                quality: quality,
                source: source,
                calibrationVersion: nil
            )
        )
    }

    private func estimateLearnedFusion(
        endNs: Int64,
        candidates: [CandidateEstimate]
    ) -> CandidateEstimate? {
        var weightedTerms: [(Double, Double)] = []
        for candidate in candidates {
            let bias = calibration.biasByCandidate[candidate.source] ?? 0.0
            let reliability = calibration.reliabilityByCandidate[candidate.source] ?? 1.0
            let correctedRate = candidate.rateBpm - bias
            let weight = max(candidate.quality, 0.05) * reliability
            weightedTerms.append((correctedRate, weight))
        }
        guard !weightedTerms.isEmpty else {
            return nil
        }
        let totalWeight = weightedTerms.reduce(0.0) { $0 + $1.1 }
        guard totalWeight > 0 else {
            return nil
        }
        let rateBpm = weightedTerms.reduce(0.0) { $0 + $1.0 * $1.1 } / totalWeight
        let quality = totalWeight / Double(weightedTerms.count)
        return smoothedEstimate(
            CandidateEstimate(
                estimatedAtNs: endNs,
                rateBpm: rateBpm,
                quality: quality,
                source: "learned_fusion",
                calibrationVersion: calibration.version
            )
        )
    }

    private func smoothedEstimate(_ estimate: CandidateEstimate) -> CandidateEstimate {
        guard let previousRate = previousRateBySource[estimate.source] else {
            return estimate
        }
        let alpha = estimate.source == "learned_fusion" ? fusionSmoothingAlpha : singleSourceSmoothingAlpha
        return CandidateEstimate(
            estimatedAtNs: estimate.estimatedAtNs,
            rateBpm: alpha * estimate.rateBpm + (1.0 - alpha) * previousRate,
            quality: estimate.quality,
            source: estimate.source,
            calibrationVersion: estimate.calibrationVersion
        )
    }

    private func latestTimeNsValue() -> Int64 {
        max(accSamples.last?.timestampNs ?? 0, ecgSamples.last?.timestampNs ?? 0, beats.last?.timestampNs ?? 0)
    }
}

func buildReferenceIntervalsFromLabels(
    labels: [PhaseLabel],
    minCycleSeconds: Double = 1.5,
    maxCycleSeconds: Double = 15.0
) -> [ReferenceInterval] {
    var grouped: [String: [Int64]] = [:]
    for label in labels {
        grouped[label.phaseCode, default: []].append(label.recordedAtNs)
    }

    let minimumNs = Int64(minCycleSeconds * 1_000_000_000.0)
    let maximumNs = Int64(maxCycleSeconds * 1_000_000_000.0)
    var intervals: [ReferenceInterval] = []
    for (phaseCode, timestamps) in grouped {
        let ordered = timestamps.sorted()
        guard ordered.count > 1 else {
            continue
        }
        for index in 1..<ordered.count {
            let previous = ordered[index - 1]
            let current = ordered[index]
            let durationNs = current - previous
            guard durationNs >= minimumNs, durationNs <= maximumNs else {
                continue
            }
            intervals.append(
                ReferenceInterval(
                    phaseCode: phaseCode,
                    startNs: previous,
                    endNs: current,
                    rateBpm: 60_000_000_000.0 / Double(durationNs)
                )
            )
        }
    }
    return intervals.sorted {
        if $0.startNs != $1.startNs {
            return $0.startNs < $1.startNs
        }
        if $0.endNs != $1.endNs {
            return $0.endNs < $1.endNs
        }
        return $0.phaseCode < $1.phaseCode
    }
}

func referenceRateAt(timestampNs: Int64, intervals: [ReferenceInterval]) -> Double? {
    let rates = intervals
        .filter { $0.startNs <= timestampNs && timestampNs <= $0.endNs }
        .map(\.rateBpm)
    guard !rates.isEmpty else {
        return nil
    }
    return rates.reduce(0.0, +) / Double(rates.count)
}

func fitFusionCalibration(
    candidateHistoryByName: [String: [CandidateEstimate]],
    labels: [PhaseLabel],
    protocolName: String,
    annotationSessionID: Int?,
    version: Int?,
    nowNs: Int64? = nil,
    epsilon: Double = defaultCalibrationEpsilon,
    minimumPointsPerCandidate: Int = defaultMinimumCalibrationPoints
) -> FusionCalibration {
    let intervals = buildReferenceIntervalsFromLabels(labels: labels)
    guard !intervals.isEmpty else {
        return .default()
    }

    var biasByCandidate: [String: Double] = [:]
    var reliabilityByCandidate: [String: Double] = [:]
    var trainedPointCount = 0

    for (candidateName, estimates) in candidateHistoryByName {
        let errors = estimates.compactMap { estimate -> Double? in
            guard let referenceRate = referenceRateAt(timestampNs: estimate.estimatedAtNs, intervals: intervals) else {
                return nil
            }
            return estimate.rateBpm - referenceRate
        }
        guard errors.count >= minimumPointsPerCandidate else {
            continue
        }
        trainedPointCount += errors.count
        let bias = errors.reduce(0.0, +) / Double(errors.count)
        let mae = errors.map(abs).reduce(0.0, +) / Double(errors.count)
        biasByCandidate[candidateName] = bias
        reliabilityByCandidate[candidateName] = 1.0 / max(mae + epsilon, epsilon)
    }

    return FusionCalibration(
        version: version,
        protocolName: protocolName,
        annotationSessionID: annotationSessionID,
        biasByCandidate: biasByCandidate,
        reliabilityByCandidate: reliabilityByCandidate,
        minimumPointsPerCandidate: minimumPointsPerCandidate,
        epsilon: epsilon,
        trainedPointCount: trainedPointCount,
        trainedAtNs: nowNs
    )
}

public func computeRMSSDSeries(
    beats: [(Int64, Double)],
    windowSeconds: Int = 60
) -> [(Int64, Double)] {
    guard !beats.isEmpty else {
        return []
    }

    let windowNs = Int64(windowSeconds) * 1_000_000_000
    let timestamps = beats.map(\.0)
    let rrValues = beats.map(\.1)
    var startIndex = 0
    var result: [(Int64, Double)] = []

    for index in beats.indices {
        while timestamps[index] - timestamps[startIndex] > windowNs {
            startIndex += 1
        }
        let windowRR = Array(rrValues[startIndex...index])
        guard windowRR.count >= 5 else {
            continue
        }
        let diffs = differences(windowRR)
        let meanSquare = diffs.map { $0 * $0 }.reduce(0.0, +) / Double(diffs.count)
        result.append((timestamps[index], sqrt(meanSquare)))
    }

    return result
}

func rebuildLearnedFusionHistory(
    candidateHistoryByName: [String: [CandidateEstimate]],
    calibration: FusionCalibration,
    smoothingAlpha: Double = defaultFusionSmoothingAlpha
) -> [CandidateEstimate] {
    var grouped: [Int64: [CandidateEstimate]] = [:]
    for (candidateName, estimates) in candidateHistoryByName where candidateName != "learned_fusion" {
        for estimate in estimates {
            grouped[estimate.estimatedAtNs, default: []].append(estimate)
        }
    }

    var previousRate: Double?
    var history: [CandidateEstimate] = []
    for timestamp in grouped.keys.sorted() {
        let estimates = grouped[timestamp] ?? []
        var weightedTerms: [(Double, Double)] = []
        for estimate in estimates {
            let bias = calibration.biasByCandidate[estimate.source] ?? 0.0
            let reliability = calibration.reliabilityByCandidate[estimate.source] ?? 1.0
            let correctedRate = estimate.rateBpm - bias
            let weight = max(estimate.quality, 0.05) * reliability
            weightedTerms.append((correctedRate, weight))
        }
        let totalWeight = weightedTerms.reduce(0.0) { $0 + $1.1 }
        guard totalWeight > 0 else {
            continue
        }
        var rateBpm = weightedTerms.reduce(0.0) { $0 + $1.0 * $1.1 } / totalWeight
        if let previousRate {
            rateBpm = smoothingAlpha * rateBpm + (1.0 - smoothingAlpha) * previousRate
        }
        previousRate = rateBpm
        history.append(
            CandidateEstimate(
                estimatedAtNs: timestamp,
                rateBpm: rateBpm,
                quality: totalWeight / Double(weightedTerms.count),
                source: "learned_fusion",
                calibrationVersion: calibration.version
            )
        )
    }
    return history
}

private struct BiquadCoefficients {
    let b0: Double
    let b1: Double
    let b2: Double
    let a1: Double
    let a2: Double

    static func lowPass(sampleRateHz: Double, cutoffHz: Double, q: Double = 1.0 / sqrt(2.0)) -> BiquadCoefficients {
        biquad(sampleRateHz: sampleRateHz, type: .lowPass, frequencyHz: cutoffHz, q: q)
    }

    static func bandPass(sampleRateHz: Double, centerHz: Double, q: Double) -> BiquadCoefficients {
        biquad(sampleRateHz: sampleRateHz, type: .bandPass, frequencyHz: centerHz, q: q)
    }

    private enum FilterType {
        case lowPass
        case bandPass
    }

    private static func biquad(sampleRateHz: Double, type: FilterType, frequencyHz: Double, q: Double) -> BiquadCoefficients {
        let omega = 2.0 * Double.pi * frequencyHz / sampleRateHz
        let alpha = sin(omega) / (2.0 * q)
        let cosOmega = cos(omega)

        let rawB0: Double
        let rawB1: Double
        let rawB2: Double
        let rawA0 = 1.0 + alpha
        let rawA1 = -2.0 * cosOmega
        let rawA2 = 1.0 - alpha

        switch type {
        case .lowPass:
            rawB0 = (1.0 - cosOmega) / 2.0
            rawB1 = 1.0 - cosOmega
            rawB2 = (1.0 - cosOmega) / 2.0
        case .bandPass:
            rawB0 = alpha
            rawB1 = 0.0
            rawB2 = -alpha
        }

        return BiquadCoefficients(
            b0: rawB0 / rawA0,
            b1: rawB1 / rawA0,
            b2: rawB2 / rawA0,
            a1: rawA1 / rawA0,
            a2: rawA2 / rawA0
        )
    }
}

private struct SpectrumPoint {
    let frequencyHz: Double
    let power: Double
}

private func bandpassedRespiratoryWaveform(_ waveform: [Double], sampleRateHz: Double) -> [Double] {
    let detrended = detrendLinearly(waveform)
    let centerHz = (respiratoryBandHz.low + respiratoryBandHz.high) / 2.0
    let bandwidthHz = respiratoryBandHz.high - respiratoryBandHz.low
    let q = centerHz / max(bandwidthHz, 0.01)
    return zeroPhaseBiquadFilter(
        detrended,
        coefficients: BiquadCoefficients.bandPass(
            sampleRateHz: sampleRateHz,
            centerHz: centerHz,
            q: q
        )
    )
}

private func zeroPhaseBiquadFilter(_ input: [Double], coefficients: BiquadCoefficients) -> [Double] {
    guard input.count >= 3 else {
        return input
    }
    let forward = biquadFilter(input, coefficients: coefficients)
    return biquadFilter(forward.reversed(), coefficients: coefficients).reversed()
}

private func biquadFilter<S: Sequence>(_ input: S, coefficients: BiquadCoefficients) -> [Double] where S.Element == Double {
    var x1 = 0.0
    var x2 = 0.0
    var y1 = 0.0
    var y2 = 0.0
    var output: [Double] = []
    output.reserveCapacity(input.underestimatedCount)
    for x0 in input {
        let y0 = coefficients.b0 * x0
            + coefficients.b1 * x1
            + coefficients.b2 * x2
            - coefficients.a1 * y1
            - coefficients.a2 * y2
        output.append(y0)
        x2 = x1
        x1 = x0
        y2 = y1
        y1 = y0
    }
    return output
}

private func principalComponentSignal(from xyz: [[Double]]) -> [Double] {
    guard !xyz.isEmpty else {
        return []
    }

    let means = (0..<3).map { axis in
        xyz.map { $0[axis] }.reduce(0.0, +) / Double(xyz.count)
    }
    let centered = xyz.map { sample in
        zip(sample, means).map(-)
    }

    var covariance = Array(repeating: Array(repeating: 0.0, count: 3), count: 3)
    let scale = 1.0 / max(Double(centered.count - 1), 1.0)
    for sample in centered {
        for row in 0..<3 {
            for column in 0..<3 {
                covariance[row][column] += sample[row] * sample[column] * scale
            }
        }
    }

    var vector = [1.0, 1.0, 1.0]
    for _ in 0..<16 {
        let next = (0..<3).map { row in
            dot(covariance[row], vector)
        }
        let norm = sqrt(dot(next, next))
        guard norm > 0 else {
            break
        }
        vector = next.map { $0 / norm }
    }

    return centered.map { dot($0, vector) }
}

private func strideDownsample(_ values: [Double], factor: Int) -> [Double] {
    guard factor > 1 else {
        return values
    }
    return stride(from: 0, to: values.count, by: factor).map { values[$0] }
}

private func gradient(_ values: [Double]) -> [Double] {
    guard values.count > 1 else {
        return values
    }
    var result = Array(repeating: 0.0, count: values.count)
    result[0] = values[1] - values[0]
    result[values.count - 1] = values[values.count - 1] - values[values.count - 2]
    guard values.count > 2 else {
        return result
    }
    for index in 1..<(values.count - 1) {
        result[index] = (values[index + 1] - values[index - 1]) / 2.0
    }
    return result
}

private func findPeaks(in values: [Double], minimumDistance: Int, prominence: Double) -> [Int] {
    guard values.count >= 3 else {
        return []
    }
    let minDistance = max(minimumDistance, 1)
    var peaks: [Int] = []
    var lastAcceptedIndex = -minDistance
    for index in 1..<(values.count - 1) {
        let current = values[index]
        guard current > values[index - 1], current >= values[index + 1] else {
            continue
        }

        let leftMinimum = values[0...index].min() ?? current
        let rightMinimum = values[index..<values.count].min() ?? current
        let localProminence = current - max(leftMinimum, rightMinimum)
        guard localProminence >= prominence else {
            continue
        }

        if index - lastAcceptedIndex >= minDistance {
            peaks.append(index)
            lastAcceptedIndex = index
        } else if let last = peaks.last, current > values[last] {
            peaks[peaks.count - 1] = index
            lastAcceptedIndex = index
        }
    }
    return peaks
}

private func powerSpectrum(signal: [Double], sampleRateHz: Double) -> [SpectrumPoint] {
    let mean = signal.reduce(0.0, +) / Double(signal.count)
    let centered = signal.map { $0 - mean }
    let count = centered.count
    guard count > 1 else {
        return []
    }

    var result: [SpectrumPoint] = []
    let half = count / 2
    result.reserveCapacity(half)
    for k in 1...half {
        let frequency = Double(k) * sampleRateHz / Double(count)
        var real = 0.0
        var imaginary = 0.0
        for (index, value) in centered.enumerated() {
            let angle = -2.0 * Double.pi * Double(k * index) / Double(count)
            real += value * cos(angle)
            imaginary += value * sin(angle)
        }
        let power = (real * real + imaginary * imaginary) / Double(count)
        result.append(SpectrumPoint(frequencyHz: frequency, power: power))
    }
    return result
}

private func normalizedAutocorrelation(_ values: [Double]) -> [Double] {
    guard !values.isEmpty else {
        return []
    }
    var result = Array(repeating: 0.0, count: values.count)
    for lag in 0..<values.count {
        var sum = 0.0
        for index in 0..<(values.count - lag) {
            sum += values[index] * values[index + lag]
        }
        result[lag] = sum
    }
    guard let zeroLag = result.first, zeroLag != 0 else {
        return result
    }
    return result.map { $0 / zeroLag }
}

private func detrendLinearly(_ values: [Double]) -> [Double] {
    guard values.count >= 2 else {
        return values
    }
    let count = Double(values.count)
    let xMean = (count - 1.0) / 2.0
    let yMean = values.reduce(0.0, +) / count
    var numerator = 0.0
    var denominator = 0.0
    for (index, value) in values.enumerated() {
        let x = Double(index)
        numerator += (x - xMean) * (value - yMean)
        denominator += (x - xMean) * (x - xMean)
    }
    let slope = denominator == 0 ? 0.0 : numerator / denominator
    let intercept = yMean - slope * xMean
    return values.enumerated().map { index, value in
        value - (slope * Double(index) + intercept)
    }
}

private func interpolateLinear(x: [Double], y: [Double], xNew: [Double]) -> [Double] {
    guard x.count == y.count, !x.isEmpty else {
        return []
    }
    var output: [Double] = []
    output.reserveCapacity(xNew.count)
    var index = 0
    for target in xNew {
        while index + 1 < x.count && x[index + 1] < target {
            index += 1
        }
        if target <= x[0] {
            output.append(y[0])
            continue
        }
        if target >= x[x.count - 1] {
            output.append(y[y.count - 1])
            continue
        }
        let leftX = x[index]
        let rightX = x[index + 1]
        let leftY = y[index]
        let rightY = y[index + 1]
        let fraction = (target - leftX) / (rightX - leftX)
        output.append(leftY + fraction * (rightY - leftY))
    }
    return output
}

private func linspace(start: Double, end: Double, count: Int) -> [Double] {
    guard count > 1 else {
        return [start]
    }
    let step = (end - start) / Double(count - 1)
    return (0..<count).map { start + Double($0) * step }
}

private func uniformRange(start: Double, end: Double, step: Double) -> [Double] {
    guard step > 0, end > start else {
        return []
    }
    let count = Int(((end - start) / step).rounded(.down))
    guard count > 0 else {
        return []
    }
    return (0..<count).map { start + Double($0) * step }
}

private func differences(_ values: [Double]) -> [Double] {
    guard values.count > 1 else {
        return []
    }
    return zip(values.dropFirst(), values).map(-)
}

private func median(_ values: [Double]) -> Double {
    guard !values.isEmpty else {
        return 0.0
    }
    let sorted = values.sorted()
    let middle = sorted.count / 2
    if sorted.count.isMultiple(of: 2) {
        return (sorted[middle - 1] + sorted[middle]) / 2.0
    }
    return sorted[middle]
}

private func percentile(_ values: [Double], _ fraction: Double) -> Double {
    guard !values.isEmpty else {
        return 0.0
    }
    let sorted = values.sorted()
    let clamped = min(max(fraction, 0.0), 1.0)
    let position = Int((Double(sorted.count - 1) * clamped).rounded(.down))
    return sorted[position]
}

private func standardDeviation(_ values: [Double]) -> Double {
    guard !values.isEmpty else {
        return 0.0
    }
    let mean = values.reduce(0.0, +) / Double(values.count)
    let variance = values.map { ($0 - mean) * ($0 - mean) }.reduce(0.0, +) / Double(values.count)
    return sqrt(variance)
}

private func dot(_ lhs: [Double], _ rhs: [Double]) -> Double {
    zip(lhs, rhs).reduce(0.0) { $0 + $1.0 * $1.1 }
}
