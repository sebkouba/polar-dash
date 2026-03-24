import Foundation

public enum DefaultCalibrationStore {
    public static let resourceName = "default_breathing_calibration"
    public static let resourceExtension = "json"

    public static func load(from url: URL) -> FusionCalibration? {
        guard let data = try? Data(contentsOf: url) else {
            return nil
        }
        return try? JSONDecoder().decode(FusionCalibration.self, from: data)
    }

    public static func loadBundledCalibration() -> FusionCalibration? {
        guard let url = Bundle.module.url(forResource: resourceName, withExtension: resourceExtension) else {
            return nil
        }
        return load(from: url)
    }
}
