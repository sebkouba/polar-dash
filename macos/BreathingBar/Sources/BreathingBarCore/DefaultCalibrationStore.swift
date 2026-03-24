import Foundation

public enum DefaultCalibrationStore {
    public static let repoRelativePath = "defaults/default_breathing_calibration.json"

    public static func load(from url: URL) -> FusionCalibration? {
        guard let data = try? Data(contentsOf: url) else {
            return nil
        }
        return try? JSONDecoder().decode(FusionCalibration.self, from: data)
    }

    public static func loadFromRepo(searchRoots: [URL]? = nil) -> FusionCalibration? {
        let searchRoots = searchRoots ?? defaultSearchRoots()
        guard let url = locateRepoDefaultCalibrationURL(searchRoots: searchRoots) else {
            return nil
        }
        return load(from: url)
    }

    static func locateRepoDefaultCalibrationURL(searchRoots: [URL]) -> URL? {
        let fileManager = FileManager.default
        for root in searchRoots {
            var current = root.standardizedFileURL
            while true {
                let candidate = current.appendingPathComponent(repoRelativePath)
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
        return nil
    }

    static func defaultSearchRoots(
        currentDirectoryPath: String = FileManager.default.currentDirectoryPath,
        executablePath: String = CommandLine.arguments[0]
    ) -> [URL] {
        [
            URL(fileURLWithPath: currentDirectoryPath),
            URL(fileURLWithPath: executablePath).deletingLastPathComponent(),
        ]
    }
}
