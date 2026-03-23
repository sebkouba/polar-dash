// swift-tools-version: 6.2
import PackageDescription

let package = Package(
    name: "BreathingBar",
    platforms: [
        .macOS(.v14)
    ],
    products: [
        .executable(name: "BreathingBar", targets: ["BreathingBar"])
    ],
    targets: [
        .executableTarget(
            name: "BreathingBar",
            linkerSettings: [
                .linkedLibrary("sqlite3")
            ]
        )
    ]
)
