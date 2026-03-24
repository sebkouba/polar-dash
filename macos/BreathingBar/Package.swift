// swift-tools-version: 6.2
import PackageDescription

let package = Package(
    name: "BreathingBar",
    platforms: [
        .macOS(.v14)
    ],
    products: [
        .library(name: "BreathingBarCore", targets: ["BreathingBarCore"]),
        .executable(name: "BreathingBar", targets: ["BreathingBar"])
    ],
    targets: [
        .target(
            name: "BreathingBarCore"
        ),
        .executableTarget(
            name: "BreathingBar",
            dependencies: ["BreathingBarCore"],
            linkerSettings: [
                .linkedLibrary("sqlite3")
            ]
        ),
        .testTarget(
            name: "BreathingBarTests",
            dependencies: ["BreathingBarCore"],
        )
    ]
)
