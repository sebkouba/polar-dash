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
            name: "BreathingBarCore",
            resources: [
                .process("Resources")
            ],
            linkerSettings: [
                .linkedLibrary("sqlite3")
            ]
        ),
        .executableTarget(
            name: "BreathingBar",
            dependencies: ["BreathingBarCore"],
        ),
        .testTarget(
            name: "BreathingBarTests",
            dependencies: ["BreathingBarCore"],
        )
    ]
)
