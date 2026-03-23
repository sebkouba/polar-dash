# Code Rating
- vibe slop
- brittle
- working

# Polar Dash

Polar Dash is a fast personal prototype for turning a Polar H10 chest strap into a lightweight macOS breathing and heart-rate monitor.

I built it to get a reasonable live approximation of:

- breathing rate,
- heart rate,
- short-window HRV (RMSSD),

visible on my MacBook without needing a full training or lab setup.

It is a practical hack, not a medical device. The breathing signal is inferred from ECG, RR intervals, and chest motion, so it is useful for feedback and experimentation, not diagnosis.

![Menu bar companion](docs/images/menu-bar-demo.png)

The menu bar companion keeps the latest breathing rate, heart rate, and HRV visible at a glance.

![Breathing cockpit](docs/images/cockpit-demo.png)

The cockpit is the heavier live view: raw ECG, accelerometer traces, breathing candidates, fusion output, and a visible shortcut panel for quick labeling/calibration sessions.

## Why This Exists

Most consumer heart-rate tools are good at heart rate and rough HRV, but not at making breathing visible in a way that is always in front of you while you work.

This repo is the result of a short build focused on that exact gap:

- collect live Polar H10 BLE data,
- persist enough raw signal data to revisit the session later,
- estimate breathing from multiple imperfect signals,
- surface the result both in a full cockpit and a tiny menu bar readout.

## What's In The Repo

- `src/polar_dash/collector.py`: BLE collection from the Polar H10 into SQLite.
- `src/polar_dash/breathing.py`: ACC, ECG, RR, and fusion-based breathing estimation.
- `src/polar_dash/cockpit.py`: native Tk live cockpit for collection, visualization, and label-driven recalibration.
- `src/polar_dash/dashboard.py`: Streamlit dashboard for inspecting persisted sessions.
- `src/polar_dash/labeler_v2.py`: keyboard-driven breathing-phase labeling workflow.
- `src/polar_dash/evaluate.py`: scoring utilities for estimates versus saved labels.
- `macos/BreathingBar`: Swift menu bar companion that reads the latest snapshot from SQLite.

## Quick Start

Requirements:

- macOS
- Python 3.13+
- `uv`
- Swift toolchain / Xcode command line tools

Install dependencies:

```bash
uv sync
```

Scan for your strap:

```bash
uv run polar-dash scan --timeout 4
```

Start the live cockpit collector:

```bash
uv run polar-dash cockpit
```

In another terminal, launch the menu bar companion against the same database:

```bash
POLAR_DASH_DB=data/polar_dash.db swift run --package-path macos/BreathingBar
```

If you prefer the older browser-based view:

```bash
uv run polar-dash dashboard --port 8501
```

Then open `http://127.0.0.1:8501`.

## Useful Commands

```bash
uv run polar-dash scan --prefix "Polar H10"
uv run polar-dash collect --db data/polar_dash.db
uv run polar-dash cockpit --db data/polar_dash.db
uv run polar-dash annotate-breathing --db data/polar_dash.db
uv run polar-dash evaluate-breathing --db data/polar_dash.db
uv run polar-dash dashboard --db data/polar_dash.db --port 8501
swift build --package-path macos/BreathingBar
./scripts/install-hr-stack.sh
```

The `install-hr-stack.sh` helper installs `hron` and `hroff`, which wrap the cockpit and the menu bar app for quick local start/stop cycles.

## Keyboard Shortcuts

The cockpit shows the shortcuts in the sidebar, and the label keys are active once you start a label session:

- `F`: mark "finished exhaling"
- `G`: mark "finished inhaling"
- `Esc`: close the cockpit

## How Breathing Is Estimated

The estimator does not measure respiration directly. It builds a plausible rate estimate by combining:

- chest accelerometer motion,
- ECG-derived respiration features,
- RR-interval rhythm information,
- a learned fusion step with simple calibration support.

Research notes and the reasoning behind that approach live in [docs/breathing-rate-research.md](docs/breathing-rate-research.md).

## Repo Hygiene

This repository is intentionally prepared for public upload:

- local databases are ignored,
- runtime logs and scratch captures are ignored,
- no device IDs or machine-specific absolute paths are referenced in tracked files,
- the committed screenshots are sanitized demo assets rather than committed live databases.

Ignored local-only paths include `data/`, `tmp/`, and `.hr_stack/`.

## Verification

```bash
uv run python -m py_compile src/polar_dash/*.py
swift build --package-path macos/BreathingBar
```

## Limitations

- Breathing rate is approximate and sensitive to movement, strap placement, and signal quality.
- The menu bar app is macOS-only.
- The cockpit is intentionally utilitarian; this was built to be useful quickly, not polished into a product.
