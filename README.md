# Polar Dash

`polar-dash` is a uv-managed Python app for collecting Polar H10 BLE data into SQLite and visualising it in a live dashboard.

## What It Captures

- Raw heart-rate service frames with RR intervals.
- Raw ECG frames from the Polar Measurement Data service.
- Raw accelerometer frames from the chest strap.
- Derived breathing estimates persisted from fused ECG and accelerometer signals.
- Manual breathing phase labels with nanosecond timestamps.
- Collector session metadata and connection events.

## What The Dashboard Shows

- Heart rate across time.
- Rolling HRV across time using RMSSD from RR intervals.
- Estimated breathing rate across time using fused H10 ECG and accelerometer data.
- A short raw ECG preview strip for sanity checks.

Breathing rate is an estimate derived from ECG and chest motion, not a direct respiration measurement, so it is still sensitive to movement and signal quality.

## Quick Start

```bash
uv sync
uv run polar-dash scan --timeout 4
uv run polar-dash collect
```

In a second terminal:

```bash
uv run polar-dash dashboard --port 8501
```

Then open [http://127.0.0.1:8501](http://127.0.0.1:8501).

## Data Location

The collector writes to `data/polar_dash.db` by default. You can override that with `--db`.

### Tables

- `sessions`: connection sessions and device metadata.
- `hr_frames`: raw heart-rate frames plus RR intervals.
- `ecg_frames`: raw ECG frames.
- `acc_frames`: raw accelerometer frames.
- `breathing_estimates`: derived breathing-rate points for lightweight clients.
- `annotation_sessions`: manual labeling sessions for supervised experiments.
- `breathing_phase_labels`: high-resolution breathing phase markers keyed to live data.
- `collector_events`: connection and streaming events.

## Useful Commands

```bash
uv run polar-dash scan --prefix "Polar H10"
uv run polar-dash collect --scan-timeout 10 --reconnect-delay 3
uv run polar-dash dashboard --db data/polar_dash.db --port 8501
uv run polar-dash backfill-breathing --db data/polar_dash.db
uv run polar-dash annotate-breathing --db data/polar_dash.db
swift run --package-path macos/BreathingBar
```

## Manual Labeling Widget

Use the Python labeling widget when you want supervised breathing-phase data aligned with the raw ECG/ACC capture:

```bash
uv run polar-dash annotate-breathing
```

The widget stays on top, shows the current breathing estimate plus recent history, and records these keys with nanosecond timestamps:

- `L`: started inhaling
- `H`: finished inhaling
- `J`: started exhaling
- `K`: finished exhaling
- `U`: undo last label
- `Q` or `Esc`: quit

Each label is linked to:

- the active sensor session when available,
- the nearest breathing estimate in the database,
- the full raw ECG/ACC stream already being stored continuously.

That gives us the data needed to build a labeled dataset and tune the estimator later.

## Research Notes

Breathing-rate research findings and experiment guidance are in [docs/breathing-rate-research.md](/Users/sebastian/code/polar-dash/docs/breathing-rate-research.md).

## Native Menu Bar App

The macOS menu bar app lives in `macos/BreathingBar`.

It reads `breathing_estimates` from SQLite, shows the current breathing rate plus a tiny sparkline in the menu bar, and flashes the indicator background when the current rate is below or above the configured thresholds.

Threshold defaults:

- Low flash threshold: `8.0 br/min`
- High flash threshold: `24.0 br/min`

You can change both thresholds from the app’s popover window after launching it.
