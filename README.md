# Polar Dash

`polar-dash` is a uv-managed Python app for collecting Polar H10 BLE data into SQLite and visualising it in a live dashboard.

## What It Captures

- Raw heart-rate service frames with RR intervals.
- Raw ECG frames from the Polar Measurement Data service.
- Raw accelerometer frames from the chest strap.
- Collector session metadata and connection events.

## What The Dashboard Shows

- Heart rate across time.
- Rolling HRV across time using RMSSD from RR intervals.
- Estimated breathing rate across time using the H10 accelerometer.
- A short raw ECG preview strip for sanity checks.

Breathing rate is an estimate derived from chest motion, not a direct respiration measurement, so it works best when you are relatively still.

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

## Useful Commands

```bash
uv run polar-dash scan --prefix "Polar H10"
uv run polar-dash collect --scan-timeout 10 --reconnect-delay 3
uv run polar-dash dashboard --db data/polar_dash.db --port 8501
```
