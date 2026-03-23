from __future__ import annotations

import argparse
import asyncio
import logging
import os
from pathlib import Path
from typing import Sequence

from streamlit.web.bootstrap import run as run_streamlit

from polar_dash.collector import CollectorConfig, run_collection, scan_for_devices


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="polar-dash",
        description="Collect and inspect live Polar H10 physiology data.",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    scan_parser = subparsers.add_parser("scan", help="Scan for nearby Polar devices.")
    scan_parser.add_argument(
        "--prefix",
        default="Polar H10",
        help="Match devices whose advertised name contains this string.",
    )
    scan_parser.add_argument(
        "--timeout",
        type=float,
        default=8.0,
        help="BLE scan timeout in seconds.",
    )

    collect_parser = subparsers.add_parser(
        "collect",
        help="Continuously collect data into SQLite.",
    )
    collect_parser.add_argument(
        "--prefix",
        default="Polar H10",
        help="Match devices whose advertised name contains this string.",
    )
    collect_parser.add_argument(
        "--db",
        default="data/polar_dash.db",
        help="SQLite file for persisted raw data.",
    )
    collect_parser.add_argument(
        "--scan-timeout",
        type=float,
        default=10.0,
        help="BLE scan timeout in seconds.",
    )
    collect_parser.add_argument(
        "--reconnect-delay",
        type=float,
        default=3.0,
        help="Delay before retrying after a disconnect or failed scan.",
    )
    collect_parser.add_argument(
        "--no-ecg",
        action="store_true",
        help="Disable ECG streaming.",
    )
    collect_parser.add_argument(
        "--no-acc",
        action="store_true",
        help="Disable accelerometer streaming.",
    )
    collect_parser.add_argument(
        "--once",
        action="store_true",
        help="Exit instead of retrying forever if the device is missing or disconnects.",
    )

    dashboard_parser = subparsers.add_parser(
        "dashboard",
        help="Launch the live Streamlit dashboard.",
    )
    dashboard_parser.add_argument(
        "--db",
        default="data/polar_dash.db",
        help="SQLite file to read for persisted raw data.",
    )
    dashboard_parser.add_argument(
        "--host",
        default="127.0.0.1",
        help="Streamlit bind address.",
    )
    dashboard_parser.add_argument(
        "--port",
        type=int,
        default=8501,
        help="Streamlit port.",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )
    parser = _build_parser()
    args = parser.parse_args(argv)

    try:
        if args.command == "scan":
            devices = asyncio.run(scan_for_devices(args.prefix, args.timeout))
            if not devices:
                print("No matching devices found.")
                return 1
            for device in devices:
                rssi = device["rssi"]
                rssi_text = f" RSSI={rssi}" if rssi is not None else ""
                print(f'{device["name"]} [{device["address"]}]{rssi_text}')
            return 0

        if args.command == "collect":
            config = CollectorConfig(
                device_name_prefix=args.prefix,
                db_path=args.db,
                scan_timeout=args.scan_timeout,
                reconnect_delay=args.reconnect_delay,
                capture_ecg=not args.no_ecg,
                capture_acc=not args.no_acc,
                once=args.once,
            )
            asyncio.run(run_collection(config))
            return 0

        if args.command == "dashboard":
            os.environ["POLAR_DASH_DB"] = str(Path(args.db).expanduser().resolve())
            dashboard_path = Path(__file__).with_name("dashboard.py")
            run_streamlit(
                str(dashboard_path),
                False,
                [],
                {
                    "server.headless": True,
                    "server.address": args.host,
                    "server.port": args.port,
                    "browser.gatherUsageStats": False,
                },
            )
            return 0
    except KeyboardInterrupt:
        print("Interrupted.")
        return 130

    parser.print_help()
    return 1
