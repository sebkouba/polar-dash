from __future__ import annotations

import json
import sqlite3
import time
from pathlib import Path
from typing import Any, Sequence

DEFAULT_DB_PATH = Path("data/polar_dash.db")


class Storage:
    def __init__(self, db_path: Path | str = DEFAULT_DB_PATH) -> None:
        self.db_path = Path(db_path).expanduser().resolve()
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.connection = sqlite3.connect(self.db_path)
        self.connection.row_factory = sqlite3.Row
        self._configure()
        self._initialize()

    def _configure(self) -> None:
        self.connection.execute("PRAGMA foreign_keys = ON")
        self.connection.execute("PRAGMA journal_mode = WAL")
        self.connection.execute("PRAGMA synchronous = NORMAL")

    def _initialize(self) -> None:
        self.connection.executescript(
            """
            CREATE TABLE IF NOT EXISTS sessions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                started_at_ns INTEGER NOT NULL,
                ended_at_ns INTEGER,
                device_name TEXT NOT NULL,
                device_address TEXT NOT NULL,
                battery_percent INTEGER,
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            );

            CREATE TABLE IF NOT EXISTS hr_frames (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                session_id INTEGER NOT NULL REFERENCES sessions(id) ON DELETE CASCADE,
                recorded_at_ns INTEGER NOT NULL,
                average_hr_bpm REAL NOT NULL,
                rr_intervals_ms_json TEXT NOT NULL,
                energy_kj INTEGER,
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            );

            CREATE TABLE IF NOT EXISTS ecg_frames (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                session_id INTEGER NOT NULL REFERENCES sessions(id) ON DELETE CASCADE,
                sensor_recorded_at_ns INTEGER NOT NULL,
                sample_rate_hz INTEGER NOT NULL,
                sample_count INTEGER NOT NULL,
                samples_json TEXT NOT NULL,
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            );

            CREATE TABLE IF NOT EXISTS acc_frames (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                session_id INTEGER NOT NULL REFERENCES sessions(id) ON DELETE CASCADE,
                sensor_recorded_at_ns INTEGER NOT NULL,
                sample_rate_hz INTEGER NOT NULL,
                sample_count INTEGER NOT NULL,
                samples_json TEXT NOT NULL,
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            );

            CREATE TABLE IF NOT EXISTS collector_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                session_id INTEGER REFERENCES sessions(id) ON DELETE CASCADE,
                recorded_at_ns INTEGER NOT NULL,
                level TEXT NOT NULL,
                event_type TEXT NOT NULL,
                details_json TEXT NOT NULL,
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            );

            CREATE TABLE IF NOT EXISTS breathing_estimates (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                session_id INTEGER NOT NULL REFERENCES sessions(id) ON DELETE CASCADE,
                estimated_at_ns INTEGER NOT NULL,
                breaths_per_min REAL NOT NULL,
                window_seconds INTEGER NOT NULL,
                source TEXT NOT NULL DEFAULT 'acc',
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(session_id, estimated_at_ns, source)
            );

            CREATE TABLE IF NOT EXISTS annotation_sessions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                started_at_ns INTEGER NOT NULL,
                ended_at_ns INTEGER,
                name TEXT NOT NULL,
                protocol_name TEXT NOT NULL,
                linked_session_id INTEGER REFERENCES sessions(id) ON DELETE SET NULL,
                notes_json TEXT NOT NULL DEFAULT '{}',
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            );

            CREATE TABLE IF NOT EXISTS breathing_phase_labels (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                annotation_session_id INTEGER NOT NULL REFERENCES annotation_sessions(id) ON DELETE CASCADE,
                sensor_session_id INTEGER REFERENCES sessions(id) ON DELETE SET NULL,
                recorded_at_ns INTEGER NOT NULL,
                phase_code TEXT NOT NULL,
                key_name TEXT NOT NULL,
                breathing_estimate_bpm REAL,
                breathing_estimate_source TEXT,
                breathing_estimate_time_ns INTEGER,
                estimate_age_ms REAL,
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            );

            CREATE INDEX IF NOT EXISTS idx_sessions_started_at
                ON sessions(started_at_ns DESC);
            CREATE INDEX IF NOT EXISTS idx_hr_frames_session_time
                ON hr_frames(session_id, recorded_at_ns);
            CREATE INDEX IF NOT EXISTS idx_ecg_frames_session_time
                ON ecg_frames(session_id, sensor_recorded_at_ns);
            CREATE INDEX IF NOT EXISTS idx_acc_frames_session_time
                ON acc_frames(session_id, sensor_recorded_at_ns);
            CREATE INDEX IF NOT EXISTS idx_collector_events_session_time
                ON collector_events(session_id, recorded_at_ns);
            CREATE INDEX IF NOT EXISTS idx_breathing_estimates_session_time
                ON breathing_estimates(session_id, estimated_at_ns);
            CREATE INDEX IF NOT EXISTS idx_annotation_sessions_started_at
                ON annotation_sessions(started_at_ns DESC);
            CREATE INDEX IF NOT EXISTS idx_breathing_phase_labels_session_time
                ON breathing_phase_labels(annotation_session_id, recorded_at_ns);
            CREATE INDEX IF NOT EXISTS idx_breathing_phase_labels_sensor_time
                ON breathing_phase_labels(sensor_session_id, recorded_at_ns);
            """
        )
        self.connection.commit()

    def close(self) -> None:
        self.connection.close()

    def start_session(self, device_name: str, device_address: str) -> int:
        started_at_ns = time.time_ns()
        cursor = self.connection.execute(
            """
            INSERT INTO sessions (started_at_ns, device_name, device_address)
            VALUES (?, ?, ?)
            """,
            (started_at_ns, device_name, device_address),
        )
        self.connection.commit()
        return int(cursor.lastrowid)

    def close_session(self, session_id: int) -> None:
        self.connection.execute(
            "UPDATE sessions SET ended_at_ns = ? WHERE id = ?",
            (time.time_ns(), session_id),
        )
        self.connection.commit()

    def update_session_battery(self, session_id: int, battery_percent: int) -> None:
        self.connection.execute(
            "UPDATE sessions SET battery_percent = ? WHERE id = ?",
            (battery_percent, session_id),
        )
        self.connection.commit()

    def insert_hr_frame(
        self,
        session_id: int,
        recorded_at_ns: int,
        average_hr_bpm: float,
        rr_intervals_ms: Sequence[int],
        energy_kj: int | None,
    ) -> None:
        self.connection.execute(
            """
            INSERT INTO hr_frames (
                session_id,
                recorded_at_ns,
                average_hr_bpm,
                rr_intervals_ms_json,
                energy_kj
            )
            VALUES (?, ?, ?, ?, ?)
            """,
            (
                session_id,
                recorded_at_ns,
                average_hr_bpm,
                json.dumps(list(rr_intervals_ms)),
                energy_kj,
            ),
        )
        self.connection.commit()

    def insert_ecg_frame(
        self,
        session_id: int,
        sensor_recorded_at_ns: int,
        sample_rate_hz: int,
        samples: Sequence[int],
    ) -> None:
        payload = list(samples)
        self.connection.execute(
            """
            INSERT INTO ecg_frames (
                session_id,
                sensor_recorded_at_ns,
                sample_rate_hz,
                sample_count,
                samples_json
            )
            VALUES (?, ?, ?, ?, ?)
            """,
            (
                session_id,
                sensor_recorded_at_ns,
                sample_rate_hz,
                len(payload),
                json.dumps(payload),
            ),
        )
        self.connection.commit()

    def insert_acc_frame(
        self,
        session_id: int,
        sensor_recorded_at_ns: int,
        sample_rate_hz: int,
        samples: Sequence[tuple[int, int, int]],
    ) -> None:
        payload = [list(sample) for sample in samples]
        self.connection.execute(
            """
            INSERT INTO acc_frames (
                session_id,
                sensor_recorded_at_ns,
                sample_rate_hz,
                sample_count,
                samples_json
            )
            VALUES (?, ?, ?, ?, ?)
            """,
            (
                session_id,
                sensor_recorded_at_ns,
                sample_rate_hz,
                len(payload),
                json.dumps(payload),
            ),
        )
        self.connection.commit()

    def insert_event(
        self,
        event_type: str,
        details: dict[str, Any],
        *,
        level: str = "INFO",
        session_id: int | None = None,
        recorded_at_ns: int | None = None,
    ) -> None:
        self.connection.execute(
            """
            INSERT INTO collector_events (
                session_id,
                recorded_at_ns,
                level,
                event_type,
                details_json
            )
            VALUES (?, ?, ?, ?, ?)
            """,
            (
                session_id,
                recorded_at_ns or time.time_ns(),
                level,
                event_type,
                json.dumps(details),
            ),
        )
        self.connection.commit()

    def insert_breathing_estimate(
        self,
        session_id: int,
        estimated_at_ns: int,
        breaths_per_min: float,
        window_seconds: int,
        *,
        source: str = "acc",
    ) -> None:
        self.connection.execute(
            """
            INSERT OR REPLACE INTO breathing_estimates (
                session_id,
                estimated_at_ns,
                breaths_per_min,
                window_seconds,
                source
            )
            VALUES (?, ?, ?, ?, ?)
            """,
            (
                session_id,
                estimated_at_ns,
                breaths_per_min,
                window_seconds,
                source,
            ),
        )
        self.connection.commit()

    def start_annotation_session(
        self,
        name: str,
        *,
        protocol_name: str,
        linked_session_id: int | None = None,
        notes: dict[str, Any] | None = None,
    ) -> int:
        cursor = self.connection.execute(
            """
            INSERT INTO annotation_sessions (
                started_at_ns,
                name,
                protocol_name,
                linked_session_id,
                notes_json
            )
            VALUES (?, ?, ?, ?, ?)
            """,
            (
                time.time_ns(),
                name,
                protocol_name,
                linked_session_id,
                json.dumps(notes or {}),
            ),
        )
        self.connection.commit()
        return int(cursor.lastrowid)

    def close_annotation_session(self, annotation_session_id: int) -> None:
        self.connection.execute(
            """
            UPDATE annotation_sessions
            SET ended_at_ns = ?
            WHERE id = ?
            """,
            (time.time_ns(), annotation_session_id),
        )
        self.connection.commit()

    def get_annotation_session(self, annotation_session_id: int) -> sqlite3.Row | None:
        return self.connection.execute(
            """
            SELECT *
            FROM annotation_sessions
            WHERE id = ?
            """,
            (annotation_session_id,),
        ).fetchone()

    def list_annotation_sessions(self, *, include_active: bool = True) -> list[sqlite3.Row]:
        active_filter = ""
        if not include_active:
            active_filter = "WHERE annotation_sessions.ended_at_ns IS NOT NULL"
        return list(
            self.connection.execute(
                f"""
                SELECT
                    annotation_sessions.*,
                    COUNT(breathing_phase_labels.id) AS label_count
                FROM annotation_sessions
                LEFT JOIN breathing_phase_labels
                    ON breathing_phase_labels.annotation_session_id = annotation_sessions.id
                {active_filter}
                GROUP BY annotation_sessions.id
                ORDER BY annotation_sessions.started_at_ns DESC
                """
            ).fetchall()
        )

    def delete_annotation_session(self, annotation_session_id: int) -> bool:
        cursor = self.connection.execute(
            """
            DELETE FROM annotation_sessions
            WHERE id = ?
            """,
            (annotation_session_id,),
        )
        self.connection.commit()
        return cursor.rowcount > 0

    def find_sensor_session_at(self, recorded_at_ns: int | None = None) -> sqlite3.Row | None:
        target_ns = recorded_at_ns or time.time_ns()
        row = self.find_active_sensor_session_at(target_ns)
        if row is not None:
            return row
        return self.connection.execute(
            """
            SELECT *
            FROM sessions
            WHERE started_at_ns <= ?
            ORDER BY started_at_ns DESC
            LIMIT 1
            """,
            (target_ns,),
        ).fetchone()

    def find_active_sensor_session_at(self, recorded_at_ns: int | None = None) -> sqlite3.Row | None:
        target_ns = recorded_at_ns or time.time_ns()
        return self.connection.execute(
            """
            SELECT *
            FROM sessions
            WHERE started_at_ns <= ?
              AND (ended_at_ns IS NULL OR ended_at_ns >= ?)
            ORDER BY started_at_ns DESC
            LIMIT 1
            """,
            (target_ns, target_ns),
        ).fetchone()

    def find_live_sensor_session_at(
        self,
        recorded_at_ns: int | None = None,
        *,
        max_idle_ns: int = 10_000_000_000,
    ) -> sqlite3.Row | None:
        target_ns = recorded_at_ns or time.time_ns()
        return self.connection.execute(
            """
            SELECT sessions.*
            FROM sessions
            WHERE sessions.started_at_ns <= ?
              AND (sessions.ended_at_ns IS NULL OR sessions.ended_at_ns >= ?)
              AND EXISTS (
                  SELECT 1
                  FROM acc_frames
                  WHERE acc_frames.session_id = sessions.id
                    AND acc_frames.sensor_recorded_at_ns BETWEEN ? AND ?
              )
            ORDER BY sessions.started_at_ns DESC
            LIMIT 1
            """,
            (
                target_ns,
                target_ns,
                target_ns - max_idle_ns,
                target_ns,
            ),
        ).fetchone()

    def find_nearest_breathing_estimate(
        self,
        recorded_at_ns: int,
        *,
        sensor_session_id: int | None = None,
        max_gap_ns: int = 30_000_000_000,
    ) -> sqlite3.Row | None:
        session_filter = ""
        params: list[Any] = [recorded_at_ns, recorded_at_ns, max_gap_ns]
        if sensor_session_id is not None:
            session_filter = "AND session_id = ?"
            params.append(sensor_session_id)

        row = self.connection.execute(
            f"""
            SELECT *,
                   ABS(estimated_at_ns - ?) AS absolute_gap_ns
            FROM breathing_estimates
            WHERE ABS(estimated_at_ns - ?) <= ?
              {session_filter}
            ORDER BY absolute_gap_ns ASC
                   , CASE source
                        WHEN 'fusion' THEN 0
                        WHEN 'acc-pca' THEN 1
                        WHEN 'acc' THEN 2
                        WHEN 'ecg-qrs-slope' THEN 3
                        ELSE 4
                     END ASC
            LIMIT 1
            """,
            params,
        ).fetchone()
        return row

    def insert_breathing_phase_label(
        self,
        annotation_session_id: int,
        *,
        recorded_at_ns: int,
        phase_code: str,
        key_name: str,
        sensor_session_id: int | None,
        breathing_estimate_bpm: float | None,
        breathing_estimate_source: str | None,
        breathing_estimate_time_ns: int | None,
        estimate_age_ms: float | None,
    ) -> int:
        cursor = self.connection.execute(
            """
            INSERT INTO breathing_phase_labels (
                annotation_session_id,
                sensor_session_id,
                recorded_at_ns,
                phase_code,
                key_name,
                breathing_estimate_bpm,
                breathing_estimate_source,
                breathing_estimate_time_ns,
                estimate_age_ms
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                annotation_session_id,
                sensor_session_id,
                recorded_at_ns,
                phase_code,
                key_name,
                breathing_estimate_bpm,
                breathing_estimate_source,
                breathing_estimate_time_ns,
                estimate_age_ms,
            ),
        )
        self.connection.commit()
        return int(cursor.lastrowid)

    def delete_last_breathing_phase_label(self, annotation_session_id: int) -> bool:
        row = self.connection.execute(
            """
            SELECT id
            FROM breathing_phase_labels
            WHERE annotation_session_id = ?
            ORDER BY recorded_at_ns DESC
            LIMIT 1
            """,
            (annotation_session_id,),
        ).fetchone()
        if row is None:
            return False
        self.connection.execute(
            "DELETE FROM breathing_phase_labels WHERE id = ?",
            (int(row["id"]),),
        )
        self.connection.commit()
        return True
