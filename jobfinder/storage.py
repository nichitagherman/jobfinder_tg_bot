from __future__ import annotations

import json
import sqlite3
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from typing import Iterable, Iterator, List, Optional, Sequence

from .models import NormalizedJob


SCHEMA = """
CREATE TABLE IF NOT EXISTS jobs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    external_id TEXT NOT NULL,
    portal TEXT NOT NULL,
    source TEXT NOT NULL,
    title TEXT NOT NULL,
    company TEXT NOT NULL,
    country_code TEXT NOT NULL,
    state TEXT NOT NULL,
    city TEXT NOT NULL,
    timezone TEXT NOT NULL,
    timezone_offset INTEGER,
    work_place_json TEXT NOT NULL,
    work_type_json TEXT NOT NULL,
    contract_type_json TEXT NOT NULL,
    career_level_json TEXT NOT NULL,
    occupation TEXT NOT NULL,
    industry TEXT NOT NULL,
    language TEXT NOT NULL,
    is_direct INTEGER NOT NULL,
    is_recruiter INTEGER NOT NULL,
    date_created TEXT,
    date_active TEXT,
    date_expired TEXT,
    canonical_url TEXT NOT NULL,
    description TEXT NOT NULL,
    duplicate_fingerprint TEXT NOT NULL,
    is_canonical INTEGER NOT NULL DEFAULT 0,
    sent_at TEXT,
    fetched_at TEXT NOT NULL,
    raw_json TEXT NOT NULL,
    UNIQUE (portal, source, external_id)
);

CREATE TABLE IF NOT EXISTS runs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    started_at TEXT NOT NULL,
    ended_at TEXT,
    status TEXT NOT NULL,
    api_requests_made INTEGER NOT NULL DEFAULT 0,
    jobs_fetched INTEGER NOT NULL DEFAULT 0,
    jobs_inserted INTEGER NOT NULL DEFAULT 0,
    jobs_canonical INTEGER NOT NULL DEFAULT 0,
    was_truncated_by_request_cap INTEGER NOT NULL DEFAULT 0,
    was_truncated_by_job_cap INTEGER NOT NULL DEFAULT 0,
    error_message TEXT
);

CREATE TABLE IF NOT EXISTS checkpoints (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    last_successful_upper_bound TEXT
);

INSERT OR IGNORE INTO checkpoints (id, last_successful_upper_bound) VALUES (1, NULL);
"""


class Storage:
    def __init__(self, db_path: Path):
        self.db_path = db_path
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_db()

    def _init_db(self) -> None:
        with self.connect() as conn:
            conn.executescript(SCHEMA)

    @contextmanager
    def connect(self) -> Iterator[sqlite3.Connection]:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        try:
            yield conn
            conn.commit()
        finally:
            conn.close()

    def create_run(self, started_at: datetime) -> int:
        with self.connect() as conn:
            cursor = conn.execute(
                "INSERT INTO runs (started_at, status) VALUES (?, ?)",
                (started_at.isoformat(), "running"),
            )
            return int(cursor.lastrowid)

    def finalize_run(
        self,
        run_id: int,
        *,
        ended_at: datetime,
        status: str,
        api_requests_made: int,
        jobs_fetched: int,
        jobs_inserted: int,
        jobs_canonical: int,
        was_truncated_by_request_cap: bool,
        was_truncated_by_job_cap: bool,
        error_message: Optional[str] = None,
    ) -> None:
        with self.connect() as conn:
            conn.execute(
                """
                UPDATE runs
                SET ended_at = ?, status = ?, api_requests_made = ?, jobs_fetched = ?,
                    jobs_inserted = ?, jobs_canonical = ?, was_truncated_by_request_cap = ?,
                    was_truncated_by_job_cap = ?, error_message = ?
                WHERE id = ?
                """,
                (
                    ended_at.isoformat(),
                    status,
                    api_requests_made,
                    jobs_fetched,
                    jobs_inserted,
                    jobs_canonical,
                    int(was_truncated_by_request_cap),
                    int(was_truncated_by_job_cap),
                    error_message,
                    run_id,
                ),
            )

    def get_last_checkpoint(self) -> Optional[datetime]:
        with self.connect() as conn:
            row = conn.execute(
                "SELECT last_successful_upper_bound FROM checkpoints WHERE id = 1"
            ).fetchone()
        if not row or not row["last_successful_upper_bound"]:
            return None
        return datetime.fromisoformat(row["last_successful_upper_bound"])

    def update_checkpoint(self, upper_bound: datetime) -> None:
        with self.connect() as conn:
            conn.execute(
                "UPDATE checkpoints SET last_successful_upper_bound = ? WHERE id = 1",
                (upper_bound.isoformat(),),
            )

    def upsert_jobs(self, jobs: Sequence[NormalizedJob]) -> int:
        inserted = 0
        with self.connect() as conn:
            for job in jobs:
                cursor = conn.execute(
                    """
                    INSERT INTO jobs (
                        external_id, portal, source, title, company, country_code, state, city,
                        timezone, timezone_offset, work_place_json, work_type_json, contract_type_json,
                        career_level_json, occupation, industry, language, is_direct, is_recruiter,
                        date_created, date_active, date_expired, canonical_url, description,
                        duplicate_fingerprint, is_canonical, sent_at, fetched_at, raw_json
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(portal, source, external_id) DO UPDATE SET
                        title = excluded.title,
                        company = excluded.company,
                        country_code = excluded.country_code,
                        state = excluded.state,
                        city = excluded.city,
                        timezone = excluded.timezone,
                        timezone_offset = excluded.timezone_offset,
                        work_place_json = excluded.work_place_json,
                        work_type_json = excluded.work_type_json,
                        contract_type_json = excluded.contract_type_json,
                        career_level_json = excluded.career_level_json,
                        occupation = excluded.occupation,
                        industry = excluded.industry,
                        language = excluded.language,
                        is_direct = excluded.is_direct,
                        is_recruiter = excluded.is_recruiter,
                        date_created = excluded.date_created,
                        date_active = excluded.date_active,
                        date_expired = excluded.date_expired,
                        canonical_url = excluded.canonical_url,
                        description = excluded.description,
                        duplicate_fingerprint = excluded.duplicate_fingerprint,
                        fetched_at = excluded.fetched_at,
                        raw_json = excluded.raw_json
                    """,
                    (
                        job.external_id,
                        job.portal,
                        job.source,
                        job.title,
                        job.company,
                        job.country_code,
                        job.state,
                        job.city,
                        job.timezone,
                        job.timezone_offset,
                        json.dumps(job.work_place),
                        json.dumps(job.work_type),
                        json.dumps(job.contract_type),
                        json.dumps(job.career_level),
                        job.occupation,
                        job.industry,
                        job.language,
                        int(job.is_direct),
                        int(job.is_recruiter),
                        job.date_created,
                        job.date_active,
                        job.date_expired,
                        job.canonical_url,
                        job.description,
                        job.duplicate_fingerprint,
                        int(job.is_canonical),
                        None,
                        job.fetched_at,
                        json.dumps(job.raw_json),
                    ),
                )
                inserted += cursor.rowcount if cursor.rowcount > 0 else 0
        return inserted

    def update_canonical_flags(self, canonical_urls: Iterable[str]) -> None:
        canonical_urls = list(canonical_urls)
        with self.connect() as conn:
            conn.execute("UPDATE jobs SET is_canonical = 0")
            if canonical_urls:
                conn.executemany(
                    "UPDATE jobs SET is_canonical = 1 WHERE canonical_url = ?",
                    [(url,) for url in canonical_urls],
                )

    def get_all_jobs(self) -> List[NormalizedJob]:
        with self.connect() as conn:
            rows = list(conn.execute("SELECT * FROM jobs ORDER BY fetched_at DESC").fetchall())
        return [self._row_to_job(row) for row in rows]

    def get_unsent_canonical_jobs(self) -> List[sqlite3.Row]:
        with self.connect() as conn:
            return list(
                conn.execute(
                    """
                    SELECT *
                    FROM jobs
                    WHERE is_canonical = 1 AND sent_at IS NULL
                    ORDER BY COALESCE(date_created, fetched_at) DESC
                    """
                ).fetchall()
            )

    def mark_jobs_sent(self, canonical_urls: Iterable[str], sent_at: datetime) -> None:
        urls = list(canonical_urls)
        if not urls:
            return
        with self.connect() as conn:
            conn.executemany(
                "UPDATE jobs SET sent_at = ? WHERE canonical_url = ?",
                [(sent_at.isoformat(), url) for url in urls],
            )

    @staticmethod
    def _row_to_job(row: sqlite3.Row) -> NormalizedJob:
        return NormalizedJob(
            external_id=row["external_id"],
            portal=row["portal"],
            source=row["source"],
            title=row["title"],
            company=row["company"],
            country_code=row["country_code"],
            state=row["state"],
            city=row["city"],
            timezone=row["timezone"],
            timezone_offset=row["timezone_offset"],
            work_place=json.loads(row["work_place_json"]),
            work_type=json.loads(row["work_type_json"]),
            contract_type=json.loads(row["contract_type_json"]),
            career_level=json.loads(row["career_level_json"]),
            occupation=row["occupation"],
            industry=row["industry"],
            language=row["language"],
            is_direct=bool(row["is_direct"]),
            is_recruiter=bool(row["is_recruiter"]),
            date_created=row["date_created"],
            date_active=row["date_active"],
            date_expired=row["date_expired"],
            canonical_url=row["canonical_url"],
            description=row["description"],
            duplicate_fingerprint=row["duplicate_fingerprint"],
            is_canonical=bool(row["is_canonical"]),
            fetched_at=row["fetched_at"],
            raw_json=json.loads(row["raw_json"]),
        )
