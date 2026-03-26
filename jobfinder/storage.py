from __future__ import annotations

import json
import logging
import sqlite3
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from typing import Iterable, Iterator, List, Optional, Sequence

from .models import NormalizedJob


LOGGER = logging.getLogger(__name__)


SCHEMA = """
CREATE TABLE IF NOT EXISTS jobs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    external_id TEXT NOT NULL,
    collector TEXT NOT NULL DEFAULT 'jobdatafeeds',
    query_text TEXT NOT NULL DEFAULT '',
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
    incomplete_titles_json TEXT NOT NULL DEFAULT '[]',
    error_message TEXT
);

CREATE TABLE IF NOT EXISTS checkpoints (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    last_successful_upper_bound TEXT
);

INSERT OR IGNORE INTO checkpoints (id, last_successful_upper_bound) VALUES (1, NULL);
"""

UPSERT_JOBS_SQL = """
INSERT INTO jobs (
    external_id, collector, query_text, portal, source, title, company, country_code, state, city,
    timezone, timezone_offset, work_place_json, work_type_json, contract_type_json,
    career_level_json, occupation, industry, language, is_direct, is_recruiter,
    date_created, date_active, date_expired, canonical_url, description,
    duplicate_fingerprint, is_canonical, sent_at, fetched_at, raw_json
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(portal, source, external_id) DO UPDATE SET
    collector = excluded.collector,
    query_text = excluded.query_text,
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
"""


class Storage:
    def __init__(self, db_path: Path):
        self.db_path = db_path
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_db()
        LOGGER.info("Storage initialized: db_path=%s", self.db_path)

    def _init_db(self) -> None:
        with self.connect() as conn:
            conn.executescript(SCHEMA)
            run_columns = self._table_columns(conn, "runs")
            job_columns = self._table_columns(conn, "jobs")
            if "collector" not in job_columns:
                conn.execute("ALTER TABLE jobs ADD COLUMN collector TEXT NOT NULL DEFAULT 'jobdatafeeds'")
                conn.execute("UPDATE jobs SET collector = 'jobdatafeeds' WHERE collector = '' OR collector IS NULL")
            if "query_text" not in job_columns:
                conn.execute("ALTER TABLE jobs ADD COLUMN query_text TEXT NOT NULL DEFAULT ''")
            if "incomplete_titles_json" not in run_columns:
                conn.execute(
                    "ALTER TABLE runs ADD COLUMN incomplete_titles_json TEXT NOT NULL DEFAULT '[]'"
                )

    @staticmethod
    def _table_columns(conn: sqlite3.Connection, table_name: str) -> set[str]:
        return {row["name"] for row in conn.execute(f"PRAGMA table_info({table_name})").fetchall()}

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
            run_id = int(cursor.lastrowid)
        LOGGER.info("Run created: run_id=%s started_at=%s", run_id, started_at.isoformat())
        return run_id

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
        incomplete_titles: Sequence[str],
        error_message: Optional[str] = None,
    ) -> None:
        with self.connect() as conn:
            conn.execute(
                """
                UPDATE runs
                SET ended_at = ?, status = ?, api_requests_made = ?, jobs_fetched = ?,
                    jobs_inserted = ?, jobs_canonical = ?, was_truncated_by_request_cap = ?,
                    incomplete_titles_json = ?, error_message = ?
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
                    json.dumps(sorted(incomplete_titles)),
                    error_message,
                    run_id,
                ),
            )
        LOGGER.info(
            "Run finalized: run_id=%s status=%s api_requests=%s jobs_fetched=%s jobs_inserted=%s jobs_canonical=%s truncated=%s incomplete_titles=%s",
            run_id,
            status,
            api_requests_made,
            jobs_fetched,
            jobs_inserted,
            jobs_canonical,
            was_truncated_by_request_cap,
            list(sorted(incomplete_titles)),
        )

    def get_run(self, run_id: int) -> sqlite3.Row:
        with self.connect() as conn:
            row = conn.execute("SELECT * FROM runs WHERE id = ?", (run_id,)).fetchone()
        if row is None:
            raise ValueError(f"Unknown run id: {run_id}")
        return row

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
        LOGGER.info("Checkpoint updated: upper_bound=%s", upper_bound.isoformat())

    def upsert_jobs(self, jobs: Sequence[NormalizedJob]) -> int:
        inserted = 0
        with self.connect() as conn:
            for job in jobs:
                cursor = conn.execute(UPSERT_JOBS_SQL, self._job_to_row(job))
                inserted += cursor.rowcount if cursor.rowcount > 0 else 0
        LOGGER.info("Jobs upserted: batch_size=%s sqlite_rowcount_sum=%s", len(jobs), inserted)
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
        LOGGER.info("Canonical flags updated: canonical_urls=%s", len(canonical_urls))

    def get_all_jobs(self) -> List[NormalizedJob]:
        with self.connect() as conn:
            rows = list(conn.execute("SELECT * FROM jobs ORDER BY fetched_at DESC").fetchall())
        LOGGER.info("Loaded all jobs from storage: count=%s", len(rows))
        return [self._row_to_job(row) for row in rows]

    def get_unsent_canonical_jobs(self) -> List[sqlite3.Row]:
        with self.connect() as conn:
            rows = list(
                conn.execute(
                    """
                    SELECT *
                    FROM jobs
                    WHERE is_canonical = 1 AND sent_at IS NULL
                    ORDER BY COALESCE(date_created, fetched_at) DESC
                    """
                ).fetchall()
            )
        LOGGER.info("Loaded unsent canonical jobs: count=%s", len(rows))
        return rows

    def mark_jobs_sent(self, canonical_urls: Iterable[str], sent_at: datetime) -> None:
        urls = list(canonical_urls)
        if not urls:
            LOGGER.info("No jobs to mark as sent.")
            return
        with self.connect() as conn:
            conn.executemany(
                "UPDATE jobs SET sent_at = ? WHERE canonical_url = ?",
                [(sent_at.isoformat(), url) for url in urls],
            )
        LOGGER.info("Jobs marked as sent: count=%s sent_at=%s", len(urls), sent_at.isoformat())

    @staticmethod
    def _job_to_row(job: NormalizedJob) -> tuple:
        return (
            job.external_id,
            job.collector,
            job.query_text,
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
        )

    @staticmethod
    def _row_to_job(row: sqlite3.Row) -> NormalizedJob:
        return NormalizedJob(
            external_id=row["external_id"],
            collector=row["collector"],
            query_text=row["query_text"],
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
