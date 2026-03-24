from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, List, Optional


@dataclass
class NormalizedJob:
    external_id: str
    portal: str
    source: str
    title: str
    company: str
    country_code: str
    state: str
    city: str
    timezone: str
    timezone_offset: Optional[int]
    work_place: List[str]
    work_type: List[str]
    contract_type: List[str]
    career_level: List[str]
    occupation: str
    industry: str
    language: str
    is_direct: bool
    is_recruiter: bool
    date_created: Optional[str]
    date_active: Optional[str]
    date_expired: Optional[str]
    canonical_url: str
    description: str
    duplicate_fingerprint: str
    is_canonical: bool
    fetched_at: str
    raw_json: Dict[str, Any]


@dataclass
class FetchSummary:
    jobs: List[NormalizedJob]
    api_requests_made: int
    jobs_fetched: int
    was_truncated_by_request_cap: bool
    was_truncated_by_job_cap: bool


@dataclass
class RunContext:
    started_at: datetime
    upper_bound: datetime
    lower_bound: Optional[datetime]
