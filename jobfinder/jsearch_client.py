from __future__ import annotations

import hashlib
import json
import logging
import time
from collections import deque
from datetime import datetime, timedelta
from typing import Deque, Dict, List, Optional, Sequence, Tuple
from urllib.error import HTTPError
from urllib.parse import urlencode, urlsplit, urlunsplit
from urllib.request import Request, urlopen

from .config import Settings
from .dedupe import build_duplicate_fingerprint, normalize_text
from .jobdatafeeds_client import excluded_by_seniority_title, title_matches
from .logging_utils import FILTERED_OUT_LOGGER_NAME
from .models import FetchSummary, NormalizedJob, RunContext


LOGGER = logging.getLogger(__name__)
FILTERED_OUT_LOGGER = logging.getLogger(FILTERED_OUT_LOGGER_NAME)
PROVIDER_NAME = "jsearch"
REQUEST_COOLDOWN_SECONDS = 2.0
RATE_LIMIT_RETRY_SECONDS = 5.0
PAGE_SIZE = 10


def _parse_iso(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    normalized = value.replace("Z", "+00:00")
    try:
        return datetime.fromisoformat(normalized)
    except ValueError:
        return None


def _ensure_list(value: object) -> List[str]:
    if isinstance(value, list):
        return [str(item) for item in value if str(item).strip()]
    if value is None:
        return []
    text = str(value).strip()
    return [text] if text else []


def _normalize_portal(publisher: str, canonical_url: str) -> str:
    publisher_normalized = normalize_text(publisher).replace(" ", "_")
    netloc = urlsplit(canonical_url).netloc.lower()
    if "linkedin.com" in netloc:
        return "linkedin"
    if publisher_normalized == "linkedin":
        return "linkedin"
    return publisher_normalized


def _normalize_canonical_url(url: str) -> str:
    if not url:
        return url
    parts = urlsplit(url)
    netloc = parts.netloc.lower()
    if netloc == "de.linkedin.com":
        return urlunsplit((parts.scheme, "linkedin.com", parts.path, parts.query, parts.fragment))
    return url


def _choose_apply_url(raw_job: Dict[str, object]) -> Tuple[str, bool]:
    apply_options = raw_job.get("apply_options")
    if isinstance(apply_options, list):
        for option in apply_options:
            if isinstance(option, dict) and option.get("is_direct") and option.get("apply_link"):
                return _normalize_canonical_url(str(option["apply_link"])), True
    top_level = str(raw_job.get("job_apply_link") or "").strip()
    if top_level:
        return _normalize_canonical_url(top_level), bool(raw_job.get("job_apply_is_direct"))
    google_link = str(raw_job.get("job_google_link") or "").strip()
    return google_link, False


def _hash_external_id(raw_job: Dict[str, object], canonical_url: str) -> str:
    payload = "|".join(
        [
            str(raw_job.get("job_title") or ""),
            str(raw_job.get("employer_name") or ""),
            canonical_url,
            str(raw_job.get("job_google_link") or ""),
        ]
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def normalize_job(raw_job: Dict[str, object], fetched_at: datetime) -> NormalizedJob:
    canonical_url, is_direct = _choose_apply_url(raw_job)
    external_id = str(raw_job.get("job_id") or "").strip()
    if not external_id:
        external_id = _hash_external_id(raw_job, canonical_url)

    portal = _normalize_portal(str(raw_job.get("job_publisher") or ""), canonical_url)
    company = str(raw_job.get("employer_name") or "")
    city = str(raw_job.get("job_city") or "")
    state = str(raw_job.get("job_state") or "")
    country_code = str(raw_job.get("job_country") or "")
    work_type = [value.lower() for value in _ensure_list(raw_job.get("job_employment_types"))]

    fingerprint = build_duplicate_fingerprint(
        title=str(raw_job.get("job_title") or ""),
        company=company,
        description=str(raw_job.get("job_description") or ""),
    )

    return NormalizedJob(
        external_id=external_id,
        collector=PROVIDER_NAME,
        portal=portal,
        source=PROVIDER_NAME,
        title=str(raw_job.get("job_title") or ""),
        company=company,
        country_code=country_code,
        state=state,
        city=city,
        timezone="",
        timezone_offset=None,
        work_place=[],
        work_type=work_type,
        contract_type=[],
        career_level=[],
        occupation="",
        industry="",
        language="",
        is_direct=is_direct,
        is_recruiter=False,
        date_created=str(raw_job.get("job_posted_at_datetime_utc") or ""),
        date_active="",
        date_expired="",
        canonical_url=canonical_url,
        description=str(raw_job.get("job_description") or ""),
        duplicate_fingerprint=fingerprint,
        is_canonical=False,
        fetched_at=fetched_at.isoformat(),
        raw_json=raw_job,
    )


def select_date_posted(context: RunContext) -> str:
    if context.lower_bound is None:
        return "anytime"
    window = context.upper_bound - context.lower_bound
    if window <= timedelta(days=1):
        return "today"
    if window <= timedelta(days=3):
        return "3days"
    if window <= timedelta(days=7):
        return "week"
    if window <= timedelta(days=30):
        return "month"
    return "anytime"


class JSearchClient:
    def __init__(self, settings: Settings):
        self.settings = settings
        self._last_request_monotonic: float | None = None

    def _sleep(self, seconds: float) -> None:
        time.sleep(seconds)

    def _apply_request_cooldown(self) -> None:
        if self._last_request_monotonic is None:
            return
        elapsed = time.monotonic() - self._last_request_monotonic
        remaining = REQUEST_COOLDOWN_SECONDS - elapsed
        if remaining > 0:
            LOGGER.info("Cooling down before next API request: provider=%s sleep_seconds=%.2f", PROVIDER_NAME, remaining)
            self._sleep(remaining)

    def _mark_request_attempt(self) -> None:
        self._last_request_monotonic = time.monotonic()

    def _log_filtered_out_job(
        self,
        *,
        reason: str,
        job: NormalizedJob,
        context: RunContext,
        remote_query: bool,
        details: Optional[Dict[str, object]] = None,
    ) -> None:
        FILTERED_OUT_LOGGER.info(
            json.dumps(
                {
                    "reason": reason,
                    "provider": PROVIDER_NAME,
                    "title": job.title,
                    "company": job.company,
                    "portal": job.portal,
                    "source": job.source,
                    "city": job.city,
                    "state": job.state,
                    "country_code": job.country_code,
                    "date_created": job.date_created,
                    "canonical_url": job.canonical_url,
                    "remote_query": remote_query,
                    "lower_bound": context.lower_bound.isoformat() if context.lower_bound else None,
                    "upper_bound": context.upper_bound.isoformat(),
                    "details": details or {},
                    "raw_job": job.raw_json,
                },
                ensure_ascii=True,
            )
        )

    def _perform_request(self, params: Dict[str, str]) -> Dict[str, object]:
        self._apply_request_cooldown()
        query = urlencode(params)
        curl_like = (
            "curl --request GET "
            f"--url '{self.settings.jsearch_base_url}?{query}' "
            "--header 'Content-Type: application/json' "
            f"--header 'x-rapidapi-host: {self.settings.jsearch_api_host}' "
            "--header 'x-rapidapi-key: [REDACTED]'"
        )
        LOGGER.info(
            "Requesting JSearch: provider=%s page=%s query=%s work_from_home=%s date_posted=%s",
            PROVIDER_NAME,
            params.get("page"),
            params.get("query"),
            params.get("work_from_home", ""),
            params.get("date_posted"),
        )
        LOGGER.info("JSearch cURL provider=%s: %s", PROVIDER_NAME, curl_like)
        request = Request(
            f"{self.settings.jsearch_base_url}?{query}",
            headers={
                "Content-Type": "application/json",
                "x-rapidapi-host": self.settings.jsearch_api_host,
                "x-rapidapi-key": self.settings.jsearch_api_key or "",
            },
            method="GET",
        )

        attempt = 1
        while True:
            self._mark_request_attempt()
            try:
                with urlopen(request, timeout=30) as response:
                    payload = json.loads(response.read().decode("utf-8"))
                break
            except HTTPError as exc:
                if exc.code == 429 and attempt == 1:
                    LOGGER.warning(
                        "JSearch rate limited the request: provider=%s query=%s page=%s cooldown_seconds=%.1f retry_attempt=%s",
                        PROVIDER_NAME,
                        params.get("query"),
                        params.get("page"),
                        RATE_LIMIT_RETRY_SECONDS,
                        attempt + 1,
                    )
                    self._sleep(RATE_LIMIT_RETRY_SECONDS)
                    attempt += 1
                    continue
                raise

        LOGGER.info(
            "JSearch response received: provider=%s page=%s status=%s raw_results=%s",
            PROVIDER_NAME,
            params.get("page"),
            payload.get("status"),
            len(payload.get("data", [])) if isinstance(payload.get("data"), list) else 0,
        )
        return payload

    def _build_query_params(
        self,
        *,
        title: str,
        page: int,
        context: RunContext,
        remote_query: bool,
    ) -> Dict[str, str]:
        params = {
            "query": title if remote_query else f"{title} in Berlin",
            "page": str(page),
            "num_pages": "1",
            "country": self.settings.search_country_code,
            "date_posted": select_date_posted(context),
        }
        if remote_query:
            params["work_from_home"] = "true"
        return params

    def _normalize_page_jobs(
        self,
        raw_items: List[object],
        context: RunContext,
        *,
        remote_query: bool,
    ) -> List[NormalizedJob]:
        normalized_page: List[NormalizedJob] = []
        for raw_item in raw_items:
            if not isinstance(raw_item, dict):
                continue
            job = normalize_job(raw_item, context.started_at)
            if not title_matches(job, self.settings.search_titles):
                self._log_filtered_out_job(
                    reason=f"{PROVIDER_NAME}_title_mismatch",
                    job=job,
                    context=context,
                    remote_query=remote_query,
                )
                continue
            seniority_markers = excluded_by_seniority_title(job)
            if seniority_markers:
                self._log_filtered_out_job(
                    reason=f"{PROVIDER_NAME}_seniority_title_excluded",
                    job=job,
                    context=context,
                    remote_query=remote_query,
                    details={"matched_markers": seniority_markers},
                )
                continue
            posted_at = _parse_iso(job.date_created)
            if context.lower_bound and posted_at and posted_at <= context.lower_bound:
                self._log_filtered_out_job(
                    reason=f"{PROVIDER_NAME}_before_or_equal_lower_bound",
                    job=job,
                    context=context,
                    remote_query=remote_query,
                )
                continue
            if posted_at and posted_at > context.upper_bound:
                self._log_filtered_out_job(
                    reason=f"{PROVIDER_NAME}_after_upper_bound",
                    job=job,
                    context=context,
                    remote_query=remote_query,
                )
                continue
            normalized_page.append(job)
        LOGGER.info(
            "Normalized JSearch page: provider=%s kept=%s remote_query=%s",
            PROVIDER_NAME,
            len(normalized_page),
            remote_query,
        )
        return normalized_page

    def _fetch_mode_jobs(
        self,
        context: RunContext,
        *,
        remote_query: bool,
        starting_api_requests: int,
    ) -> FetchSummary:
        jobs: List[NormalizedJob] = []
        api_requests_made = starting_api_requests
        truncated_by_request_cap = False
        incomplete_titles: set[str] = set()
        queue: Deque[tuple[str, int]] = deque((title, 1) for title in self.settings.search_titles)

        while queue:
            if api_requests_made >= self.settings.jsearch_max_api_requests_per_run:
                truncated_by_request_cap = True
                incomplete_titles.update(title for title, _ in queue)
                LOGGER.warning(
                    "Request cap reached for JSearch fetch: provider=%s remote_query=%s cap=%s incomplete_titles=%s",
                    PROVIDER_NAME,
                    remote_query,
                    self.settings.jsearch_max_api_requests_per_run,
                    sorted(incomplete_titles),
                )
                break

            title, page = queue.popleft()
            params = self._build_query_params(title=title, page=page, context=context, remote_query=remote_query)
            payload = self._perform_request(params)
            api_requests_made += 1
            result = payload.get("data", [])
            if not isinstance(result, list):
                result = []

            normalized_page = self._normalize_page_jobs(result, context, remote_query=remote_query)
            jobs.extend(normalized_page)
            has_more_pages = len(result) >= PAGE_SIZE
            LOGGER.info(
                "JSearch title page processed: provider=%s title=%s page=%s kept=%s raw=%s has_more_pages=%s queue_remaining=%s remote_query=%s",
                PROVIDER_NAME,
                title,
                page,
                len(normalized_page),
                len(result),
                has_more_pages,
                len(queue),
                remote_query,
            )
            if has_more_pages:
                queue.append((title, page + 1))

        return FetchSummary(
            jobs=jobs,
            api_requests_made=api_requests_made - starting_api_requests,
            jobs_fetched=len(jobs),
            was_truncated_by_request_cap=truncated_by_request_cap,
            incomplete_titles=sorted(incomplete_titles),
        )

    def fetch_jobs(self, context: RunContext, *, include_remote: bool = True) -> FetchSummary:
        jobs: List[NormalizedJob] = []
        api_requests_made = 0
        truncated_by_request_cap = False
        incomplete_titles: set[str] = set()

        LOGGER.info(
            "Starting fetch cycle: provider=%s include_remote=%s request_cap=%s titles=%s lower_bound=%s upper_bound=%s",
            PROVIDER_NAME,
            include_remote,
            self.settings.jsearch_max_api_requests_per_run,
            self.settings.search_titles,
            context.lower_bound.isoformat() if context.lower_bound else None,
            context.upper_bound.isoformat(),
        )

        for remote_query in [False, True] if include_remote else [False]:
            summary = self._fetch_mode_jobs(
                context,
                remote_query=remote_query,
                starting_api_requests=api_requests_made,
            )
            jobs.extend(summary.jobs)
            api_requests_made += summary.api_requests_made
            truncated_by_request_cap = truncated_by_request_cap or summary.was_truncated_by_request_cap
            incomplete_titles.update(summary.incomplete_titles)
            if truncated_by_request_cap:
                break

        LOGGER.info(
            "Fetch cycle finished: provider=%s jobs=%s api_requests=%s truncated=%s incomplete_titles=%s",
            PROVIDER_NAME,
            len(jobs),
            api_requests_made,
            truncated_by_request_cap,
            sorted(incomplete_titles),
        )

        return FetchSummary(
            jobs=jobs,
            api_requests_made=api_requests_made,
            jobs_fetched=len(jobs),
            was_truncated_by_request_cap=truncated_by_request_cap,
            incomplete_titles=sorted(incomplete_titles),
        )
