from __future__ import annotations

from collections import deque
import hashlib
import json
import logging
from datetime import datetime
import time
from typing import Deque, Dict, Iterable, List, Optional, Tuple
from urllib.error import HTTPError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from .config import SearchPreset, Settings
from .dedupe import build_duplicate_fingerprint, normalize_text
from .models import FetchSummary, NormalizedJob, RunContext


LOGGER = logging.getLogger(__name__)
REQUEST_COOLDOWN_SECONDS = 1.1
RATE_LIMIT_RETRY_SECONDS = 5.0


def _parse_iso(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    normalized = value.replace("Z", "+00:00")
    try:
        return datetime.fromisoformat(normalized)
    except ValueError:
        return None


def _get_nested(data: Dict[str, object], *keys: str) -> object:
    current = data
    for key in keys:
        if not isinstance(current, dict):
            return None
        current = current.get(key)
    return current


def _ensure_list(value: object) -> List[str]:
    if isinstance(value, list):
        return [str(item) for item in value if str(item).strip()]
    if value is None:
        return []
    text = str(value).strip()
    return [text] if text else []


def _hash_external_id(raw_job: Dict[str, object], canonical_url: str) -> str:
    payload = "|".join(
        [
            str(raw_job.get("portal", "")),
            str(raw_job.get("source", "")),
            str(raw_job.get("title", "")),
            str(raw_job.get("company", "")),
            canonical_url,
        ]
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def normalize_job(raw_job: Dict[str, object], fetched_at: datetime) -> NormalizedJob:
    json_ld = raw_job.get("jsonLD") if isinstance(raw_job.get("jsonLD"), dict) else {}
    canonical_url = (
        str(raw_job.get("externalApplyUrl") or "")
        or str(_get_nested(json_ld, "url") or "")
        or str(raw_job.get("url") or "")
    )
    external_id = str(_get_nested(json_ld, "identifier") or "")
    if not external_id:
        external_id = _hash_external_id(raw_job, canonical_url)

    company = str(raw_job.get("company") or _get_nested(json_ld, "hiringOrganization", "name") or "")
    city = str(raw_job.get("city") or _get_nested(json_ld, "jobLocation", "address", "addressLocality") or "")
    state = str(raw_job.get("state") or _get_nested(json_ld, "jobLocation", "address", "addressRegion") or "")
    country_code = str(
        raw_job.get("countryCode") or _get_nested(json_ld, "jobLocation", "address", "addressCountry") or ""
    )
    timezone_offset = raw_job.get("timezoneOffset")
    if timezone_offset in ("", None):
        timezone_offset = None
    else:
        timezone_offset = int(timezone_offset)

    work_place = _ensure_list(raw_job.get("workPlace"))
    fingerprint = build_duplicate_fingerprint(
        title=str(raw_job.get("title") or _get_nested(json_ld, "title") or ""),
        company=company,
        workplace=work_place,
        city=city,
        state=state,
        canonical_url=canonical_url,
    )

    return NormalizedJob(
        external_id=external_id,
        portal=str(raw_job.get("portal") or ""),
        source=str(raw_job.get("source") or ""),
        title=str(raw_job.get("title") or _get_nested(json_ld, "title") or ""),
        company=company,
        country_code=country_code,
        state=state,
        city=city,
        timezone=str(raw_job.get("timezone") or ""),
        timezone_offset=timezone_offset,
        work_place=work_place,
        work_type=_ensure_list(raw_job.get("workType")),
        contract_type=_ensure_list(raw_job.get("contractType")),
        career_level=_ensure_list(raw_job.get("careerLevel")),
        occupation=str(raw_job.get("occupation") or _get_nested(json_ld, "relevantOccupation") or ""),
        industry=str(raw_job.get("industry") or _get_nested(json_ld, "industry") or ""),
        language=str(raw_job.get("language") or ""),
        is_direct=bool(raw_job.get("isDirect")),
        is_recruiter=bool(raw_job.get("isRecruiter")),
        date_created=str(raw_job.get("dateCreated") or _get_nested(json_ld, "datePosted") or ""),
        date_active=str(raw_job.get("dateActive") or ""),
        date_expired=str(raw_job.get("dateExpired") or _get_nested(json_ld, "validThrough") or ""),
        canonical_url=canonical_url,
        description=str(_get_nested(json_ld, "description") or raw_job.get("description") or ""),
        duplicate_fingerprint=fingerprint,
        is_canonical=False,
        fetched_at=fetched_at.isoformat(),
        raw_json=raw_job,
    )


def build_query_params(
    preset: SearchPreset,
    page: int,
    lower_bound: Optional[datetime],
    upper_bound: datetime,
    *,
    title_override: Optional[str] = None,
) -> Dict[str, str]:
    params = dict(preset.query_params)
    params["page"] = str(page)
    if title_override is not None:
        params["title"] = f'"{title_override}"'
    # The API documentation is inconsistent between dateCreated and dateCreatedMin/Max.
    # We send both bounds when available and rely on local dedupe if the API falls back to day precision.
    params["dateCreatedMax"] = upper_bound.date().isoformat()
    if lower_bound:
        params["dateCreatedMin"] = lower_bound.date().isoformat()
    return {key: value for key, value in params.items() if value not in ("", None)}


def remote_berlin_compatible(job: NormalizedJob) -> bool:
    work_blob = normalize_text(" ".join(job.work_place))
    if "remote" not in work_blob:
        return False

    text_blob = normalize_text(
        " ".join(
            [
                job.description,
                job.city,
                job.state,
                job.country_code,
                str(_get_nested(job.raw_json, "jsonLD", "applicantLocationRequirements") or ""),
                str(_get_nested(job.raw_json, "jsonLD", "jobLocation", "name") or ""),
            ]
        )
    )
    blocked_markers = (
        "us only",
        "united states only",
        "uk only",
        "canada only",
        "must be based in us",
        "must reside in us",
    )
    if any(marker in text_blob for marker in blocked_markers):
        return False

    if job.country_code.lower() in {"", "de", "germany"}:
        return True
    if job.timezone_offset is None:
        return True
    return -1 <= job.timezone_offset <= 3


def title_matches(job: NormalizedJob, search_titles: Iterable[str]) -> bool:
    haystack = normalize_text(" ".join([job.title, job.description, job.occupation]))
    return any(normalize_text(term) in haystack for term in search_titles)


class JobDataFeedsClient:
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
            LOGGER.info("Cooling down before next API request: sleep_seconds=%.2f", remaining)
            self._sleep(remaining)

    def _mark_request_attempt(self) -> None:
        self._last_request_monotonic = time.monotonic()

    def _perform_request(self, params: Dict[str, str]) -> Dict[str, object]:
        self._apply_request_cooldown()
        query = urlencode(params)
        LOGGER.info(
            "Requesting JobDataFeeds: page=%s title=%s workPlace=%s dateCreatedMin=%s dateCreatedMax=%s",
            params.get("page"),
            params.get("title"),
            params.get("workPlace", ""),
            params.get("dateCreatedMin", ""),
            params.get("dateCreatedMax", ""),
        )
        request = Request(
            f"{self.settings.rapidapi_base_url}?{query}",
            headers={
                "Content-Type": "application/json",
                "x-rapidapi-host": self.settings.jobdatafeeds_api_host,
                "x-rapidapi-key": self.settings.jobdatafeeds_api_key,
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
                        "JobDataFeeds rate limited the request: page=%s title=%s cooldown_seconds=%.1f retry_attempt=%s",
                        params.get("page"),
                        params.get("title"),
                        RATE_LIMIT_RETRY_SECONDS,
                        attempt + 1,
                    )
                    self._sleep(RATE_LIMIT_RETRY_SECONDS)
                    attempt += 1
                    continue
                raise
        LOGGER.info(
            "JobDataFeeds response received: page=%s totalCount=%s pageSize=%s raw_results=%s",
            params.get("page"),
            payload.get("totalCount"),
            payload.get("pageSize"),
            len(payload.get("result", [])) if isinstance(payload.get("result"), list) else 0,
        )
        return payload

    def _normalize_page_jobs(
        self,
        raw_items: List[object],
        context: RunContext,
        *,
        remote_only: bool,
    ) -> List[NormalizedJob]:
        normalized_page: List[NormalizedJob] = []
        rejected_wrong_title = 0
        rejected_wrong_location = 0
        rejected_out_of_window = 0
        for raw_item in raw_items:
            if not isinstance(raw_item, dict):
                continue
            job = normalize_job(raw_item, context.started_at)
            if not title_matches(job, self.settings.search_titles):
                rejected_wrong_title += 1
                continue
            if remote_only:
                if not remote_berlin_compatible(job):
                    rejected_wrong_location += 1
                    continue
            posted_at = _parse_iso(job.date_created)
            if context.lower_bound and posted_at and posted_at <= context.lower_bound:
                rejected_out_of_window += 1
                continue
            if posted_at and posted_at > context.upper_bound:
                rejected_out_of_window += 1
                continue
            normalized_page.append(job)
        LOGGER.info(
            "Normalized page: kept=%s rejected_title=%s rejected_location=%s rejected_window=%s remote_only=%s",
            len(normalized_page),
            rejected_wrong_title,
            rejected_wrong_location,
            rejected_out_of_window,
            remote_only,
        )
        return normalized_page

    def _fetch_local_jobs(
        self,
        preset: SearchPreset,
        context: RunContext,
    ) -> FetchSummary:
        jobs: List[NormalizedJob] = []
        api_requests_made = 0
        truncated_by_request_cap = False
        incomplete_titles: set[str] = set()
        queue: Deque[tuple[str, int]] = deque((title, 1) for title in self.settings.search_titles)

        while queue:
            if api_requests_made >= self.settings.max_api_requests_per_run:
                truncated_by_request_cap = True
                incomplete_titles.update(title for title, _ in queue)
                LOGGER.warning(
                    "Request cap reached for local fetch: cap=%s incomplete_titles=%s",
                    self.settings.max_api_requests_per_run,
                    sorted(incomplete_titles),
                )
                break

            title, page = queue.popleft()
            params = build_query_params(
                preset,
                page,
                context.lower_bound,
                context.upper_bound,
                title_override=title,
            )
            payload = self._perform_request(params)
            api_requests_made += 1
            result = payload.get("result", [])
            if not isinstance(result, list):
                result = []

            normalized_page = self._normalize_page_jobs(result, context, remote_only=False)
            page_size = int(payload.get("pageSize", 10) or 10)
            has_more_pages = bool(result) and page_size > 0 and len(result) >= page_size
            jobs.extend(normalized_page)
            LOGGER.info(
                "Local title page processed: title=%s page=%s kept=%s raw=%s has_more_pages=%s queue_remaining=%s",
                title,
                page,
                len(normalized_page),
                len(result),
                has_more_pages,
                len(queue),
            )

            if has_more_pages:
                queue.append((title, page + 1))

        return FetchSummary(
            jobs=jobs,
            api_requests_made=api_requests_made,
            jobs_fetched=len(jobs),
            was_truncated_by_request_cap=truncated_by_request_cap,
            incomplete_titles=sorted(incomplete_titles),
        )

    def _fetch_preset_jobs(
        self,
        preset: SearchPreset,
        context: RunContext,
        *,
        starting_api_requests: int,
    ) -> FetchSummary:
        jobs: List[NormalizedJob] = []
        api_requests_made = starting_api_requests
        truncated_by_request_cap = False

        page = 1
        while True:
            if api_requests_made >= self.settings.max_api_requests_per_run:
                truncated_by_request_cap = True
                LOGGER.warning(
                    "Request cap reached for preset fetch: preset=%s cap=%s",
                    preset.name,
                    self.settings.max_api_requests_per_run,
                )
                break

            params = build_query_params(preset, page, context.lower_bound, context.upper_bound)
            payload = self._perform_request(params)
            api_requests_made += 1
            result = payload.get("result", [])
            if not isinstance(result, list) or not result:
                LOGGER.info("Preset fetch ended: preset=%s page=%s raw_results=0", preset.name, page)
                break

            normalized_page = self._normalize_page_jobs(result, context, remote_only=preset.remote_only)
            jobs.extend(normalized_page)
            LOGGER.info(
                "Preset page processed: preset=%s page=%s kept=%s raw=%s",
                preset.name,
                page,
                len(normalized_page),
                len(result),
            )
            page += 1
            page_size = int(payload.get("pageSize", 10) or 10)
            total_count = int(payload.get("totalCount", 0) or 0)
            if page_size * (page - 1) >= total_count:
                LOGGER.info(
                    "Preset fetch complete: preset=%s pages=%s total_count=%s kept=%s",
                    preset.name,
                    page - 1,
                    total_count,
                    len(jobs),
                )
                break

        return FetchSummary(
            jobs=jobs,
            api_requests_made=api_requests_made - starting_api_requests,
            jobs_fetched=len(jobs),
            was_truncated_by_request_cap=truncated_by_request_cap,
            incomplete_titles=[],
        )

    def fetch_jobs(self, context: RunContext, *, include_remote: bool = True) -> FetchSummary:
        jobs: List[NormalizedJob] = []
        api_requests_made = 0
        truncated_by_request_cap = False
        incomplete_titles: set[str] = set()

        LOGGER.info(
            "Starting fetch cycle: include_remote=%s presets=%s request_cap=%s titles=%s lower_bound=%s upper_bound=%s",
            include_remote,
            [preset.name for preset in self.settings.build_presets(include_remote=include_remote)],
            self.settings.max_api_requests_per_run,
            self.settings.search_titles,
            context.lower_bound.isoformat() if context.lower_bound else None,
            context.upper_bound.isoformat(),
        )
        for preset in self.settings.build_presets(include_remote=include_remote):
            if not preset.remote_only:
                summary = self._fetch_local_jobs(preset, context)
            else:
                summary = self._fetch_preset_jobs(
                    preset,
                    context,
                    starting_api_requests=api_requests_made,
                )

            jobs.extend(summary.jobs)
            api_requests_made += summary.api_requests_made
            truncated_by_request_cap = truncated_by_request_cap or summary.was_truncated_by_request_cap
            incomplete_titles.update(summary.incomplete_titles)

            if truncated_by_request_cap:
                break

        LOGGER.info(
            "Fetch cycle finished: jobs=%s api_requests=%s truncated=%s incomplete_titles=%s",
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
