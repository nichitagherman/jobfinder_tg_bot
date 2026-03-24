from __future__ import annotations

import hashlib
import json
from datetime import datetime
from typing import Dict, Iterable, List, Optional, Tuple
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from .config import SearchPreset, Settings
from .dedupe import build_duplicate_fingerprint, normalize_text
from .models import FetchSummary, NormalizedJob, RunContext


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
) -> Dict[str, str]:
    params = dict(preset.query_params)
    params["page"] = str(page)
    # The API documentation is inconsistent between dateCreated and dateCreatedMin/Max.
    # We send both bounds when available and rely on local dedupe if the API falls back to day precision.
    params["dateCreatedMax"] = upper_bound.date().isoformat()
    if lower_bound:
        params["dateCreatedMin"] = lower_bound.date().isoformat()
    return {key: value for key, value in params.items() if value not in ("", None)}


def berlin_match(job: NormalizedJob) -> bool:
    location_blob = normalize_text(
        " ".join(
            [
                job.city,
                job.state,
                job.country_code,
                str(job.raw_json.get("locale", "")),
                str(_get_nested(job.raw_json, "jsonLD", "jobLocation", "name") or ""),
            ]
        )
    )
    return any(term in location_blob for term in ("berlin"))


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

    def _perform_request(self, params: Dict[str, str]) -> Dict[str, object]:
        query = urlencode(params)
        request = Request(
            f"{self.settings.rapidapi_base_url}?{query}",
            headers={
                "Content-Type": "application/json",
                "x-rapidapi-host": self.settings.jobdatafeeds_api_host,
                "x-rapidapi-key": self.settings.jobdatafeeds_api_key,
            },
            method="GET",
        )
        with urlopen(request, timeout=30) as response:
            return json.loads(response.read().decode("utf-8"))

    def fetch_jobs(self, context: RunContext, *, include_remote: bool = True) -> FetchSummary:
        jobs: List[NormalizedJob] = []
        api_requests_made = 0
        truncated_by_request_cap = False
        truncated_by_job_cap = False

        for preset in self.settings.build_presets(include_remote=include_remote):
            page = 1
            while True:
                if api_requests_made >= self.settings.max_api_requests_per_run:
                    truncated_by_request_cap = True
                    break
                if len(jobs) >= self.settings.max_jobs_per_run:
                    truncated_by_job_cap = True
                    break

                params = build_query_params(preset, page, context.lower_bound, context.upper_bound)
                payload = self._perform_request(params)
                api_requests_made += 1
                result = payload.get("result", [])
                if not isinstance(result, list) or not result:
                    break

                normalized_page: List[NormalizedJob] = []
                for raw_item in result:
                    if not isinstance(raw_item, dict):
                        continue
                    job = normalize_job(raw_item, context.started_at)
                    if not title_matches(job, self.settings.search_titles):
                        continue
                    if preset.remote_only:
                        if not remote_berlin_compatible(job):
                            continue
                    else:
                        if not berlin_match(job):
                            continue
                    posted_at = _parse_iso(job.date_created)
                    if context.lower_bound and posted_at and posted_at <= context.lower_bound:
                        continue
                    if posted_at and posted_at > context.upper_bound:
                        continue
                    normalized_page.append(job)

                remaining = self.settings.max_jobs_per_run - len(jobs)
                jobs.extend(normalized_page[:remaining])
                if len(normalized_page) > remaining:
                    truncated_by_job_cap = True
                    break
                if len(jobs) >= self.settings.max_jobs_per_run:
                    truncated_by_job_cap = True
                    break
                page += 1
                page_size = int(payload.get("pageSize", 10) or 10)
                total_count = int(payload.get("totalCount", 0) or 0)
                if page_size * (page - 1) >= total_count:
                    break

            if truncated_by_request_cap or truncated_by_job_cap:
                break

        return FetchSummary(
            jobs=jobs,
            api_requests_made=api_requests_made,
            jobs_fetched=len(jobs),
            was_truncated_by_request_cap=truncated_by_request_cap,
            was_truncated_by_job_cap=truncated_by_job_cap,
        )
