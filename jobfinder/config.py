from __future__ import annotations

import os
import re
import tomllib
from dataclasses import dataclass
from datetime import time
from pathlib import Path
from typing import Any, Dict, List, Optional


_TITLE_TOKEN_RE = re.compile(r"[A-Za-z0-9]+")


def load_dotenv(path: Path) -> None:
    if not path.exists():
        return
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip("'").strip('"')
        os.environ[key] = value


def _get_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def _get_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None:
        return default
    return int(raw)


def _get_optional(name: str) -> Optional[str]:
    value = os.getenv(name)
    if value is None:
        return None
    stripped = value.strip()
    return stripped or None


def _get_list(name: str) -> List[str]:
    raw = os.getenv(name)
    if raw is None:
        return []
    return [item.strip() for item in raw.split(",") if item.strip()]


def build_api_title_query(title: str) -> str:
    tokens = [token.lower() for token in _TITLE_TOKEN_RE.findall(title)]
    if not tokens:
        return title.strip()
    return ",".join(f"+{token}" for token in tokens)


@dataclass(frozen=True)
class SearchPreset:
    name: str
    query_params: Dict[str, str]
    remote_only: bool


@dataclass(frozen=True)
class Settings:
    jobdatafeeds_api_key: str
    jobdatafeeds_api_host: str
    jsearch_enabled: bool
    jsearch_api_key: Optional[str]
    jsearch_api_host: str
    telegram_bot_token: str
    telegram_chat_ids: List[str]
    db_path: Path
    timezone: str
    notification_times: List[time]
    jobdatafeeds_max_api_requests_per_run: int
    jsearch_max_api_requests_per_run: int
    search_titles: List[str]
    priority_companies: List[str]
    search_country_code: str
    allow_all_sources: bool
    cv_path: Optional[Path]
    cover_letter_path: Optional[Path]
    log_path: Path
    filtered_out_jobs_log_path: Path
    env_path: Path
    filters_path: Path

    @property
    def rapidapi_base_url(self) -> str:
        return f"https://{self.jobdatafeeds_api_host}/api/v2/jobs/search"

    @property
    def jsearch_base_url(self) -> str:
        return f"https://{self.jsearch_api_host}/search"

    def build_presets(self, *, include_remote: bool = True) -> List[SearchPreset]:
        title_query = " OR ".join(build_api_title_query(title) for title in self.search_titles)
        common = {
            "format": "json",
            "title": title_query,
            "industry": "-construction",
        }
        presets = [
            SearchPreset(
                name="berlin_all_workplaces",
                query_params={
                    **common,
                    "geoPointLat": "52.5200",
                    "geoPointLng": "13.4050",
                    "geoDistance": "15mi",
                },
                remote_only=False,
            )
        ]
        if include_remote:
            presets.append(
                SearchPreset(
                    name="remote_berlin_compatible",
                    query_params={**common, "workPlace": "remote"},
                    remote_only=True,
                )
            )
        return presets


def load_filter_titles(path: Path) -> List[str]:
    payload = load_filter_payload(path)
    job_titles = payload.get("job_titles")
    if not isinstance(job_titles, list):
        raise ValueError(f"Filter config must define a 'job_titles' list: {path}")
    titles = [str(item).strip() for item in job_titles if str(item).strip()]
    if not titles:
        raise ValueError(f"Filter config 'job_titles' list must not be empty: {path}")
    return titles


def load_priority_companies(path: Path) -> List[str]:
    payload = load_filter_payload(path)
    priority_companies = payload.get("priority_companies", [])
    if not isinstance(priority_companies, list):
        raise ValueError(f"Filter config 'priority_companies' must be a list when present: {path}")
    return [str(item).strip() for item in priority_companies if str(item).strip()]


def load_notification_times(path: Path) -> List[time]:
    payload = load_filter_payload(path)
    notification_times = payload.get("notification_times")
    if not isinstance(notification_times, list):
        raise ValueError(f"Filter config must define a 'notification_times' list: {path}")
    parsed: List[time] = []
    for value in notification_times:
        text = str(value).strip()
        if not text:
            continue
        try:
            hour_text, minute_text = text.split(":", 1)
            parsed.append(time(hour=int(hour_text), minute=int(minute_text)))
        except (ValueError, TypeError) as exc:
            raise ValueError(f"Invalid notification time '{value}' in {path}; expected HH:MM") from exc
    if not parsed:
        raise ValueError(f"Filter config 'notification_times' list must not be empty: {path}")
    return sorted(parsed)


def load_filter_payload(path: Path) -> Dict[str, Any]:
    if not path.exists():
        raise ValueError(f"Missing filter config file: {path}")
    with path.open("rb") as handle:
        payload = tomllib.load(handle)
    if not isinstance(payload, dict):
        raise ValueError(f"Filter config must be a TOML table: {path}")
    return payload


def load_settings(env_path: str = ".env", filters_path: Optional[str] = None) -> Settings:
    env_file = Path(env_path)
    load_dotenv(env_file)
    resolved_filters_path = Path(filters_path) if filters_path else env_file.parent / "jobfinder_filters.toml"

    telegram_chat_ids = _get_list("TELEGRAM_CHAT_IDS")
    if not telegram_chat_ids:
        legacy_chat_id = _get_optional("TELEGRAM_CHAT_ID")
        if legacy_chat_id:
            telegram_chat_ids = [legacy_chat_id]

    missing = [
        name
        for name in ("JOBDATAFEEDS_API_TOKEN", "TELEGRAM_BOT_TOKEN")
        if not _get_optional(name)
    ]
    if not telegram_chat_ids:
        missing.append("TELEGRAM_CHAT_ID or TELEGRAM_CHAT_IDS")
    if missing:
        raise ValueError(f"Missing required environment variables: {', '.join(missing)}")

    db_path = Path(os.getenv("DB_PATH", "runtime/jobfinder.sqlite3"))
    cv_path = _get_optional("CV_PATH")
    cover_letter_path = _get_optional("COVER_LETTER_PATH")
    log_path = Path(os.getenv("LOG_PATH", str(db_path.parent / "jobfinder.log")))
    filtered_out_jobs_log_path = Path(
        os.getenv("FILTERED_OUT_JOBS_LOG_PATH", str(db_path.parent / "filtered_out_jobs.jsonl"))
    )

    return Settings(
        jobdatafeeds_api_key=os.environ["JOBDATAFEEDS_API_TOKEN"],
        jobdatafeeds_api_host=os.getenv(
            "JOBDATAFEEDS_API_HOST", "daily-international-job-postings.p.rapidapi.com"
        ),
        jsearch_enabled=_get_bool("ENABLE_JSEARCH", False),
        jsearch_api_key=_get_optional("JSEARCH_API_KEY"),
        jsearch_api_host=os.getenv("JSEARCH_API_HOST", "jsearch.p.rapidapi.com"),
        telegram_bot_token=os.environ["TELEGRAM_BOT_TOKEN"],
        telegram_chat_ids=telegram_chat_ids,
        db_path=db_path,
        timezone=os.getenv("TIMEZONE", "Europe/Berlin"),
        notification_times=load_notification_times(resolved_filters_path),
        jobdatafeeds_max_api_requests_per_run=_get_int("JOBDATAFEEDS_MAX_API_REQUESTS_PER_RUN", 2),
        jsearch_max_api_requests_per_run=_get_int("JSEARCH_MAX_API_REQUESTS_PER_RUN", 2),
        search_titles=load_filter_titles(resolved_filters_path),
        priority_companies=load_priority_companies(resolved_filters_path),
        search_country_code=os.getenv("SEARCH_COUNTRY_CODE", "de"),
        allow_all_sources=_get_bool("ALLOW_ALL_SOURCES", True),
        cv_path=Path(cv_path) if cv_path else None,
        cover_letter_path=Path(cover_letter_path) if cover_letter_path else None,
        log_path=log_path,
        filtered_out_jobs_log_path=filtered_out_jobs_log_path,
        env_path=env_file,
        filters_path=resolved_filters_path,
    )
