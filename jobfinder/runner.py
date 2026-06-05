from __future__ import annotations

import argparse
import json
import logging
import sys
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

from .config import load_settings
from .dedupe import mark_canonical_jobs
from .jobdatafeeds_client import JobDataFeedsClient
from .jsearch_client import JSearchClient
from .logging_utils import FILTERED_OUT_LOGGER_NAME, setup_logging
from .models import FetchSummary, RunContext
from .source_filters import excluded_by_source_domain
from .storage import Storage
from .telegram_client import TelegramClient, build_digest_messages
from .title_filters import excluded_by_german_title, excluded_by_title


LOGGER = logging.getLogger(__name__)
FILTERED_OUT_LOGGER = logging.getLogger(FILTERED_OUT_LOGGER_NAME)


def _prefix_incomplete_titles(provider: str, titles: list[str]) -> list[str]:
    return [f"{provider}: {title}" for title in titles]


def _build_clients(settings):
    clients = [("jobdatafeeds", JobDataFeedsClient(settings))]
    if settings.jsearch_enabled and settings.jsearch_api_key:
        clients.append(("jsearch", JSearchClient(settings)))
    return clients


def previous_scheduled_runtime(now_local: datetime, notification_times) -> datetime:
    today = now_local.date()
    prior_today = [
        datetime.combine(today, scheduled_time, now_local.tzinfo)
        for scheduled_time in notification_times
        if scheduled_time < now_local.timetz().replace(tzinfo=None)
    ]
    if prior_today:
        return prior_today[-1]
    previous_day = today - timedelta(days=1)
    return datetime.combine(previous_day, notification_times[-1], now_local.tzinfo)


def _sort_jobs_for_output(rows, priority_companies):
    priority_companies = {company.strip().lower() for company in priority_companies}
    collector_rank = {"jobdatafeeds": 0, "jsearch": 1}
    rows = sorted(rows, key=lambda row: row["date_created"] or row["fetched_at"] or "", reverse=True)
    rows = sorted(rows, key=lambda row: 0 if (row["company"] or "").strip().lower() in priority_companies else 1)
    return sorted(rows, key=lambda row: collector_rank.get((row["collector"] or "").strip().lower(), 99))


def _resolve_lower_bound(storage: Storage, now_local: datetime, notification_times) -> datetime:
    lower_bound = storage.get_last_checkpoint()
    if lower_bound is not None:
        LOGGER.info("Loaded checkpoint lower bound: %s", lower_bound.isoformat())
        return lower_bound

    lower_bound = previous_scheduled_runtime(now_local, notification_times).astimezone(timezone.utc)
    LOGGER.info("No checkpoint found; using previous scheduled runtime as lower bound: %s", lower_bound.isoformat())
    return lower_bound


def _aggregate_fetch_summaries(clients, context: RunContext, *, include_remote: bool) -> FetchSummary:
    jobs = []
    api_requests_made = 0
    jobs_fetched = 0
    was_truncated = False
    incomplete_titles: list[str] = []

    for provider_name, client in clients:
        provider_summary = client.fetch_jobs(context, include_remote=include_remote)
        LOGGER.info(
            "Provider fetch summary: provider=%s jobs=%s api_requests=%s truncated=%s incomplete_titles=%s",
            provider_name,
            provider_summary.jobs_fetched,
            provider_summary.api_requests_made,
            provider_summary.was_truncated_by_request_cap,
            provider_summary.incomplete_titles,
        )
        jobs.extend(provider_summary.jobs)
        api_requests_made += provider_summary.api_requests_made
        jobs_fetched += provider_summary.jobs_fetched
        was_truncated = was_truncated or provider_summary.was_truncated_by_request_cap
        incomplete_titles.extend(_prefix_incomplete_titles(provider_name, provider_summary.incomplete_titles))

    return FetchSummary(
        jobs=jobs,
        api_requests_made=api_requests_made,
        jobs_fetched=jobs_fetched,
        was_truncated_by_request_cap=was_truncated,
        incomplete_titles=incomplete_titles,
    )


def _log_filtered_out_title(job, context: RunContext, matched_markers) -> None:
    payload = {
        "reason": f"{job.collector}_title_excluded",
        "provider": job.collector,
        "title": job.title,
    }
    FILTERED_OUT_LOGGER.info(json.dumps(payload, ensure_ascii=True))


def _log_filtered_out_title_language(job, context: RunContext, *, detected_language: str, confidence: float, threshold: float) -> None:
    payload = {
        "reason": f"{job.collector}_title_language_excluded",
        "provider": job.collector,
        "title": job.title,
    }
    FILTERED_OUT_LOGGER.info(json.dumps(payload, ensure_ascii=True))


def _log_filtered_out_source_domain(job, context: RunContext, matched_domains) -> None:
    payload = {
        "reason": f"{job.collector}_source_domain_excluded",
        "provider": job.collector,
        "title": job.title,
        "canonical_url": job.canonical_url,
        "matched_domains": list(matched_domains),
    }
    FILTERED_OUT_LOGGER.info(json.dumps(payload, ensure_ascii=True))


def _apply_job_exclusions(jobs, context: RunContext, settings):
    kept_jobs = []
    for job in jobs:
        matched_domains = excluded_by_source_domain(job, settings.excluded_source_domains)
        if matched_domains:
            _log_filtered_out_source_domain(job, context, matched_domains)
            continue

        matched_markers = excluded_by_title(job, settings.excluded_job_title_markers)
        if matched_markers:
            _log_filtered_out_title(job, context, matched_markers)
            continue

        language_detection = excluded_by_german_title(
            job,
            enabled=settings.exclude_german_job_titles,
            threshold=settings.german_job_title_confidence_threshold,
        )
        if language_detection is not None:
            _log_filtered_out_title_language(
                job,
                context,
                detected_language=language_detection.detected_language,
                confidence=language_detection.confidence,
                threshold=settings.german_job_title_confidence_threshold,
            )
            continue
        kept_jobs.append(job)
    return kept_jobs


def run_daily(
    env_path: str = ".env",
    *,
    dry_run: bool = False,
    include_remote: bool = False,
    filters_path: str | None = None,
) -> int:
    settings = load_settings(env_path, filters_path=filters_path)
    setup_logging(settings, dry_run=dry_run)
    now_local = datetime.now(ZoneInfo(settings.timezone))
    upper_bound = now_local.astimezone(timezone.utc)
    LOGGER.info(
        "Starting run: env_path=%s filters_path=%s include_remote=%s dry_run=%s timezone=%s db_path=%s log_path=%s",
        settings.env_path,
        settings.filters_path,
        include_remote,
        dry_run,
        settings.timezone,
        settings.db_path,
        settings.log_path,
    )

    storage = Storage(settings.db_path)
    clients = _build_clients(settings)
    telegram = TelegramClient(settings.telegram_bot_token, settings.telegram_chat_ids)

    lower_bound = _resolve_lower_bound(storage, now_local, settings.notification_times)
    context = RunContext(started_at=upper_bound, upper_bound=upper_bound, lower_bound=lower_bound)
    run_id = storage.create_run(upper_bound)

    try:
        fetch_summary = _aggregate_fetch_summaries(clients, context, include_remote=include_remote)
        LOGGER.info(
            "Fetch summary: jobs=%s api_requests=%s truncated=%s incomplete_titles=%s",
            fetch_summary.jobs_fetched,
            fetch_summary.api_requests_made,
            fetch_summary.was_truncated_by_request_cap,
            fetch_summary.incomplete_titles,
        )
        filtered_jobs = _apply_job_exclusions(fetch_summary.jobs, context, settings)
        LOGGER.info(
            "Job exclusions applied: fetched_jobs=%s kept_jobs=%s excluded_jobs=%s",
            len(fetch_summary.jobs),
            len(filtered_jobs),
            len(fetch_summary.jobs) - len(filtered_jobs),
        )
        jobs = mark_canonical_jobs(filtered_jobs)
        LOGGER.info("Dedupe complete: fetched_jobs=%s canonical_candidates=%s", len(fetch_summary.jobs), sum(1 for job in jobs if job.is_canonical))
        inserted = storage.upsert_jobs(jobs)
        all_jobs = mark_canonical_jobs(storage.get_all_jobs())
        canonical_urls = [job.canonical_url for job in all_jobs if job.is_canonical]
        storage.update_canonical_flags(canonical_urls)
        unsent_rows = _sort_jobs_for_output(
            storage.get_unsent_canonical_jobs(),
            settings.priority_companies,
        )
        messages = build_digest_messages(
            unsent_rows,
            truncated=fetch_summary.was_truncated_by_request_cap,
            empty_notice=True,
            lower_bound=context.lower_bound,
            upper_bound=context.upper_bound,
            incomplete_titles=fetch_summary.incomplete_titles,
        )
        LOGGER.info(
            "Prepared digest: unsent_rows=%s messages=%s dry_run=%s",
            len(unsent_rows),
            len(messages),
            dry_run,
        )
        if not dry_run:
            sent_at = telegram.send_messages(messages)
            storage.mark_jobs_sent([row["canonical_url"] for row in unsent_rows], sent_at)
            storage.update_checkpoint(upper_bound)
            LOGGER.info("Run completed successfully and checkpoint advanced.")
        else:
            LOGGER.info("Dry run complete; Telegram send skipped and checkpoint not advanced.")

        storage.finalize_run(
            run_id,
            ended_at=datetime.now(timezone.utc),
            status="success" if not dry_run else "dry_run",
            api_requests_made=fetch_summary.api_requests_made,
            jobs_fetched=fetch_summary.jobs_fetched,
            jobs_inserted=inserted,
            jobs_canonical=len(canonical_urls),
            was_truncated_by_request_cap=fetch_summary.was_truncated_by_request_cap,
            incomplete_titles=fetch_summary.incomplete_titles,
        )
        return 0
    except Exception as exc:
        LOGGER.exception("Run failed: %s", exc)
        storage.finalize_run(
            run_id,
            ended_at=datetime.now(timezone.utc),
            status="failed",
            api_requests_made=0,
            jobs_fetched=0,
            jobs_inserted=0,
            jobs_canonical=0,
            was_truncated_by_request_cap=False,
            incomplete_titles=[],
            error_message=str(exc),
        )
        raise


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run the local Telegram job digest bot.")
    parser.add_argument("--env-file", default=".env", help="Path to the local environment file.")
    parser.add_argument(
        "--filters-file",
        default=None,
        help="Path to the TOML filter config. Defaults to jobfinder_filters.toml next to the env file.",
    )
    parser.add_argument("--dry-run", action="store_true", help="Fetch and store data without sending Telegram messages.")
    parser.add_argument(
        "--include-remote",
        action="store_true",
        help="Also query the remote jobs preset. By default only local Berlin jobs are queried.",
    )
    args = parser.parse_args(argv)
    return run_daily(
        args.env_file,
        dry_run=args.dry_run,
        include_remote=args.include_remote,
        filters_path=args.filters_file,
    )


if __name__ == "__main__":
    sys.exit(main())
