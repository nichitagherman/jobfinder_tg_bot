from __future__ import annotations

import argparse
import logging
import sys
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

from .config import load_settings
from .dedupe import mark_canonical_jobs
from .jobdatafeeds_client import JobDataFeedsClient
from .logging_utils import setup_logging
from .models import RunContext
from .storage import Storage
from .telegram_client import TelegramClient, build_digest_messages


LOGGER = logging.getLogger(__name__)


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
    rows = sorted(rows, key=lambda row: row["date_created"] or row["fetched_at"] or "", reverse=True)
    return sorted(rows, key=lambda row: 0 if (row["company"] or "").strip().lower() in priority_companies else 1)


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
    client = JobDataFeedsClient(settings)
    telegram = TelegramClient(settings.telegram_bot_token, settings.telegram_chat_ids)

    lower_bound = storage.get_last_checkpoint()
    if lower_bound is None:
        lower_bound = previous_scheduled_runtime(now_local, settings.notification_times).astimezone(
            timezone.utc
        )
        LOGGER.info("No checkpoint found; using previous scheduled runtime as lower bound: %s", lower_bound.isoformat())
    else:
        LOGGER.info("Loaded checkpoint lower bound: %s", lower_bound.isoformat())
    context = RunContext(started_at=upper_bound, upper_bound=upper_bound, lower_bound=lower_bound)
    run_id = storage.create_run(upper_bound)

    try:
        fetch_summary = client.fetch_jobs(context, include_remote=include_remote)
        LOGGER.info(
            "Fetch summary: jobs=%s api_requests=%s truncated=%s incomplete_titles=%s",
            fetch_summary.jobs_fetched,
            fetch_summary.api_requests_made,
            fetch_summary.was_truncated_by_request_cap,
            fetch_summary.incomplete_titles,
        )
        jobs = mark_canonical_jobs(fetch_summary.jobs)
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
