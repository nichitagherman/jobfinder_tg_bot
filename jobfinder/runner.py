from __future__ import annotations

import argparse
import sys
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

from .config import load_settings
from .dedupe import mark_canonical_jobs
from .jobdatafeeds_client import JobDataFeedsClient
from .models import RunContext
from .storage import Storage
from .telegram_client import TelegramClient, build_digest_messages


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


def run_daily(
    env_path: str = ".env",
    *,
    dry_run: bool = False,
    include_remote: bool = False,
    filters_path: str | None = None,
) -> int:
    settings = load_settings(env_path, filters_path=filters_path)
    now_local = datetime.now(ZoneInfo(settings.timezone))
    upper_bound = now_local.astimezone(timezone.utc)

    storage = Storage(settings.db_path)
    client = JobDataFeedsClient(settings)
    telegram = TelegramClient(settings.telegram_bot_token, settings.telegram_chat_id)

    lower_bound = storage.get_last_checkpoint()
    if lower_bound is None:
        lower_bound = previous_scheduled_runtime(now_local, settings.notification_times).astimezone(
            timezone.utc
        )
    context = RunContext(started_at=upper_bound, upper_bound=upper_bound, lower_bound=lower_bound)
    run_id = storage.create_run(upper_bound)

    try:
        fetch_summary = client.fetch_jobs(context, include_remote=include_remote)
        jobs = mark_canonical_jobs(fetch_summary.jobs)
        inserted = storage.upsert_jobs(jobs)
        all_jobs = mark_canonical_jobs(storage.get_all_jobs())
        canonical_urls = [job.canonical_url for job in all_jobs if job.is_canonical]
        storage.update_canonical_flags(canonical_urls)
        unsent_rows = storage.get_unsent_canonical_jobs()
        messages = build_digest_messages(
            unsent_rows,
            truncated=(
                fetch_summary.was_truncated_by_job_cap
                or fetch_summary.was_truncated_by_request_cap
            ),
            empty_notice=True,
        )
        if not dry_run:
            sent_at = telegram.send_messages(messages)
            storage.mark_jobs_sent([row["canonical_url"] for row in unsent_rows], sent_at)
            storage.update_checkpoint(upper_bound)

        storage.finalize_run(
            run_id,
            ended_at=datetime.now(timezone.utc),
            status="success" if not dry_run else "dry_run",
            api_requests_made=fetch_summary.api_requests_made,
            jobs_fetched=fetch_summary.jobs_fetched,
            jobs_inserted=inserted,
            jobs_canonical=len(canonical_urls),
            was_truncated_by_request_cap=fetch_summary.was_truncated_by_request_cap,
            was_truncated_by_job_cap=fetch_summary.was_truncated_by_job_cap,
        )
        return 0
    except Exception as exc:
        storage.finalize_run(
            run_id,
            ended_at=datetime.now(timezone.utc),
            status="failed",
            api_requests_made=0,
            jobs_fetched=0,
            jobs_inserted=0,
            jobs_canonical=0,
            was_truncated_by_request_cap=False,
            was_truncated_by_job_cap=False,
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
        help="Also query the remote jobs preset. By default only local Berlin/Brandenburg jobs are queried.",
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
