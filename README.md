# Jobfinder Telegram Bot

Local-first Python bot that fetches new jobs from the RapidAPI-hosted JobDataFeeds endpoint, stores them in SQLite, deduplicates overlapping listings, and sends a once-daily Telegram digest.

## What It Does

- Queries Berlin/Brandenburg jobs by default
- Can optionally include a second preset for fully remote jobs that appear compatible with working from Berlin
- Stores raw and normalized job data in SQLite
- Deduplicates across sources and prefers LinkedIn when duplicates overlap
- Sends a Telegram digest with application links
- Tracks the last successful fetch window to avoid resending old jobs
- Enforces strict API safety caps with these defaults:
  - `MAX_API_REQUESTS_PER_RUN=2`
  - `MAX_JOBS_PER_RUN=2`

## Local Setup

1. Use the Conda environment for this project:

```bash
conda activate jobfinder-tg-bot
python --version
```

The project now targets Python 3.13 explicitly.

2. Copy `.env.example` to `.env` and fill in the values:

```bash
cp .env.example .env
```

3. Edit [`jobfinder_filters.toml`](/Users/nikitagerman/Desktop/jobfinder_tg_bot/jobfinder_filters.toml) to control which job title variants are matched.

4. Run a dry run first:

```bash
python -m jobfinder.runner --dry-run
```

This default run queries local jobs only. To include remote jobs too:

```bash
python -m jobfinder.runner --dry-run --include-remote
```

5. Run the real send:

```bash
python -m jobfinder.runner
```

6. Run the tests:

```bash
python -m unittest discover -s tests -v
```

## Notes

- Title variants live in `jobfinder_filters.toml`; changing that file affects both the API title query and local post-filtering.
- The implementation assumes the JobDataFeeds API can be queried with page-based pagination and JSON output.
- Date filtering is sent with `dateCreatedMin` / `dateCreatedMax`; if the provider applies day-level precision, the SQLite checkpoint and dedupe logic still prevent repeat Telegram sends.
- If you ever need to recreate the environment instead of cloning it, use `environment.yml`.
