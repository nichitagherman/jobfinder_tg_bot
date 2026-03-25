# Jobfinder Telegram Bot

Local-first Python bot that fetches new jobs from the RapidAPI-hosted JobDataFeeds endpoint, stores them in SQLite, deduplicates overlapping listings, and sends Telegram digests multiple times per day.

## What It Does

- Queries Berlin jobs by default
- Uses a Berlin-centered geo filter for local jobs instead of an exact `city=Berlin` match
- Can optionally include a second preset for fully remote jobs that appear compatible with working from Berlin
- Uses configured notification times to define the intended run schedule and the initial fetch window
- Stores raw and normalized job data in SQLite
- Deduplicates across sources and prefers LinkedIn when duplicates overlap
- Sends a Telegram digest with application links
- Tracks the last successful fetch window to avoid resending old jobs
- Writes detailed logs to console and to `runtime/jobfinder.log` by default
- Enforces strict API safety caps with these defaults:
  - `MAX_API_REQUESTS_PER_RUN=8`

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

3. Edit [`jobfinder_filters.toml`](/Users/nikitagerman/Desktop/jobfinder_tg_bot/jobfinder_filters.toml) to control:
   - which job title variants are matched
   - which notification times are expected, currently `11:00`, `14:00`, and `18:00`
   - logging stays enabled by default; override `LOG_PATH` in `.env` only if you want the log file elsewhere

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

## Scheduling

The bot now assumes three local notification times from `jobfinder_filters.toml`:

```toml
notification_times = ["11:00", "14:00", "18:00"]
```

The runner still uses the last successful checkpoint when one exists. If no checkpoint exists yet, it derives the lower bound from the previous configured notification slot, so:

- a run at `14:00` starts from `11:00`
- a run at `18:00` starts from `14:00`
- a run at `11:00` starts from the previous day's `18:00`

## Notes

- Title variants live in `jobfinder_filters.toml`; for local searches the bot now queries them fairly, one title page at a time before deeper pagination.
- Notification times also live in `jobfinder_filters.toml`; they control the expected run cadence and the fallback initial fetch window.
- `MAX_API_REQUESTS_PER_RUN` is the only collection limiter.
- Detailed logs are enabled by default and go to `runtime/jobfinder.log` plus stdout/stderr.
- The implementation assumes the JobDataFeeds API can be queried with page-based pagination and JSON output.
- Date filtering is still sent with `dateCreatedMin` / `dateCreatedMax`; for multiple same-day runs this may hit the same calendar day upstream, but the exact SQLite checkpoint and local timestamp filtering still constrain results to the relevant time window.
- Local Berlin targeting now uses `geoPointLat`, `geoPointLng`, and `geoDistance` rather than `city=Berlin`.
- If a run stops before all likely pages are fetched, the DB stores the incomplete titles for that run and the Telegram digest warns about them.
- If you ever need to recreate the environment instead of cloning it, use `environment.yml`.
