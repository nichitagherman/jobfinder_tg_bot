import tempfile
import unittest
import json
from datetime import datetime, time, timezone
from pathlib import Path
from urllib.error import HTTPError
from unittest.mock import patch
from zoneinfo import ZoneInfo

from jobfinder.config import load_settings
from jobfinder.dedupe import choose_canonical, mark_canonical_jobs
from jobfinder.jobdatafeeds_client import (
    JobDataFeedsClient,
    build_query_params,
    excluded_by_seniority_title,
    normalize_job,
    remote_berlin_compatible,
    title_matches,
)
from jobfinder.runner import previous_scheduled_runtime
from jobfinder.storage import Storage
from jobfinder.telegram_client import build_digest_messages


SAMPLE_JOB = {
    "portal": "linkedin",
    "source": "monster_de",
    "dateCreated": "2025-01-21T12:00:00.000Z",
    "dateExpired": "2025-03-22T12:00:00Z",
    "dateActive": "2025-03-22T12:00:00Z",
    "isDirect": True,
    "isRecruiter": True,
    "title": "Project Management Lead",
    "countryCode": "de",
    "state": "Berlin",
    "city": "Berlin",
    "language": "en",
    "locale": "en_DE",
    "timezone": "CET",
    "timezoneOffset": 1,
    "company": "Microsoft",
    "industry": "Technology",
    "occupation": "Manager",
    "workPlace": ["remote"],
    "workType": ["fulltime"],
    "contractType": [],
    "careerLevel": [],
    "jsonLD": {
        "identifier": "abc123",
        "validThrough": "2025-04-22T00:04:27Z",
        "description": "Project management for international programs in Berlin and remote.",
        "industry": "Technology",
        "title": "Project Management Lead",
        "url": "https://www.linkedin.com/jobs/view/abc123",
        "relevantOccupation": "Manager",
        "applicantLocationRequirements": "CET Timezone",
        "hiringOrganization": {"name": "Microsoft"},
        "jobLocation": {
            "name": "Berlin, Germany",
            "address": {
                "addressLocality": "Berlin",
                "addressCountry": "Germany",
                "addressRegion": "Berlin",
            },
        },
        "datePosted": "2025-01-22",
    },
}


DEFAULT_FILTERS = """notification_times = [
  "11:00",
  "14:00",
  "18:00",
]

job_titles = [
  "project manager",
  "project management",
  "business analyst",
  "business analytics",
  "strategy",
]
"""


def write_config_files(root: Path) -> tuple[Path, Path]:
    env_path = root / ".env"
    env_path.write_text(
        "\n".join(
            [
                "JOBDATAFEEDS_API_TOKEN=test-token",
                "TELEGRAM_BOT_TOKEN=test-bot",
                "TELEGRAM_CHAT_ID=12345",
            ]
        ),
        encoding="utf-8",
    )
    filters_path = root / "jobfinder_filters.toml"
    filters_path.write_text(DEFAULT_FILTERS, encoding="utf-8")
    return env_path, filters_path


class ConfigTests(unittest.TestCase):
    def test_load_settings_defaults(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            env_path, _ = write_config_files(Path(tmpdir))
            settings = load_settings(str(env_path))
            self.assertEqual(settings.max_api_requests_per_run, 2)
            self.assertEqual(len(settings.build_presets()), 2)
            self.assertEqual(
                settings.search_titles,
                [
                    "project manager",
                    "project management",
                    "business analyst",
                    "business analytics",
                    "strategy",
                ],
            )
            self.assertEqual(
                settings.notification_times,
                [time(11, 0), time(14, 0), time(18, 0)],
            )

    def test_build_presets_can_exclude_remote(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            env_path, _ = write_config_files(Path(tmpdir))
            settings = load_settings(str(env_path))
            presets = settings.build_presets(include_remote=False)
            self.assertEqual(len(presets), 1)
            self.assertEqual(presets[0].name, "berlin_all_workplaces")

    def test_load_settings_can_use_custom_filters_path(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            env_path, _ = write_config_files(root)
            custom_filters = root / "custom_filters.toml"
            custom_filters.write_text(
                'notification_times = ["09:00", "17:00"]\njob_titles = ["strategy"]\n',
                encoding="utf-8",
            )
            settings = load_settings(str(env_path), filters_path=str(custom_filters))
            self.assertEqual(settings.search_titles, ["strategy"])
            self.assertEqual(settings.notification_times, [time(9, 0), time(17, 0)])
            self.assertEqual(settings.filters_path, custom_filters)


class ScheduleTests(unittest.TestCase):
    def test_previous_scheduled_runtime_uses_prior_same_day_slot(self):
        now_local = datetime(2026, 3, 24, 14, 30, tzinfo=ZoneInfo("Europe/Berlin"))
        previous = previous_scheduled_runtime(now_local, [time(11, 0), time(14, 0), time(18, 0)])
        self.assertEqual(previous, datetime(2026, 3, 24, 14, 0, tzinfo=ZoneInfo("Europe/Berlin")))

    def test_previous_scheduled_runtime_wraps_to_previous_day(self):
        now_local = datetime(2026, 3, 24, 11, 0, tzinfo=ZoneInfo("Europe/Berlin"))
        previous = previous_scheduled_runtime(now_local, [time(11, 0), time(14, 0), time(18, 0)])
        self.assertEqual(previous, datetime(2026, 3, 23, 18, 0, tzinfo=ZoneInfo("Europe/Berlin")))


class QueryTests(unittest.TestCase):
    def test_build_query_params_only_non_empty(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            env_path, _ = write_config_files(Path(tmpdir))
            settings = load_settings(str(env_path))
            preset = settings.build_presets()[0]
            params = build_query_params(
                preset,
                page=1,
                lower_bound=datetime(2025, 1, 1, tzinfo=timezone.utc),
                upper_bound=datetime(2025, 1, 2, tzinfo=timezone.utc),
            )
            self.assertEqual(params["page"], "1")
            self.assertEqual(params["format"], "json")
            self.assertEqual(params["geoPointLat"], "52.5200")
            self.assertEqual(params["geoPointLng"], "13.4050")
            self.assertEqual(params["geoDistance"], "15mi")
            self.assertEqual(params["dateCreatedMin"], "2025-01-01")
            self.assertEqual(params["dateCreatedMax"], "2025-01-02")
            self.assertEqual(
                params["title"],
                "+project,+manager OR +project,+management OR +business,+analyst OR +business,+analytics OR +strategy",
            )
            self.assertNotIn("isActive", params)
            self.assertNotIn("", params.keys())

    def test_client_applies_cooldown_between_requests(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            env_path, _ = write_config_files(Path(tmpdir))
            settings = load_settings(str(env_path))
            client = CooldownClient(settings)
            client._last_request_monotonic = 100.0
            with patch("jobfinder.jobdatafeeds_client.time.monotonic", side_effect=[100.4]):
                client._apply_request_cooldown()
            self.assertEqual(len(client.sleep_calls), 1)
            self.assertGreater(client.sleep_calls[0], 0.69)

    def test_client_retries_once_after_429(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            env_path, _ = write_config_files(Path(tmpdir))
            settings = load_settings(str(env_path))
            client = Retry429Client(settings)

            class FakeResponse:
                def __enter__(self):
                    return self

                def __exit__(self, exc_type, exc, tb):
                    return False

                def read(self):
                    return b'{"result": [], "pageSize": 10, "totalCount": 0}'

            responses = [
                HTTPError(
                    url=settings.rapidapi_base_url,
                    code=429,
                    msg="Too Many Requests",
                    hdrs=None,
                    fp=None,
                ),
                FakeResponse(),
            ]

            def fake_urlopen(request, timeout=30):
                response = responses.pop(0)
                if isinstance(response, Exception):
                    raise response
                return response

            with patch("jobfinder.jobdatafeeds_client.urlopen", side_effect=fake_urlopen):
                payload = client._perform_request({"page": "1", "title": "+project,+manager"})

            self.assertEqual(payload["totalCount"], 0)
            self.assertEqual(client.sleep_calls, [5.0])


class FakeJobDataFeedsClient(JobDataFeedsClient):
    def __init__(self, settings, payloads):
        super().__init__(settings)
        self.payloads = payloads
        self.requests = []

    def _perform_request(self, params):
        self.requests.append(dict(params))
        key = (params.get("title"), int(params["page"]))
        return self.payloads.get(key, {"result": [], "pageSize": 10, "totalCount": 0})


class CooldownClient(JobDataFeedsClient):
    def __init__(self, settings):
        super().__init__(settings)
        self.sleep_calls = []
        self.monotonic_values = iter([100.0, 100.4])

    def _sleep(self, seconds):
        self.sleep_calls.append(seconds)


class Retry429Client(JobDataFeedsClient):
    def __init__(self, settings):
        super().__init__(settings)
        self.sleep_calls = []
        self.attempts = 0

    def _sleep(self, seconds):
        self.sleep_calls.append(seconds)

    def _apply_request_cooldown(self):
        return

    def _mark_request_attempt(self):
        return


def make_raw_job(title: str, identifier: str) -> dict:
    raw = dict(SAMPLE_JOB)
    raw["dateCreated"] = "2026-03-24T12:00:00.000Z"
    raw["dateActive"] = "2026-03-24T12:00:00Z"
    raw["dateExpired"] = "2026-04-24T12:00:00Z"
    raw["title"] = title
    raw["jsonLD"] = dict(SAMPLE_JOB["jsonLD"])
    raw["jsonLD"]["title"] = title
    raw["jsonLD"]["identifier"] = identifier
    raw["jsonLD"]["url"] = f"https://example.com/{identifier}"
    raw["jsonLD"]["datePosted"] = "2026-03-24"
    raw["jsonLD"]["validThrough"] = "2026-04-24T12:00:00Z"
    return raw


class NormalizationTests(unittest.TestCase):
    def test_normalize_job_maps_payload(self):
        job = normalize_job(SAMPLE_JOB, datetime(2025, 1, 23, tzinfo=timezone.utc))
        self.assertEqual(job.external_id, "abc123")
        self.assertEqual(job.canonical_url, "https://www.linkedin.com/jobs/view/abc123")
        self.assertEqual(job.company, "Microsoft")
        self.assertEqual(job.city, "Berlin")
        self.assertEqual(job.work_place, ["remote"])

    def test_normalize_job_rewrites_de_linkedin_host(self):
        raw = dict(SAMPLE_JOB)
        raw["jsonLD"] = dict(SAMPLE_JOB["jsonLD"])
        raw["jsonLD"]["url"] = "https://de.linkedin.com/jobs/view/abc123?tracking=1"
        job = normalize_job(raw, datetime(2025, 1, 23, tzinfo=timezone.utc))
        self.assertEqual(job.canonical_url, "https://linkedin.com/jobs/view/abc123?tracking=1")

    def test_filters_accept_expected_jobs(self):
        job = normalize_job(SAMPLE_JOB, datetime(2025, 1, 23, tzinfo=timezone.utc))
        self.assertTrue(remote_berlin_compatible(job))

    def test_remote_filter_rejects_non_compatible_jobs(self):
        raw = dict(SAMPLE_JOB)
        raw["jsonLD"] = dict(SAMPLE_JOB["jsonLD"])
        raw["jsonLD"]["applicantLocationRequirements"] = "United States only"
        job = normalize_job(raw, datetime(2025, 1, 23, tzinfo=timezone.utc))
        self.assertFalse(remote_berlin_compatible(job))

    def test_title_matches_business_analytics_variant(self):
        raw = dict(SAMPLE_JOB)
        raw["title"] = "Business Analytics Specialist"
        raw["jsonLD"] = dict(SAMPLE_JOB["jsonLD"])
        raw["jsonLD"]["title"] = "Business Analytics Specialist"
        job = normalize_job(raw, datetime(2025, 1, 23, tzinfo=timezone.utc))
        self.assertTrue(
            title_matches(
                job,
                ["project manager", "project management", "business analyst", "business analytics", "strategy"],
            )
        )

    def test_title_matches_strategy_variant_without_matching_strategist(self):
        strategy_job_raw = dict(SAMPLE_JOB)
        strategy_job_raw["title"] = "Head of Strategy"
        strategy_job_raw["jsonLD"] = dict(SAMPLE_JOB["jsonLD"])
        strategy_job_raw["jsonLD"]["title"] = "Head of Strategy"
        strategy_job = normalize_job(strategy_job_raw, datetime(2025, 1, 23, tzinfo=timezone.utc))
        self.assertTrue(title_matches(strategy_job, ["strategy"]))

        strategist_job_raw = dict(SAMPLE_JOB)
        strategist_job_raw["title"] = "Strategist"
        strategist_job_raw["jsonLD"] = dict(SAMPLE_JOB["jsonLD"])
        strategist_job_raw["jsonLD"]["title"] = "Strategist"
        strategist_job = normalize_job(strategist_job_raw, datetime(2025, 1, 23, tzinfo=timezone.utc))
        self.assertFalse(title_matches(strategist_job, ["strategy"]))

    def test_excluded_by_seniority_title_matches_conservative_markers(self):
        senior_job_raw = dict(SAMPLE_JOB)
        senior_job_raw["title"] = "Head of Strategy"
        senior_job_raw["jsonLD"] = dict(SAMPLE_JOB["jsonLD"])
        senior_job_raw["jsonLD"]["title"] = "Head of Strategy"
        senior_job = normalize_job(senior_job_raw, datetime(2025, 1, 23, tzinfo=timezone.utc))
        self.assertIn("head", excluded_by_seniority_title(senior_job))

        team_lead_raw = dict(SAMPLE_JOB)
        team_lead_raw["title"] = "Team Lead - Client Operations Specialist"
        team_lead_raw["jsonLD"] = dict(SAMPLE_JOB["jsonLD"])
        team_lead_raw["jsonLD"]["title"] = "Team Lead - Client Operations Specialist"
        team_lead_job = normalize_job(team_lead_raw, datetime(2025, 1, 23, tzinfo=timezone.utc))
        markers = excluded_by_seniority_title(team_lead_job)
        self.assertIn("team lead", markers)
        self.assertIn("lead", markers)

    def test_excluded_by_seniority_title_allows_mid_titles(self):
        raw = dict(SAMPLE_JOB)
        raw["title"] = "Business Analyst Web and Mobile Banking"
        raw["jsonLD"] = dict(SAMPLE_JOB["jsonLD"])
        raw["jsonLD"]["title"] = "Business Analyst Web and Mobile Banking"
        job = normalize_job(raw, datetime(2025, 1, 23, tzinfo=timezone.utc))
        self.assertEqual(excluded_by_seniority_title(job), [])


class DedupeTests(unittest.TestCase):
    def _job(self, portal, source, url, title="Project Management Lead", company="Microsoft"):
        base = normalize_job(SAMPLE_JOB, datetime(2025, 1, 23, tzinfo=timezone.utc))
        base.portal = portal
        base.source = source
        base.canonical_url = url
        base.external_id = f"{portal}-{source}"
        return base

    def test_linkedin_wins_duplicate_group(self):
        linkedin = self._job("linkedin", "monster_de", "https://linkedin.example")
        stepstone = self._job("stepstone", "stepstone", "https://stepstone.example")
        winner = choose_canonical([stepstone, linkedin])
        self.assertEqual(winner.portal, "linkedin")

    def test_mark_canonical_jobs_marks_one(self):
        linkedin = self._job("linkedin", "monster_de", "https://linkedin.example")
        stepstone = self._job("stepstone", "stepstone", "https://stepstone.example")
        jobs = mark_canonical_jobs([linkedin, stepstone])
        self.assertEqual(sum(1 for job in jobs if job.is_canonical), 1)


class StorageTests(unittest.TestCase):
    def test_checkpoint_and_unsent_behavior(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            storage = Storage(Path(tmpdir) / "jobs.sqlite3")
            job = normalize_job(SAMPLE_JOB, datetime(2025, 1, 23, tzinfo=timezone.utc))
            job.is_canonical = True
            storage.upsert_jobs([job])
            storage.update_canonical_flags([job.canonical_url])
            unsent = storage.get_unsent_canonical_jobs()
            self.assertEqual(len(unsent), 1)
            storage.mark_jobs_sent([job.canonical_url], datetime(2025, 1, 23, tzinfo=timezone.utc))
            unsent_after = storage.get_unsent_canonical_jobs()
            self.assertEqual(len(unsent_after), 0)

    def test_get_all_jobs_round_trip(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            storage = Storage(Path(tmpdir) / "jobs.sqlite3")
            job = normalize_job(SAMPLE_JOB, datetime(2025, 1, 23, tzinfo=timezone.utc))
            storage.upsert_jobs([job])
            jobs = storage.get_all_jobs()
            self.assertEqual(len(jobs), 1)
            self.assertEqual(jobs[0].title, "Project Management Lead")

    def test_finalize_run_persists_incomplete_titles_and_query_count(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            storage = Storage(Path(tmpdir) / "jobs.sqlite3")
            run_id = storage.create_run(datetime(2025, 1, 23, tzinfo=timezone.utc))
            storage.finalize_run(
                run_id,
                ended_at=datetime(2025, 1, 23, 1, tzinfo=timezone.utc),
                status="success",
                api_requests_made=4,
                jobs_fetched=12,
                jobs_inserted=12,
                jobs_canonical=10,
                was_truncated_by_request_cap=True,
                incomplete_titles=["strategy", "business analyst"],
            )
            run = storage.get_run(run_id)
            self.assertEqual(run["api_requests_made"], 4)
            self.assertEqual(
                json.loads(run["incomplete_titles_json"]),
                ["business analyst", "strategy"],
            )


class FetchSchedulingTests(unittest.TestCase):
    def _settings(self, root: Path, *, max_requests: int = 5):
        env_path = root / ".env"
        env_path.write_text(
            "\n".join(
                [
                    "JOBDATAFEEDS_API_TOKEN=test-token",
                    "TELEGRAM_BOT_TOKEN=test-bot",
                    "TELEGRAM_CHAT_ID=12345",
                    f"MAX_API_REQUESTS_PER_RUN={max_requests}",
                ]
            ),
            encoding="utf-8",
        )
        filters = root / "jobfinder_filters.toml"
        filters.write_text(
            "\n".join(
                [
                    'notification_times = ["11:00", "14:00", "18:00"]',
                    'job_titles = ["alpha", "beta", "gamma"]',
                    "",
                ]
            ),
            encoding="utf-8",
        )
        return load_settings(str(env_path), filters_path=str(filters))

    def test_fetch_jobs_gives_every_title_page_one_before_page_two(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            settings = self._settings(Path(tmpdir), max_requests=5)
            payloads = {
                ("+alpha", 1): {"result": [make_raw_job("Alpha role", f"a1-{i}") for i in range(10)], "pageSize": 10, "totalCount": 20},
                ("+beta", 1): {"result": [make_raw_job("Beta role", f"b1-{i}") for i in range(3)], "pageSize": 10, "totalCount": 3},
                ("+gamma", 1): {"result": [make_raw_job("Gamma role", f"g1-{i}") for i in range(10)], "pageSize": 10, "totalCount": 20},
                ("+alpha", 2): {"result": [make_raw_job("Alpha role", f"a2-{i}") for i in range(2)], "pageSize": 10, "totalCount": 20},
                ("+gamma", 2): {"result": [make_raw_job("Gamma role", f"g2-{i}") for i in range(2)], "pageSize": 10, "totalCount": 20},
            }
            client = FakeJobDataFeedsClient(settings, payloads)
            context = previous_scheduled_runtime(
                datetime(2026, 3, 24, 14, 30, tzinfo=ZoneInfo("Europe/Berlin")),
                settings.notification_times,
            )
            summary = client.fetch_jobs(
                type("Ctx", (), {
                    "started_at": datetime(2026, 3, 24, 14, 30, tzinfo=timezone.utc),
                    "upper_bound": datetime(2026, 3, 24, 14, 30, tzinfo=timezone.utc),
                    "lower_bound": context.astimezone(timezone.utc),
                })(),
                include_remote=False,
            )
            seen = [(req["title"], req["page"]) for req in client.requests]
            self.assertEqual(
                seen,
                [("+alpha", "1"), ("+beta", "1"), ("+gamma", "1"), ("+alpha", "2"), ("+gamma", "2")],
            )
            self.assertEqual(summary.api_requests_made, 5)

    def test_fetch_jobs_marks_incomplete_titles_when_request_cap_hits(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            settings = self._settings(Path(tmpdir), max_requests=4)
            payloads = {
                ("+alpha", 1): {"result": [make_raw_job("Alpha role", f"a1-{i}") for i in range(10)], "pageSize": 10, "totalCount": 20},
                ("+beta", 1): {"result": [make_raw_job("Beta role", f"b1-{i}") for i in range(10)], "pageSize": 10, "totalCount": 20},
                ("+gamma", 1): {"result": [make_raw_job("Gamma role", f"g1-{i}") for i in range(10)], "pageSize": 10, "totalCount": 20},
                ("+alpha", 2): {"result": [make_raw_job("Alpha role", f"a2-{i}") for i in range(2)], "pageSize": 10, "totalCount": 20},
            }
            client = FakeJobDataFeedsClient(settings, payloads)
            context = type("Ctx", (), {
                "started_at": datetime(2026, 3, 24, 14, 30, tzinfo=timezone.utc),
                "upper_bound": datetime(2026, 3, 24, 14, 30, tzinfo=timezone.utc),
                "lower_bound": datetime(2026, 3, 24, 11, 0, tzinfo=timezone.utc),
            })()
            summary = client.fetch_jobs(context, include_remote=False)
            self.assertTrue(summary.was_truncated_by_request_cap)
            self.assertEqual(summary.incomplete_titles, ["beta", "gamma"])


class TelegramTests(unittest.TestCase):
    def test_empty_digest_message(self):
        messages = build_digest_messages([], truncated=False, empty_notice=True)
        self.assertEqual(messages, ["No new matching jobs were found in the last run."])

    def test_truncated_digest_mentions_incomplete_titles(self):
        messages = build_digest_messages(
            [{"work_place_json": "[]", "city": "Berlin", "state": "Berlin", "country_code": "de", "date_created": "2025-01-01T18:00:00+00:00", "fetched_at": "2025-01-01T18:00:00+00:00", "title": "Role", "company": "Comp", "portal": "linkedin", "source": "x", "canonical_url": "https://example.com"}],
            truncated=True,
            empty_notice=True,
            lower_bound=datetime(2025, 1, 1, 17, 0, tzinfo=timezone.utc),
            upper_bound=datetime(2025, 1, 1, 18, 0, tzinfo=timezone.utc),
            incomplete_titles=["strategy", "business analyst"],
        )
        self.assertIn("Jobs posted from 01.01.2025 18:00-01.01.2025 19:00", messages[0])
        self.assertIn("Incomplete titles: strategy, business analyst", messages[0])
        self.assertIn("<b>Role</b>", messages[0])
        self.assertIn("<i>Comp</i>", messages[0])
        self.assertIn("Posted: 01.01.2025 19:00", messages[0])
        self.assertIn("Jobs posted from 01.01.2025 18:00-01.01.2025 19:00\n\n<b>Role</b>", messages[0])
        self.assertTrue(messages[0].endswith("Incomplete titles: strategy, business analyst"))

    def test_multiple_jobs_are_separated_by_blank_lines(self):
        rows = [
            {
                "work_place_json": "[]",
                "city": "Berlin",
                "state": "Berlin",
                "country_code": "de",
                "date_created": "2025-01-01T18:00:00+00:00",
                "fetched_at": "2025-01-01T18:00:00+00:00",
                "title": "Role One",
                "company": "Comp One",
                "portal": "linkedin",
                "source": "x",
                "canonical_url": "https://example.com/1",
            },
            {
                "work_place_json": "[]",
                "city": "Berlin",
                "state": "Berlin",
                "country_code": "de",
                "date_created": "2025-01-01T19:00:00+00:00",
                "fetched_at": "2025-01-01T19:00:00+00:00",
                "title": "Role Two",
                "company": "Comp Two",
                "portal": "linkedin",
                "source": "x",
                "canonical_url": "https://example.com/2",
            },
        ]
        messages = build_digest_messages(
            rows,
            truncated=False,
            empty_notice=True,
            lower_bound=datetime(2025, 1, 1, 17, 0, tzinfo=timezone.utc),
            upper_bound=datetime(2025, 1, 1, 19, 0, tzinfo=timezone.utc),
        )
        self.assertIn("<b>Role One</b>\n<i>Comp One</i>", messages[0])
        self.assertIn("<b>Role Two</b>\n<i>Comp Two</i>", messages[0])
        self.assertIn("Apply: https://example.com/1\n\n<b>Role Two</b>", messages[0])


if __name__ == "__main__":
    unittest.main()
