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
from jobfinder.jsearch_client import JSearchClient, normalize_job as normalize_jsearch_job, select_date_posted
import jobfinder.runner as runner_module
from jobfinder.runner import _sort_jobs_for_output, previous_scheduled_runtime
from jobfinder.storage import Storage
from jobfinder.telegram_client import build_digest_messages
from jobfinder.title_filters import detect_title_language, excluded_by_german_title


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

SAMPLE_JSEARCH_JOB = {
    "job_id": "mTqkb_t5iIrrC7xqAAAAAA==",
    "job_title": "PMO-Manager:in - Turnaround Programm – Berlin",
    "employer_name": "Stadler",
    "job_publisher": "LinkedIn",
    "job_employment_type": "Vollzeit",
    "job_employment_types": ["FULLTIME"],
    "job_apply_link": "https://de.linkedin.com/jobs/view/pmo-manager-in-turnaround-programm-%E2%80%93-berlin-at-stadler-4390701473",
    "job_apply_is_direct": False,
    "apply_options": [
        {
            "apply_link": "https://de.linkedin.com/jobs/view/pmo-manager-in-turnaround-programm-%E2%80%93-berlin-at-stadler-4390701473",
            "is_direct": False,
            "publisher": "LinkedIn",
        }
    ],
    "job_description": "Project manager role in Berlin.",
    "job_is_remote": False,
    "job_posted_at": "vor 5 Stunden",
    "job_posted_at_datetime_utc": "2026-03-26T11:00:00.000Z",
    "job_location": "Berlin • über LinkedIn",
    "job_city": None,
    "job_state": None,
    "job_country": None,
    "job_google_link": "https://www.google.com/search?ibp=htl;jobs&q=project+manager+in+Berlin",
}


DEFAULT_FILTERS = """notification_times = [
  "11:00",
  "14:00",
  "18:00",
]

priority_companies = [
  "Zalando",
  "Delivery Hero",
  "HelloFresh",
  "N26",
  "AUTO1 Group",
  "Trade Republic",
  "GetYourGuide",
  "Raisin",
  "Omio",
  "sennder",
  "Contentful",
]

jobdatafeeds_job_titles = [
  "project manager",
  "project management",
  "business analyst",
  "business analytics",
  "strategy",
]

jsearch_job_titles = [
  "project manager",
  "project management",
  "business analyst",
  "business analytics",
  "strategy analyst",
]

excluded_job_title_markers = [
  "HR",
  "Account",
  "Senior",
  "Technical",
  "Representative",
  "People",
  "FinOps",
  "Engineering",
  "Techno",
  "Engineer",
  "Security",
  "Vice President",
  "Head",
  "Lead",
  "Director",
  "VP",
  "Principal",
  "Chief",
]

exclude_german_job_titles = true
german_job_title_confidence_threshold = 0.85
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
            self.assertEqual(settings.jobdatafeeds_max_api_requests_per_run, 2)
            self.assertEqual(len(settings.build_presets()), 2)
            self.assertEqual(settings.telegram_chat_ids, ["12345"])
            self.assertEqual(
                settings.jobdatafeeds_search_titles,
                [
                    "project manager",
                    "project management",
                    "business analyst",
                    "business analytics",
                    "strategy",
                ],
            )
            self.assertEqual(
                settings.jsearch_search_titles,
                [
                    "project manager",
                    "project management",
                    "business analyst",
                    "business analytics",
                    "strategy analyst",
                ],
            )
            self.assertEqual(
                settings.excluded_job_title_markers,
                [
                    "HR",
                    "Account",
                    "Senior",
                    "Technical",
                    "Representative",
                    "People",
                    "FinOps",
                    "Engineering",
                    "Techno",
                    "Engineer",
                    "Security",
                    "Vice President",
                    "Head",
                    "Lead",
                    "Director",
                    "VP",
                    "Principal",
                    "Chief",
                ],
            )
            self.assertEqual(
                settings.notification_times,
                [time(11, 0), time(14, 0), time(18, 0)],
            )
            self.assertTrue(settings.exclude_german_job_titles)
            self.assertEqual(settings.german_job_title_confidence_threshold, 0.85)
            self.assertIn("Delivery Hero", settings.priority_companies)

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
                '\n'.join(
                    [
                        'notification_times = ["09:00", "17:00"]',
                        'jobdatafeeds_job_titles = ["strategy"]',
                        'jsearch_job_titles = ["strategy analyst"]',
                        'excluded_job_title_markers = ["Senior", "Engineer"]',
                        "exclude_german_job_titles = false",
                        "german_job_title_confidence_threshold = 0.9",
                        '',
                    ]
                ),
                encoding="utf-8",
            )
            settings = load_settings(str(env_path), filters_path=str(custom_filters))
            self.assertEqual(settings.jobdatafeeds_search_titles, ["strategy"])
            self.assertEqual(settings.jsearch_search_titles, ["strategy analyst"])
            self.assertEqual(settings.excluded_job_title_markers, ["Senior", "Engineer"])
            self.assertFalse(settings.exclude_german_job_titles)
            self.assertEqual(settings.german_job_title_confidence_threshold, 0.9)
            self.assertEqual(settings.notification_times, [time(9, 0), time(17, 0)])
            self.assertEqual(settings.filters_path, custom_filters)

    def test_load_settings_supports_multiple_chat_ids(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            env_path, _ = write_config_files(root)
            env_path.write_text(
                "\n".join(
                    [
                        "JOBDATAFEEDS_API_TOKEN=test-token",
                        "TELEGRAM_BOT_TOKEN=test-bot",
                        "TELEGRAM_CHAT_IDS=12345,67890",
                    ]
                ),
                encoding="utf-8",
            )
            settings = load_settings(str(env_path))
            self.assertEqual(settings.telegram_chat_ids, ["12345", "67890"])

    def test_load_settings_supports_optional_jsearch(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            env_path, _ = write_config_files(root)
            env_path.write_text(
                "\n".join(
                    [
                        "JOBDATAFEEDS_API_TOKEN=test-token",
                        "TELEGRAM_BOT_TOKEN=test-bot",
                        "TELEGRAM_CHAT_ID=12345",
                        "ENABLE_JSEARCH=true",
                        "JSEARCH_API_KEY=jsearch-token",
                        "JSEARCH_MAX_API_REQUESTS_PER_RUN=4",
                    ]
                ),
                encoding="utf-8",
            )
            settings = load_settings(str(env_path))
            self.assertTrue(settings.jsearch_enabled)
            self.assertEqual(settings.jsearch_api_key, "jsearch-token")
            self.assertEqual(settings.jsearch_api_host, "jsearch.p.rapidapi.com")
            self.assertEqual(settings.jsearch_max_api_requests_per_run, 4)


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
            self.assertEqual(params["industry"], "-construction")
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


class FakeJSearchClient(JSearchClient):
    def __init__(self, settings, payloads):
        super().__init__(settings)
        self.payloads = payloads
        self.requests = []

    def _perform_request(self, params):
        self.requests.append(dict(params))
        key = (params.get("query"), int(params["page"]), params.get("work_from_home", "false"))
        return self.payloads.get(key, {"status": "OK", "data": []})


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
        job = normalize_job(SAMPLE_JOB, datetime(2025, 1, 23, tzinfo=timezone.utc), query_text="project manager")
        self.assertEqual(job.collector, "jobdatafeeds")
        self.assertEqual(job.query_text, "project manager")
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
        senior_job_raw["title"] = "Senior Strategy Manager"
        senior_job_raw["jsonLD"] = dict(SAMPLE_JOB["jsonLD"])
        senior_job_raw["jsonLD"]["title"] = "Senior Strategy Manager"
        senior_job = normalize_job(senior_job_raw, datetime(2025, 1, 23, tzinfo=timezone.utc))
        self.assertIn("Senior", excluded_by_seniority_title(senior_job, ["Senior", "Lead"]))

        team_lead_raw = dict(SAMPLE_JOB)
        team_lead_raw["title"] = "Team Lead - Client Operations Specialist"
        team_lead_raw["jsonLD"] = dict(SAMPLE_JOB["jsonLD"])
        team_lead_raw["jsonLD"]["title"] = "Team Lead - Client Operations Specialist"
        team_lead_job = normalize_job(team_lead_raw, datetime(2025, 1, 23, tzinfo=timezone.utc))
        markers = excluded_by_seniority_title(team_lead_job, ["Senior", "Lead"])
        self.assertIn("Lead", markers)

    def test_excluded_by_seniority_title_matches_requested_case_insensitive_markers(self):
        raw = dict(SAMPLE_JOB)
        raw["title"] = "TECHNICAL Account Engineer"
        raw["jsonLD"] = dict(SAMPLE_JOB["jsonLD"])
        raw["jsonLD"]["title"] = "TECHNICAL Account Engineer"
        job = normalize_job(raw, datetime(2025, 1, 23, tzinfo=timezone.utc))
        markers = excluded_by_seniority_title(job, ["Technical", "Account", "Engineer"])
        self.assertIn("Technical", markers)
        self.assertIn("Account", markers)
        self.assertIn("Engineer", markers)

    def test_excluded_by_seniority_title_allows_mid_titles(self):
        raw = dict(SAMPLE_JOB)
        raw["title"] = "Business Analyst Web and Mobile Banking"
        raw["jsonLD"] = dict(SAMPLE_JOB["jsonLD"])
        raw["jsonLD"]["title"] = "Business Analyst Web and Mobile Banking"
        job = normalize_job(raw, datetime(2025, 1, 23, tzinfo=timezone.utc))
        self.assertEqual(excluded_by_seniority_title(job, ["Senior", "Engineer"]), [])

    def test_normalize_job_extracts_salary_from_base_salary(self):
        raw = dict(SAMPLE_JOB)
        raw["title"] = "Business Analyst"
        raw["jsonLD"] = dict(SAMPLE_JOB["jsonLD"])
        raw["jsonLD"]["title"] = "Business Analyst"
        raw["jsonLD"]["salaryCurrency"] = "EUR"
        raw["jsonLD"]["baseSalary"] = {
            "@type": "MonetaryAmount",
            "value": {"minValue": "50000", "maxValue": "70000", "unitText": "Year"},
        }
        job = normalize_job(raw, datetime(2025, 1, 23, tzinfo=timezone.utc))
        self.assertEqual(job.salary_min, 50000.0)
        self.assertEqual(job.salary_max, 70000.0)
        self.assertEqual(job.salary_currency, "EUR")
        self.assertEqual(job.salary_period, "year")

    def test_normalize_job_extracts_hourly_single_value_salary(self):
        raw = dict(SAMPLE_JOB)
        raw["jsonLD"] = dict(SAMPLE_JOB["jsonLD"])
        raw["jsonLD"]["salaryCurrency"] = "EUR"
        raw["jsonLD"]["baseSalary"] = {
            "@type": "MonetaryAmount",
            "value": {"minValue": "22.00", "maxValue": "22.00", "unitText": "HOUR"},
        }
        job = normalize_job(raw, datetime(2025, 1, 23, tzinfo=timezone.utc))
        self.assertEqual(job.salary_min, 22.0)
        self.assertEqual(job.salary_max, 22.0)
        self.assertEqual(job.salary_period, "hour")

    def test_normalize_job_falls_back_to_top_level_salary(self):
        raw = dict(SAMPLE_JOB)
        raw["jsonLD"] = dict(SAMPLE_JOB["jsonLD"])
        raw["jsonLD"].pop("baseSalary", None)
        raw["jsonLD"]["salaryCurrency"] = "EUR"
        raw["minSalary"] = 40000
        raw["maxSalary"] = 65000
        job = normalize_job(raw, datetime(2025, 1, 23, tzinfo=timezone.utc))
        self.assertEqual(job.salary_min, 40000.0)
        self.assertEqual(job.salary_max, 65000.0)
        self.assertEqual(job.salary_currency, "EUR")

    def test_normalize_job_keeps_salary_empty_when_missing(self):
        job = normalize_job(SAMPLE_JOB, datetime(2025, 1, 23, tzinfo=timezone.utc))
        self.assertIsNone(job.salary_min)
        self.assertIsNone(job.salary_max)
        self.assertIsNone(job.salary_currency)
        self.assertIsNone(job.salary_period)


class JSearchTests(unittest.TestCase):
    def test_select_date_posted_uses_smallest_supported_bucket(self):
        context = type(
            "Ctx",
            (),
            {
                "lower_bound": datetime(2026, 3, 26, 9, 0, tzinfo=timezone.utc),
                "upper_bound": datetime(2026, 3, 26, 12, 0, tzinfo=timezone.utc),
            },
        )()
        self.assertEqual(select_date_posted(context), "today")

        context = type(
            "Ctx",
            (),
            {
                "lower_bound": datetime(2026, 3, 20, 9, 0, tzinfo=timezone.utc),
                "upper_bound": datetime(2026, 3, 26, 12, 0, tzinfo=timezone.utc),
            },
        )()
        self.assertEqual(select_date_posted(context), "week")

        context = type("Ctx", (), {"lower_bound": None, "upper_bound": datetime(2026, 3, 26, 12, 0, tzinfo=timezone.utc)})()
        self.assertEqual(select_date_posted(context), "anytime")

    def test_select_date_posted_respects_calendar_day_boundaries(self):
        context = type(
            "Ctx",
            (),
            {
                "lower_bound": datetime(2026, 3, 25, 22, 0, tzinfo=timezone.utc),
                "upper_bound": datetime(2026, 3, 26, 8, 0, tzinfo=timezone.utc),
            },
        )()
        self.assertEqual(select_date_posted(context), "3days")

        context = type(
            "Ctx",
            (),
            {
                "lower_bound": datetime(2026, 3, 26, 0, 0, tzinfo=timezone.utc),
                "upper_bound": datetime(2026, 3, 26, 8, 0, tzinfo=timezone.utc),
            },
        )()
        self.assertEqual(select_date_posted(context), "today")

    def test_normalize_jsearch_job_maps_payload(self):
        job = normalize_jsearch_job(
            SAMPLE_JSEARCH_JOB,
            datetime(2026, 3, 26, 12, 0, tzinfo=timezone.utc),
            query_text="project manager in Berlin",
        )
        self.assertEqual(job.collector, "jsearch")
        self.assertEqual(job.query_text, "project manager in Berlin")
        self.assertEqual(job.external_id, SAMPLE_JSEARCH_JOB["job_id"])
        self.assertEqual(job.portal, "linkedin")
        self.assertEqual(job.source, "jsearch")
        self.assertEqual(job.work_place, [])
        self.assertEqual(job.work_type, ["fulltime"])
        self.assertIsNone(job.salary_min)
        self.assertIsNone(job.salary_max)
        self.assertIsNone(job.salary_currency)
        self.assertIsNone(job.salary_period)
        self.assertEqual(
            job.canonical_url,
            "https://linkedin.com/jobs/view/pmo-manager-in-turnaround-programm-%E2%80%93-berlin-at-stadler-4390701473",
        )

    def test_normalize_jsearch_job_prefers_direct_apply_option(self):
        raw = dict(SAMPLE_JSEARCH_JOB)
        raw["job_apply_link"] = "https://example.com/fallback"
        raw["job_apply_is_direct"] = False
        raw["apply_options"] = [
            {"publisher": "Indeed", "apply_link": "https://example.com/non-direct", "is_direct": False},
            {"publisher": "XING", "apply_link": "https://example.com/direct", "is_direct": True},
        ]
        job = normalize_jsearch_job(raw, datetime(2026, 3, 26, 12, 0, tzinfo=timezone.utc), query_text="project manager in Berlin")
        self.assertEqual(job.canonical_url, "https://example.com/direct")
        self.assertTrue(job.is_direct)


class TitleLanguageTests(unittest.TestCase):
    def _job(self, title: str):
        raw = dict(SAMPLE_JOB)
        raw["title"] = title
        raw["jsonLD"] = dict(SAMPLE_JOB["jsonLD"])
        raw["jsonLD"]["title"] = title
        return normalize_job(raw, datetime(2025, 1, 23, tzinfo=timezone.utc))

    def test_detect_title_language_marks_clearly_german_titles(self):
        detection = detect_title_language("Projektmanager Digitalisierung")
        self.assertEqual(detection.detected_language, "german")
        self.assertGreaterEqual(detection.confidence, 0.85)

        detection = detect_title_language("Assistenz der Geschäftsführung - Finance & Operations")
        self.assertEqual(detection.detected_language, "german")
        self.assertGreaterEqual(detection.confidence, 0.85)

    def test_detect_title_language_keeps_english_titles(self):
        self.assertEqual(detect_title_language("Strategy Analyst").detected_language, "english")
        self.assertEqual(detect_title_language("Operations Manager").detected_language, "english")

    def test_excluded_by_german_title_keeps_mixed_titles_below_threshold(self):
        job = self._job("PMO-Manager:in - Turnaround Programm – Berlin")
        self.assertIsNone(excluded_by_german_title(job, enabled=True, threshold=0.85))

        job = self._job("Technical Consultant DCS Retire - Schwerpunkt SQL & SAP; m/w/d")
        self.assertIsNone(excluded_by_german_title(job, enabled=True, threshold=0.85))

    def test_excluded_by_german_title_respects_enablement_and_threshold(self):
        job = self._job("Projektmanager Digitalisierung")
        self.assertIsNone(excluded_by_german_title(job, enabled=False, threshold=0.85))
        detection = excluded_by_german_title(job, enabled=True, threshold=0.85)
        self.assertIsNotNone(detection)
        assert detection is not None
        self.assertEqual(detection.detected_language, "german")
        self.assertGreaterEqual(detection.confidence, 0.85)


class DedupeTests(unittest.TestCase):
    def _job(self, portal, source, url, title="Project Management Lead", company="Microsoft"):
        base = normalize_job(SAMPLE_JOB, datetime(2025, 1, 23, tzinfo=timezone.utc))
        base.portal = portal
        base.source = source
        base.canonical_url = url
        base.external_id = f"{portal}-{source}"
        base.title = title
        base.company = company
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

    def test_mark_canonical_jobs_keeps_different_companies_separate(self):
        first = self._job("linkedin", "monster_de", "https://linkedin.example", company="Microsoft")
        second = self._job("stepstone", "stepstone", "https://stepstone.example", company="Google")
        jobs = mark_canonical_jobs([first, second])
        self.assertEqual(sum(1 for job in jobs if job.is_canonical), 2)


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
            job.salary_min = 50000.0
            job.salary_max = 70000.0
            job.salary_currency = "EUR"
            job.salary_period = "year"
            storage.upsert_jobs([job])
            jobs = storage.get_all_jobs()
            self.assertEqual(len(jobs), 1)
            self.assertEqual(jobs[0].collector, "jobdatafeeds")
            self.assertEqual(jobs[0].query_text, "")
            self.assertEqual(jobs[0].title, "Project Management Lead")
            self.assertEqual(jobs[0].salary_min, 50000.0)
            self.assertEqual(jobs[0].salary_max, 70000.0)
            self.assertEqual(jobs[0].salary_currency, "EUR")
            self.assertEqual(jobs[0].salary_period, "year")

    def test_storage_migrates_salary_columns(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "jobs.sqlite3"
            import sqlite3

            conn = sqlite3.connect(db_path)
            conn.executescript(
                """
                CREATE TABLE jobs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    external_id TEXT NOT NULL,
                    collector TEXT NOT NULL DEFAULT 'jobdatafeeds',
                    query_text TEXT NOT NULL DEFAULT '',
                    portal TEXT NOT NULL,
                    source TEXT NOT NULL,
                    title TEXT NOT NULL,
                    company TEXT NOT NULL,
                    country_code TEXT NOT NULL,
                    state TEXT NOT NULL,
                    city TEXT NOT NULL,
                    timezone TEXT NOT NULL,
                    timezone_offset INTEGER,
                    work_place_json TEXT NOT NULL,
                    work_type_json TEXT NOT NULL,
                    contract_type_json TEXT NOT NULL,
                    career_level_json TEXT NOT NULL,
                    occupation TEXT NOT NULL,
                    industry TEXT NOT NULL,
                    language TEXT NOT NULL,
                    is_direct INTEGER NOT NULL,
                    is_recruiter INTEGER NOT NULL,
                    date_created TEXT,
                    date_active TEXT,
                    date_expired TEXT,
                    canonical_url TEXT NOT NULL,
                    description TEXT NOT NULL,
                    duplicate_fingerprint TEXT NOT NULL,
                    is_canonical INTEGER NOT NULL DEFAULT 0,
                    sent_at TEXT,
                    fetched_at TEXT NOT NULL,
                    raw_json TEXT NOT NULL,
                    UNIQUE (portal, source, external_id)
                );
                CREATE TABLE runs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    started_at TEXT NOT NULL,
                    ended_at TEXT,
                    status TEXT NOT NULL,
                    api_requests_made INTEGER NOT NULL DEFAULT 0,
                    jobs_fetched INTEGER NOT NULL DEFAULT 0,
                    jobs_inserted INTEGER NOT NULL DEFAULT 0,
                    jobs_canonical INTEGER NOT NULL DEFAULT 0,
                    was_truncated_by_request_cap INTEGER NOT NULL DEFAULT 0,
                    incomplete_titles_json TEXT NOT NULL DEFAULT '[]',
                    error_message TEXT
                );
                CREATE TABLE checkpoints (
                    id INTEGER PRIMARY KEY CHECK (id = 1),
                    last_successful_upper_bound TEXT
                );
                INSERT INTO checkpoints (id, last_successful_upper_bound) VALUES (1, NULL);
                """
            )
            conn.commit()
            conn.close()

            storage = Storage(db_path)
            with storage.connect() as migrated:
                columns = {row["name"] for row in migrated.execute("PRAGMA table_info(jobs)").fetchall()}
            self.assertIn("salary_min", columns)
            self.assertIn("salary_max", columns)
            self.assertIn("salary_currency", columns)
            self.assertIn("salary_period", columns)

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
                    f"JOBDATAFEEDS_MAX_API_REQUESTS_PER_RUN={max_requests}",
                ]
            ),
            encoding="utf-8",
        )
        filters = root / "jobfinder_filters.toml"
        filters.write_text(
            "\n".join(
                [
                    'notification_times = ["11:00", "14:00", "18:00"]',
                    'jobdatafeeds_job_titles = ["alpha", "beta", "gamma"]',
                    'jsearch_job_titles = ["alpha analyst", "beta analyst", "gamma analyst"]',
                    'excluded_job_title_markers = ["Senior", "Engineer"]',
                    "exclude_german_job_titles = true",
                    "german_job_title_confidence_threshold = 0.85",
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


class JSearchFetchTests(unittest.TestCase):
    def _settings(self, root: Path, *, max_requests: int = 5):
        env_path = root / ".env"
        env_path.write_text(
            "\n".join(
                [
                    "JOBDATAFEEDS_API_TOKEN=test-token",
                    "TELEGRAM_BOT_TOKEN=test-bot",
                    "TELEGRAM_CHAT_ID=12345",
                    "ENABLE_JSEARCH=true",
                    "JSEARCH_API_KEY=test-jsearch-token",
                    f"JSEARCH_MAX_API_REQUESTS_PER_RUN={max_requests}",
                ]
            ),
            encoding="utf-8",
        )
        filters = root / "jobfinder_filters.toml"
        filters.write_text(
            "\n".join(
                [
                    'notification_times = ["11:00", "14:00", "18:00"]',
                    'jobdatafeeds_job_titles = ["alpha", "beta", "gamma"]',
                    'jsearch_job_titles = ["alpha analyst", "beta analyst", "gamma analyst"]',
                    'excluded_job_title_markers = ["Senior", "Engineer"]',
                    "exclude_german_job_titles = true",
                    "german_job_title_confidence_threshold = 0.85",
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
                ("alpha analyst in Berlin", 1, "false"): {"status": "OK", "data": [dict(SAMPLE_JSEARCH_JOB, job_title="Alpha Analyst", job_id=f"a1-{i}") for i in range(10)]},
                ("beta analyst in Berlin", 1, "false"): {"status": "OK", "data": [dict(SAMPLE_JSEARCH_JOB, job_title="Beta Analyst", job_id=f"b1-{i}") for i in range(3)]},
                ("gamma analyst in Berlin", 1, "false"): {"status": "OK", "data": [dict(SAMPLE_JSEARCH_JOB, job_title="Gamma Analyst", job_id=f"g1-{i}") for i in range(10)]},
                ("alpha analyst in Berlin", 2, "false"): {"status": "OK", "data": [dict(SAMPLE_JSEARCH_JOB, job_title="Alpha Analyst", job_id=f"a2-{i}") for i in range(2)]},
                ("gamma analyst in Berlin", 2, "false"): {"status": "OK", "data": [dict(SAMPLE_JSEARCH_JOB, job_title="Gamma Analyst", job_id=f"g2-{i}") for i in range(2)]},
            }
            client = FakeJSearchClient(settings, payloads)
            context = type("Ctx", (), {
                "started_at": datetime(2026, 3, 26, 12, 0, tzinfo=timezone.utc),
                "upper_bound": datetime(2026, 3, 26, 12, 0, tzinfo=timezone.utc),
                "lower_bound": datetime(2026, 3, 26, 9, 0, tzinfo=timezone.utc),
            })()
            summary = client.fetch_jobs(context, include_remote=False)
            seen = [(req["query"], req["page"]) for req in client.requests]
            self.assertEqual(
                seen,
                [("alpha analyst in Berlin", "1"), ("beta analyst in Berlin", "1"), ("gamma analyst in Berlin", "1"), ("alpha analyst in Berlin", "2"), ("gamma analyst in Berlin", "2")],
            )
            self.assertEqual(summary.api_requests_made, 5)

    def test_fetch_jobs_marks_incomplete_titles_when_request_cap_hits(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            settings = self._settings(Path(tmpdir), max_requests=4)
            payloads = {
                ("alpha analyst in Berlin", 1, "false"): {"status": "OK", "data": [dict(SAMPLE_JSEARCH_JOB, job_title="Alpha Analyst", job_id=f"a1-{i}") for i in range(10)]},
                ("beta analyst in Berlin", 1, "false"): {"status": "OK", "data": [dict(SAMPLE_JSEARCH_JOB, job_title="Beta Analyst", job_id=f"b1-{i}") for i in range(10)]},
                ("gamma analyst in Berlin", 1, "false"): {"status": "OK", "data": [dict(SAMPLE_JSEARCH_JOB, job_title="Gamma Analyst", job_id=f"g1-{i}") for i in range(10)]},
                ("alpha analyst in Berlin", 2, "false"): {"status": "OK", "data": [dict(SAMPLE_JSEARCH_JOB, job_title="Alpha Analyst", job_id=f"a2-{i}") for i in range(2)]},
            }
            client = FakeJSearchClient(settings, payloads)
            context = type("Ctx", (), {
                "started_at": datetime(2026, 3, 26, 12, 0, tzinfo=timezone.utc),
                "upper_bound": datetime(2026, 3, 26, 12, 0, tzinfo=timezone.utc),
                "lower_bound": datetime(2026, 3, 26, 9, 0, tzinfo=timezone.utc),
            })()
            summary = client.fetch_jobs(context, include_remote=False)
            self.assertTrue(summary.was_truncated_by_request_cap)
            self.assertEqual(summary.incomplete_titles, ["beta analyst", "gamma analyst"])


class RunnerAggregationTests(unittest.TestCase):
    def test_run_daily_aggregates_provider_summaries(self):
        class FakeStorage:
            last_instance = None

            def __init__(self, db_path):
                self.db_path = db_path
                self.finalized = None
                FakeStorage.last_instance = self

            def get_last_checkpoint(self):
                return datetime(2026, 3, 26, 9, 0, tzinfo=timezone.utc)

            def create_run(self, started_at):
                return 1

            def upsert_jobs(self, jobs):
                self.jobs = list(jobs)
                return len(jobs)

            def get_all_jobs(self):
                return list(self.jobs)

            def update_canonical_flags(self, canonical_urls):
                self.canonical_urls = list(canonical_urls)

            def get_unsent_canonical_jobs(self):
                return []

            def mark_jobs_sent(self, canonical_urls, sent_at):
                self.sent = (list(canonical_urls), sent_at)

            def update_checkpoint(self, upper_bound):
                self.checkpoint = upper_bound

            def finalize_run(self, run_id, **kwargs):
                self.finalized = kwargs

        class FakeTelegramClient:
            def __init__(self, bot_token, chat_ids):
                self.bot_token = bot_token
                self.chat_ids = chat_ids

            def send_messages(self, messages):
                return datetime(2026, 3, 26, 12, 0, tzinfo=timezone.utc)

        class FakeJobDataFeedsClientForRunner:
            def __init__(self, settings):
                self.settings = settings

            def fetch_jobs(self, context, *, include_remote=True):
                return runner_module.FetchSummary(
                    jobs=[normalize_job(SAMPLE_JOB, context.started_at)],
                    api_requests_made=2,
                    jobs_fetched=1,
                    was_truncated_by_request_cap=False,
                    incomplete_titles=["alpha"],
                )

        class FakeJSearchClientForRunner:
            def __init__(self, settings):
                self.settings = settings

            def fetch_jobs(self, context, *, include_remote=True):
                return runner_module.FetchSummary(
                    jobs=[normalize_jsearch_job(SAMPLE_JSEARCH_JOB, context.started_at)],
                    api_requests_made=1,
                    jobs_fetched=1,
                    was_truncated_by_request_cap=True,
                    incomplete_titles=["beta"],
                )

        with tempfile.TemporaryDirectory() as tmpdir:
            env_path, filters_path = write_config_files(Path(tmpdir))
            env_path.write_text(
                "\n".join(
                    [
                        "JOBDATAFEEDS_API_TOKEN=test-token",
                        "TELEGRAM_BOT_TOKEN=test-bot",
                        "TELEGRAM_CHAT_ID=12345",
                        "ENABLE_JSEARCH=true",
                        "JSEARCH_API_KEY=test-jsearch-token",
                    ]
                ),
                encoding="utf-8",
            )

            with patch.object(runner_module, "setup_logging"), patch.object(runner_module, "Storage", FakeStorage), patch.object(runner_module, "TelegramClient", FakeTelegramClient), patch.object(runner_module, "JobDataFeedsClient", FakeJobDataFeedsClientForRunner), patch.object(runner_module, "JSearchClient", FakeJSearchClientForRunner):
                exit_code = runner_module.run_daily(str(env_path), dry_run=True, filters_path=str(filters_path))

            self.assertEqual(exit_code, 0)
            self.assertEqual(FakeStorage.last_instance.finalized["api_requests_made"], 3)
            self.assertEqual(FakeStorage.last_instance.finalized["jobs_fetched"], 2)
            self.assertTrue(FakeStorage.last_instance.finalized["was_truncated_by_request_cap"])
            self.assertEqual(
                FakeStorage.last_instance.finalized["incomplete_titles"],
                ["jobdatafeeds: alpha", "jsearch: beta"],
            )

    def test_run_daily_filters_excluded_titles_from_all_collectors(self):
        class FakeStorage:
            last_instance = None

            def __init__(self, db_path):
                self.db_path = db_path
                self.finalized = None
                self.jobs = []
                FakeStorage.last_instance = self

            def get_last_checkpoint(self):
                return datetime(2026, 3, 26, 9, 0, tzinfo=timezone.utc)

            def create_run(self, started_at):
                return 1

            def upsert_jobs(self, jobs):
                self.jobs = list(jobs)
                return len(jobs)

            def get_all_jobs(self):
                return list(self.jobs)

            def update_canonical_flags(self, canonical_urls):
                self.canonical_urls = list(canonical_urls)

            def get_unsent_canonical_jobs(self):
                return []

            def mark_jobs_sent(self, canonical_urls, sent_at):
                self.sent = (list(canonical_urls), sent_at)

            def update_checkpoint(self, upper_bound):
                self.checkpoint = upper_bound

            def finalize_run(self, run_id, **kwargs):
                self.finalized = kwargs

        class FakeTelegramClient:
            def __init__(self, bot_token, chat_ids):
                self.bot_token = bot_token
                self.chat_ids = chat_ids

            def send_messages(self, messages):
                return datetime(2026, 3, 26, 12, 0, tzinfo=timezone.utc)

        class FakeJobDataFeedsClientForRunner:
            def __init__(self, settings):
                self.settings = settings

            def fetch_jobs(self, context, *, include_remote=True):
                raw = dict(SAMPLE_JOB)
                raw["title"] = "Operations Manager"
                raw["jsonLD"] = dict(SAMPLE_JOB["jsonLD"])
                raw["jsonLD"]["title"] = "Operations Manager"
                return runner_module.FetchSummary(
                    jobs=[normalize_job(raw, context.started_at)],
                    api_requests_made=1,
                    jobs_fetched=1,
                    was_truncated_by_request_cap=False,
                    incomplete_titles=[],
                )

        class FakeJSearchClientForRunner:
            def __init__(self, settings):
                self.settings = settings

            def fetch_jobs(self, context, *, include_remote=True):
                raw = dict(SAMPLE_JSEARCH_JOB)
                raw["job_title"] = "Operations Engineer"
                return runner_module.FetchSummary(
                    jobs=[normalize_jsearch_job(raw, context.started_at)],
                    api_requests_made=1,
                    jobs_fetched=1,
                    was_truncated_by_request_cap=False,
                    incomplete_titles=[],
                )

        with tempfile.TemporaryDirectory() as tmpdir:
            env_path, filters_path = write_config_files(Path(tmpdir))
            env_path.write_text(
                "\n".join(
                    [
                        "JOBDATAFEEDS_API_TOKEN=test-token",
                        "TELEGRAM_BOT_TOKEN=test-bot",
                        "TELEGRAM_CHAT_ID=12345",
                        "ENABLE_JSEARCH=true",
                        "JSEARCH_API_KEY=test-jsearch-token",
                    ]
                ),
                encoding="utf-8",
            )

            with patch.object(runner_module, "setup_logging"), patch.object(runner_module, "Storage", FakeStorage), patch.object(runner_module, "TelegramClient", FakeTelegramClient), patch.object(runner_module, "JobDataFeedsClient", FakeJobDataFeedsClientForRunner), patch.object(runner_module, "JSearchClient", FakeJSearchClientForRunner):
                exit_code = runner_module.run_daily(str(env_path), dry_run=True, filters_path=str(filters_path))

            self.assertEqual(exit_code, 0)
            self.assertEqual(len(FakeStorage.last_instance.jobs), 1)
            self.assertEqual(FakeStorage.last_instance.jobs[0].collector, "jobdatafeeds")
            self.assertEqual(FakeStorage.last_instance.jobs[0].title, "Operations Manager")

    def test_run_daily_filters_german_titles_and_logs_language_details(self):
        class FakeStorage:
            last_instance = None

            def __init__(self, db_path):
                self.db_path = db_path
                self.finalized = None
                self.jobs = []
                FakeStorage.last_instance = self

            def get_last_checkpoint(self):
                return datetime(2026, 3, 26, 9, 0, tzinfo=timezone.utc)

            def create_run(self, started_at):
                return 1

            def upsert_jobs(self, jobs):
                self.jobs = list(jobs)
                return len(jobs)

            def get_all_jobs(self):
                return list(self.jobs)

            def update_canonical_flags(self, canonical_urls):
                self.canonical_urls = list(canonical_urls)

            def get_unsent_canonical_jobs(self):
                return []

            def mark_jobs_sent(self, canonical_urls, sent_at):
                self.sent = (list(canonical_urls), sent_at)

            def update_checkpoint(self, upper_bound):
                self.checkpoint = upper_bound

            def finalize_run(self, run_id, **kwargs):
                self.finalized = kwargs

        class FakeTelegramClient:
            def __init__(self, bot_token, chat_ids):
                self.bot_token = bot_token
                self.chat_ids = chat_ids

            def send_messages(self, messages):
                return datetime(2026, 3, 26, 12, 0, tzinfo=timezone.utc)

        class FakeFilteredOutLogger:
            def __init__(self):
                self.messages = []

            def info(self, message):
                self.messages.append(message)

        class FakeJobDataFeedsClientForRunner:
            def __init__(self, settings):
                self.settings = settings

            def fetch_jobs(self, context, *, include_remote=True):
                raw = dict(SAMPLE_JOB)
                raw["title"] = "Projektmanager Digitalisierung"
                raw["jsonLD"] = dict(SAMPLE_JOB["jsonLD"])
                raw["jsonLD"]["title"] = "Projektmanager Digitalisierung"
                return runner_module.FetchSummary(
                    jobs=[normalize_job(raw, context.started_at, query_text="project manager")],
                    api_requests_made=1,
                    jobs_fetched=1,
                    was_truncated_by_request_cap=False,
                    incomplete_titles=[],
                )

        class FakeJSearchClientForRunner:
            def __init__(self, settings):
                self.settings = settings

            def fetch_jobs(self, context, *, include_remote=True):
                raw = dict(SAMPLE_JSEARCH_JOB)
                raw["job_title"] = "Operations Manager"
                return runner_module.FetchSummary(
                    jobs=[normalize_jsearch_job(raw, context.started_at, query_text="operations manager in Berlin")],
                    api_requests_made=1,
                    jobs_fetched=1,
                    was_truncated_by_request_cap=False,
                    incomplete_titles=[],
                )

        with tempfile.TemporaryDirectory() as tmpdir:
            env_path, filters_path = write_config_files(Path(tmpdir))
            env_path.write_text(
                "\n".join(
                    [
                        "JOBDATAFEEDS_API_TOKEN=test-token",
                        "TELEGRAM_BOT_TOKEN=test-bot",
                        "TELEGRAM_CHAT_ID=12345",
                        "ENABLE_JSEARCH=true",
                        "JSEARCH_API_KEY=test-jsearch-token",
                    ]
                ),
                encoding="utf-8",
            )
            fake_filtered_out_logger = FakeFilteredOutLogger()

            with patch.object(runner_module, "setup_logging"), patch.object(runner_module, "Storage", FakeStorage), patch.object(runner_module, "TelegramClient", FakeTelegramClient), patch.object(runner_module, "JobDataFeedsClient", FakeJobDataFeedsClientForRunner), patch.object(runner_module, "JSearchClient", FakeJSearchClientForRunner), patch.object(runner_module, "FILTERED_OUT_LOGGER", fake_filtered_out_logger):
                exit_code = runner_module.run_daily(str(env_path), dry_run=True, filters_path=str(filters_path))

            self.assertEqual(exit_code, 0)
            self.assertEqual(len(FakeStorage.last_instance.jobs), 1)
            self.assertEqual(FakeStorage.last_instance.jobs[0].collector, "jsearch")
            payload = json.loads(fake_filtered_out_logger.messages[0])
            self.assertEqual(
                payload,
                {
                    "reason": "jobdatafeeds_title_language_excluded",
                    "provider": "jobdatafeeds",
                    "title": "Projektmanager Digitalisierung",
                },
            )


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
        expected_header = (
            "Jobs posted from "
            f"{datetime(2025, 1, 1, 17, 0, tzinfo=timezone.utc).astimezone().strftime('%d.%m.%Y %H:%M')}-"
            f"{datetime(2025, 1, 1, 18, 0, tzinfo=timezone.utc).astimezone().strftime('%d.%m.%Y %H:%M')}"
        )
        expected_posted = datetime(2025, 1, 1, 18, 0, tzinfo=timezone.utc).astimezone().strftime("%d.%m.%Y %H:%M")
        self.assertIn(expected_header, messages[0])
        self.assertIn("Incomplete titles: strategy, business analyst", messages[0])
        self.assertIn("<b>Role</b>", messages[0])
        self.assertIn("<i>Comp</i>", messages[0])
        self.assertIn(f"Posted: {expected_posted}", messages[0])
        self.assertIn(f"{expected_header}\n\n<b>Role</b>", messages[0])
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
        self.assertIn('Apply: <a href="https://example.com/1">example</a>\n\n<b>Role Two</b>', messages[0])

    def test_telegram_message_formats_salary_range(self):
        rows = [
            {
                "work_place_json": "[]",
                "city": "Berlin",
                "state": "Berlin",
                "country_code": "de",
                "date_created": "2025-01-01T18:00:00+00:00",
                "fetched_at": "2025-01-01T18:00:00+00:00",
                "title": "Role",
                "company": "Comp",
                "portal": "linkedin",
                "source": "x",
                "canonical_url": "https://example.com",
                "salary_min": 50000.0,
                "salary_max": 70000.0,
                "salary_currency": "EUR",
                "salary_period": "year",
            }
        ]
        messages = build_digest_messages(rows, truncated=False, empty_notice=True)
        self.assertIn("Salary: EUR 50,000-70,000 / year", messages[0])

    def test_telegram_message_formats_salary_minimum_only(self):
        rows = [
            {
                "work_place_json": "[]",
                "city": "Berlin",
                "state": "Berlin",
                "country_code": "de",
                "date_created": "2025-01-01T18:00:00+00:00",
                "fetched_at": "2025-01-01T18:00:00+00:00",
                "title": "Role",
                "company": "Comp",
                "portal": "linkedin",
                "source": "x",
                "canonical_url": "https://example.com",
                "salary_min": 50000.0,
                "salary_max": None,
                "salary_currency": "EUR",
                "salary_period": "year",
            }
        ]
        messages = build_digest_messages(rows, truncated=False, empty_notice=True)
        self.assertIn("Salary: from EUR 50,000 / year", messages[0])

    def test_telegram_message_omits_salary_without_numeric_value(self):
        rows = [
            {
                "work_place_json": "[]",
                "city": "Berlin",
                "state": "Berlin",
                "country_code": "de",
                "date_created": "2025-01-01T18:00:00+00:00",
                "fetched_at": "2025-01-01T18:00:00+00:00",
                "title": "Role",
                "company": "Comp",
                "portal": "linkedin",
                "source": "x",
                "canonical_url": "https://example.com",
                "salary_min": None,
                "salary_max": None,
                "salary_currency": "EUR",
                "salary_period": "year",
            }
        ]
        messages = build_digest_messages(rows, truncated=False, empty_notice=True)
        self.assertNotIn("Salary:", messages[0])

    def test_telegram_message_uses_short_clickable_link_label(self):
        rows = [
            {
                "work_place_json": "[]",
                "city": "Berlin",
                "state": "Berlin",
                "country_code": "de",
                "date_created": "2025-01-01T18:00:00+00:00",
                "fetched_at": "2025-01-01T18:00:00+00:00",
                "title": "Role",
                "company": "Comp",
                "portal": "linkedin",
                "source": "x",
                "canonical_url": "https://linkedin.com/jobs/view/abc123",
            }
        ]
        messages = build_digest_messages(rows, truncated=False, empty_notice=True)
        self.assertIn('Apply: <a href="https://linkedin.com/jobs/view/abc123">linkedin</a>', messages[0])

    def test_telegram_message_strips_common_host_prefixes(self):
        rows = [
            {
                "work_place_json": "[]",
                "city": "Berlin",
                "state": "Berlin",
                "country_code": "de",
                "date_created": "2025-01-01T18:00:00+00:00",
                "fetched_at": "2025-01-01T18:00:00+00:00",
                "title": "Role",
                "company": "Comp",
                "portal": "x",
                "source": "x",
                "canonical_url": "https://www.arbeitsagentur.de/jobsuche/jobdetail/123",
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
                "portal": "x",
                "source": "x",
                "canonical_url": "https://job-boards.eu.greenhouse.io/company/jobs/1",
            },
            {
                "work_place_json": "[]",
                "city": "Berlin",
                "state": "Berlin",
                "country_code": "de",
                "date_created": "2025-01-01T20:00:00+00:00",
                "fetched_at": "2025-01-01T20:00:00+00:00",
                "title": "Role Three",
                "company": "Comp Three",
                "portal": "x",
                "source": "x",
                "canonical_url": "https://de.linkedin.com/jobs/view/abc",
            },
        ]
        messages = build_digest_messages(rows, truncated=False, empty_notice=True)
        self.assertIn('Apply: <a href="https://www.arbeitsagentur.de/jobsuche/jobdetail/123">arbeitsagentur</a>', messages[0])
        self.assertIn('Apply: <a href="https://job-boards.eu.greenhouse.io/company/jobs/1">greenhouse</a>', messages[0])
        self.assertIn('Apply: <a href="https://de.linkedin.com/jobs/view/abc">linkedin</a>', messages[0])

    def test_telegram_message_falls_back_to_link_for_empty_or_malformed_url(self):
        rows = [
            {
                "work_place_json": "[]",
                "city": "Berlin",
                "state": "Berlin",
                "country_code": "de",
                "date_created": "2025-01-01T18:00:00+00:00",
                "fetched_at": "2025-01-01T18:00:00+00:00",
                "title": "Role",
                "company": "Comp",
                "portal": "x",
                "source": "x",
                "canonical_url": "",
            }
        ]
        messages = build_digest_messages(rows, truncated=False, empty_notice=True)
        self.assertIn('Apply: <a href="">link</a>', messages[0])

    def test_sort_jobs_for_output_orders_by_collector_then_priority_then_recency(self):
        rows = [
            {"company": "Other Co", "collector": "jsearch", "date_created": "2026-03-26T10:00:00+00:00", "fetched_at": "2026-03-26T10:00:00+00:00"},
            {"company": "delivery hero", "collector": "jobdatafeeds", "date_created": "2026-03-25T10:00:00+00:00", "fetched_at": "2026-03-25T10:00:00+00:00"},
            {"company": "Zalando", "collector": "jobdatafeeds", "date_created": "2026-03-26T09:00:00+00:00", "fetched_at": "2026-03-26T09:00:00+00:00"},
            {"company": "AUTO1", "collector": "jsearch", "date_created": "2026-03-26T11:00:00+00:00", "fetched_at": "2026-03-26T11:00:00+00:00"},
        ]
        sorted_rows = _sort_jobs_for_output(rows, ["Zalando", "Delivery Hero", "AUTO1 Group"])
        self.assertEqual(
            [(row["collector"], row["company"]) for row in sorted_rows],
            [
                ("jobdatafeeds", "Zalando"),
                ("jobdatafeeds", "delivery hero"),
                ("jsearch", "AUTO1"),
                ("jsearch", "Other Co"),
            ],
        )

    def test_telegram_client_sends_each_message_to_each_chat_id(self):
        from jobfinder.telegram_client import TelegramClient

        sent_payloads = []

        class FakeResponse:
            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, tb):
                return False

            def read(self):
                return b'{"ok": true}'

        def fake_urlopen(request, timeout=30):
            sent_payloads.append(json.loads(request.data.decode("utf-8")))
            return FakeResponse()

        with patch("jobfinder.telegram_client.urlopen", side_effect=fake_urlopen):
            TelegramClient("test-bot", ["123", "456"]).send_messages(["hello", "world"])

        self.assertEqual(len(sent_payloads), 4)
        self.assertEqual([payload["chat_id"] for payload in sent_payloads], ["123", "123", "456", "456"])


if __name__ == "__main__":
    unittest.main()
