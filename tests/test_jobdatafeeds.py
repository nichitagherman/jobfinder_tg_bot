import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

from jobfinder.config import load_settings
from jobfinder.dedupe import choose_canonical, mark_canonical_jobs
from jobfinder.jobdatafeeds_client import (
    berlin_brandenburg_match,
    build_query_params,
    normalize_job,
    remote_berlin_compatible,
    title_matches,
)
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


DEFAULT_FILTERS = """job_titles = [
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
            self.assertEqual(settings.max_jobs_per_run, 2)
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

    def test_build_presets_can_exclude_remote(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            env_path, _ = write_config_files(Path(tmpdir))
            settings = load_settings(str(env_path))
            presets = settings.build_presets(include_remote=False)
            self.assertEqual(len(presets), 1)
            self.assertEqual(presets[0].name, "berlin_brandenburg_all_workplaces")

    def test_load_settings_can_use_custom_filters_path(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            env_path, _ = write_config_files(root)
            custom_filters = root / "custom_filters.toml"
            custom_filters.write_text('job_titles = ["strategy"]\n', encoding="utf-8")
            settings = load_settings(str(env_path), filters_path=str(custom_filters))
            self.assertEqual(settings.search_titles, ["strategy"])
            self.assertEqual(settings.filters_path, custom_filters)


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
            self.assertEqual(params["isActive"], "true")
            self.assertEqual(params["dateCreatedMin"], "2025-01-01")
            self.assertEqual(params["dateCreatedMax"], "2025-01-02")
            self.assertEqual(
                params["title"],
                '"project manager" OR "project management" OR "business analyst" OR "business analytics" OR "strategy"',
            )
            self.assertNotIn("", params.keys())


class NormalizationTests(unittest.TestCase):
    def test_normalize_job_maps_payload(self):
        job = normalize_job(SAMPLE_JOB, datetime(2025, 1, 23, tzinfo=timezone.utc))
        self.assertEqual(job.external_id, "abc123")
        self.assertEqual(job.canonical_url, "https://www.linkedin.com/jobs/view/abc123")
        self.assertEqual(job.company, "Microsoft")
        self.assertEqual(job.city, "Berlin")
        self.assertEqual(job.work_place, ["remote"])

    def test_filters_accept_expected_jobs(self):
        job = normalize_job(SAMPLE_JOB, datetime(2025, 1, 23, tzinfo=timezone.utc))
        self.assertTrue(berlin_brandenburg_match(job))
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


class TelegramTests(unittest.TestCase):
    def test_empty_digest_message(self):
        messages = build_digest_messages([], truncated=False, empty_notice=True)
        self.assertEqual(messages, ["No new matching jobs were found in the last run."])


if __name__ == "__main__":
    unittest.main()
