from __future__ import annotations

import hashlib
import re
from typing import Iterable, List

from .models import NormalizedJob


_NON_ALNUM = re.compile(r"[^a-z0-9]+")


def normalize_text(value: str) -> str:
    lowered = value.lower().strip()
    return _NON_ALNUM.sub(" ", lowered).strip()


def build_duplicate_fingerprint(
    title: str,
    company: str,
    description: str,
) -> str:
    key = _join_normalized_parts(title, company, description)
    return hashlib.sha256(key.encode("utf-8")).hexdigest()


def build_similarity_key(job: NormalizedJob) -> str:
    return _join_normalized_parts(job.title, job.company, job.description)


def _join_normalized_parts(*parts: str) -> str:
    return "|".join(normalize_text(part) for part in parts)


def _source_rank(job: NormalizedJob) -> tuple:
    portal = (job.portal or "").lower()
    has_description = bool(job.description.strip())
    stable_id = bool(job.external_id.strip())
    return (
        0 if portal == "linkedin" else 1,
        0 if job.is_direct else 1,
        0 if has_description else 1,
        0 if stable_id else 1,
        portal,
        (job.source or "").lower(),
        job.canonical_url,
    )


def choose_canonical(jobs: Iterable[NormalizedJob]) -> NormalizedJob:
    return min(list(jobs), key=_source_rank)


def mark_canonical_jobs(jobs: List[NormalizedJob]) -> List[NormalizedJob]:
    by_key = {}
    for job in jobs:
        by_key.setdefault(build_similarity_key(job), []).append(job)

    canonical_urls = {choose_canonical(group).canonical_url for group in by_key.values()}
    for job in jobs:
        job.is_canonical = job.canonical_url in canonical_urls
    return jobs
