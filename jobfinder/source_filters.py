from __future__ import annotations

from typing import Iterable, List
from urllib.parse import urlsplit

from .models import NormalizedJob


def normalize_domain(value: str) -> str:
    text = value.strip().lower()
    if not text:
        return ""
    parsed = urlsplit(text if "://" in text else f"//{text}")
    domain = parsed.hostname or text.split("/", 1)[0]
    return domain.removeprefix("www.").strip(".")


def excluded_by_source_domain(job: NormalizedJob, excluded_domains: Iterable[str]) -> List[str]:
    job_domain = normalize_domain(job.canonical_url)
    if not job_domain:
        return []

    matched_domains: List[str] = []
    for domain in excluded_domains:
        normalized_domain = normalize_domain(str(domain))
        if normalized_domain and (
            job_domain == normalized_domain or job_domain.endswith(f".{normalized_domain}")
        ):
            matched_domains.append(str(domain))
    return matched_domains
