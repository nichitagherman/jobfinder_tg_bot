from __future__ import annotations

from typing import Iterable, List

from .dedupe import normalize_text
from .models import NormalizedJob


def excluded_by_title(job: NormalizedJob, excluded_markers: Iterable[str]) -> List[str]:
    normalized_title = normalize_text(job.title)
    return [str(marker) for marker in excluded_markers if normalize_text(str(marker)) in normalized_title]
