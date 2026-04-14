from __future__ import annotations

from dataclasses import dataclass
from functools import lru_cache
from typing import Iterable, List

from lingua import Language, LanguageDetectorBuilder
from .dedupe import normalize_text
from .models import NormalizedJob


def excluded_by_title(job: NormalizedJob, excluded_markers: Iterable[str]) -> List[str]:
    normalized_title = normalize_text(job.title)
    return [str(marker) for marker in excluded_markers if normalize_text(str(marker)) in normalized_title]


@dataclass(frozen=True)
class TitleLanguageDetection:
    detected_language: str
    confidence: float


@lru_cache(maxsize=1)
def _title_language_detector():
    return LanguageDetectorBuilder.from_languages(Language.GERMAN, Language.ENGLISH).build()


def detect_title_language(title: str) -> TitleLanguageDetection:
    stripped = title.strip()
    if not stripped:
        return TitleLanguageDetection(detected_language="unknown", confidence=0.0)

    detector = _title_language_detector()
    detected_language = detector.detect_language_of(stripped)
    confidence_values = detector.compute_language_confidence_values(stripped)
    confidence = confidence_values[0].value if confidence_values else 0.0

    if detected_language == Language.GERMAN:
        return TitleLanguageDetection(detected_language="german", confidence=confidence)
    if detected_language == Language.ENGLISH:
        return TitleLanguageDetection(detected_language="english", confidence=confidence)
    return TitleLanguageDetection(detected_language="unknown", confidence=confidence)


def excluded_by_german_title(job: NormalizedJob, *, enabled: bool, threshold: float) -> TitleLanguageDetection | None:
    if not enabled:
        return None

    detection = detect_title_language(job.title)
    if detection.detected_language != "german":
        return None
    if detection.confidence < threshold:
        return None
    return detection
