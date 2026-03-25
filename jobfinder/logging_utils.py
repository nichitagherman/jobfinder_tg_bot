from __future__ import annotations

import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path

from .config import Settings

FILTERED_OUT_LOGGER_NAME = "jobfinder.filtered_out_jobs"


def setup_logging(settings: Settings, *, dry_run: bool) -> None:
    log_path = settings.log_path
    filtered_out_jobs_log_path = settings.filtered_out_jobs_log_path
    log_path.parent.mkdir(parents=True, exist_ok=True)
    filtered_out_jobs_log_path.parent.mkdir(parents=True, exist_ok=True)

    formatter = logging.Formatter(
        fmt="%(asctime)s %(levelname)s [%(name)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)
    root_logger.handlers.clear()

    file_handler = RotatingFileHandler(
        log_path,
        maxBytes=1_000_000,
        backupCount=5,
        encoding="utf-8",
    )
    file_handler.setFormatter(formatter)

    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)

    root_logger.addHandler(file_handler)
    root_logger.addHandler(stream_handler)

    filtered_out_logger = logging.getLogger(FILTERED_OUT_LOGGER_NAME)
    filtered_out_logger.setLevel(logging.INFO)
    filtered_out_logger.handlers.clear()
    filtered_out_logger.propagate = False

    filtered_out_handler = RotatingFileHandler(
        filtered_out_jobs_log_path,
        maxBytes=5_000_000,
        backupCount=5,
        encoding="utf-8",
    )
    filtered_out_handler.setFormatter(logging.Formatter("%(message)s"))
    filtered_out_logger.addHandler(filtered_out_handler)

    logging.getLogger(__name__).info(
        "Logging initialized: log_path=%s filtered_out_jobs_log_path=%s dry_run=%s",
        log_path,
        filtered_out_jobs_log_path,
        dry_run,
    )
