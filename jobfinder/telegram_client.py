from __future__ import annotations

import html
import json
import logging
from datetime import datetime
from typing import Iterable, List, Optional, Sequence
from urllib.request import Request, urlopen


MAX_MESSAGE_LENGTH = 4000
LOGGER = logging.getLogger(__name__)


def _parse_timestamp(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    normalized = value.replace("Z", "+00:00")
    try:
        return datetime.fromisoformat(normalized)
    except ValueError:
        return None


def _format_timestamp(value: Optional[str]) -> str:
    parsed = _parse_timestamp(value)
    if parsed is None:
        return value or ""
    return parsed.astimezone().strftime("%d.%m.%Y %H:%M")


def _chunks_blocks(blocks: Sequence[str]) -> List[str]:
    messages: List[str] = []
    current = ""
    for block in blocks:
        candidate = f"{current}\n\n{block}".strip() if current else block
        if len(candidate) > MAX_MESSAGE_LENGTH and current:
            messages.append(current)
            current = block
        else:
            current = candidate
    if current:
        messages.append(current)
    return messages


def format_job_line(row) -> str:
    posted = _format_timestamp(row["date_created"] or row["fetched_at"])
    title = html.escape(row["title"])
    company = html.escape(row["company"])
    apply_url = html.escape(row["canonical_url"], quote=True)
    return (
        f"<b>{title}</b>\n"
        f"<i>{company}</i>\n"
        f"Posted: {posted}\n"
        f"Apply: {apply_url}"
    )


def build_digest_messages(
    rows: Sequence,
    *,
    truncated: bool,
    empty_notice: bool,
    lower_bound: Optional[datetime] = None,
    upper_bound: Optional[datetime] = None,
    incomplete_titles: Sequence[str] | None = None,
) -> List[str]:
    if not rows:
        return ["No new matching jobs were found in the last run."] if empty_notice else []

    if lower_bound and upper_bound:
        header = (
            "Jobs posted from "
            f"{lower_bound.astimezone().strftime('%d.%m.%Y %H:%M')}-"
            f"{upper_bound.astimezone().strftime('%d.%m.%Y %H:%M')}"
        )
    else:
        header = "Jobs posted from unknown-unknown"

    blocks = [header]
    blocks.extend(format_job_line(row) for row in rows)

    if truncated:
        warning = "Warning: the fetch stopped early because the configured request cap was reached."
        if incomplete_titles:
            warning += f"\nIncomplete titles: {', '.join(incomplete_titles)}"
        blocks.append(warning)

    return _chunks_blocks(blocks)


class TelegramClient:
    def __init__(self, bot_token: str, chat_ids: Sequence[str]):
        self.bot_token = bot_token
        self.chat_ids = list(chat_ids)

    @property
    def _send_message_url(self) -> str:
        return f"https://api.telegram.org/bot{self.bot_token}/sendMessage"

    def send_messages(self, messages: Iterable[str]) -> datetime:
        messages = list(messages)
        LOGGER.info("Sending Telegram messages: count=%s chat_ids=%s", len(messages), len(self.chat_ids))
        sent_at = datetime.now().astimezone()
        for chat_id in self.chat_ids:
            for index, message in enumerate(messages, start=1):
                LOGGER.info(
                    "Sending Telegram message %s/%s to chat_id=%s: chars=%s",
                    index,
                    len(messages),
                    chat_id,
                    len(message),
                )
                payload = json.dumps(
                    {
                        "chat_id": chat_id,
                        "text": message,
                        "parse_mode": "HTML",
                        "disable_web_page_preview": True,
                    }
                ).encode("utf-8")
                request = Request(
                    self._send_message_url,
                    data=payload,
                    headers={"Content-Type": "application/json"},
                    method="POST",
                )
                with urlopen(request, timeout=30) as response:
                    body = json.loads(response.read().decode("utf-8"))
                    if not body.get("ok"):
                        raise RuntimeError(f"Telegram send failed: {body}")
        LOGGER.info("Telegram send complete: count=%s chat_ids=%s sent_at=%s", len(messages), len(self.chat_ids), sent_at.isoformat())
        return sent_at
