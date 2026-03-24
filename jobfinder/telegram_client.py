from __future__ import annotations

import json
from datetime import datetime
from typing import Iterable, List, Sequence
from urllib.request import Request, urlopen


MAX_MESSAGE_LENGTH = 4000


def _chunks(lines: Sequence[str]) -> List[str]:
    messages: List[str] = []
    current = ""
    for line in lines:
        candidate = f"{current}\n{line}".strip() if current else line
        if len(candidate) > MAX_MESSAGE_LENGTH and current:
            messages.append(current)
            current = line
        else:
            current = candidate
    if current:
        messages.append(current)
    return messages


def format_job_line(row) -> str:
    workplace = ", ".join(json.loads(row["work_place_json"])) or "n/a"
    location = ", ".join(part for part in (row["city"], row["state"], row["country_code"]) if part) or "Remote"
    posted = row["date_created"] or row["fetched_at"]
    return (
        f"*{row['title']}*\n"
        f"{row['company']} | {row['portal']}/{row['source']}\n"
        f"{workplace} | {location}\n"
        f"Posted: {posted}\n"
        f"Apply: {row['canonical_url']}"
    )


def build_digest_messages(rows: Sequence, *, truncated: bool, empty_notice: bool) -> List[str]:
    if not rows:
        return ["No new matching jobs were found in the last run."] if empty_notice else []

    header = "Daily job digest"
    if truncated:
        header += "\nWarning: the fetch stopped early because the configured request/job cap was reached."
    lines = [header, ""] + [format_job_line(row) for row in rows]
    return _chunks(lines)


class TelegramClient:
    def __init__(self, bot_token: str, chat_id: str):
        self.bot_token = bot_token
        self.chat_id = chat_id

    @property
    def _send_message_url(self) -> str:
        return f"https://api.telegram.org/bot{self.bot_token}/sendMessage"

    def send_messages(self, messages: Iterable[str]) -> datetime:
        sent_at = datetime.now().astimezone()
        for message in messages:
            payload = json.dumps(
                {
                    "chat_id": self.chat_id,
                    "text": message,
                    "parse_mode": "Markdown",
                    "disable_web_page_preview": False,
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
        return sent_at
