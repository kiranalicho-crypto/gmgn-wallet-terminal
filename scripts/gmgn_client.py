"""Small, retrying wrapper around the official ``gmgn-cli`` read commands."""

from __future__ import annotations

import json
import os
import re
import subprocess
import time
from dataclasses import dataclass
from typing import Any, Iterable


class GmgnError(RuntimeError):
    """Raised when gmgn-cli cannot return a valid response."""


@dataclass(frozen=True)
class GmgnResult:
    command: tuple[str, ...]
    payload: Any


def _reset_timestamp(text: str) -> int | None:
    for pattern in (
        r'"reset_at"\s*:\s*(\d+)',
        r"reset_at[=: ]+(\d+)",
        r"X-RateLimit-Reset[=: ]+(\d+)",
    ):
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            return int(match.group(1))
    return None


def unwrap_data(payload: Any) -> Any:
    """Return ``payload.data`` when the CLI uses an envelope."""
    if isinstance(payload, dict) and "data" in payload:
        data = payload.get("data")
        if data is not None:
            return data
    return payload


def as_number(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def token_address(item: dict[str, Any]) -> str:
    token = item.get("token")
    if isinstance(token, dict):
        value = token.get("address") or token.get("token_address")
        if value:
            return str(value)
    value = item.get("token_address") or item.get("address")
    return str(value or "")


class GmgnClient:
    def __init__(
        self,
        *,
        timeout_seconds: int = 180,
        max_attempts: int = 4,
        min_delay_seconds: float = 0.35,
    ) -> None:
        if not os.environ.get("GMGN_API_KEY", "").strip():
            raise GmgnError("GMGN_API_KEY ortam değişkeni bulunamadı.")
        self.timeout_seconds = timeout_seconds
        self.max_attempts = max_attempts
        self.min_delay_seconds = min_delay_seconds

    def run(self, arguments: Iterable[str]) -> GmgnResult:
        command = (
            "npx",
            "--yes",
            "gmgn-cli",
            *tuple(str(arg) for arg in arguments),
            "--raw",
        )

        for attempt in range(1, self.max_attempts + 1):
            try:
                completed = subprocess.run(
                    command,
                    capture_output=True,
                    text=True,
                    timeout=self.timeout_seconds,
                    check=False,
                    env=os.environ.copy(),
                )
            except subprocess.TimeoutExpired as exc:
                if attempt == self.max_attempts:
                    raise GmgnError(
                        f"GMGN komutu timeout oldu: {' '.join(command)}"
                    ) from exc
                time.sleep(min(60, 5 * attempt))
                continue

            stdout = completed.stdout.strip()
            stderr = completed.stderr.strip()

            if completed.returncode == 0 and stdout:
                try:
                    payload = json.loads(stdout)
                except json.JSONDecodeError as exc:
                    raise GmgnError(
                        "GMGN çıktısı JSON değil. "
                        f"Başlangıç: {stdout[:500]}"
                    ) from exc
                time.sleep(self.min_delay_seconds)
                return GmgnResult(command=command, payload=payload)

            combined = f"{stderr}\n{stdout}".strip()
            transient = any(
                marker in combined.upper()
                for marker in (
                    "429",
                    "RATE_LIMIT",
                    "EAI_AGAIN",
                    "ECONNRESET",
                    "ETIMEDOUT",
                    "502",
                    "503",
                    "504",
                )
            )

            if transient and attempt < self.max_attempts:
                reset = _reset_timestamp(combined)
                if reset:
                    wait = max(1, reset - int(time.time()) + 2)
                else:
                    wait = min(90, 10 * attempt)
                time.sleep(wait)
                continue

            raise GmgnError(
                "GMGN komutu başarısız. "
                f"exit={completed.returncode}; command={' '.join(command)}; "
                f"error={combined[:1500]}"
            )

        raise GmgnError("GMGN komutu tekrar denemelerden sonra başarısız.")

    def created_tokens(self, creator: str) -> dict[str, Any]:
        result = self.run(
            (
                "portfolio",
                "created-tokens",
                "--chain",
                "sol",
                "--wallet",
                creator,
                "--order-by",
                "token_ath_mc",
                "--direction",
                "desc",
                "--migrate-state",
                "migrated",
            )
        )
        data = unwrap_data(result.payload)
        if not isinstance(data, dict):
            raise GmgnError("created-tokens cevabında data nesnesi yok.")
        return data

    def holdings_all(self, wallet: str) -> list[dict[str, Any]]:
        cursor: str | None = None
        holdings: list[dict[str, Any]] = []
        seen_cursors: set[str] = set()

        while True:
            args = [
                "portfolio",
                "holdings",
                "--chain",
                "sol",
                "--wallet",
                wallet,
                "--limit",
                "50",
                "--order-by",
                "realized_profit",
                "--direction",
                "desc",
                "--sell-out",
                "--show-small",
            ]
            if cursor:
                args.extend(("--cursor", cursor))
            result = self.run(args)
            data = unwrap_data(result.payload)
            if not isinstance(data, dict):
                raise GmgnError("holdings cevabında data nesnesi yok.")
            page = data.get("holdings")
            if not isinstance(page, list):
                raise GmgnError("holdings cevabında holdings listesi yok.")
            holdings.extend(item for item in page if isinstance(item, dict))
            next_cursor = data.get("next") or data.get("next_cursor")
            if not next_cursor:
                break
            cursor = str(next_cursor)
            if cursor in seen_cursors:
                raise GmgnError("holdings pagination cursor döngüsü oluştu.")
            seen_cursors.add(cursor)

        return holdings

    def activity_all(
        self,
        wallet: str,
        token: str,
    ) -> list[dict[str, Any]]:
        cursor: str | None = None
        activities: list[dict[str, Any]] = []
        seen_cursors: set[str] = set()

        while True:
            args = [
                "portfolio",
                "activity",
                "--chain",
                "sol",
                "--wallet",
                wallet,
                "--token",
                token,
                "--limit",
                "50",
                "--type",
                "buy",
                "--type",
                "sell",
            ]
            if cursor:
                args.extend(("--cursor", cursor))
            result = self.run(args)
            data = unwrap_data(result.payload)
            if not isinstance(data, dict):
                raise GmgnError("activity cevabında data nesnesi yok.")
            page = data.get("activities")
            if not isinstance(page, list):
                raise GmgnError("activity cevabında activities listesi yok.")
            activities.extend(item for item in page if isinstance(item, dict))
            next_cursor = data.get("next") or data.get("next_cursor")
            if not next_cursor:
                break
            cursor = str(next_cursor)
            if cursor in seen_cursors:
                raise GmgnError("activity pagination cursor döngüsü oluştu.")
            seen_cursors.add(cursor)

        return activities
