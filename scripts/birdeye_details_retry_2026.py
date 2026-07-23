#!/usr/bin/env python3
from __future__ import annotations

import json
import os
import time
from pathlib import Path
from typing import Any

import requests

VERSION = "2026-07-23-birdeye-details-retry-v3"
BASE_URL = "https://public-api.birdeye.so"
ENDPOINT = "/wallet/v2/pnl/details"
OUTPUT_DIR = Path("output-birdeye-details-retry")


def safe_rate_headers(headers: requests.structures.CaseInsensitiveDict[str]) -> dict[str, str]:
    allowed = (
        "retry-after",
        "ratelimit",
        "rate-limit",
        "remaining",
        "reset",
        "request-id",
    )
    return {
        str(k): str(v)
        for k, v in headers.items()
        if any(marker in str(k).lower() for marker in allowed)
    }


def retry_delay(response: requests.Response | None, attempt: int) -> int:
    if response is not None:
        raw = response.headers.get("Retry-After")
        if raw:
            try:
                return max(1, min(180, int(float(raw))))
            except ValueError:
                pass
    # Deliberately conservative for the Wallet API group.
    return min(120, [5, 15, 30, 60, 120][min(attempt - 1, 4)])


def main() -> int:
    api_key = os.environ.get("BIRDEYE_API_KEY", "").strip()
    if not api_key:
        raise SystemExit("BIRDEYE_API_KEY is missing")

    wallet_path = Path("data/birdeye_test_wallet.txt")
    if not wallet_path.is_file():
        raise SystemExit("data/birdeye_test_wallet.txt is missing")
    wallet = wallet_path.read_text(encoding="utf-8").strip()
    if not wallet:
        raise SystemExit("Test wallet is empty")

    headers = {
        "X-API-KEY": api_key,
        "x-chain": "solana",
        "accept": "application/json",
        "content-type": "application/json",
    }
    body = {
        "wallet": wallet,
        "duration": "all",
        "position_scope": "cumulative",
        "sort_type": "desc",
        "sort_by": "last_trade",
        "offset": 0,
        "limit": 100,
    }

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    attempts: list[dict[str, Any]] = []
    final_response: requests.Response | None = None
    final_payload: Any = None

    # Initial pause prevents an immediate collision with another account-level request.
    time.sleep(3)

    for attempt in range(1, 6):
        response: requests.Response | None = None
        try:
            response = requests.post(
                BASE_URL + ENDPOINT,
                headers=headers,
                json=body,
                timeout=90,
            )
            final_response = response
            try:
                payload = response.json()
            except ValueError:
                payload = {"raw_text": response.text[:5000]}
            final_payload = payload

            attempts.append(
                {
                    "attempt": attempt,
                    "status_code": response.status_code,
                    "ok": response.ok,
                    "safe_rate_headers": safe_rate_headers(response.headers),
                    "response": payload,
                }
            )

            if response.status_code == 200:
                break
            if response.status_code != 429:
                break

            if attempt < 5:
                delay = retry_delay(response, attempt)
                print(f"HTTP 429; retrying after {delay} seconds", flush=True)
                time.sleep(delay)

        except requests.RequestException as exc:
            attempts.append(
                {
                    "attempt": attempt,
                    "status_code": None,
                    "ok": False,
                    "network_error": str(exc),
                }
            )
            if attempt < 5:
                delay = retry_delay(response, attempt)
                print(f"Network error; retrying after {delay} seconds", flush=True)
                time.sleep(delay)

    status = final_response.status_code if final_response is not None else None
    classification = {
        200: "accessible",
        400: "bad_request",
        401: "authentication_error",
        403: "plan_or_permission_restriction",
        429: "rate_limit",
    }.get(status, "network_or_server_error" if status is None or status >= 500 else "other")

    raw_report = {
        "version": VERSION,
        "wallet": wallet,
        "endpoint": ENDPOINT,
        "request_body_without_secret": body,
        "attempts": attempts,
        "final_status": status,
        "final_classification": classification,
        "details_access": status == 200,
        "provider_limit_note": (
            "A 200 response proves endpoint access, not complete historical coverage. "
            "Birdeye notes that protocol trade data may not be fully backfilled."
        ),
    }
    (OUTPUT_DIR / "birdeye_details_retry_raw.json").write_text(
        json.dumps(raw_report, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    summary = {
        "version": VERSION,
        "wallet": wallet,
        "details_status": status,
        "details_classification": classification,
        "details_access": status == 200,
        "attempt_count": len(attempts),
        "safe_decision": (
            "compare_with_moralis_on_five_clean_wallets"
            if status == 200
            else "inspect_retry_artifact_before_bulk_scan"
        ),
    }
    (OUTPUT_DIR / "birdeye_details_retry_report.json").write_text(
        json.dumps(summary, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    print(json.dumps(summary, ensure_ascii=False, indent=2), flush=True)

    # Diagnostic statuses should still upload artifacts.
    if status is None or (isinstance(status, int) and status >= 500):
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
