#!/usr/bin/env python3
from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

import requests

VERSION = "2026-07-23-birdeye-access-test-v2"
BASE = "https://public-api.birdeye.so"


def call(method: str, path: str, headers: dict[str, str], **kwargs: Any) -> dict[str, Any]:
    try:
        response = requests.request(
            method,
            BASE + path,
            headers=headers,
            timeout=60,
            **kwargs,
        )
    except requests.RequestException as exc:
        return {
            "method": method,
            "path": path,
            "status_code": 0,
            "ok": False,
            "error_type": "network_error",
            "error": str(exc),
            "response": {},
        }

    try:
        payload: Any = response.json()
    except ValueError:
        payload = {"raw_text": response.text[:5000]}

    return {
        "method": method,
        "path": path,
        "status_code": response.status_code,
        "ok": response.ok,
        "response": payload,
    }


def classify(status_code: int) -> str:
    return {
        0: "network_error",
        200: "accessible",
        400: "request_rejected_check_payload",
        401: "api_key_or_header_error",
        403: "plan_or_allowlist_restriction",
        429: "rate_limit",
    }.get(status_code, "server_or_unexpected_error" if status_code >= 500 else "unexpected_status")


def main() -> None:
    key = os.environ.get("BIRDEYE_API_KEY", "").strip()
    if not key:
        raise SystemExit("BIRDEYE_API_KEY is missing")

    wallet_path = Path("data/birdeye_test_wallet.txt")
    if not wallet_path.is_file():
        raise SystemExit("data/birdeye_test_wallet.txt is missing")
    wallet = wallet_path.read_text(encoding="utf-8").strip()
    if not wallet:
        raise SystemExit("Test wallet is empty")

    headers = {
        "X-API-KEY": key,
        "x-chain": "solana",
        "accept": "application/json",
        "content-type": "application/json",
    }

    summary_result = call(
        "GET",
        "/wallet/v2/pnl/summary",
        headers,
        params={
            "wallet": wallet,
            "duration": "all",
            "position_scope": "cumulative",
        },
    )

    # Birdeye's current official schema permits only "last_trade" for sort_by.
    details_result = call(
        "POST",
        "/wallet/v2/pnl/details",
        headers,
        json={
            "wallet": wallet,
            "duration": "all",
            "position_scope": "cumulative",
            "sort_type": "desc",
            "sort_by": "last_trade",
            "offset": 0,
            "limit": 100,
        },
    )

    results = [summary_result, details_result]
    out = Path("output-birdeye-access-test")
    out.mkdir(parents=True, exist_ok=True)

    raw = {
        "version": VERSION,
        "wallet": wallet,
        "provider_limit_note": (
            "Birdeye states that per-protocol trade data may not be fully backfilled. "
            "A 200 response proves endpoint access, not complete historical truth."
        ),
        "results": results,
    }
    (out / "birdeye_access_test_raw.json").write_text(
        json.dumps(raw, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )

    summary_status = int(summary_result.get("status_code") or 0)
    details_status = int(details_result.get("status_code") or 0)
    report = {
        "version": VERSION,
        "wallet": wallet,
        "summary_status": summary_status,
        "details_status": details_status,
        "summary_classification": classify(summary_status),
        "details_classification": classify(details_status),
        "summary_access": summary_status == 200,
        "details_access": details_status == 200,
        "test_passed": summary_status == 200 and details_status == 200,
        "safe_decision": (
            "compare_with_moralis_on_known_wallets"
            if summary_status == 200 and details_status == 200
            else "inspect_status_before_any_bulk_scan"
        ),
    }
    (out / "birdeye_access_test_report.json").write_text(
        json.dumps(report, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    print(json.dumps(report, indent=2, ensure_ascii=False))

    if any(int(r.get("status_code") or 0) >= 500 for r in results):
        raise SystemExit("Birdeye server error; inspect artifact and retry later.")


if __name__ == "__main__":
    main()
