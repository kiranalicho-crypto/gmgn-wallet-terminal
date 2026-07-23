#!/usr/bin/env python3
from __future__ import annotations

import json
import os
from pathlib import Path
import requests

VERSION = "2026-07-23-birdeye-access-test-v1"
BASE = "https://public-api.birdeye.so"


def call(method: str, path: str, headers: dict[str, str], **kwargs):
    response = requests.request(method, BASE + path, headers=headers, timeout=60, **kwargs)
    try:
        payload = response.json()
    except Exception:
        payload = {"raw_text": response.text[:5000]}
    return {
        "method": method,
        "path": path,
        "status_code": response.status_code,
        "ok": response.ok,
        "response": payload,
    }


def main() -> None:
    key = os.environ.get("BIRDEYE_API_KEY", "").strip()
    if not key:
        raise SystemExit("BIRDEYE_API_KEY is missing")

    wallet = Path("data/birdeye_test_wallet.txt").read_text(encoding="utf-8").strip()
    headers = {
        "X-API-KEY": key,
        "x-chain": "solana",
        "accept": "application/json",
        "content-type": "application/json",
    }

    results = []

    results.append(call(
        "GET",
        "/wallet/v2/pnl/summary",
        headers,
        params={
            "wallet": wallet,
            "duration": "all",
            "position_scope": "cumulative",
        },
    ))

    results.append(call(
        "POST",
        "/wallet/v2/pnl/details",
        headers,
        json={
            "wallet": wallet,
            "duration": "all",
            "position_scope": "cumulative",
            "sort_type": "desc",
            "sort_by": "realized_profit",
            "offset": 0,
            "limit": 100,
        },
    ))

    out = Path("output-birdeye-access-test")
    out.mkdir(parents=True, exist_ok=True)
    (out / "birdeye_access_test_raw.json").write_text(
        json.dumps({"version": VERSION, "wallet": wallet, "results": results}, indent=2),
        encoding="utf-8",
    )

    summary = {
        "version": VERSION,
        "wallet": wallet,
        "summary_status": results[0]["status_code"],
        "details_status": results[1]["status_code"],
        "summary_access": results[0]["status_code"] == 200,
        "details_access": results[1]["status_code"] == 200,
        "test_passed": all(r["status_code"] == 200 for r in results),
    }
    (out / "birdeye_access_test_report.json").write_text(
        json.dumps(summary, indent=2), encoding="utf-8"
    )
    print(json.dumps(summary, indent=2))

    # Do not fail the workflow on 401/403: upload the diagnostic artifact first.
    # Fail only on unexpected server/network-style responses.
    if any(r["status_code"] >= 500 for r in results):
        raise SystemExit("Birdeye server error; inspect artifact and retry later.")


if __name__ == "__main__":
    main()
