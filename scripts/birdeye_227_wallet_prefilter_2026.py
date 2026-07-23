#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import os
import time
from pathlib import Path
from typing import Any

import requests

VERSION = "2026-07-24-birdeye-227-wallet-prefilter-v3"
BASE_URL = "https://public-api.birdeye.so"
DETAILS_ENDPOINT = "/wallet/v2/pnl/details"
CREDITS_ENDPOINT = "/utils/v1/credits"
DEFAULT_INPUT = Path("data/x_227_wallet_seed_2026.csv")
DEFAULT_OUTPUT = Path("output-birdeye-227-wallet-prefilter")

BASE_OR_STABLE_MINTS = {
    "So11111111111111111111111111111111111111112",
    "So11111111111111111111111111111111111111111",
    "11111111111111111111111111111111",
    "Es9vMFrzaCERmJfrF4H2FYDZRrCNF2no6YEeqvMZggq",
    "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",
}
TRANSIENT_STATUS = {429, 500, 502, 503, 504}
THRESHOLDS = (30_000.0, 50_000.0, 60_000.0, 75_000.0)


def fnum(value: Any) -> float | None:
    try:
        if value in (None, ""):
            return None
        number = float(value)
        return number if number == number else None
    except (TypeError, ValueError):
        return None


def retry_delay(response: requests.Response | None, attempt: int) -> int:
    if response is not None:
        raw = response.headers.get("Retry-After")
        if raw:
            try:
                return max(3, min(180, int(float(raw))))
            except ValueError:
                pass
    return [5, 15, 30, 60, 120][min(attempt - 1, 4)]


def request_json(
    session: requests.Session,
    method: str,
    endpoint: str,
    headers: dict[str, str],
    *,
    body: dict[str, Any] | None = None,
    max_attempts: int = 5,
) -> tuple[dict[str, Any], int]:
    last_status: int | None = None
    for attempt in range(1, max_attempts + 1):
        response: requests.Response | None = None
        try:
            response = session.request(
                method,
                BASE_URL + endpoint,
                headers=headers,
                json=body,
                timeout=90,
            )
            last_status = response.status_code
            try:
                payload = response.json()
            except ValueError:
                payload = {"raw_text": response.text[:5000]}

            if response.status_code == 200:
                if not isinstance(payload, dict):
                    raise RuntimeError("Response is not a JSON object")
                return payload, response.status_code

            if response.status_code not in TRANSIENT_STATUS:
                raise RuntimeError(
                    f"{endpoint}: HTTP {response.status_code}; "
                    f"{json.dumps(payload, ensure_ascii=False)[:1000]}"
                )
        except requests.RequestException as exc:
            if attempt == max_attempts:
                raise RuntimeError(f"{endpoint}: network error: {exc}") from exc

        if attempt < max_attempts:
            delay = retry_delay(response, attempt)
            print(
                f"{endpoint}: transient/rate-limit; waiting {delay}s "
                f"(attempt {attempt}/{max_attempts})",
                flush=True,
            )
            time.sleep(delay)

    raise RuntimeError(f"{endpoint}: retries exhausted; final_status={last_status}")


def credits_snapshot(
    session: requests.Session,
    api_key: str,
    output_path: Path,
) -> dict[str, Any]:
    headers = {
        "X-API-KEY": api_key,
        "accept": "application/json",
    }
    try:
        payload, status = request_json(
            session, "GET", CREDITS_ENDPOINT, headers, max_attempts=5
        )
        result = {"status": status, "payload": payload}
    except Exception as exc:
        result = {"status": None, "error": str(exc)}
    output_path.write_text(
        json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    return result


def read_wallet_seed(path: Path) -> tuple[list[dict[str, str]], dict[str, set[str]]]:
    with path.open(encoding="utf-8", newline="") as handle:
        rows = list(csv.DictReader(handle))
    if not rows:
        raise RuntimeError("Input seed is empty")

    required = {
        "wallet", "token_mint", "token_symbol",
        "strict_direct_swap_pass", "max_realized_lot_multiple",
    }
    missing = required - set(rows[0])
    if missing:
        raise RuntimeError(f"Input missing columns: {sorted(missing)}")

    wallet_rows: list[dict[str, str]] = []
    wallet_x_mints: dict[str, set[str]] = {}
    seen: set[str] = set()

    for row in rows:
        if str(row.get("strict_direct_swap_pass", "")).lower() != "true":
            continue
        wallet = str(row.get("wallet") or "").strip()
        mint = str(row.get("token_mint") or "").strip()
        if not wallet or not mint:
            continue
        wallet_x_mints.setdefault(wallet, set()).add(mint)
        if wallet not in seen:
            seen.add(wallet)
            wallet_rows.append(row)

    if len(wallet_rows) != 227:
        raise RuntimeError(f"Expected exactly 227 wallets, found {len(wallet_rows)}")
    return wallet_rows, wallet_x_mints


def request_details_page(
    session: requests.Session,
    api_key: str,
    wallet: str,
    offset: int,
    max_attempts: int,
) -> dict[str, Any]:
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
        "offset": offset,
        "limit": 100,
    }
    payload, _ = request_json(
        session,
        "POST",
        DETAILS_ENDPOINT,
        headers,
        body=body,
        max_attempts=max_attempts,
    )
    if payload.get("success") is not True:
        raise RuntimeError(
            f"{wallet} offset={offset}: HTTP 200 but success is not true"
        )
    return payload


def write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fields = sorted({key for row in rows for key in row})
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--version", action="store_true")
    parser.add_argument("--input", default=str(DEFAULT_INPUT))
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT))
    parser.add_argument("--request-spacing-seconds", type=float, default=3.0)
    parser.add_argument("--max-pages-per-wallet", type=int, default=20)
    parser.add_argument("--max-attempts", type=int, default=5)
    args = parser.parse_args()

    if args.version:
        print(VERSION)
        return 0

    api_key = os.environ.get("BIRDEYE_API_KEY", "").strip()
    if not api_key:
        raise SystemExit("BIRDEYE_API_KEY is missing")

    output_dir = Path(args.output_dir)
    raw_root = output_dir / "raw_birdeye"
    output_dir.mkdir(parents=True, exist_ok=True)
    raw_root.mkdir(parents=True, exist_ok=True)

    wallets, wallet_x_mints = read_wallet_seed(Path(args.input))
    session = requests.Session()

    credit_before = credits_snapshot(
        session, api_key, output_dir / "credits_before.json"
    )
    time.sleep(args.request_spacing_seconds)

    all_rows: list[dict[str, Any]] = []
    wallet_summary: list[dict[str, Any]] = []
    incomplete: list[dict[str, Any]] = []
    successful_details_calls = 0

    for index, seed_row in enumerate(wallets, start=1):
        wallet = seed_row["wallet"]
        x_mints = wallet_x_mints[wallet]
        current_rows: list[dict[str, Any]] = []
        status = "complete"
        error = ""
        page_count = 0

        print(f"[{index}/227] {wallet}", flush=True)

        try:
            for page_index in range(args.max_pages_per_wallet):
                offset = page_index * 100
                payload = request_details_page(
                    session, api_key, wallet, offset, args.max_attempts
                )
                successful_details_calls += 1
                page_count += 1

                raw_path = raw_root / wallet / f"offset_{offset:05d}.json"
                raw_path.parent.mkdir(parents=True, exist_ok=True)
                raw_path.write_text(
                    json.dumps(payload, ensure_ascii=False, indent=2),
                    encoding="utf-8",
                )

                tokens = (payload.get("data") or {}).get("tokens") or []
                if not isinstance(tokens, list):
                    raise RuntimeError("data.tokens is not a list")

                for item in tokens:
                    if not isinstance(item, dict):
                        continue
                    mint = str(item.get("address") or "").strip()
                    if not mint:
                        continue
                    pnl = item.get("pnl") or {}
                    realized = fnum(pnl.get("realized_profit_usd"))
                    is_x = mint in x_mints
                    is_base = mint in BASE_OR_STABLE_MINTS
                    eligible_y = not is_x and not is_base
                    row = {
                        "wallet": wallet,
                        "x_token_mints": "|".join(sorted(x_mints)),
                        "token_mint": mint,
                        "token_symbol": item.get("symbol", ""),
                        "is_x_token": is_x,
                        "is_base_or_stable": is_base,
                        "birdeye_realized_profit_usd": realized,
                        "candidate_30k": bool(
                            eligible_y and realized is not None and realized >= 30_000
                        ),
                        "candidate_50k": bool(
                            eligible_y and realized is not None and realized >= 50_000
                        ),
                        "candidate_60k": bool(
                            eligible_y and realized is not None and realized >= 60_000
                        ),
                        "candidate_75k": bool(
                            eligible_y and realized is not None and realized >= 75_000
                        ),
                        "final_verified": False,
                        "note": (
                            "Birdeye candidate only. Holding, unrealized PnL and "
                            "transfers are not selection criteria."
                        ),
                    }
                    current_rows.append(row)

                print(f"  offset={offset}; tokens={len(tokens)}", flush=True)
                if len(tokens) < 100:
                    break
                time.sleep(args.request_spacing_seconds)
            else:
                status = "incomplete_max_pages"
                error = "Maximum page limit reached without a short final page."
        except Exception as exc:
            status = "incomplete_error"
            error = str(exc)

        deduped: dict[str, dict[str, Any]] = {}
        for row in current_rows:
            deduped.setdefault(str(row["token_mint"]), row)
        current_rows = list(deduped.values())
        all_rows.extend(current_rows)

        summary_row = {
            "wallet": wallet,
            "x_token_mints": "|".join(sorted(x_mints)),
            "x_max_realized_lot_multiple_from_seed": seed_row.get(
                "max_realized_lot_multiple", ""
            ),
            "status": status,
            "page_count": page_count,
            "token_count": len(current_rows),
            "candidate_30k_count": sum(r["candidate_30k"] for r in current_rows),
            "candidate_50k_count": sum(r["candidate_50k"] for r in current_rows),
            "candidate_60k_count": sum(r["candidate_60k"] for r in current_rows),
            "candidate_75k_count": sum(r["candidate_75k"] for r in current_rows),
            "error": error,
        }
        wallet_summary.append(summary_row)
        if status != "complete":
            incomplete.append(summary_row)

        if index < len(wallets):
            time.sleep(args.request_spacing_seconds)

    time.sleep(args.request_spacing_seconds)
    credit_after = credits_snapshot(
        session, api_key, output_dir / "credits_after.json"
    )

    write_csv(output_dir / "all_token_pnl_rows.csv", all_rows)
    write_csv(output_dir / "wallet_scan_summary.csv", wallet_summary)
    write_csv(output_dir / "incomplete_wallets.csv", incomplete)

    for threshold in THRESHOLDS:
        key = f"candidate_{int(threshold/1000)}k"
        candidates = [row for row in all_rows if row[key]]
        candidates.sort(
            key=lambda row: fnum(row["birdeye_realized_profit_usd"]) or float("-inf"),
            reverse=True,
        )
        write_csv(
            output_dir / f"y_candidates_{int(threshold/1000)}k.csv",
            candidates,
        )

    report = {
        "version": VERSION,
        "input_wallet_count": len(wallets),
        "input_wallet_token_pair_count": sum(len(v) for v in wallet_x_mints.values()),
        "complete_wallet_count": sum(r["status"] == "complete" for r in wallet_summary),
        "incomplete_wallet_count": len(incomplete),
        "successful_details_calls": successful_details_calls,
        "details_compute_units_expected": successful_details_calls * 40,
        "credit_snapshot_calls_expected_cu": 2,
        "candidate_counts": {
            str(int(t)): sum(
                row[f"candidate_{int(t/1000)}k"] for row in all_rows
            )
            for t in THRESHOLDS
        },
        "credits_before_raw": credit_before,
        "credits_after_raw": credit_after,
        "complete": len(incomplete) == 0,
        "rules": {
            "holding_is_not_a_filter": True,
            "transfer_in_out_is_not_a_filter": True,
            "x_seed_rule": (
                "At least one proven sub-$50K market-cap buy allocation "
                "was sold at 25x or more."
            ),
            "y_is_different_token": True,
            "threshold_outputs_usd": list(THRESHOLDS),
        },
    }
    (output_dir / "birdeye_227_wallet_prefilter_report.json").write_text(
        json.dumps(report, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    print(json.dumps(report, ensure_ascii=False, indent=2), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
