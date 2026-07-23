#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import os
import sys
import time
from pathlib import Path
from typing import Any

import requests

VERSION = "2026-07-23-birdeye-83-wallet-prefilter-v2"
BASE_URL = "https://public-api.birdeye.so"
ENDPOINT = "/wallet/v2/pnl/details"
DEFAULT_INPUT = Path("data/x_corrected_provisional_seed_v2.csv")
DEFAULT_OUTPUT = Path("output-birdeye-83-wallet-prefilter")

BASE_OR_STABLE_MINTS = {
    "So11111111111111111111111111111111111111112",  # wrapped SOL
    "So11111111111111111111111111111111111111111",
    "11111111111111111111111111111111",
    "Es9vMFrzaCERmJfrF4H2FYDZRrCNF2no6YEeqvMZggq",  # USDT
    "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",  # USDC
}

TRANSIENT_STATUS = {429, 500, 502, 503, 504}


def fnum(value: Any) -> float | None:
    try:
        if value in (None, ""):
            return None
        result = float(value)
        return result if result == result else None
    except (TypeError, ValueError):
        return None


def safe_headers(headers: requests.structures.CaseInsensitiveDict[str]) -> dict[str, str]:
    markers = (
        "retry-after", "ratelimit", "rate-limit", "remaining",
        "reset", "request-id", "compute",
    )
    return {
        str(k): str(v)
        for k, v in headers.items()
        if any(marker in str(k).lower() for marker in markers)
    }


def retry_delay(response: requests.Response | None, attempt: int) -> int:
    if response is not None:
        raw = response.headers.get("Retry-After")
        if raw:
            try:
                return max(3, min(180, int(float(raw))))
            except ValueError:
                pass
    return [5, 15, 30, 60, 120][min(attempt - 1, 4)]


def request_page(
    session: requests.Session,
    api_key: str,
    wallet: str,
    offset: int,
    max_attempts: int,
) -> tuple[dict[str, Any], dict[str, str]]:
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

    last_response: requests.Response | None = None
    for attempt in range(1, max_attempts + 1):
        try:
            response = session.post(
                BASE_URL + ENDPOINT,
                headers=headers,
                json=body,
                timeout=90,
            )
            last_response = response
            try:
                payload = response.json()
            except ValueError:
                payload = {"raw_text": response.text[:5000]}

            if response.status_code == 200:
                if not isinstance(payload, dict) or payload.get("success") is not True:
                    raise RuntimeError(
                        f"{wallet} offset={offset}: HTTP 200 but success is not true"
                    )
                return payload, safe_headers(response.headers)

            if response.status_code not in TRANSIENT_STATUS:
                raise RuntimeError(
                    f"{wallet} offset={offset}: HTTP {response.status_code}; "
                    f"body={json.dumps(payload, ensure_ascii=False)[:1000]}"
                )

        except requests.RequestException as exc:
            if attempt == max_attempts:
                raise RuntimeError(
                    f"{wallet} offset={offset}: network error: {exc}"
                ) from exc

        if attempt < max_attempts:
            delay = retry_delay(last_response, attempt)
            print(
                f"{wallet} offset={offset}: transient/rate-limit; "
                f"waiting {delay}s (attempt {attempt}/{max_attempts})",
                flush=True,
            )
            time.sleep(delay)

    status = last_response.status_code if last_response is not None else None
    raise RuntimeError(
        f"{wallet} offset={offset}: retries exhausted; final_status={status}"
    )


def read_wallets(path: Path) -> list[dict[str, str]]:
    with path.open(encoding="utf-8", newline="") as handle:
        rows = list(csv.DictReader(handle))
    required = {"wallet", "x_token_mint", "provisional_x_v2_pass"}
    missing = required - set(rows[0].keys() if rows else [])
    if missing:
        raise RuntimeError(f"Input missing columns: {sorted(missing)}")

    selected: list[dict[str, str]] = []
    seen: set[str] = set()
    for row in rows:
        if str(row.get("provisional_x_v2_pass", "")).lower() != "true":
            continue
        wallet = str(row.get("wallet") or "").strip()
        x_mint = str(row.get("x_token_mint") or "").strip()
        if not wallet or not x_mint or wallet in seen:
            continue
        seen.add(wallet)
        selected.append(row)

    if len(selected) != 83:
        raise RuntimeError(
            f"Expected exactly 83 provisional X wallets, found {len(selected)}"
        )
    return selected


def write_csv(path: Path, rows: list[dict[str, Any]], fields: list[str] | None = None) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if fields is None:
        fields = sorted({key for row in rows for key in row})
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def token_row(
    wallet_row: dict[str, str],
    item: dict[str, Any],
    safety_threshold: float,
    strict_threshold: float,
) -> dict[str, Any]:
    wallet = wallet_row["wallet"]
    x_mint = wallet_row["x_token_mint"]
    mint = str(item.get("address") or "").strip()
    quantity = item.get("quantity") or {}
    cashflow = item.get("cashflow_usd") or {}
    pnl = item.get("pnl") or {}
    pricing = item.get("pricing") or {}
    realized = fnum(pnl.get("realized_profit_usd"))
    is_x = mint == x_mint
    is_base = mint in BASE_OR_STABLE_MINTS
    eligible_y = not is_x and not is_base

    return {
        "wallet": wallet,
        "x_token_mint": x_mint,
        "x_token_symbol": wallet_row.get("x_token_symbol", ""),
        "token_mint": mint,
        "token_symbol": item.get("symbol", ""),
        "is_x_token": is_x,
        "is_base_or_stable": is_base,
        "trade_count": (item.get("counts") or {}).get("total_trade", ""),
        "buy_count": (item.get("counts") or {}).get("total_buy", ""),
        "sell_count": (item.get("counts") or {}).get("total_sell", ""),
        "total_bought_amount": quantity.get("total_bought_amount", ""),
        "total_sold_amount": quantity.get("total_sold_amount", ""),
        # IMPORTANT: the PnL response can state holding_check=false.
        # Therefore holding is retained only as provider output and is never
        # used to infer transfers or historical completeness.
        "provider_holding_amount_unverified": quantity.get("holding", ""),
        "total_invested_usd": cashflow.get("total_invested", ""),
        "total_sold_usd": cashflow.get("total_sold", ""),
        "cost_of_quantity_sold_usd": cashflow.get("cost_of_quantity_sold", ""),
        "provider_current_value_usd_unverified": cashflow.get("current_value", ""),
        "birdeye_realized_profit_usd": realized,
        "birdeye_unrealized_usd_unverified": pnl.get("unrealized_usd", ""),
        "birdeye_total_pnl_usd_unverified": pnl.get("total_usd", ""),
        "current_price_usd": pricing.get("current_price", ""),
        "last_trade_unix_time": item.get("last_trade_unix_time", ""),
        "y_safety_prefilter_30k": (
            eligible_y and realized is not None and realized >= safety_threshold
        ),
        "y_strict_prefilter_50k": (
            eligible_y and realized is not None and realized >= strict_threshold
        ),
        "final_y_eligible": False,
        "final_y_reason": (
            "Birdeye is candidate generation only; provider protocol history "
            "may not be fully backfilled and transfer-aware on-chain FIFO "
            "verification has not been applied."
        ),
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--version", action="store_true")
    parser.add_argument("--input", default=str(DEFAULT_INPUT))
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT))
    parser.add_argument("--safety-threshold-usd", type=float, default=30_000)
    parser.add_argument("--strict-threshold-usd", type=float, default=50_000)
    parser.add_argument("--max-pages-per-wallet", type=int, default=20)
    parser.add_argument("--max-attempts", type=int, default=5)
    parser.add_argument("--request-spacing-seconds", type=float, default=3.0)
    args = parser.parse_args()

    if args.version:
        print(VERSION)
        return 0
    if args.safety_threshold_usd >= args.strict_threshold_usd:
        raise SystemExit("Safety threshold must be below strict threshold.")
    if args.max_pages_per_wallet < 1:
        raise SystemExit("max-pages-per-wallet must be >= 1")

    api_key = os.environ.get("BIRDEYE_API_KEY", "").strip()
    if not api_key:
        raise SystemExit("BIRDEYE_API_KEY is missing")

    input_path = Path(args.input)
    output_dir = Path(args.output_dir)
    raw_root = output_dir / "raw_birdeye"
    output_dir.mkdir(parents=True, exist_ok=True)
    raw_root.mkdir(parents=True, exist_ok=True)

    wallets = read_wallets(input_path)
    session = requests.Session()

    token_rows: list[dict[str, Any]] = []
    wallet_rows: list[dict[str, Any]] = []
    incomplete_rows: list[dict[str, Any]] = []

    for wallet_index, wallet_row in enumerate(wallets, start=1):
        wallet = wallet_row["wallet"]
        wallet_token_rows: list[dict[str, Any]] = []
        page_count = 0
        status = "complete"
        error = ""

        print(f"[{wallet_index}/{len(wallets)}] {wallet}", flush=True)
        try:
            for page_index in range(args.max_pages_per_wallet):
                offset = page_index * 100
                payload, rate_headers = request_page(
                    session,
                    api_key,
                    wallet,
                    offset,
                    args.max_attempts,
                )
                page_count += 1
                raw_path = raw_root / wallet / f"offset_{offset:05d}.json"
                raw_path.parent.mkdir(parents=True, exist_ok=True)
                raw_path.write_text(
                    json.dumps(payload, ensure_ascii=False, indent=2),
                    encoding="utf-8",
                )

                data = payload.get("data") or {}
                tokens = data.get("tokens") or []
                if not isinstance(tokens, list):
                    raise RuntimeError("data.tokens is not a list")

                for item in tokens:
                    if not isinstance(item, dict):
                        continue
                    mint = str(item.get("address") or "").strip()
                    if not mint:
                        continue
                    wallet_token_rows.append(
                        token_row(
                            wallet_row,
                            item,
                            args.safety_threshold_usd,
                            args.strict_threshold_usd,
                        )
                    )

                print(
                    f"  offset={offset}; tokens={len(tokens)}; "
                    f"rate={rate_headers}",
                    flush=True,
                )

                if len(tokens) < 100:
                    break
                time.sleep(args.request_spacing_seconds)
            else:
                status = "incomplete_max_pages"
                error = (
                    f"Reached max_pages_per_wallet={args.max_pages_per_wallet} "
                    "without a short final page."
                )
        except Exception as exc:
            status = "incomplete_error"
            error = str(exc)

        # Deduplicate by mint, retaining the latest-trade sorted first occurrence.
        deduped: dict[str, dict[str, Any]] = {}
        for row in wallet_token_rows:
            mint = str(row["token_mint"])
            if mint and mint not in deduped:
                deduped[mint] = row
        wallet_token_rows = list(deduped.values())
        token_rows.extend(wallet_token_rows)

        safety_count = sum(bool(row["y_safety_prefilter_30k"]) for row in wallet_token_rows)
        strict_count = sum(bool(row["y_strict_prefilter_50k"]) for row in wallet_token_rows)
        max_y = max(
            (
                float(row["birdeye_realized_profit_usd"])
                for row in wallet_token_rows
                if not row["is_x_token"]
                and not row["is_base_or_stable"]
                and row["birdeye_realized_profit_usd"] is not None
            ),
            default=None,
        )
        wallet_summary = {
            "wallet": wallet,
            "x_token_mint": wallet_row["x_token_mint"],
            "x_token_symbol": wallet_row.get("x_token_symbol", ""),
            "status": status,
            "page_count": page_count,
            "token_count": len(wallet_token_rows),
            "maximum_non_x_non_base_realized_profit_usd": max_y,
            "safety_30k_candidate_count": safety_count,
            "strict_50k_candidate_count": strict_count,
            "error": error,
        }
        wallet_rows.append(wallet_summary)
        if status != "complete":
            incomplete_rows.append(wallet_summary)

        if wallet_index < len(wallets):
            time.sleep(args.request_spacing_seconds)

    safety_candidates = [
        row for row in token_rows if row["y_safety_prefilter_30k"]
    ]
    strict_candidates = [
        row for row in token_rows if row["y_strict_prefilter_50k"]
    ]

    write_csv(output_dir / "all_token_pnl_rows.csv", token_rows)
    write_csv(output_dir / "wallet_scan_summary.csv", wallet_rows)
    write_csv(output_dir / "y_safety_candidates_30k.csv", safety_candidates)
    write_csv(output_dir / "y_strict_candidates_50k.csv", strict_candidates)
    write_csv(output_dir / "incomplete_wallets.csv", incomplete_rows)

    report = {
        "version": VERSION,
        "input_wallet_count": len(wallets),
        "complete_wallet_count": sum(row["status"] == "complete" for row in wallet_rows),
        "incomplete_wallet_count": len(incomplete_rows),
        "token_row_count": len(token_rows),
        "safety_candidate_30k_count": len(safety_candidates),
        "strict_candidate_50k_count": len(strict_candidates),
        "thresholds": {
            "safety_usd": args.safety_threshold_usd,
            "strict_usd": args.strict_threshold_usd,
        },
        "provider_limits": {
            "birdeye_is_candidate_generator_only": True,
            "protocol_trade_history_may_not_be_fully_backfilled": True,
            "holding_field_not_used_for_transfer_inference": True,
            "unrealized_and_current_value_not_used_for_y": True,
        },
        "decision": (
            "Send every 30k+ token to transfer-aware on-chain ledger verification. "
            "A 50k+ Birdeye result is not a final Y pass. Wallets below 30k are "
            "not final negatives until a second independent history source or "
            "on-chain coverage check is completed."
        ),
        "complete": len(incomplete_rows) == 0,
    }
    (output_dir / "birdeye_83_wallet_prefilter_report.json").write_text(
        json.dumps(report, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    print(json.dumps(report, ensure_ascii=False, indent=2), flush=True)

    # Artifact must be uploaded even when incomplete; workflow gate runs afterward.
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
