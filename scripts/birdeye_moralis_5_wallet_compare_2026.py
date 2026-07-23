#!/usr/bin/env python3
from __future__ import annotations

import csv
import gzip
import json
import math
import os
import time
from collections import defaultdict, deque
from pathlib import Path
from typing import Any

import requests

VERSION = "2026-07-23-birdeye-moralis-5-wallet-compare-v1"
BASE_URL = "https://public-api.birdeye.so"
DETAILS_ENDPOINT = "/wallet/v2/pnl/details"
INPUT_WALLETS = Path("data/birdeye_moralis_compare_wallets.csv")
EVENTS_DIR = Path("data/moralis_events")
OUTPUT_DIR = Path("output-birdeye-moralis-5-wallet-compare")
RAW_DIR = OUTPUT_DIR / "raw_birdeye"
BASE_OR_STABLE_MINTS = {
    "So11111111111111111111111111111111111111112",
    "11111111111111111111111111111111",
    "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",
    "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB",
}
EPSILON = 1e-12


def fnum(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        result = float(value)
    except (TypeError, ValueError):
        return None
    return result if math.isfinite(result) else None


def safe_rate_headers(headers: requests.structures.CaseInsensitiveDict[str]) -> dict[str, str]:
    markers = ("retry-after", "ratelimit", "rate-limit", "remaining", "reset", "request-id")
    return {
        str(key): str(value)
        for key, value in headers.items()
        if any(marker in str(key).lower() for marker in markers)
    }


def request_details_page(
    session: requests.Session,
    api_key: str,
    wallet: str,
    offset: int,
    max_attempts: int = 5,
) -> tuple[dict[str, Any], dict[str, str], int]:
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
    last_status = 0
    for attempt in range(1, max_attempts + 1):
        response = session.post(
            BASE_URL + DETAILS_ENDPOINT,
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
            if not isinstance(payload, dict) or payload.get("success") is not True:
                raise RuntimeError(
                    f"{wallet} offset={offset}: HTTP 200 but success is not true"
                )
            return payload, safe_rate_headers(response.headers), response.status_code

        if response.status_code != 429:
            raise RuntimeError(
                f"{wallet} offset={offset}: HTTP {response.status_code}; "
                f"body={json.dumps(payload, ensure_ascii=False)[:1000]}"
            )

        if attempt == max_attempts:
            break
        retry_after = response.headers.get("Retry-After")
        try:
            delay = int(float(retry_after)) if retry_after else [5, 15, 30, 60][attempt - 1]
        except ValueError:
            delay = [5, 15, 30, 60][attempt - 1]
        delay = max(3, min(180, delay))
        print(f"{wallet}: HTTP 429; waiting {delay}s", flush=True)
        time.sleep(delay)

    raise RuntimeError(f"{wallet} offset={offset}: still rate-limited after retries ({last_status})")


def fetch_birdeye_wallet(
    session: requests.Session,
    api_key: str,
    wallet: str,
    max_pages: int = 20,
) -> tuple[dict[str, dict[str, Any]], dict[str, Any]]:
    tokens_by_mint: dict[str, dict[str, Any]] = {}
    page_meta: list[dict[str, Any]] = []
    summary: dict[str, Any] = {}

    for page_index in range(max_pages):
        offset = page_index * 100
        payload, rate_headers, status = request_details_page(
            session, api_key, wallet, offset
        )
        raw_path = RAW_DIR / wallet / f"offset_{offset:05d}.json"
        raw_path.parent.mkdir(parents=True, exist_ok=True)
        raw_path.write_text(
            json.dumps(payload, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

        data = payload.get("data") or {}
        page_tokens = data.get("tokens") or []
        if not isinstance(page_tokens, list):
            raise RuntimeError(f"{wallet} offset={offset}: data.tokens is not a list")

        for item in page_tokens:
            if not isinstance(item, dict):
                continue
            mint = str(item.get("address") or "").strip()
            if mint:
                tokens_by_mint[mint] = item

        if page_index == 0 and isinstance(data.get("summary"), dict):
            summary = data["summary"]

        page_meta.append(
            {
                "offset": offset,
                "returned_tokens": len(page_tokens),
                "status": status,
                "safe_rate_headers": rate_headers,
            }
        )
        print(f"{wallet}: Birdeye offset={offset}, tokens={len(page_tokens)}", flush=True)

        if len(page_tokens) < 100:
            break

        # 3 seconds => at most 20 Wallet API calls per minute.
        time.sleep(3)
    else:
        raise RuntimeError(f"{wallet}: Birdeye pagination exceeded max_pages={max_pages}")

    return tokens_by_mint, {
        "page_count": len(page_meta),
        "token_count": len(tokens_by_mint),
        "pages": page_meta,
        "summary": summary,
    }


def read_moralis_events(wallet: str) -> list[dict[str, Any]]:
    path = EVENTS_DIR / f"{wallet}.csv.gz"
    if not path.is_file():
        raise RuntimeError(f"Missing Moralis event file: {path}")
    rows: list[dict[str, Any]] = []
    seen_hashes: set[str] = set()
    with gzip.open(path, "rt", encoding="utf-8", newline="") as handle:
        for row_index, row in enumerate(csv.DictReader(handle)):
            event_hash = str(row.get("event_sha256") or "")
            if event_hash and event_hash in seen_hashes:
                continue
            if event_hash:
                seen_hashes.add(event_hash)
            row["_source_row_index"] = row_index
            rows.append(row)
    return rows


def moralis_fifo(wallet: str) -> tuple[dict[str, dict[str, Any]], list[dict[str, Any]]]:
    rows = read_moralis_events(wallet)
    rows.sort(
        key=lambda row: (
            int(float(row.get("timestamp_epoch") or 0)),
            int(float(row.get("block_number") or 0)),
            int(float(row.get("transaction_index") or 0)),
            int(row["_source_row_index"]),
        )
    )

    multi_leg_counts: dict[tuple[str, str, str], int] = defaultdict(int)
    for row in rows:
        key = (
            str(row.get("transaction_hash") or ""),
            str(row.get("token_mint") or ""),
            str(row.get("event_type") or ""),
        )
        multi_leg_counts[key] += 1

    by_mint: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        mint = str(row.get("token_mint") or "").strip()
        if mint:
            by_mint[mint].append(row)

    results: dict[str, dict[str, Any]] = {}
    anomalies: list[dict[str, Any]] = []

    for mint, events in by_mint.items():
        lots: deque[list[float]] = deque()
        total_bought = total_buy_cost = 0.0
        total_sold = total_sell_revenue = 0.0
        matched_amount = matched_cost = matched_revenue = 0.0
        unmatched_sell = 0.0
        same_tx_multi_leg = 0

        for event in events:
            event_type = str(event.get("event_type") or "").lower()
            amount = fnum(event.get("token_amount")) or 0.0
            usd_amount = fnum(event.get("usd_amount")) or 0.0
            if amount <= EPSILON:
                continue

            key = (
                str(event.get("transaction_hash") or ""),
                mint,
                event_type,
            )
            if multi_leg_counts[key] > 1:
                same_tx_multi_leg += 1

            if event_type == "buy":
                unit_cost = usd_amount / amount if amount > EPSILON else 0.0
                lots.append([amount, unit_cost])
                total_bought += amount
                total_buy_cost += usd_amount
            elif event_type == "sell":
                total_sold += amount
                total_sell_revenue += usd_amount
                remaining = amount
                while remaining > EPSILON and lots:
                    lot_amount, unit_cost = lots[0]
                    consumed = min(remaining, lot_amount)
                    revenue_piece = usd_amount * (consumed / amount)
                    matched_amount += consumed
                    matched_cost += consumed * unit_cost
                    matched_revenue += revenue_piece
                    lot_amount -= consumed
                    remaining -= consumed
                    if lot_amount <= EPSILON:
                        lots.popleft()
                    else:
                        lots[0][0] = lot_amount
                if remaining > EPSILON:
                    unmatched_sell += remaining

        remaining_inventory = sum(lot[0] for lot in lots)
        realized_profit = matched_revenue - matched_cost

        results[mint] = {
            "moralis_total_bought_amount": total_bought,
            "moralis_total_buy_cost_usd": total_buy_cost,
            "moralis_total_sold_amount": total_sold,
            "moralis_total_sell_revenue_usd": total_sell_revenue,
            "moralis_fifo_matched_amount": matched_amount,
            "moralis_fifo_matched_cost_usd": matched_cost,
            "moralis_fifo_matched_revenue_usd": matched_revenue,
            "moralis_fifo_realized_profit_usd": realized_profit,
            "moralis_fifo_remaining_inventory": remaining_inventory,
            "moralis_unmatched_sell_amount": unmatched_sell,
            "moralis_same_tx_multi_leg_event_count": same_tx_multi_leg,
            "moralis_event_count": len(events),
        }
        if same_tx_multi_leg:
            anomalies.append(
                {
                    "wallet": wallet,
                    "token_mint": mint,
                    "anomaly": "same_tx_same_token_same_side_multiple_events",
                    "event_count": same_tx_multi_leg,
                    "note": (
                        "May be legitimate multi-leg routing or parser double counting; "
                        "must be checked against wallet net balance changes."
                    ),
                }
            )
        if unmatched_sell > EPSILON:
            anomalies.append(
                {
                    "wallet": wallet,
                    "token_mint": mint,
                    "anomaly": "sell_exceeds_observed_buy_inventory",
                    "event_count": "",
                    "note": f"unmatched_sell_amount={unmatched_sell}",
                }
            )

    return results, anomalies


def birdeye_metrics(item: dict[str, Any]) -> dict[str, Any]:
    quantity = item.get("quantity") or {}
    cashflow = item.get("cashflow_usd") or {}
    pnl = item.get("pnl") or {}
    pricing = item.get("pricing") or {}
    bought = fnum(quantity.get("total_bought_amount"))
    sold = fnum(quantity.get("total_sold_amount"))
    holding = fnum(quantity.get("holding"))
    current_price = fnum(pricing.get("current_price"))
    current_value = fnum(cashflow.get("current_value"))
    expected_current_value = (
        holding * current_price
        if holding is not None and current_price is not None
        else None
    )
    quantity_delta = (
        bought - sold - holding
        if bought is not None and sold is not None and holding is not None
        else None
    )
    quantity_scale = max(abs(bought or 0), abs(sold or 0), abs(holding or 0), EPSILON)
    delta_ratio = abs(quantity_delta) / quantity_scale if quantity_delta is not None else None
    current_value_diff = (
        current_value - expected_current_value
        if current_value is not None and expected_current_value is not None
        else None
    )
    return {
        "birdeye_symbol": item.get("symbol") or "",
        "birdeye_total_bought_amount": bought,
        "birdeye_total_sold_amount": sold,
        "birdeye_holding_amount": holding,
        "birdeye_quantity_reconciliation_delta": quantity_delta,
        "birdeye_quantity_delta_ratio": delta_ratio,
        "birdeye_total_invested_usd": fnum(cashflow.get("total_invested")),
        "birdeye_total_sold_usd": fnum(cashflow.get("total_sold")),
        "birdeye_cost_of_quantity_sold_usd": fnum(cashflow.get("cost_of_quantity_sold")),
        "birdeye_current_value_usd": current_value,
        "birdeye_expected_current_value_usd": expected_current_value,
        "birdeye_current_value_diff_usd": current_value_diff,
        "birdeye_realized_profit_usd": fnum(pnl.get("realized_profit_usd")),
        "birdeye_unrealized_usd": fnum(pnl.get("unrealized_usd")),
        "birdeye_total_pnl_usd": fnum(pnl.get("total_usd")),
        "birdeye_current_price_usd": current_price,
        "birdeye_last_trade_unix_time": item.get("last_trade_unix_time") or "",
    }


def write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fields: list[str] = []
    for row in rows:
        for key in row:
            if key not in fields:
                fields.append(key)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def main() -> int:
    api_key = os.environ.get("BIRDEYE_API_KEY", "").strip()
    if not api_key:
        raise SystemExit("BIRDEYE_API_KEY is missing")
    if not INPUT_WALLETS.is_file():
        raise SystemExit(f"Missing {INPUT_WALLETS}")

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    wallet_inputs = list(csv.DictReader(INPUT_WALLETS.open(encoding="utf-8", newline="")))
    session = requests.Session()

    comparison_rows: list[dict[str, Any]] = []
    anomaly_rows: list[dict[str, Any]] = []
    wallet_reports: list[dict[str, Any]] = []

    for wallet_index, wallet_row in enumerate(wallet_inputs):
        wallet = wallet_row["wallet"]
        x_mint = wallet_row["x_token_mint"]
        print(f"Processing {wallet} ({wallet_index + 1}/{len(wallet_inputs)})", flush=True)

        birdeye_tokens, birdeye_report = fetch_birdeye_wallet(session, api_key, wallet)
        moralis_tokens, moralis_anomalies = moralis_fifo(wallet)
        anomaly_rows.extend(moralis_anomalies)

        all_mints = sorted(set(birdeye_tokens) | set(moralis_tokens))
        wallet_disagreement_count = 0
        wallet_transfer_flag_count = 0

        for mint in all_mints:
            bmetrics = birdeye_metrics(birdeye_tokens[mint]) if mint in birdeye_tokens else {}
            mmetrics = moralis_tokens.get(mint, {})
            bprofit = fnum(bmetrics.get("birdeye_realized_profit_usd"))
            mprofit = fnum(mmetrics.get("moralis_fifo_realized_profit_usd"))
            diff = (
                bprofit - mprofit
                if bprofit is not None and mprofit is not None
                else None
            )
            denom = max(abs(bprofit or 0), abs(mprofit or 0), 1.0)
            diff_ratio = abs(diff) / denom if diff is not None else None
            transfer_flag = (
                (fnum(bmetrics.get("birdeye_quantity_delta_ratio")) or 0) > 0.01
            )
            disagreement = diff_ratio is not None and diff_ratio > 0.10
            wallet_disagreement_count += int(disagreement)
            wallet_transfer_flag_count += int(transfer_flag)

            comparison_rows.append(
                {
                    "wallet": wallet,
                    "x_token_mint": x_mint,
                    "x_token_symbol": wallet_row.get("x_token_symbol", ""),
                    "token_mint": mint,
                    "is_x_token": mint == x_mint,
                    "is_base_or_stable": mint in BASE_OR_STABLE_MINTS,
                    **bmetrics,
                    **mmetrics,
                    "realized_profit_difference_usd": diff,
                    "realized_profit_difference_ratio": diff_ratio,
                    "provider_realized_disagreement_gt_10pct": disagreement,
                    "birdeye_transfer_or_missing_history_flag": transfer_flag,
                    "birdeye_y_50k_prefilter": (
                        bprofit is not None
                        and bprofit >= 50_000
                        and mint != x_mint
                        and mint not in BASE_OR_STABLE_MINTS
                    ),
                    "moralis_y_50k_prefilter": (
                        mprofit is not None
                        and mprofit >= 50_000
                        and mint != x_mint
                        and mint not in BASE_OR_STABLE_MINTS
                    ),
                    "final_y_eligible": False,
                    "final_y_eligible_reason": (
                        "Comparison only; transfer-aware on-chain ledger not yet applied"
                    ),
                }
            )

        wallet_reports.append(
            {
                "wallet": wallet,
                "x_token_mint": x_mint,
                "birdeye_token_count": birdeye_report["token_count"],
                "birdeye_page_count": birdeye_report["page_count"],
                "moralis_token_count": len(moralis_tokens),
                "provider_disagreement_token_count": wallet_disagreement_count,
                "birdeye_transfer_or_missing_history_token_count": wallet_transfer_flag_count,
                "moralis_anomaly_count": sum(
                    1 for row in moralis_anomalies if row["wallet"] == wallet
                ),
                "safe_result": "comparison_only_not_final_pnl",
            }
        )

        if wallet_index + 1 < len(wallet_inputs):
            time.sleep(3)

    write_csv(OUTPUT_DIR / "token_level_comparison.csv", comparison_rows)
    write_csv(OUTPUT_DIR / "wallet_comparison_summary.csv", wallet_reports)
    write_csv(OUTPUT_DIR / "anomalies.csv", anomaly_rows)

    report = {
        "version": VERSION,
        "wallet_count": len(wallet_inputs),
        "token_comparison_row_count": len(comparison_rows),
        "wallet_reports": wallet_reports,
        "critical_rules": {
            "birdeye_200_proves_access_not_historical_completeness": True,
            "moralis_swap_events_are_not_transfer_aware": True,
            "provider_pnl_is_not_final_evidence": True,
            "x_token_excluded_from_y": True,
            "base_and_stable_assets_excluded_from_y": True,
            "final_y_requires_transfer_aware_on_chain_ledger": True,
        },
        "decision_gate": {
            "bulk_birdeye_scan_allowed": False,
            "reason": (
                "Review token-level disagreements, Birdeye quantity reconciliation flags, "
                "and Moralis multi-leg anomalies first."
            ),
        },
    }
    (OUTPUT_DIR / "comparison_report.json").write_text(
        json.dumps(report, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    print(json.dumps(report, ensure_ascii=False, indent=2), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
