#!/usr/bin/env python3
"""Moralis-based direct X swap verification and cluster-trace seed generation.

This stage verifies actual wallet swaps for each wallet-token pair. It does not
pretend token transfers are sales. Every pair is retained as a later cluster
trace seed so A->B->C flows can be examined in the next stage.
"""
from __future__ import annotations

import argparse
import csv
import gzip
import hashlib
import json
import math
import os
import random
import sys
import time
from collections import deque
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Optional

import pandas as pd
import requests

VERSION = "2026-07-21-x-chain-verify-v1"
API_BASE = "https://solana-gateway.moralis.io/account/mainnet"
PAIR_KEY = ["token_mint", "wallet"]


def finite_float(value: Any) -> Optional[float]:
    try:
        if value is None or value == "":
            return None
        v = float(value)
        return v if math.isfinite(v) else None
    except (TypeError, ValueError):
        return None


def finite_int(value: Any) -> Optional[int]:
    v = finite_float(value)
    return int(v) if v is not None else None


def parse_ts(value: Any) -> Optional[datetime]:
    if value is None or value == "":
        return None
    if isinstance(value, (int, float)):
        try:
            return datetime.fromtimestamp(float(value), tz=timezone.utc)
        except (ValueError, OSError, OverflowError):
            return None
    s = str(value).strip()
    if not s:
        return None
    if s.isdigit():
        try:
            return datetime.fromtimestamp(int(s), tz=timezone.utc)
        except (ValueError, OSError, OverflowError):
            return None
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00")).astimezone(timezone.utc)
    except ValueError:
        return None


def iso(dt: Optional[datetime]) -> str:
    return dt.isoformat().replace("+00:00", "Z") if dt else ""


def truthy(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if value is None or (isinstance(value, float) and math.isnan(value)):
        return False
    return str(value).strip().lower() in {"1", "true", "yes", "y"}


class RateLimiter:
    def __init__(self, requests_per_second: float) -> None:
        self.interval = 1.0 / max(requests_per_second, 0.01)
        self.last = 0.0

    def wait(self) -> None:
        now = time.monotonic()
        sleep_for = self.interval - (now - self.last)
        if sleep_for > 0:
            time.sleep(sleep_for)
        self.last = time.monotonic()


class MoralisClient:
    def __init__(self, api_key: str, rps: float, timeout: int = 45, retries: int = 6) -> None:
        self.session = requests.Session()
        self.session.headers.update({"X-API-Key": api_key, "Accept": "application/json"})
        self.limiter = RateLimiter(rps)
        self.timeout = timeout
        self.retries = retries

    def get(self, url: str, params: dict[str, Any]) -> tuple[Optional[dict[str, Any]], dict[str, Any]]:
        last_error = ""
        for attempt in range(self.retries + 1):
            self.limiter.wait()
            started = time.monotonic()
            try:
                response = self.session.get(url, params=params, timeout=self.timeout)
                elapsed_ms = round((time.monotonic() - started) * 1000)
                meta = {
                    "status_code": response.status_code,
                    "elapsed_ms": elapsed_ms,
                    "attempt": attempt + 1,
                    "url": response.url,
                }
                if response.status_code == 200:
                    try:
                        return response.json(), meta
                    except ValueError as exc:
                        last_error = f"invalid_json:{exc}"
                elif response.status_code in {408, 425, 429, 500, 502, 503, 504}:
                    last_error = f"http_{response.status_code}:{response.text[:300]}"
                else:
                    meta["error"] = response.text[:1000]
                    return None, meta
            except requests.RequestException as exc:
                last_error = f"request_error:{type(exc).__name__}:{exc}"
                meta = {"status_code": None, "attempt": attempt + 1, "error": last_error, "url": url}
            if attempt < self.retries:
                time.sleep(min(60.0, (2 ** attempt) + random.random()))
        meta["error"] = last_error
        return None, meta

    def wallet_token_swaps(self, wallet: str, mint: str, max_pages: int) -> tuple[list[dict[str, Any]], list[dict[str, Any]], str]:
        url = f"{API_BASE}/{wallet}/swaps"
        cursor: Optional[str] = None
        items: list[dict[str, Any]] = []
        pages: list[dict[str, Any]] = []
        status = "ok"
        seen_cursors: set[str] = set()
        for page_no in range(1, max_pages + 1):
            params: dict[str, Any] = {
                "limit": 100,
                "order": "ASC",
                "transactionTypes": "buy,sell",
                "tokenAddress": mint,
            }
            if cursor:
                params["cursor"] = cursor
            payload, meta = self.get(url, params)
            page_record = {"page_no": page_no, "request_meta": meta, "payload": payload}
            pages.append(page_record)
            if payload is None:
                status = "api_error"
                break
            result = payload.get("result")
            if not isinstance(result, list):
                status = "invalid_response"
                break
            items.extend(x for x in result if isinstance(x, dict))
            next_cursor = payload.get("cursor")
            if not next_cursor:
                break
            next_cursor = str(next_cursor)
            if next_cursor in seen_cursors:
                status = "cursor_loop"
                break
            seen_cursors.add(next_cursor)
            cursor = next_cursor
        else:
            if cursor:
                status = "max_pages_reached"
        return items, pages, status


def token_obj(item: dict[str, Any], side: str) -> dict[str, Any]:
    obj = item.get(side)
    return obj if isinstance(obj, dict) else {}


def extract_event(item: dict[str, Any], mint: str, wallet: str) -> Optional[dict[str, Any]]:
    bought = token_obj(item, "bought")
    sold = token_obj(item, "sold")
    bought_addr = str(bought.get("address") or "")
    sold_addr = str(sold.get("address") or "")
    if bought_addr == mint and sold_addr != mint:
        event_type, target = "buy", bought
    elif sold_addr == mint and bought_addr != mint:
        event_type, target = "sell", sold
    else:
        tx_type = str(item.get("transactionType") or "").lower()
        if tx_type == "buy" and bought_addr == mint:
            event_type, target = "buy", bought
        elif tx_type == "sell" and sold_addr == mint:
            event_type, target = "sell", sold
        else:
            return None

    amount = finite_float(target.get("amount"))
    usd_price = finite_float(target.get("usdPrice"))
    usd_amount = finite_float(target.get("usdAmount"))
    if usd_amount is not None:
        usd_amount = abs(usd_amount)
    total_value = finite_float(item.get("totalValueUsd"))
    if (usd_amount is None or usd_amount == 0) and total_value is not None:
        usd_amount = abs(total_value)
    if (usd_price is None or usd_price <= 0) and amount and usd_amount is not None:
        usd_price = usd_amount / amount
    if (usd_amount is None or usd_amount == 0) and amount and usd_price is not None:
        usd_amount = amount * usd_price

    dt = parse_ts(item.get("blockTimestamp"))
    tx_hash = str(item.get("transactionHash") or item.get("signature") or "")
    tx_index = item.get("transactionIndex")
    dedupe_raw = f"{tx_hash}|{tx_index}|{event_type}|{amount}|{usd_amount}|{wallet}|{mint}"
    return {
        "token_mint": mint,
        "wallet": wallet,
        "event_type": event_type,
        "block_timestamp": iso(dt),
        "timestamp_epoch": int(dt.timestamp()) if dt else None,
        "block_number": finite_int(item.get("blockNumber")),
        "transaction_hash": tx_hash,
        "transaction_index": tx_index,
        "amount": amount,
        "usd_price": usd_price,
        "usd_amount": usd_amount,
        "exchange_name": item.get("exchangeName"),
        "pair_address": item.get("pairAddress"),
        "pair_label": item.get("pairLabel"),
        "sub_category": item.get("subCategory"),
        "event_sha256": hashlib.sha256(dedupe_raw.encode()).hexdigest(),
    }


def sort_events(events: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    deduped: dict[str, dict[str, Any]] = {}
    for event in events:
        deduped[event["event_sha256"]] = event
    return sorted(
        deduped.values(),
        key=lambda e: (
            e.get("timestamp_epoch") if e.get("timestamp_epoch") is not None else 2**63 - 1,
            e.get("block_number") if e.get("block_number") is not None else 2**63 - 1,
            str(e.get("transaction_hash") or ""),
            str(e.get("transaction_index") or ""),
        ),
    )


def fifo_verify(events: list[dict[str, Any]], supply: float, entry_cap: float, multiple_min: float, min_sale_usd: float) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    lots: deque[dict[str, Any]] = deque()
    allocations: list[dict[str, Any]] = []
    buys = [e for e in events if e["event_type"] == "buy"]
    sells = [e for e in events if e["event_type"] == "sell"]
    first_buy = buys[0] if buys else None
    first_buy_mc = (first_buy.get("usd_price") or 0) * supply if first_buy and first_buy.get("usd_price") else None
    min_buy_mc: Optional[float] = None
    eligible_buy_count = 0
    unmatched_sell_amount = 0.0

    for event in events:
        amount = finite_float(event.get("amount"))
        price = finite_float(event.get("usd_price"))
        if amount is None or amount <= 0:
            continue
        if event["event_type"] == "buy":
            buy_mc = price * supply if price is not None and price > 0 else None
            if buy_mc is not None:
                min_buy_mc = buy_mc if min_buy_mc is None else min(min_buy_mc, buy_mc)
                if buy_mc < entry_cap:
                    eligible_buy_count += 1
            lots.append({
                "remaining": amount,
                "buy_amount": amount,
                "buy_price": price,
                "buy_usd_amount": finite_float(event.get("usd_amount")),
                "buy_market_cap_usd": buy_mc,
                "buy_timestamp": event.get("block_timestamp"),
                "buy_transaction_hash": event.get("transaction_hash"),
            })
            continue

        remaining_sell = amount
        while remaining_sell > 1e-18 and lots:
            lot = lots[0]
            qty = min(remaining_sell, lot["remaining"])
            buy_price = finite_float(lot.get("buy_price"))
            sell_price = price
            buy_cost = qty * buy_price if buy_price is not None else None
            sale_income = qty * sell_price if sell_price is not None else None
            mult = sell_price / buy_price if buy_price and sell_price is not None else None
            buy_mc = finite_float(lot.get("buy_market_cap_usd"))
            eligible = buy_mc is not None and buy_mc < entry_cap
            passes = bool(
                eligible
                and mult is not None
                and mult >= multiple_min
                and sale_income is not None
                and sale_income >= min_sale_usd
            )
            allocations.append({
                "token_mint": event["token_mint"],
                "wallet": event["wallet"],
                "buy_transaction_hash": lot.get("buy_transaction_hash"),
                "buy_timestamp": lot.get("buy_timestamp"),
                "buy_market_cap_usd": buy_mc,
                "buy_price_usd": buy_price,
                "sell_transaction_hash": event.get("transaction_hash"),
                "sell_timestamp": event.get("block_timestamp"),
                "sell_price_usd": sell_price,
                "matched_token_amount": qty,
                "matched_buy_cost_usd": buy_cost,
                "matched_sale_income_usd": sale_income,
                "matched_realized_profit_usd": (sale_income - buy_cost) if sale_income is not None and buy_cost is not None else None,
                "realized_multiple": mult,
                "eligible_sub_50k_lot": eligible,
                "allocation_pass": passes,
            })
            lot["remaining"] -= qty
            remaining_sell -= qty
            if lot["remaining"] <= 1e-18:
                lots.popleft()
        if remaining_sell > 1e-18:
            unmatched_sell_amount += remaining_sell

    passing_allocations = [a for a in allocations if a["allocation_pass"]]
    eligible_allocs = [a for a in allocations if a["eligible_sub_50k_lot"]]
    matched_cost = sum(a["matched_buy_cost_usd"] or 0 for a in eligible_allocs)
    matched_income = sum(a["matched_sale_income_usd"] or 0 for a in eligible_allocs)
    realized_profit = matched_income - matched_cost
    aggregate_multiple = matched_income / matched_cost if matched_cost > 0 else None
    max_multiple = max((a["realized_multiple"] for a in eligible_allocs if a["realized_multiple"] is not None), default=None)
    pass_income = sum(a["matched_sale_income_usd"] or 0 for a in passing_allocations)
    pass_profit = sum(a["matched_realized_profit_usd"] or 0 for a in passing_allocations)

    summary = {
        "moralis_buy_event_count": len(buys),
        "moralis_sell_event_count": len(sells),
        "first_actual_buy_timestamp": first_buy.get("block_timestamp") if first_buy else "",
        "first_actual_buy_transaction_hash": first_buy.get("transaction_hash") if first_buy else "",
        "first_actual_buy_market_cap_usd": first_buy_mc,
        "minimum_actual_buy_market_cap_usd": min_buy_mc,
        "eligible_sub_50k_buy_count": eligible_buy_count,
        "fifo_allocation_count": len(allocations),
        "passing_fifo_allocation_count": len(passing_allocations),
        "max_realized_lot_multiple": max_multiple,
        "eligible_lots_matched_cost_usd": matched_cost,
        "eligible_lots_matched_sale_income_usd": matched_income,
        "eligible_lots_realized_profit_usd": realized_profit,
        "eligible_lots_aggregate_realized_multiple": aggregate_multiple,
        "passing_allocations_sale_income_usd": pass_income,
        "passing_allocations_realized_profit_usd": pass_profit,
        "unmatched_sell_token_amount": unmatched_sell_amount,
        "remaining_bought_token_amount": sum(lot["remaining"] for lot in lots),
        "strict_direct_swap_pass": bool(passing_allocations),
    }
    return summary, allocations


def select_rows(df: pd.DataFrame, shard_index: int, shard_count: int, limit_pairs: int) -> pd.DataFrame:
    df = df.sort_values(PAIR_KEY).drop_duplicates(PAIR_KEY).reset_index(drop=True)
    if shard_count > 1:
        df = df[df.index % shard_count == shard_index]
    if limit_pairs > 0:
        # Preserve tier variety in probe mode where possible.
        if shard_count == 1 and "tier" in df.columns:
            groups = []
            per_tier = max(1, limit_pairs // max(1, df["tier"].nunique()))
            for _, g in df.groupby("tier", sort=True):
                groups.append(g.head(per_tier))
            selected = pd.concat(groups).drop_duplicates(PAIR_KEY).head(limit_pairs)
            if len(selected) < limit_pairs:
                extra = df.merge(selected[PAIR_KEY], on=PAIR_KEY, how="left", indicator=True)
                extra = extra[extra["_merge"] == "left_only"].drop(columns="_merge").head(limit_pairs - len(selected))
                selected = pd.concat([selected, extra])
            df = selected
        else:
            df = df.head(limit_pairs)
    return df.reset_index(drop=True)


def write_csv(path: Path, rows: list[dict[str, Any]], field_order: Optional[list[str]] = None) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        if field_order:
            pd.DataFrame(columns=field_order).to_csv(path, index=False)
        else:
            path.write_text("", encoding="utf-8")
        return
    df = pd.DataFrame(rows)
    if field_order:
        extra = [c for c in df.columns if c not in field_order]
        df = df.reindex(columns=field_order + extra)
    df.to_csv(path, index=False)


def run_scan(args: argparse.Namespace) -> None:
    api_key = os.environ.get("MORALIS_API_KEY", "").strip()
    if not api_key:
        raise SystemExit("MORALIS_API_KEY is missing")
    input_path = Path(args.input)
    outdir = Path(args.output_dir)
    outdir.mkdir(parents=True, exist_ok=True)
    df = pd.read_csv(input_path)
    missing = [c for c in ["token_mint", "wallet", "token_supply_used"] if c not in df.columns]
    if missing:
        raise SystemExit(f"Input missing columns: {missing}")
    selected = select_rows(df, args.shard_index, args.shard_count, args.limit_pairs)
    label = args.label or (f"shard_{args.shard_index:02d}_of_{args.shard_count:02d}" if args.shard_count > 1 else "probe")
    client = MoralisClient(api_key, args.requests_per_second, timeout=args.timeout, retries=args.retries)

    pair_rows: list[dict[str, Any]] = []
    event_rows: list[dict[str, Any]] = []
    allocation_rows: list[dict[str, Any]] = []
    raw_path = outdir / f"x_chain_raw_pages_{label}.jsonl.gz"
    status_counts: dict[str, int] = {}

    with gzip.open(raw_path, "wt", encoding="utf-8") as rawf:
        for i, row in selected.iterrows():
            mint = str(row["token_mint"])
            wallet = str(row["wallet"])
            supply = finite_float(row.get("token_supply_used"))
            base = row.to_dict()
            if supply is None or supply <= 0:
                status = "invalid_supply"
                items, pages = [], []
            else:
                items, pages, status = client.wallet_token_swaps(wallet, mint, args.max_pages)
            rawf.write(json.dumps({
                "token_mint": mint,
                "wallet": wallet,
                "status": status,
                "pages": pages,
            }, separators=(",", ":"), ensure_ascii=False) + "\n")

            events = sort_events(e for item in items if (e := extract_event(item, mint, wallet)) is not None)
            event_rows.extend(events)
            if supply is not None and supply > 0:
                verification, allocations = fifo_verify(
                    events, supply, args.entry_mcap_max_usd, args.realized_multiple_min, args.min_sale_usd
                )
            else:
                verification, allocations = fifo_verify([], 1.0, args.entry_mcap_max_usd, args.realized_multiple_min, args.min_sale_usd)
            allocation_rows.extend(allocations)

            transfer_in = truthy(row.get("transfer_in"))
            transfer_out_value = finite_float(row.get("history_transfer_out_income_usd")) or 0.0
            transfer_signal = transfer_in or transfer_out_value > 0 or verification["unmatched_sell_token_amount"] > 0
            strict_pass = bool(verification["strict_direct_swap_pass"])
            strict_clean = strict_pass and not transfer_signal
            if not strict_pass or transfer_signal:
                priority = "HIGH"
            else:
                priority = "MEDIUM"
            pair_result = {
                **base,
                "api_status": status,
                "api_pages": len(pages),
                "api_raw_item_count": len(items),
                "parsed_target_swap_event_count": len(events),
                **verification,
                "transfer_contamination_signal": transfer_signal,
                "strict_clean_pass": strict_clean,
                # The user explicitly asked not to miss linked wallets. Keep every X pair as a cluster seed.
                "cluster_trace_required": True,
                "cluster_trace_priority": priority,
                "verification_rule": (
                    f"FIFO allocation from an actual buy lot below ${args.entry_mcap_max_usd:,.0f} "
                    f"to an actual sale at >= {args.realized_multiple_min:g}x; internal transfers are not sales"
                ),
            }
            pair_rows.append(pair_result)
            status_counts[status] = status_counts.get(status, 0) + 1
            print(f"[{i+1}/{len(selected)}] {status} events={len(events)} pass={strict_pass} {mint[:8]} {wallet[:8]}", flush=True)

    pair_file = outdir / f"x_chain_pair_results_{label}.csv"
    event_file = outdir / f"x_chain_swap_events_{label}.csv.gz"
    allocation_file = outdir / f"x_chain_fifo_allocations_{label}.csv.gz"
    strict_file = outdir / f"x_chain_strict_pass_{label}.csv"
    seed_file = outdir / f"x_cluster_trace_seeds_{label}.csv"
    pd.DataFrame(pair_rows).to_csv(pair_file, index=False)
    pd.DataFrame(event_rows).to_csv(event_file, index=False, compression="gzip")
    pd.DataFrame(allocation_rows).to_csv(allocation_file, index=False, compression="gzip")
    pair_df = pd.DataFrame(pair_rows)
    pair_df[pair_df["strict_direct_swap_pass"] == True].to_csv(strict_file, index=False)  # noqa: E712
    seed_cols = [c for c in [
        "token_mint", "token_symbol", "token_name", "token_supply_used", "wallet", "tier",
        "api_status", "strict_direct_swap_pass", "strict_clean_pass", "transfer_contamination_signal",
        "cluster_trace_priority", "minimum_actual_buy_market_cap_usd", "max_realized_lot_multiple",
        "passing_allocations_realized_profit_usd", "history_transfer_out_income_usd", "transfer_in"
    ] if c in pair_df.columns]
    pair_df[seed_cols].to_csv(seed_file, index=False)

    report = {
        "script_version": VERSION,
        "mode": "scan",
        "label": label,
        "input_pair_count": len(selected),
        "status_counts": status_counts,
        "strict_direct_swap_pass_count": int(pair_df["strict_direct_swap_pass"].sum()),
        "strict_clean_pass_count": int(pair_df["strict_clean_pass"].sum()),
        "unique_wallets_strict_pass": int(pair_df.loc[pair_df["strict_direct_swap_pass"], "wallet"].nunique()),
        "unique_x_tokens_strict_pass": int(pair_df.loc[pair_df["strict_direct_swap_pass"], "token_mint"].nunique()),
        "cluster_trace_seed_count": len(pair_df),
        "configured_rules": {
            "entry_mcap_max_usd": args.entry_mcap_max_usd,
            "realized_multiple_min": args.realized_multiple_min,
            "min_sale_usd": args.min_sale_usd,
            "fifo_matching": True,
            "transfer_is_not_sale": True,
            "all_pairs_retained_for_cluster_trace": True,
        },
        "limitations": {
            "moralis_wallet_swaps_do_not_provide_wallet_to_wallet_token_transfer_graph": True,
            "token_supply_used_is_the_existing_research_supply_and_near_threshold_entries_need_final_chain_supply_review": True,
            "cluster_trace_is_the_next_separate_stage": True,
        },
    }
    (outdir / f"x_chain_verify_report_{label}.json").write_text(json.dumps(report, indent=2), encoding="utf-8")


def concat_csv(files: list[Path], output: Path, compression: Optional[str] = None) -> pd.DataFrame:
    frames = []
    for f in files:
        try:
            frames.append(pd.read_csv(f, compression="infer"))
        except pd.errors.EmptyDataError:
            continue
    df = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
    if not df.empty and set(PAIR_KEY).issubset(df.columns) and "event_sha256" not in df.columns and "buy_transaction_hash" not in df.columns:
        df = df.drop_duplicates(PAIR_KEY)
    df.to_csv(output, index=False, compression=compression)
    return df


def run_merge(args: argparse.Namespace) -> None:
    root = Path(args.input_dir)
    outdir = Path(args.output_dir)
    outdir.mkdir(parents=True, exist_ok=True)
    pair_files = sorted(root.rglob("x_chain_pair_results_shard_*.csv"))
    event_files = sorted(root.rglob("x_chain_swap_events_shard_*.csv.gz"))
    allocation_files = sorted(root.rglob("x_chain_fifo_allocations_shard_*.csv.gz"))
    raw_files = sorted(root.rglob("x_chain_raw_pages_shard_*.jsonl.gz"))
    report_files = sorted(root.rglob("x_chain_verify_report_shard_*.json"))
    if not pair_files:
        raise SystemExit(f"No shard pair files found under {root}")

    pairs = concat_csv(pair_files, outdir / "x_chain_pair_results_2026.csv")
    events = concat_csv(event_files, outdir / "x_chain_swap_events_2026.csv.gz", compression="gzip")
    allocations = concat_csv(allocation_files, outdir / "x_chain_fifo_allocations_2026.csv.gz", compression="gzip")
    strict = pairs[pairs["strict_direct_swap_pass"] == True].copy()  # noqa: E712
    strict.to_csv(outdir / "x_chain_strict_pass_2026.csv", index=False)
    clean = pairs[pairs["strict_clean_pass"] == True].copy()  # noqa: E712
    clean.to_csv(outdir / "x_chain_strict_clean_pass_2026.csv", index=False)
    seed_cols = [c for c in [
        "token_mint", "token_symbol", "token_name", "token_supply_used", "wallet", "tier",
        "api_status", "strict_direct_swap_pass", "strict_clean_pass", "transfer_contamination_signal",
        "cluster_trace_priority", "minimum_actual_buy_market_cap_usd", "max_realized_lot_multiple",
        "passing_allocations_realized_profit_usd", "history_transfer_out_income_usd", "transfer_in"
    ] if c in pairs.columns]
    pairs[seed_cols].to_csv(outdir / "x_cluster_trace_seeds_2026.csv", index=False)

    combined_raw = outdir / "x_chain_raw_pages_2026.jsonl.gz"
    with gzip.open(combined_raw, "wb") as dst:
        for path in raw_files:
            with gzip.open(path, "rb") as src:
                while True:
                    chunk = src.read(1024 * 1024)
                    if not chunk:
                        break
                    dst.write(chunk)

    shard_reports = []
    for f in report_files:
        try:
            shard_reports.append(json.loads(f.read_text(encoding="utf-8")))
        except Exception:
            pass
    status_counts: dict[str, int] = {}
    for r in shard_reports:
        for k, v in r.get("status_counts", {}).items():
            status_counts[k] = status_counts.get(k, 0) + int(v)
    report = {
        "script_version": VERSION,
        "mode": "merge",
        "found_shards": len(pair_files),
        "pair_count": len(pairs),
        "unique_wallet_count": int(pairs["wallet"].nunique()) if not pairs.empty else 0,
        "unique_x_token_count": int(pairs["token_mint"].nunique()) if not pairs.empty else 0,
        "status_counts": status_counts,
        "parsed_swap_event_count": len(events),
        "fifo_allocation_count": len(allocations),
        "strict_direct_swap_pass_count": int(pairs["strict_direct_swap_pass"].sum()) if not pairs.empty else 0,
        "strict_clean_pass_count": int(pairs["strict_clean_pass"].sum()) if not pairs.empty else 0,
        "unique_wallets_strict_pass": int(strict["wallet"].nunique()) if not strict.empty else 0,
        "unique_x_tokens_strict_pass": int(strict["token_mint"].nunique()) if not strict.empty else 0,
        "cluster_trace_seed_count": len(pairs),
        "next_stage": "Trace token transfers and funding links for every seed, then re-run lot matching at cluster level across A->B->C without a fixed sale-delay window.",
        "shard_reports": shard_reports,
    }
    (outdir / "x_chain_verify_report_2026.json").write_text(json.dumps(report, indent=2), encoding="utf-8")


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser()
    p.add_argument("--version", action="store_true")
    p.add_argument("--mode", choices=["probe", "scan", "merge"])
    p.add_argument("--input", default="data/x_chain_verification_input_2026.csv")
    p.add_argument("--input-dir", default="downloaded")
    p.add_argument("--output-dir", default="output")
    p.add_argument("--label", default="")
    p.add_argument("--shard-index", type=int, default=0)
    p.add_argument("--shard-count", type=int, default=1)
    p.add_argument("--limit-pairs", type=int, default=0)
    p.add_argument("--requests-per-second", type=float, default=1.5)
    p.add_argument("--entry-mcap-max-usd", type=float, default=50000)
    p.add_argument("--realized-multiple-min", type=float, default=25)
    p.add_argument("--min-sale-usd", type=float, default=0)
    p.add_argument("--max-pages", type=int, default=20)
    p.add_argument("--timeout", type=int, default=45)
    p.add_argument("--retries", type=int, default=6)
    return p


def main() -> None:
    args = build_parser().parse_args()
    if args.version:
        print(VERSION)
        return
    if not args.mode:
        raise SystemExit("--mode is required")
    if args.mode == "probe":
        if args.limit_pairs <= 0:
            args.limit_pairs = 9
        args.shard_index = 0
        args.shard_count = 1
        args.label = args.label or "probe"
        run_scan(args)
    elif args.mode == "scan":
        if args.shard_index < 0 or args.shard_index >= args.shard_count:
            raise SystemExit("invalid shard index/count")
        run_scan(args)
    else:
        run_merge(args)


if __name__ == "__main__":
    main()
