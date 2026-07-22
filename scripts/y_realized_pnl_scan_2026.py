#!/usr/bin/env python3
"""All-time Y-token realized PnL scan for strict 2026 X wallets.

Research rule:
- The same wallet must have a strict X pass (actual buy below configured X cap
  and actual sale at >= configured multiple; supplied by the completed X stage).
- Y must be a different token from every X token that passed for that wallet.
- Y realized PnL is computed only from that same wallet's actual Moralis swaps.
- Transfers are not sales. A sell quantity that cannot be matched to an earlier
  buy in the same wallet is treated as transfer-in contamination and cannot be
  a strict Y pass.
- The API scan is all-time; Y is not restricted to 2026 or Pump.fun.
- Every token-level result is retained so 75k can later be changed to 50k
  without repeating the API scan.
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
import time
from collections import defaultdict, deque
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Optional

import requests

VERSION = "2026-07-22-y-realized-pnl-scan-v1"
API_BASE = "https://solana-gateway.moralis.io/account/mainnet"

BASE_TOKEN_ADDRESSES = {
    "So11111111111111111111111111111111111111112",  # WSOL
    "11111111111111111111111111111111",             # native/system SOL representation
    "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v", # USDC
    "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB", # USDT
}
BASE_SYMBOLS = {"SOL", "WSOL", "USDC", "USDT", "USD1", "PYUSD", "USDS"}

STATUS_FIELDS = [
    "wallet", "status", "api_pages", "api_raw_item_count",
    "parsed_token_event_count", "token_pnl_row_count", "next_cursor_remaining",
    "first_swap_timestamp", "last_swap_timestamp", "error",
]
EVENT_FIELDS = [
    "wallet", "token_mint", "token_symbol", "token_name", "event_type",
    "block_timestamp", "timestamp_epoch", "block_number", "transaction_hash",
    "transaction_index", "token_amount", "usd_price", "usd_amount",
    "exchange_name", "pair_address", "pair_label", "sub_category", "event_sha256",
]
PNL_FIELDS = [
    "wallet", "token_mint", "token_symbol", "token_name",
    "buy_event_count", "sell_event_count", "priced_buy_event_count", "priced_sell_event_count",
    "total_bought_token_amount", "total_sold_token_amount", "matched_sold_token_amount",
    "unmatched_sell_token_amount", "remaining_bought_token_amount",
    "matched_sell_quantity_coverage", "priced_sell_quantity_coverage",
    "matched_buy_cost_usd", "matched_sale_income_usd", "realized_profit_usd",
    "realized_multiple_on_matched_cost", "first_buy_timestamp", "last_sell_timestamp",
    "wallet_scan_status", "wallet_history_complete", "is_base_or_stable",
    "is_wallet_x_token", "x_token_count_for_wallet", "x_tokens_json",
    "same_wallet_buy_and_sell", "transfer_in_contamination_signal",
    "strict_y_eligible_before_threshold", "raw_event_count",
]


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


def bool_value(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    return str(value or "").strip().lower() in {"1", "true", "yes", "y"}


def read_csv(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        return list(csv.DictReader(f))


def write_csv(path: Path, rows: Iterable[dict[str, Any]], fields: list[str], gzip_output: bool = False) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    opener = gzip.open if gzip_output else open
    with opener(path, "wt", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow({k: "" if row.get(k) is None else row.get(k) for k in fields})


def atomic_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(path)


class RateLimiter:
    def __init__(self, requests_per_second: float) -> None:
        self.interval = 1.0 / max(float(requests_per_second), 0.01)
        self.last = 0.0

    def wait(self) -> None:
        now = time.monotonic()
        delay = self.interval - (now - self.last)
        if delay > 0:
            time.sleep(delay)
        self.last = time.monotonic()


class MoralisClient:
    def __init__(self, api_key: str, rps: float, timeout: int, retries: int) -> None:
        self.session = requests.Session()
        self.session.headers.update({"X-API-Key": api_key, "Accept": "application/json"})
        self.limiter = RateLimiter(rps)
        self.timeout = timeout
        self.retries = retries
        self.calls = 0
        self.retry_count = 0

    def get(self, url: str, params: dict[str, Any]) -> tuple[Optional[dict[str, Any]], dict[str, Any]]:
        last_error = ""
        meta: dict[str, Any] = {}
        for attempt in range(self.retries + 1):
            self.limiter.wait()
            self.calls += 1
            started = time.monotonic()
            try:
                response = self.session.get(url, params=params, timeout=self.timeout)
                meta = {
                    "status_code": response.status_code,
                    "elapsed_ms": round((time.monotonic() - started) * 1000),
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
                self.retry_count += 1
                time.sleep(min(60.0, (2 ** attempt) + random.random()))
        meta["error"] = last_error
        return None, meta

    def wallet_swaps(self, wallet: str, max_pages: int) -> tuple[list[dict[str, Any]], list[dict[str, Any]], str, bool]:
        url = f"{API_BASE}/{wallet}/swaps"
        cursor: Optional[str] = None
        seen_cursors: set[str] = set()
        items: list[dict[str, Any]] = []
        pages: list[dict[str, Any]] = []
        status = "ok"
        next_cursor_remaining = False
        for page_no in range(1, max_pages + 1):
            params: dict[str, Any] = {
                "limit": 100,
                "order": "ASC",
                "transactionTypes": "buy,sell",
            }
            if cursor:
                params["cursor"] = cursor
            payload, meta = self.get(url, params)
            pages.append({"wallet": wallet, "page_no": page_no, "request_meta": meta, "payload": payload})
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
                next_cursor_remaining = False
                break
            next_cursor_remaining = True
            next_cursor = str(next_cursor)
            if next_cursor in seen_cursors:
                status = "cursor_loop"
                break
            seen_cursors.add(next_cursor)
            cursor = next_cursor
        else:
            if next_cursor_remaining:
                status = "max_pages_reached"
        return items, pages, status, next_cursor_remaining


def token_obj(item: dict[str, Any], side: str) -> dict[str, Any]:
    obj = item.get(side)
    return obj if isinstance(obj, dict) else {}


def token_event(item: dict[str, Any], wallet: str, side: str, event_type: str) -> Optional[dict[str, Any]]:
    token = token_obj(item, side)
    mint = str(token.get("address") or "").strip()
    if not mint:
        return None
    amount = finite_float(token.get("amount"))
    if amount is None or amount <= 0:
        return None
    usd_price = finite_float(token.get("usdPrice"))
    usd_amount = finite_float(token.get("usdAmount"))
    if usd_amount is not None:
        usd_amount = abs(usd_amount)
    total_value = finite_float(item.get("totalValueUsd"))
    if (usd_amount is None or usd_amount <= 0) and total_value is not None:
        usd_amount = abs(total_value)
    if (usd_price is None or usd_price <= 0) and usd_amount is not None:
        usd_price = usd_amount / amount
    if (usd_amount is None or usd_amount <= 0) and usd_price is not None:
        usd_amount = amount * usd_price
    dt = parse_ts(item.get("blockTimestamp"))
    tx_hash = str(item.get("transactionHash") or item.get("signature") or "")
    tx_index = item.get("transactionIndex")
    dedupe = f"{wallet}|{tx_hash}|{tx_index}|{mint}|{event_type}|{amount}|{usd_amount}"
    return {
        "wallet": wallet,
        "token_mint": mint,
        "token_symbol": str(token.get("symbol") or ""),
        "token_name": str(token.get("name") or ""),
        "event_type": event_type,
        "block_timestamp": iso(dt),
        "timestamp_epoch": int(dt.timestamp()) if dt else None,
        "block_number": finite_int(item.get("blockNumber")),
        "transaction_hash": tx_hash,
        "transaction_index": tx_index,
        "token_amount": amount,
        "usd_price": usd_price,
        "usd_amount": usd_amount,
        "exchange_name": item.get("exchangeName"),
        "pair_address": item.get("pairAddress"),
        "pair_label": item.get("pairLabel"),
        "sub_category": item.get("subCategory"),
        "event_sha256": hashlib.sha256(dedupe.encode()).hexdigest(),
    }


def parse_wallet_events(items: list[dict[str, Any]], wallet: str) -> list[dict[str, Any]]:
    events: dict[str, dict[str, Any]] = {}
    for item in items:
        bought = token_event(item, wallet, "bought", "buy")
        sold = token_event(item, wallet, "sold", "sell")
        for event in (bought, sold):
            if event:
                events[event["event_sha256"]] = event
    return sorted(
        events.values(),
        key=lambda e: (
            e.get("timestamp_epoch") if e.get("timestamp_epoch") is not None else 2**63 - 1,
            e.get("block_number") if e.get("block_number") is not None else 2**63 - 1,
            str(e.get("transaction_hash") or ""),
            str(e.get("transaction_index") or ""),
            0 if e.get("event_type") == "sell" else 1,
        ),
    )


def strict_x_maps(rows: list[dict[str, str]]) -> tuple[dict[str, set[str]], dict[str, list[dict[str, str]]]]:
    x_tokens: dict[str, set[str]] = defaultdict(set)
    x_rows: dict[str, list[dict[str, str]]] = defaultdict(list)
    for row in rows:
        wallet = str(row.get("wallet") or "").strip()
        mint = str(row.get("token_mint") or "").strip()
        if wallet and mint:
            x_tokens[wallet].add(mint)
            x_rows[wallet].append(row)
    return x_tokens, x_rows


def compute_token_pnl(
    wallet: str,
    events: list[dict[str, Any]],
    x_tokens: set[str],
    wallet_status: str,
    min_coverage: float,
) -> list[dict[str, Any]]:
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for event in events:
        grouped[str(event["token_mint"])].append(event)
    output: list[dict[str, Any]] = []
    for mint, token_events in grouped.items():
        token_events.sort(key=lambda e: (e.get("timestamp_epoch") or 2**63 - 1, str(e.get("event_sha256"))))
        lots: deque[dict[str, float]] = deque()
        buys = sells = priced_buys = priced_sells = 0
        total_bought = total_sold = matched_sold = unmatched_sold = 0.0
        matched_cost = matched_income = 0.0
        priced_sell_qty = 0.0
        first_buy = ""
        last_sell = ""
        symbol = ""
        name = ""

        for event in token_events:
            symbol = symbol or str(event.get("token_symbol") or "")
            name = name or str(event.get("token_name") or "")
            qty = finite_float(event.get("token_amount"))
            usd_amount = finite_float(event.get("usd_amount"))
            if qty is None or qty <= 0:
                continue
            unit = (usd_amount / qty) if usd_amount is not None and usd_amount >= 0 else None
            if event["event_type"] == "buy":
                buys += 1
                total_bought += qty
                if not first_buy:
                    first_buy = str(event.get("block_timestamp") or "")
                if unit is not None:
                    priced_buys += 1
                lots.append({"remaining": qty, "unit_cost": unit if unit is not None else math.nan})
                continue

            sells += 1
            total_sold += qty
            last_sell = str(event.get("block_timestamp") or last_sell)
            if unit is not None:
                priced_sells += 1
                priced_sell_qty += qty
            remaining = qty
            while remaining > 1e-18 and lots:
                lot = lots[0]
                take = min(remaining, lot["remaining"])
                buy_unit = lot["unit_cost"]
                if unit is not None and math.isfinite(buy_unit):
                    matched_sold += take
                    matched_cost += take * buy_unit
                    matched_income += take * unit
                else:
                    unmatched_sold += take
                lot["remaining"] -= take
                remaining -= take
                if lot["remaining"] <= 1e-18:
                    lots.popleft()
            if remaining > 1e-18:
                unmatched_sold += remaining

        remaining_bought = sum(max(0.0, lot["remaining"]) for lot in lots)
        quantity_coverage = matched_sold / total_sold if total_sold > 0 else 0.0
        priced_coverage = priced_sell_qty / total_sold if total_sold > 0 else 0.0
        realized_profit = matched_income - matched_cost
        multiple = matched_income / matched_cost if matched_cost > 0 else None
        is_base = mint in BASE_TOKEN_ADDRESSES or symbol.upper() in BASE_SYMBOLS
        is_x = mint in x_tokens
        same_wallet_buy_sell = buys > 0 and sells > 0
        transfer_signal = unmatched_sold > max(1e-12, total_sold * (1.0 - min_coverage))
        complete = wallet_status == "ok"
        eligible = bool(
            complete
            and same_wallet_buy_sell
            and not is_base
            and not is_x
            and quantity_coverage >= min_coverage
            and priced_coverage >= min_coverage
            and not transfer_signal
            and matched_cost > 0
        )
        output.append({
            "wallet": wallet,
            "token_mint": mint,
            "token_symbol": symbol,
            "token_name": name,
            "buy_event_count": buys,
            "sell_event_count": sells,
            "priced_buy_event_count": priced_buys,
            "priced_sell_event_count": priced_sells,
            "total_bought_token_amount": total_bought,
            "total_sold_token_amount": total_sold,
            "matched_sold_token_amount": matched_sold,
            "unmatched_sell_token_amount": unmatched_sold,
            "remaining_bought_token_amount": remaining_bought,
            "matched_sell_quantity_coverage": quantity_coverage,
            "priced_sell_quantity_coverage": priced_coverage,
            "matched_buy_cost_usd": matched_cost,
            "matched_sale_income_usd": matched_income,
            "realized_profit_usd": realized_profit,
            "realized_multiple_on_matched_cost": multiple,
            "first_buy_timestamp": first_buy,
            "last_sell_timestamp": last_sell,
            "wallet_scan_status": wallet_status,
            "wallet_history_complete": complete,
            "is_base_or_stable": is_base,
            "is_wallet_x_token": is_x,
            "x_token_count_for_wallet": len(x_tokens),
            "x_tokens_json": json.dumps(sorted(x_tokens)),
            "same_wallet_buy_and_sell": same_wallet_buy_sell,
            "transfer_in_contamination_signal": transfer_signal,
            "strict_y_eligible_before_threshold": eligible,
            "raw_event_count": len(token_events),
        })
    return output


def candidate_rows(pnl_rows: list[dict[str, Any]], threshold: float) -> list[dict[str, Any]]:
    rows = []
    for row in pnl_rows:
        profit = finite_float(row.get("realized_profit_usd")) or 0.0
        eligible = bool_value(row.get("strict_y_eligible_before_threshold"))
        out = dict(row)
        out["y_realized_profit_threshold_usd"] = threshold
        out["strict_y_pass"] = bool(eligible and profit >= threshold)
        out["review_y_candidate"] = bool(
            profit >= threshold
            and not bool_value(row.get("is_base_or_stable"))
            and not bool_value(row.get("is_wallet_x_token"))
            and not out["strict_y_pass"]
        )
        if out["strict_y_pass"] or out["review_y_candidate"]:
            rows.append(out)
    rows.sort(key=lambda r: (not bool_value(r.get("strict_y_pass")), -(finite_float(r.get("realized_profit_usd")) or 0), str(r.get("wallet"))))
    return rows


def run_scan(args: argparse.Namespace) -> None:
    x_rows = read_csv(Path(args.input))
    x_tokens, _ = strict_x_maps(x_rows)
    wallets = sorted(x_tokens)
    if args.shard_count > 1:
        wallets = wallets[args.shard_index::args.shard_count]
    if args.limit_wallets:
        wallets = wallets[:args.limit_wallets]

    api_key = os.environ.get("MORALIS_API_KEY", "").strip()
    if not api_key:
        raise SystemExit("MORALIS_API_KEY is required")
    client = MoralisClient(api_key, args.requests_per_second, args.timeout_seconds, args.retries)
    label = "probe" if args.mode == "probe" else f"shard_{args.shard_index:02d}_of_{args.shard_count:02d}"
    out = Path(args.output_dir)
    out.mkdir(parents=True, exist_ok=True)

    statuses: list[dict[str, Any]] = []
    all_events: list[dict[str, Any]] = []
    all_pnl: list[dict[str, Any]] = []
    raw_path = out / f"y_raw_pages_{label}.jsonl.gz"
    with gzip.open(raw_path, "wt", encoding="utf-8") as raw_file:
        for idx, wallet in enumerate(wallets, 1):
            print(f"[{idx}/{len(wallets)}] wallet={wallet}", flush=True)
            items, pages, status, next_remaining = client.wallet_swaps(wallet, args.max_pages_per_wallet)
            for page in pages:
                raw_file.write(json.dumps(page, ensure_ascii=False) + "\n")
            events = parse_wallet_events(items, wallet)
            pnl = compute_token_pnl(wallet, events, x_tokens[wallet], status, args.min_matched_sell_coverage)
            all_events.extend(events)
            all_pnl.extend(pnl)
            timestamps = [e.get("block_timestamp") for e in events if e.get("block_timestamp")]
            statuses.append({
                "wallet": wallet,
                "status": status,
                "api_pages": len(pages),
                "api_raw_item_count": len(items),
                "parsed_token_event_count": len(events),
                "token_pnl_row_count": len(pnl),
                "next_cursor_remaining": next_remaining,
                "first_swap_timestamp": min(timestamps) if timestamps else "",
                "last_swap_timestamp": max(timestamps) if timestamps else "",
                "error": next((str(p.get("request_meta", {}).get("error") or "") for p in pages if p.get("request_meta", {}).get("error")), ""),
            })

    write_csv(out / f"y_wallet_status_{label}.csv", statuses, STATUS_FIELDS)
    write_csv(out / f"y_swap_events_{label}.csv.gz", all_events, EVENT_FIELDS, gzip_output=True)
    write_csv(out / f"y_token_pnl_{label}.csv", all_pnl, PNL_FIELDS)
    candidates = candidate_rows(all_pnl, args.y_realized_profit_min_usd)
    candidate_fields = PNL_FIELDS + ["y_realized_profit_threshold_usd", "strict_y_pass", "review_y_candidate"]
    write_csv(out / f"y_candidates_{label}.csv", candidates, candidate_fields)
    report = {
        "script_version": VERSION,
        "mode": args.mode,
        "label": label,
        "wallet_count": len(wallets),
        "status_counts": dict(sorted({s: sum(1 for r in statuses if r["status"] == s) for s in set(r["status"] for r in statuses)}.items())),
        "api_calls": client.calls,
        "api_retries": client.retry_count,
        "raw_swap_item_count": sum(int(r["api_raw_item_count"]) for r in statuses),
        "parsed_token_event_count": len(all_events),
        "token_pnl_row_count": len(all_pnl),
        "strict_y_pass_count_at_threshold": sum(bool_value(r.get("strict_y_pass")) for r in candidates),
        "review_y_candidate_count_at_threshold": sum(bool_value(r.get("review_y_candidate")) for r in candidates),
        "configured_rules": {
            "same_wallet_only": True,
            "y_must_differ_from_all_wallet_x_tokens": True,
            "all_time_y_scan": True,
            "transfer_is_not_sale": True,
            "fifo_realized_pnl": True,
            "min_matched_sell_coverage": args.min_matched_sell_coverage,
            "y_realized_profit_min_usd": args.y_realized_profit_min_usd,
            "all_token_pnl_rows_retained_for_refilter": True,
        },
    }
    atomic_json(out / f"y_scan_report_{label}.json", report)
    print(json.dumps(report, indent=2), flush=True)


def merge_csv_files(files: list[Path]) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    for path in files:
        rows.extend(read_csv(path))
    return rows


def read_gzip_csv(path: Path) -> list[dict[str, str]]:
    with gzip.open(path, "rt", encoding="utf-8", newline="") as f:
        return list(csv.DictReader(f))


def build_outputs(
    x_rows: list[dict[str, str]],
    pnl_rows: list[dict[str, Any]],
    statuses: list[dict[str, Any]],
    threshold: float,
    output_dir: Path,
) -> dict[str, Any]:
    x_tokens, wallet_x_rows = strict_x_maps(x_rows)
    candidates = candidate_rows(pnl_rows, threshold)
    strict_candidates = [r for r in candidates if bool_value(r.get("strict_y_pass"))]
    review_candidates = [r for r in candidates if bool_value(r.get("review_y_candidate"))]

    xy_rows: list[dict[str, Any]] = []
    for y in strict_candidates:
        for x in wallet_x_rows.get(str(y["wallet"]), []):
            xy_rows.append({
                "wallet": y["wallet"],
                "x_token_mint": x.get("token_mint"),
                "x_token_symbol": x.get("token_symbol"),
                "x_strict_clean_pass": x.get("strict_clean_pass"),
                "x_first_actual_buy_market_cap_usd": x.get("first_actual_buy_market_cap_usd"),
                "x_max_realized_lot_multiple": x.get("max_realized_lot_multiple"),
                "x_passing_allocations_realized_profit_usd": x.get("passing_allocations_realized_profit_usd"),
                "y_token_mint": y.get("token_mint"),
                "y_token_symbol": y.get("token_symbol"),
                "y_token_name": y.get("token_name"),
                "y_realized_profit_usd": y.get("realized_profit_usd"),
                "y_realized_multiple_on_matched_cost": y.get("realized_multiple_on_matched_cost"),
                "y_matched_sell_quantity_coverage": y.get("matched_sell_quantity_coverage"),
                "y_first_buy_timestamp": y.get("first_buy_timestamp"),
                "y_last_sell_timestamp": y.get("last_sell_timestamp"),
                "y_realized_profit_threshold_usd": threshold,
                "same_wallet_xy_pass": True,
            })

    finalists: list[dict[str, Any]] = []
    by_wallet: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in strict_candidates:
        by_wallet[str(row["wallet"])].append(row)
    for wallet, ys in by_wallet.items():
        best_y = max(ys, key=lambda r: finite_float(r.get("realized_profit_usd")) or -math.inf)
        xs = wallet_x_rows[wallet]
        best_x = max(xs, key=lambda r: finite_float(r.get("max_realized_lot_multiple")) or -math.inf)
        finalists.append({
            "wallet": wallet,
            "strict_x_pair_count": len(xs),
            "strict_clean_x_pair_count": sum(bool_value(r.get("strict_clean_pass")) for r in xs),
            "qualifying_y_token_count": len(ys),
            "best_x_token_mint": best_x.get("token_mint"),
            "best_x_token_symbol": best_x.get("token_symbol"),
            "best_x_max_realized_lot_multiple": best_x.get("max_realized_lot_multiple"),
            "best_x_first_actual_buy_market_cap_usd": best_x.get("first_actual_buy_market_cap_usd"),
            "best_y_token_mint": best_y.get("token_mint"),
            "best_y_token_symbol": best_y.get("token_symbol"),
            "best_y_realized_profit_usd": best_y.get("realized_profit_usd"),
            "best_y_realized_multiple_on_matched_cost": best_y.get("realized_multiple_on_matched_cost"),
            "y_realized_profit_threshold_usd": threshold,
            "same_wallet_xy_pass": True,
            "requires_final_transaction_evidence": True,
        })
    finalists.sort(key=lambda r: -(finite_float(r.get("best_y_realized_profit_usd")) or 0))

    output_dir.mkdir(parents=True, exist_ok=True)
    write_csv(output_dir / "y_wallet_status_2026.csv", statuses, STATUS_FIELDS)
    write_csv(output_dir / "y_token_pnl_all_2026.csv", pnl_rows, PNL_FIELDS)
    candidate_fields = PNL_FIELDS + ["y_realized_profit_threshold_usd", "strict_y_pass", "review_y_candidate"]
    write_csv(output_dir / "y_candidates_2026.csv", candidates, candidate_fields)
    write_csv(output_dir / "y_strict_pass_2026.csv", strict_candidates, candidate_fields)
    write_csv(output_dir / "y_review_candidates_2026.csv", review_candidates, candidate_fields)
    write_csv(output_dir / "xy_same_wallet_pairs_2026.csv", xy_rows, [
        "wallet", "x_token_mint", "x_token_symbol", "x_strict_clean_pass",
        "x_first_actual_buy_market_cap_usd", "x_max_realized_lot_multiple",
        "x_passing_allocations_realized_profit_usd", "y_token_mint", "y_token_symbol",
        "y_token_name", "y_realized_profit_usd", "y_realized_multiple_on_matched_cost",
        "y_matched_sell_quantity_coverage", "y_first_buy_timestamp", "y_last_sell_timestamp",
        "y_realized_profit_threshold_usd", "same_wallet_xy_pass",
    ])
    write_csv(output_dir / "final_wallets_2026.csv", finalists, [
        "wallet", "strict_x_pair_count", "strict_clean_x_pair_count", "qualifying_y_token_count",
        "best_x_token_mint", "best_x_token_symbol", "best_x_max_realized_lot_multiple",
        "best_x_first_actual_buy_market_cap_usd", "best_y_token_mint", "best_y_token_symbol",
        "best_y_realized_profit_usd", "best_y_realized_multiple_on_matched_cost",
        "y_realized_profit_threshold_usd", "same_wallet_xy_pass", "requires_final_transaction_evidence",
    ])
    return {
        "strict_y_pass_count": len(strict_candidates),
        "review_y_candidate_count": len(review_candidates),
        "same_wallet_xy_pair_count": len(xy_rows),
        "final_wallet_count": len(finalists),
    }


def run_merge(args: argparse.Namespace) -> None:
    root = Path(args.input_root)
    status_files = sorted(root.rglob("y_wallet_status_shard_*.csv"))
    pnl_files = sorted(root.rglob("y_token_pnl_shard_*.csv"))
    event_files = sorted(root.rglob("y_swap_events_shard_*.csv.gz"))
    report_files = sorted(root.rglob("y_scan_report_shard_*.json"))
    if len(report_files) != args.expected_shards:
        raise SystemExit(f"Expected {args.expected_shards} shard reports, found {len(report_files)}")
    if not status_files or not pnl_files:
        raise SystemExit("Shard CSV files not found")
    statuses = merge_csv_files(status_files)
    pnl_rows: list[dict[str, Any]] = merge_csv_files(pnl_files)
    x_rows = read_csv(Path(args.input))
    expected_wallets = len({r["wallet"] for r in x_rows})
    actual_wallets = len({r["wallet"] for r in statuses})
    if actual_wallets != expected_wallets:
        raise SystemExit(f"Wallet coverage mismatch: expected {expected_wallets}, found {actual_wallets}")

    out = Path(args.output_dir)
    metrics = build_outputs(x_rows, pnl_rows, statuses, args.y_realized_profit_min_usd, out)
    # Preserve all parsed events in one compressed file.
    seen: set[str] = set()
    with gzip.open(out / "y_swap_events_all_2026.csv.gz", "wt", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=EVENT_FIELDS)
        writer.writeheader()
        for path in event_files:
            for row in read_gzip_csv(path):
                key = row.get("event_sha256", "")
                if key and key in seen:
                    continue
                if key:
                    seen.add(key)
                writer.writerow({k: row.get(k, "") for k in EVENT_FIELDS})

    reports = [json.loads(p.read_text(encoding="utf-8")) for p in report_files]
    report = {
        "script_version": VERSION,
        "mode": "merge",
        "found_shards": len(report_files),
        "strict_x_pair_count": len(x_rows),
        "strict_x_wallet_count": expected_wallets,
        "wallet_status_count": len(statuses),
        "complete_wallet_count": sum(r.get("status") == "ok" for r in statuses),
        "incomplete_wallet_count": sum(r.get("status") != "ok" for r in statuses),
        "token_pnl_row_count": len(pnl_rows),
        "unique_y_token_count_before_filters": len({r.get("token_mint") for r in pnl_rows}),
        "y_realized_profit_min_usd": args.y_realized_profit_min_usd,
        "min_matched_sell_coverage": args.min_matched_sell_coverage,
        **metrics,
        "all_token_pnl_retained_for_50k_or_other_refilter": True,
        "y_period_scope": "all-time",
        "same_wallet_only": True,
        "cluster_results_used": False,
        "next_stage": "Verify finalist X and Y transaction evidence, label wallets, then load Supabase and terminal.",
        "shard_reports": reports,
    }
    atomic_json(out / "y_scan_report_2026.json", report)
    print(json.dumps(report, indent=2), flush=True)


def run_refilter(args: argparse.Namespace) -> None:
    pnl_rows = read_csv(Path(args.pnl_input))
    statuses = read_csv(Path(args.status_input))
    x_rows = read_csv(Path(args.input))
    out = Path(args.output_dir)
    metrics = build_outputs(x_rows, pnl_rows, statuses, args.y_realized_profit_min_usd, out)
    report = {
        "script_version": VERSION,
        "mode": "refilter",
        "y_realized_profit_min_usd": args.y_realized_profit_min_usd,
        "min_matched_sell_coverage": args.min_matched_sell_coverage,
        **metrics,
        "api_rescan_required": False,
    }
    atomic_json(out / "y_refilter_report_2026.json", report)
    print(json.dumps(report, indent=2), flush=True)


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--version", action="store_true")
    p.add_argument("--mode", choices=["probe", "scan", "merge", "refilter"])
    p.add_argument("--input", default="data/x_chain_strict_pass_2026.csv")
    p.add_argument("--input-root", default="")
    p.add_argument("--pnl-input", default="")
    p.add_argument("--status-input", default="")
    p.add_argument("--output-dir", default="output")
    p.add_argument("--shard-index", type=int, default=0)
    p.add_argument("--shard-count", type=int, default=1)
    p.add_argument("--expected-shards", type=int, default=4)
    p.add_argument("--limit-wallets", type=int, default=0)
    p.add_argument("--requests-per-second", type=float, default=1.5)
    p.add_argument("--max-pages-per-wallet", type=int, default=100)
    p.add_argument("--timeout-seconds", type=int, default=45)
    p.add_argument("--retries", type=int, default=6)
    p.add_argument("--y-realized-profit-min-usd", type=float, default=75000)
    p.add_argument("--min-matched-sell-coverage", type=float, default=0.98)
    args = p.parse_args()
    if args.version:
        print(VERSION)
        raise SystemExit(0)
    if not args.mode:
        p.error("--mode is required")
    if args.mode == "merge" and not args.input_root:
        p.error("--input-root is required for merge")
    if args.mode == "refilter" and (not args.pnl_input or not args.status_input):
        p.error("--pnl-input and --status-input are required for refilter")
    return args


def main() -> None:
    args = parse_args()
    if args.mode in {"probe", "scan"}:
        run_scan(args)
    elif args.mode == "merge":
        run_merge(args)
    else:
        run_refilter(args)


if __name__ == "__main__":
    main()
