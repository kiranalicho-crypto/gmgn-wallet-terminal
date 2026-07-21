#!/usr/bin/env python3
from __future__ import annotations

import argparse
import concurrent.futures
import csv
import gzip
import hashlib
import json
import math
import os
import socket
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid
from collections import Counter
from pathlib import Path
from typing import Any

SCRIPT_VERSION = "2026-07-21-x-trader-prefilter-v1"
HOST = "https://openapi.gmgn.ai"
OUTPUT_FIELDS = [
    "token_mint",
    "token_symbol",
    "token_name",
    "token_source_class",
    "token_corrected_ath_mc_usd",
    "token_supply_used",
    "wallet",
    "query_sources",
    "address_type",
    "exchange",
    "tags_json",
    "maker_token_tags_json",
    "is_suspicious",
    "transfer_in",
    "avg_cost_usd",
    "avg_entry_market_cap_usd",
    "avg_entry_under_50k",
    "history_bought_cost_usd",
    "history_sold_income_usd",
    "realized_profit_usd",
    "realized_pnl_ratio",
    "realized_total_multiple",
    "gmgn_profit_usd",
    "gmgn_profit_change_ratio",
    "buy_volume_usd",
    "sell_volume_usd",
    "buy_tx_count",
    "sell_tx_count",
    "sell_amount_percentage",
    "start_holding_at",
    "end_holding_at",
    "last_active_timestamp",
    "preliminary_x_rule_pass",
    "needs_exact_first_buy_verification",
    "raw_item_sha256",
]

def force_ipv4() -> None:
    original = socket.getaddrinfo
    def wrapped(host, port, family=0, type=0, proto=0, flags=0):
        return original(host, port, socket.AF_INET, type, proto, flags)
    socket.getaddrinfo = wrapped  # type: ignore[assignment]

def number(value: Any) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        parsed = float(str(value).replace(",", "").strip())
        return parsed if math.isfinite(parsed) else None
    except Exception:
        return None

class Limiter:
    def __init__(self, rps: float) -> None:
        self.interval = 1.0 / rps
        self.lock = threading.Lock()
        self.next_allowed = 0.0
        self.blocked_until = 0.0

    def wait(self) -> None:
        while True:
            with self.lock:
                now = time.monotonic()
                target = max(self.next_allowed, self.blocked_until)
                if target <= now:
                    self.next_allowed = now + self.interval
                    return
                delay = target - now
            time.sleep(min(max(delay, 0.01), 2.0))

    def block(self, seconds: float) -> None:
        with self.lock:
            self.blocked_until = max(
                self.blocked_until,
                time.monotonic() + max(seconds, 0.0),
            )

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--version", action="store_true")
    parser.add_argument("--mode", choices=("probe", "scan", "merge"), default="scan")
    parser.add_argument("--input")
    parser.add_argument("--input-root")
    parser.add_argument("--output-dir")
    parser.add_argument("--shard-index", type=int, default=0)
    parser.add_argument("--shard-count", type=int, default=1)
    parser.add_argument("--expected-shards", type=int, default=4)
    parser.add_argument("--requests-per-second", type=float, default=2.5)
    parser.add_argument("--workers", type=int, default=3)
    parser.add_argument("--probe-size", type=int, default=8)
    parser.add_argument("--max-attempts", type=int, default=5)
    parser.add_argument("--timeout-seconds", type=float, default=30)
    args = parser.parse_args()

    if args.version:
        print(SCRIPT_VERSION)
        raise SystemExit(0)
    if not args.output_dir:
        parser.error("--output-dir required")
    if args.mode in {"probe", "scan"} and not args.input:
        parser.error("--input required")
    if args.mode == "merge" and not args.input_root:
        parser.error("--input-root required")
    return args

def read_tokens(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8", newline="") as handle:
        rows = list(csv.DictReader(handle))
    required = {
        "mint",
        "symbol",
        "name",
        "source_class",
        "corrected_ath_market_cap_usd",
        "supply_used",
    }
    missing = required - set(rows[0].keys() if rows else [])
    if missing:
        raise RuntimeError(f"Input missing columns: {sorted(missing)}")
    return rows

def choose_probe(rows: list[dict[str, str]], size: int) -> list[dict[str, str]]:
    ordered = sorted(
        rows,
        key=lambda row: float(row["corrected_ath_market_cap_usd"]),
    )
    if len(ordered) <= size:
        return ordered
    return [
        ordered[round(i * (len(ordered) - 1) / (size - 1))]
        for i in range(size)
    ]

def reset_delay(headers: Any, body: dict[str, Any], attempt: int) -> float:
    raw = headers.get("X-RateLimit-Reset") if headers else None
    if raw:
        try:
            return max(float(raw) - time.time(), 0) + 1
        except Exception:
            pass
    raw = body.get("reset_at") if isinstance(body, dict) else None
    if raw:
        try:
            return max(float(raw) - time.time(), 0) + 1
        except Exception:
            pass
    return min(2 ** attempt, 30)

def request_traders(
    token: dict[str, str],
    tag: str,
    api_key: str,
    limiter: Limiter,
    args: argparse.Namespace,
) -> dict[str, Any]:
    query_source = "all_profit" if not tag else "sniper_profit"
    params = {
        "chain": "sol",
        "address": token["mint"],
        "limit": 100,
        "order_by": "profit",
        "direction": "desc",
        "timestamp": int(time.time()),
        "client_id": str(uuid.uuid4()),
    }
    if tag:
        params["tag"] = tag

    last_error = ""
    for attempt in range(1, args.max_attempts + 1):
        params["timestamp"] = int(time.time())
        params["client_id"] = str(uuid.uuid4())
        limiter.wait()
        url = f"{HOST}/v1/market/token_top_traders?{urllib.parse.urlencode(params)}"
        req = urllib.request.Request(
            url,
            headers={
                "X-APIKEY": api_key,
                "Content-Type": "application/json",
                "User-Agent": f"gmgn-wallet-research/{SCRIPT_VERSION}",
            },
        )
        status = 0
        headers = None
        raw = b""
        try:
            with urllib.request.urlopen(req, timeout=args.timeout_seconds) as response:
                status = int(response.status)
                headers = response.headers
                raw = response.read()
        except urllib.error.HTTPError as exc:
            status = int(exc.code)
            headers = exc.headers
            raw = exc.read()
        except Exception as exc:
            last_error = f"{type(exc).__name__}: {exc}"
            if attempt < args.max_attempts:
                time.sleep(min(2 ** (attempt - 1), 15))
                continue
            return {
                "token": token,
                "query_source": query_source,
                "status": "request_error",
                "error": last_error,
                "items": [],
                "raw": None,
            }

        try:
            envelope = json.loads(raw.decode("utf-8", errors="replace"))
        except Exception:
            last_error = f"HTTP {status}: non-JSON"
            if attempt < args.max_attempts:
                continue
            return {
                "token": token,
                "query_source": query_source,
                "status": "request_error",
                "error": last_error,
                "items": [],
                "raw": None,
            }

        if status == 429 or envelope.get("error") in {
            "RATE_LIMIT_EXCEEDED",
            "RATE_LIMIT_BANNED",
        }:
            delay = reset_delay(headers, envelope, attempt)
            limiter.block(delay)
            last_error = f"rate_limit reset_delay={delay:.2f}"
            if attempt < args.max_attempts:
                continue

        if str(envelope.get("code")) != "0":
            return {
                "token": token,
                "query_source": query_source,
                "status": "api_error",
                "error": (
                    f"HTTP {status} code={envelope.get('code')} "
                    f"error={envelope.get('error')} "
                    f"message={envelope.get('message')}"
                ),
                "items": [],
                "raw": envelope,
            }

        data = envelope.get("data")
        if isinstance(data, dict):
            items = data.get("list")
        elif isinstance(data, list):
            items = data
        else:
            items = None

        if not isinstance(items, list):
            return {
                "token": token,
                "query_source": query_source,
                "status": "schema_error",
                "error": "data.list is not a list",
                "items": [],
                "raw": envelope,
            }

        return {
            "token": token,
            "query_source": query_source,
            "status": "ok",
            "error": "",
            "items": items,
            "raw": envelope,
        }

    return {
        "token": token,
        "query_source": query_source,
        "status": "request_error",
        "error": last_error or "unknown",
        "items": [],
        "raw": None,
    }

def transform(token: dict[str, str], item: dict[str, Any], source: str) -> dict[str, Any] | None:
    wallet = str(item.get("address") or "").strip()
    if not wallet:
        return None

    supply = number(token.get("supply_used"))
    avg_cost = number(item.get("avg_cost"))
    avg_entry_mc = (
        avg_cost * supply
        if avg_cost is not None and supply is not None
        else None
    )

    bought_cost = number(item.get("history_bought_cost"))
    sold_income = number(item.get("history_sold_income"))
    realized_profit = number(item.get("realized_profit"))
    realized_pnl = number(item.get("realized_pnl"))

    realized_total_multiple = None
    if bought_cost is not None and bought_cost > 0 and sold_income is not None:
        realized_total_multiple = sold_income / bought_cost

    ratio_pass = (
        (realized_total_multiple is not None and realized_total_multiple >= 25)
        or (realized_pnl is not None and realized_pnl >= 24)
    )
    entry_pass = avg_entry_mc is not None and avg_entry_mc < 50_000
    profit_pass = realized_profit is not None and realized_profit > 0
    preliminary_pass = ratio_pass and entry_pass and profit_pass

    raw_item = json.dumps(item, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return {
        "token_mint": token["mint"],
        "token_symbol": token.get("symbol", ""),
        "token_name": token.get("name", ""),
        "token_source_class": token.get("source_class", ""),
        "token_corrected_ath_mc_usd": token.get("corrected_ath_market_cap_usd", ""),
        "token_supply_used": token.get("supply_used", ""),
        "wallet": wallet,
        "query_sources": source,
        "address_type": item.get("addr_type", ""),
        "exchange": item.get("exchange", ""),
        "tags_json": json.dumps(item.get("tags") or [], ensure_ascii=False, separators=(",", ":")),
        "maker_token_tags_json": json.dumps(item.get("maker_token_tags") or [], ensure_ascii=False, separators=(",", ":")),
        "is_suspicious": item.get("is_suspicious", ""),
        "transfer_in": item.get("transfer_in", ""),
        "avg_cost_usd": "" if avg_cost is None else format(avg_cost, ".15g"),
        "avg_entry_market_cap_usd": "" if avg_entry_mc is None else format(avg_entry_mc, ".15g"),
        "avg_entry_under_50k": "true" if entry_pass else "false" if avg_entry_mc is not None else "",
        "history_bought_cost_usd": "" if bought_cost is None else format(bought_cost, ".15g"),
        "history_sold_income_usd": "" if sold_income is None else format(sold_income, ".15g"),
        "realized_profit_usd": "" if realized_profit is None else format(realized_profit, ".15g"),
        "realized_pnl_ratio": "" if realized_pnl is None else format(realized_pnl, ".15g"),
        "realized_total_multiple": "" if realized_total_multiple is None else format(realized_total_multiple, ".15g"),
        "gmgn_profit_usd": item.get("profit", ""),
        "gmgn_profit_change_ratio": item.get("profit_change", ""),
        "buy_volume_usd": item.get("buy_volume_cur", ""),
        "sell_volume_usd": item.get("sell_volume_cur", ""),
        "buy_tx_count": item.get("buy_tx_count_cur", ""),
        "sell_tx_count": item.get("sell_tx_count_cur", ""),
        "sell_amount_percentage": item.get("sell_amount_percentage", ""),
        "start_holding_at": item.get("start_holding_at", ""),
        "end_holding_at": item.get("end_holding_at", ""),
        "last_active_timestamp": item.get("last_active_timestamp", ""),
        "preliminary_x_rule_pass": "true" if preliminary_pass else "false",
        "needs_exact_first_buy_verification": "true" if preliminary_pass else "false",
        "raw_item_sha256": hashlib.sha256(raw_item.encode("utf-8")).hexdigest(),
    }

def merge_duplicate(existing: dict[str, Any], incoming: dict[str, Any]) -> dict[str, Any]:
    sources = set(existing["query_sources"].split("|")) | set(incoming["query_sources"].split("|"))
    existing["query_sources"] = "|".join(sorted(sources))
    return existing

def write_outputs(
    out_dir: Path,
    label: str,
    responses: list[dict[str, Any]],
    args: argparse.Namespace,
) -> dict[str, Any]:
    out_dir.mkdir(parents=True, exist_ok=True)
    rows_by_key: dict[tuple[str, str], dict[str, Any]] = {}
    response_status = Counter()

    raw_path = out_dir / f"raw_trader_responses_{label}.jsonl.gz"
    with gzip.open(raw_path, "wt", encoding="utf-8") as raw_handle:
        for response in responses:
            response_status[response["status"]] += 1
            raw_handle.write(json.dumps({
                "token_mint": response["token"]["mint"],
                "query_source": response["query_source"],
                "status": response["status"],
                "error": response["error"],
                "raw": response["raw"],
            }, ensure_ascii=False, separators=(",", ":")) + "\n")

            for item in response["items"]:
                if not isinstance(item, dict):
                    continue
                row = transform(response["token"], item, response["query_source"])
                if row is None:
                    continue
                key = (row["token_mint"], row["wallet"])
                if key in rows_by_key:
                    rows_by_key[key] = merge_duplicate(rows_by_key[key], row)
                else:
                    rows_by_key[key] = row

    rows = sorted(rows_by_key.values(), key=lambda r: (r["token_mint"], r["wallet"]))
    all_path = out_dir / f"x_top_traders_{label}.csv.gz"
    with gzip.open(all_path, "wt", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=OUTPUT_FIELDS)
        writer.writeheader()
        writer.writerows(rows)

    passed = [row for row in rows if row["preliminary_x_rule_pass"] == "true"]
    passed_path = out_dir / f"x_wallet_prefilter_candidates_{label}.csv"
    with passed_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=OUTPUT_FIELDS)
        writer.writeheader()
        writer.writerows(passed)

    recognized = sum(
        1 for row in rows
        if row["avg_cost_usd"] != ""
        and row["realized_profit_usd"] != ""
    )
    report = {
        "script_version": SCRIPT_VERSION,
        "mode": args.mode,
        "label": label,
        "response_status_counts": dict(sorted(response_status.items())),
        "unique_token_wallet_rows": len(rows),
        "recognized_cost_and_realized_profit_rows": recognized,
        "preliminary_x_rule_pass_count": len(passed),
        "important_limitations": {
            "top_traders_limit_per_query": 100,
            "queries_per_token": ["all wallets by profit", "sniper wallets by profit"],
            "avg_entry_market_cap_is_only_a_prefilter": True,
            "exact_first_buy_under_50k_requires_moralis_or_chain_verification": True,
            "realized_profit_requires_final_chain_verification": True,
        },
    }
    report_path = out_dir / f"report_{label}.json"
    report_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    return report

def run_query(args: argparse.Namespace) -> int:
    api_key = os.environ.get("GMGN_API_KEY", "").strip()
    if not api_key:
        print("GMGN_API_KEY missing", file=sys.stderr)
        return 10

    force_ipv4()
    tokens = read_tokens(Path(args.input))
    if args.mode == "probe":
        selected = choose_probe(tokens, args.probe_size)
        label = "probe"
    else:
        selected = [
            token for index, token in enumerate(tokens)
            if index % args.shard_count == args.shard_index
        ]
        label = f"shard_{args.shard_index:02d}_of_{args.shard_count:02d}"

    limiter = Limiter(args.requests_per_second)
    futures = []
    responses = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=args.workers) as executor:
        for token in selected:
            futures.append(executor.submit(
                request_traders, token, "", api_key, limiter, args
            ))
            futures.append(executor.submit(
                request_traders, token, "sniper", api_key, limiter, args
            ))

        for index, future in enumerate(concurrent.futures.as_completed(futures), 1):
            responses.append(future.result())
            if index % 50 == 0 or index == len(futures):
                print(f"PROGRESS responses={index}/{len(futures)}", flush=True)

    report = write_outputs(Path(args.output_dir), label, responses, args)
    print(json.dumps(report, ensure_ascii=False, indent=2))

    if args.mode == "probe":
        if report["response_status_counts"].get("ok", 0) < 4:
            print("X_TRADER_PROBE_FAILED: too few successful responses", file=sys.stderr)
            return 11
        if report["recognized_cost_and_realized_profit_rows"] < 10:
            print("X_TRADER_PROBE_FAILED: expected fields not recognized", file=sys.stderr)
            return 12
        print("X_TRADER_PROBE_OK")
    else:
        print("X_TRADER_SHARD_FINISHED")
    return 0

def merge(args: argparse.Namespace) -> int:
    root = Path(args.input_root)
    out = Path(args.output_dir)
    out.mkdir(parents=True, exist_ok=True)

    row_paths = sorted(root.rglob("x_top_traders_shard_*.csv.gz"))
    raw_paths = sorted(root.rglob("raw_trader_responses_shard_*.jsonl.gz"))
    report_paths = sorted(root.rglob("report_shard_*.json"))
    if len(row_paths) != args.expected_shards:
        print(
            f"MERGE_ERROR expected={args.expected_shards} found={len(row_paths)}",
            file=sys.stderr,
        )
        return 20

    merged_by_key: dict[tuple[str, str], dict[str, Any]] = {}
    for path in row_paths:
        with gzip.open(path, "rt", encoding="utf-8", newline="") as handle:
            for row in csv.DictReader(handle):
                key = (row["token_mint"], row["wallet"])
                if key in merged_by_key:
                    merged_by_key[key] = merge_duplicate(merged_by_key[key], row)
                else:
                    merged_by_key[key] = row

    rows = sorted(merged_by_key.values(), key=lambda r: (r["token_mint"], r["wallet"]))
    passed = [row for row in rows if row["preliminary_x_rule_pass"] == "true"]

    all_path = out / "x_top_traders_2026.csv.gz"
    with gzip.open(all_path, "wt", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=OUTPUT_FIELDS)
        writer.writeheader()
        writer.writerows(rows)

    passed_path = out / "x_wallet_prefilter_candidates_2026.csv"
    with passed_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=OUTPUT_FIELDS)
        writer.writeheader()
        writer.writerows(passed)

    raw_out = out / "x_trader_raw_responses_2026.jsonl.gz"
    with gzip.open(raw_out, "wb") as destination:
        for path in raw_paths:
            with gzip.open(path, "rb") as source:
                while chunk := source.read(1024 * 1024):
                    destination.write(chunk)

    shard_reports = [json.loads(path.read_text(encoding="utf-8")) for path in report_paths]
    report = {
        "script_version": SCRIPT_VERSION,
        "mode": "merge",
        "found_shards": len(row_paths),
        "unique_token_wallet_rows": len(rows),
        "preliminary_x_rule_pass_count": len(passed),
        "unique_wallets_passing": len({row["wallet"] for row in passed}),
        "unique_x_tokens_with_pass": len({row["token_mint"] for row in passed}),
        "important_limitations": {
            "this_is_a_prefilter_not_final_result": True,
            "exact_first_buy_under_50k_not_yet_verified": True,
            "top_100_profit_plus_top_100_snipers_per_token": True,
        },
        "shard_reports": shard_reports,
    }
    (out / "x_trader_prefilter_report_2026.json").write_text(
        json.dumps(report, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    print(json.dumps(report, ensure_ascii=False, indent=2))
    print("X_TRADER_MERGE_OK")
    return 0

def main() -> int:
    args = parse_args()
    if args.mode == "merge":
        return merge(args)
    return run_query(args)

if __name__ == "__main__":
    raise SystemExit(main())
