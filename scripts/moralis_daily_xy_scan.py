#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import gzip
import hashlib
import json
import math
import os
import shutil
import sys
import time
import zipfile
from collections import defaultdict, deque
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import requests

VERSION = "2026-07-25-moralis-daily-xy-scan-v4-25k-resume"

API_BASE = "https://solana-gateway.moralis.io/account/mainnet"
STATE_ZIP = Path("data/moralis_state_current.zip")
STATE_DIR = Path("runtime/moralis_state")
OUTPUT_DIR = Path("output-moralis-daily-xy-scan")
X_SEED = Path("data/x_chain_strict_pass_2026.csv")
BIRDEYE_STATUS = Path("data/birdeye_wallet_scan_summary.csv")
BIRDEYE_50K = Path("data/birdeye_y_candidates_50k.csv")

BASE_MINTS = {
    "So11111111111111111111111111111111111111112",
    "So11111111111111111111111111111111111111111",
    "11111111111111111111111111111111",
    "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",
    "Es9vMFrzaCERmJfrF4H2FYDZRrCNF2no6YEeqvMZggq",
    "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB",
}
BASE_SYMBOLS = {"SOL", "WSOL", "USDC", "USDT", "USD1", "PYUSD", "USDS"}
SEARCH_THRESHOLD_USD = 25_000.0
THRESHOLDS = (25_000.0, 30_000.0, 50_000.0, 60_000.0, 75_000.0)

EVENT_FIELDS = [
    "wallet",
    "token_mint",
    "token_symbol",
    "token_name",
    "event_type",
    "block_timestamp",
    "timestamp_epoch",
    "block_number",
    "transaction_hash",
    "transaction_index",
    "token_amount",
    "usd_price",
    "usd_amount",
    "exchange_name",
    "pair_address",
    "pair_label",
    "sub_category",
    "event_sha256",
]


def now_utc() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def fnum(value: Any) -> float | None:
    try:
        if value in (None, ""):
            return None
        number = float(value)
        return number if math.isfinite(number) else None
    except (TypeError, ValueError):
        return None


def fint(value: Any, default: int = 0) -> int:
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return default


def truthy(value: Any) -> bool:
    return str(value or "").strip().lower() in {"true", "1", "yes", "y"}


def read_csv(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        return list(csv.DictReader(handle))


def write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fields = sorted({key for row in rows for key in row})
    if not fields:
        fields = ["empty"]
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def atomic_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(
        json.dumps(value, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    temporary.replace(path)


def initialize_state() -> None:
    if (STATE_DIR / "progress.json").is_file():
        return
    STATE_DIR.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(STATE_ZIP) as archive:
        archive.extractall(STATE_DIR)


def load_x_seed() -> tuple[
    dict[str, set[str]],
    dict[str, dict[str, Any]],
    list[dict[str, str]],
]:
    rows = read_csv(X_SEED)
    x_mints: dict[str, set[str]] = defaultdict(set)
    best: dict[str, dict[str, Any]] = {}
    for row in rows:
        wallet = str(row.get("wallet") or "").strip()
        mint = str(row.get("token_mint") or "").strip()
        if not wallet or not mint:
            continue
        x_mints[wallet].add(mint)
        multiple = fnum(row.get("max_realized_lot_multiple")) or -math.inf
        previous = best.get(wallet)
        if (
            previous is None
            or multiple
            > (fnum(previous.get("max_realized_lot_multiple")) or -math.inf)
        ):
            best[wallet] = dict(row)
    if len(x_mints) != 227:
        raise RuntimeError(f"Expected 227 X wallets, found {len(x_mints)}")
    return dict(x_mints), best, rows


def birdeye_complete_wallets() -> set[str]:
    return {
        str(row.get("wallet") or "").strip()
        for row in read_csv(BIRDEYE_STATUS)
        if str(row.get("status") or "") == "complete"
    }


def birdeye_known_candidates() -> dict[str, list[dict[str, Any]]]:
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in read_csv(BIRDEYE_50K):
        wallet = str(row.get("wallet") or "").strip()
        mint = str(row.get("token_mint") or "").strip()
        if not wallet or not mint:
            continue
        grouped[wallet].append(
            {
                "wallet": wallet,
                "token_mint": mint,
                "token_symbol": str(row.get("token_symbol") or ""),
                "birdeye_pnl_usd": fnum(
                    row.get("birdeye_realized_profit_usd")
                ),
            }
        )
    for wallet in grouped:
        grouped[wallet].sort(
            key=lambda row: row["birdeye_pnl_usd"] or -math.inf,
            reverse=True,
        )
    return dict(grouped)


def event_from_side(
    item: dict[str, Any],
    wallet: str,
    side: str,
    event_type: str,
) -> dict[str, Any] | None:
    token = item.get(side)
    if not isinstance(token, dict):
        return None
    mint = str(token.get("address") or "").strip()
    amount = fnum(token.get("amount"))
    if not mint or amount is None or amount <= 0:
        return None

    usd_price = fnum(token.get("usdPrice"))
    usd_amount = fnum(token.get("usdAmount"))
    if usd_amount is not None:
        usd_amount = abs(usd_amount)
    if usd_amount is None:
        total = fnum(item.get("totalValueUsd"))
        if total is not None:
            usd_amount = abs(total)

    timestamp = str(item.get("blockTimestamp") or "")
    try:
        timestamp_epoch: int | str = int(
            datetime.fromisoformat(timestamp.replace("Z", "+00:00")).timestamp()
        )
    except Exception:
        timestamp_epoch = ""

    event: dict[str, Any] = {
        "wallet": wallet,
        "token_mint": mint,
        "token_symbol": str(token.get("symbol") or ""),
        "token_name": str(token.get("name") or ""),
        "event_type": event_type,
        "block_timestamp": timestamp,
        "timestamp_epoch": timestamp_epoch,
        "block_number": item.get("blockNumber", ""),
        "transaction_hash": str(item.get("transactionHash") or ""),
        "transaction_index": item.get("transactionIndex", ""),
        "token_amount": amount,
        "usd_price": usd_price if usd_price is not None else "",
        "usd_amount": usd_amount if usd_amount is not None else "",
        "exchange_name": str(item.get("exchangeName") or ""),
        "pair_address": str(item.get("pairAddress") or ""),
        "pair_label": str(item.get("pairLabel") or ""),
        "sub_category": str(item.get("subCategory") or ""),
    }
    digest_material = "|".join(
        str(event.get(field, "")) for field in EVENT_FIELDS[:-1]
    )
    event["event_sha256"] = hashlib.sha256(
        digest_material.encode("utf-8")
    ).hexdigest()
    return event


def parse_swaps(
    items: list[dict[str, Any]],
    wallet: str,
) -> list[dict[str, Any]]:
    events: list[dict[str, Any]] = []
    for item in items:
        if not isinstance(item, dict):
            continue
        bought = event_from_side(item, wallet, "bought", "buy")
        sold = event_from_side(item, wallet, "sold", "sell")
        if bought:
            events.append(bought)
        if sold:
            events.append(sold)
    return events


def event_file(wallet: str) -> Path:
    return STATE_DIR / "events" / f"{wallet}.csv.gz"


def read_events(wallet: str) -> list[dict[str, str]]:
    path = event_file(wallet)
    if not path.exists():
        return []
    with gzip.open(path, "rt", encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def append_events(wallet: str, new_events: list[dict[str, Any]]) -> int:
    path = event_file(wallet)
    path.parent.mkdir(parents=True, exist_ok=True)
    old = read_events(wallet)
    seen = {str(row.get("event_sha256") or "") for row in old}
    added = 0
    with gzip.open(path, "wt", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=EVENT_FIELDS)
        writer.writeheader()
        for row in old:
            writer.writerow({field: row.get(field, "") for field in EVENT_FIELDS})
        for event in new_events:
            key = str(event.get("event_sha256") or "")
            if key and key in seen:
                continue
            if key:
                seen.add(key)
            writer.writerow({field: event.get(field, "") for field in EVENT_FIELDS})
            added += 1
    return added


def normalize_token_events(
    raw_events: list[dict[str, str]],
) -> tuple[list[dict[str, Any]], int]:
    """
    Exact duplicate events are already removed by event_sha256.
    This step consolidates same transaction + token + side multi-leg rows:
    identical amount/USD rows are kept once; genuinely split legs are summed.
    """
    buckets: dict[
        tuple[int, int, str, str, str],
        list[dict[str, str]],
    ] = defaultdict(list)

    for event in raw_events:
        key = (
            fint(event.get("block_number"), 2**63 - 1),
            fint(event.get("transaction_index"), 2**31 - 1),
            str(event.get("transaction_hash") or ""),
            str(event.get("token_mint") or ""),
            str(event.get("event_type") or ""),
        )
        buckets[key].append(event)

    normalized: list[dict[str, Any]] = []
    anomaly_count = 0
    for key, rows in buckets.items():
        signatures: set[tuple[float, float | None]] = set()
        unique_rows: list[dict[str, str]] = []
        for row in rows:
            quantity = fnum(row.get("token_amount")) or 0.0
            usd = fnum(row.get("usd_amount"))
            signature = (round(quantity, 12), None if usd is None else round(usd, 8))
            if signature in signatures:
                continue
            signatures.add(signature)
            unique_rows.append(row)

        if len(unique_rows) > 1:
            anomaly_count += 1

        first = unique_rows[0]
        quantity_sum = sum(fnum(row.get("token_amount")) or 0.0 for row in unique_rows)
        usd_values = [fnum(row.get("usd_amount")) for row in unique_rows]
        usd_sum = (
            sum(value for value in usd_values if value is not None)
            if any(value is not None for value in usd_values)
            else None
        )
        normalized.append(
            {
                "block_number": key[0],
                "transaction_index": key[1],
                "transaction_hash": key[2],
                "token_mint": key[3],
                "event_type": key[4],
                "token_symbol": str(first.get("token_symbol") or ""),
                "token_name": str(first.get("token_name") or ""),
                "block_timestamp": str(first.get("block_timestamp") or ""),
                "timestamp_epoch": fint(
                    first.get("timestamp_epoch"),
                    2**63 - 1,
                ),
                "token_amount": quantity_sum,
                "usd_amount": usd_sum,
            }
        )

    normalized.sort(
        key=lambda row: (
            row["block_number"],
            row["transaction_index"],
            row["timestamp_epoch"],
            row["transaction_hash"],
            0 if row["event_type"] == "buy" else 1,
        )
    )
    return normalized, anomaly_count


def calculate_wallet_token_pnl(
    wallet: str,
    x_mints: set[str],
) -> list[dict[str, Any]]:
    raw = read_events(wallet)
    normalized, multi_leg_anomalies = normalize_token_events(raw)
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for event in normalized:
        grouped[str(event["token_mint"])].append(event)

    output: list[dict[str, Any]] = []
    for mint, events in grouped.items():
        lots: deque[list[float]] = deque()
        matched_quantity = 0.0
        unmatched_sell_quantity = 0.0
        matched_cost = 0.0
        matched_income = 0.0
        total_sold = 0.0
        buy_count = 0
        sell_count = 0
        missing_usd_count = 0
        symbol = ""
        name = ""

        for event in events:
            symbol = symbol or str(event.get("token_symbol") or "")
            name = name or str(event.get("token_name") or "")
            quantity = fnum(event.get("token_amount"))
            usd = fnum(event.get("usd_amount"))
            if quantity is None or quantity <= 0:
                continue
            unit = (
                usd / quantity
                if usd is not None and usd >= 0
                else None
            )

            if event["event_type"] == "buy":
                buy_count += 1
                if unit is None:
                    missing_usd_count += 1
                lots.append([quantity, math.nan if unit is None else unit])
                continue

            if event["event_type"] != "sell":
                continue

            sell_count += 1
            total_sold += quantity
            if unit is None:
                missing_usd_count += 1
            remaining = quantity

            while remaining > 1e-12 and lots:
                take = min(remaining, lots[0][0])
                buy_unit = lots[0][1]
                if unit is not None and math.isfinite(buy_unit):
                    matched_quantity += take
                    matched_cost += take * buy_unit
                    matched_income += take * unit
                else:
                    unmatched_sell_quantity += take
                lots[0][0] -= take
                remaining -= take
                if lots[0][0] <= 1e-12:
                    lots.popleft()

            if remaining > 1e-12:
                # User decision: excess sells do not eliminate the wallet and
                # are not treated as zero-cost profit.
                unmatched_sell_quantity += remaining

        is_base = mint in BASE_MINTS or symbol.upper() in BASE_SYMBOLS
        output.append(
            {
                "wallet": wallet,
                "token_mint": mint,
                "token_symbol": symbol,
                "token_name": name,
                "buy_event_count": buy_count,
                "sell_event_count": sell_count,
                "matched_sell_quantity": matched_quantity,
                "unmatched_sell_quantity": unmatched_sell_quantity,
                "matched_buy_cost_usd": matched_cost,
                "matched_sale_income_usd": matched_income,
                "realized_profit_usd": matched_income - matched_cost,
                "realized_multiple_on_matched_cost": (
                    matched_income / matched_cost if matched_cost > 0 else None
                ),
                "matched_sell_coverage": (
                    matched_quantity / total_sold if total_sold > 0 else 0.0
                ),
                "missing_usd_event_count": missing_usd_count,
                "same_tx_multileg_anomaly_count_wallet": multi_leg_anomalies,
                "is_x_token": mint in x_mints,
                "is_base_or_stable": is_base,
            }
        )
    return output


class Budget:
    def __init__(self, max_calls: int) -> None:
        self.max_calls = max_calls
        self.calls = 0
        self.stop_reason = ""

    @property
    def remaining(self) -> int:
        return max(0, self.max_calls - self.calls)

    def can_call(self) -> bool:
        return self.calls < self.max_calls and not self.stop_reason

    def charge(self) -> None:
        self.calls += 1


class MoralisClient:
    def __init__(
        self,
        api_key: str,
        budget: Budget,
        request_interval_seconds: float,
    ) -> None:
        self.session = requests.Session()
        self.session.headers.update(
            {"X-API-Key": api_key, "Accept": "application/json"}
        )
        self.budget = budget
        self.interval = request_interval_seconds
        self.last_call_monotonic = 0.0

    def get_swaps(
        self,
        wallet: str,
        *,
        cursor: str = "",
        token_address: str = "",
    ) -> dict[str, Any] | None:
        if not self.budget.can_call():
            return None

        elapsed = time.monotonic() - self.last_call_monotonic
        if elapsed < self.interval:
            time.sleep(self.interval - elapsed)

        params: dict[str, Any] = {
            "limit": 100,
            "order": "ASC",
            "transactionTypes": "buy,sell",
        }
        if cursor:
            params["cursor"] = cursor
        if token_address:
            params["tokenAddress"] = token_address

        response: requests.Response | None = None
        for attempt in range(1, 5):
            try:
                response = self.session.get(
                    f"{API_BASE}/{wallet}/swaps",
                    params=params,
                    timeout=60,
                )
                self.budget.charge()
                self.last_call_monotonic = time.monotonic()
            except requests.RequestException as exc:
                if attempt == 4:
                    raise RuntimeError(
                        f"{wallet}: network error after retries: {exc}"
                    ) from exc
                time.sleep(min(10, 2**attempt))
                continue

            try:
                payload = response.json()
            except ValueError:
                payload = {}

            if response.status_code == 200:
                if not isinstance(payload.get("result"), list):
                    raise RuntimeError(
                        f"{wallet}: HTTP 200 but result is not a list"
                    )
                return payload

            body = response.text[:1200]
            lower = body.lower()
            if response.status_code == 429 or any(
                phrase in lower
                for phrase in ("daily limit", "rate limit", "compute unit")
            ):
                self.budget.stop_reason = (
                    f"moralis_limit_http_{response.status_code}"
                )
                return None

            if response.status_code in {500, 502, 503, 504} and attempt < 4:
                time.sleep(min(10, 2**attempt))
                continue

            raise RuntimeError(
                f"{wallet}: HTTP {response.status_code}: {body}"
            )

        return None


def load_scan_meta() -> dict[str, Any]:
    path = STATE_DIR / "xy_scan_meta.json"
    if path.exists():
        return json.loads(path.read_text(encoding="utf-8"))
    return {
        "version": VERSION,
        "qualified_wallets": {},
        "token_verifications": {},
        "run_history": [],
    }


def save_state(progress: dict[str, Any], meta: dict[str, Any]) -> None:
    atomic_json(STATE_DIR / "progress.json", progress)
    atomic_json(STATE_DIR / "xy_scan_meta.json", meta)


def token_verification_key(wallet: str, mint: str) -> str:
    return f"{wallet}|{mint}"


def verify_token_to_completion(
    client: MoralisClient,
    wallet: str,
    mint: str,
    symbol_hint: str,
    x_mints: set[str],
    progress: dict[str, Any],
    meta: dict[str, Any],
    source: str,
) -> dict[str, Any] | None:
    key = token_verification_key(wallet, mint)
    verification = meta["token_verifications"].setdefault(
        key,
        {
            "wallet": wallet,
            "token_mint": mint,
            "token_symbol_hint": symbol_hint,
            "cursor": "",
            "status": "pending",
            "api_pages": 0,
            "source": source,
            "last_error": "",
        },
    )
    if verification.get("status") == "complete":
        rows = calculate_wallet_token_pnl(wallet, x_mints)
        return next((row for row in rows if row["token_mint"] == mint), None)

    while client.budget.can_call():
        payload = client.get_swaps(
            wallet,
            cursor=str(verification.get("cursor") or ""),
            token_address=mint,
        )
        if payload is None:
            save_state(progress, meta)
            return None

        items = [item for item in payload["result"] if isinstance(item, dict)]
        append_events(wallet, parse_swaps(items, wallet))
        verification["api_pages"] = fint(verification.get("api_pages")) + 1
        verification["last_error"] = ""
        next_cursor = str(payload.get("cursor") or "")
        if next_cursor:
            verification["cursor"] = next_cursor
            verification["status"] = "partial"
        else:
            verification["cursor"] = ""
            verification["status"] = "complete"
        save_state(progress, meta)

        if not next_cursor:
            rows = calculate_wallet_token_pnl(wallet, x_mints)
            return next(
                (row for row in rows if row["token_mint"] == mint),
                None,
            )

    return None


def qualified_y_rows(
    wallet: str,
    x_mints: set[str],
    threshold: float,
) -> list[dict[str, Any]]:
    return [
        row
        for row in calculate_wallet_token_pnl(wallet, x_mints)
        if not row["is_x_token"]
        and not row["is_base_or_stable"]
        and (fnum(row["realized_profit_usd"]) or -math.inf) >= threshold
    ]


def mark_qualified(
    wallet: str,
    token_row: dict[str, Any],
    meta: dict[str, Any],
    source: str,
    today_new: set[str],
) -> None:
    previous = meta["qualified_wallets"].get(wallet)
    candidate = {
        "wallet": wallet,
        "token_mint": token_row["token_mint"],
        "token_symbol": token_row.get("token_symbol", ""),
        "realized_profit_usd": token_row["realized_profit_usd"],
        "matched_buy_cost_usd": token_row["matched_buy_cost_usd"],
        "matched_sale_income_usd": token_row["matched_sale_income_usd"],
        "matched_sell_coverage": token_row["matched_sell_coverage"],
        "missing_usd_event_count": token_row["missing_usd_event_count"],
        "same_tx_multileg_anomaly_count_wallet": token_row[
            "same_tx_multileg_anomaly_count_wallet"
        ],
        "verification_status": "token_history_complete",
        "source": source,
        "qualified_at_utc": now_utc(),
        "search_threshold_usd": SEARCH_THRESHOLD_USD,
    }
    if previous is None or (
        fnum(candidate["realized_profit_usd"]) or -math.inf
    ) > (fnum(previous.get("realized_profit_usd")) or -math.inf):
        meta["qualified_wallets"][wallet] = candidate
    if previous is None:
        today_new.add(wallet)


def resume_incomplete_token_verifications(
    client: MoralisClient,
    x_mints_by_wallet: dict[str, set[str]],
    progress: dict[str, Any],
    meta: dict[str, Any],
    today_new: set[str],
) -> None:
    pending = [
        value
        for value in meta["token_verifications"].values()
        if value.get("status") != "complete"
    ]
    pending.sort(
        key=lambda row: (
            0 if row.get("source") == "birdeye_known_50k" else 1,
            fint(row.get("api_pages")),
            str(row.get("wallet")),
            str(row.get("token_mint")),
        )
    )
    for item in pending:
        if not client.budget.can_call():
            return
        wallet = str(item["wallet"])
        mint = str(item["token_mint"])
        result = verify_token_to_completion(
            client,
            wallet,
            mint,
            str(item.get("token_symbol_hint") or ""),
            x_mints_by_wallet[wallet],
            progress,
            meta,
            str(item.get("source") or "resume"),
        )
        if result and (
            fnum(result["realized_profit_usd"]) or -math.inf
        ) >= SEARCH_THRESHOLD_USD:
            mark_qualified(
                wallet,
                result,
                meta,
                str(item.get("source") or "resume"),
                today_new,
            )
            save_state(progress, meta)


def verify_known_birdeye_candidates(
    client: MoralisClient,
    candidates: dict[str, list[dict[str, Any]]],
    x_mints_by_wallet: dict[str, set[str]],
    progress: dict[str, Any],
    meta: dict[str, Any],
    today_new: set[str],
) -> None:
    # Highest Birdeye candidate first. Stop this wallet as soon as one Y token
    # is fully verified at or above the active search threshold.
    ordered_wallets = sorted(
        candidates,
        key=lambda wallet: max(
            row["birdeye_pnl_usd"] or -math.inf
            for row in candidates[wallet]
        ),
        reverse=True,
    )
    for wallet in ordered_wallets:
        if not client.budget.can_call():
            return
        if wallet in meta["qualified_wallets"]:
            continue
        if progress["wallets"][wallet].get("status") == "ok":
            existing = qualified_y_rows(
                wallet,
                x_mints_by_wallet[wallet],
                SEARCH_THRESHOLD_USD,
            )
            if existing:
                best = max(
                    existing,
                    key=lambda row: fnum(row["realized_profit_usd"])
                    or -math.inf,
                )
                mark_qualified(
                    wallet,
                    best,
                    meta,
                    "moralis_full_wallet_existing",
                    today_new,
                )
                continue

        for candidate in candidates[wallet]:
            if not client.budget.can_call():
                return
            result = verify_token_to_completion(
                client,
                wallet,
                candidate["token_mint"],
                candidate["token_symbol"],
                x_mints_by_wallet[wallet],
                progress,
                meta,
                "birdeye_known_50k",
            )
            if result is None:
                return
            if (
                fnum(result["realized_profit_usd"]) or -math.inf
            ) >= SEARCH_THRESHOLD_USD:
                mark_qualified(
                    wallet,
                    result,
                    meta,
                    "birdeye_known_50k_then_moralis_token_complete",
                    today_new,
                )
                save_state(progress, meta)
                break


def analyze_existing_state(
    x_mints_by_wallet: dict[str, set[str]],
    progress: dict[str, Any],
    meta: dict[str, Any],
    today_new: set[str],
) -> None:
    for wallet, item in progress["wallets"].items():
        if item.get("status") != "ok":
            continue
        rows = qualified_y_rows(
            wallet,
            x_mints_by_wallet[wallet],
            SEARCH_THRESHOLD_USD,
        )
        if not rows:
            continue
        best = max(
            rows,
            key=lambda row: fnum(row["realized_profit_usd"]) or -math.inf,
        )
        mark_qualified(
            wallet,
            best,
            meta,
            "moralis_full_wallet_existing",
            today_new,
        )


def provisional_candidate_tokens(
    wallet: str,
    x_mints: set[str],
) -> list[dict[str, Any]]:
    rows = [
        row
        for row in calculate_wallet_token_pnl(wallet, x_mints)
        if not row["is_x_token"]
        and not row["is_base_or_stable"]
        and (
            fnum(row["realized_profit_usd"]) or -math.inf
        ) >= SEARCH_THRESHOLD_USD
    ]
    rows.sort(
        key=lambda row: fnum(row["realized_profit_usd"]) or -math.inf,
        reverse=True,
    )
    return rows


def priority_wallets(
    all_wallets: set[str],
    progress: dict[str, Any],
    birdeye_complete: set[str],
) -> set[str]:
    moralis_complete = {
        wallet
        for wallet, item in progress["wallets"].items()
        if item.get("status") == "ok"
    }
    # This set must shrink after every successful resume run. Never assert the
    # original first-run count here: doing so makes a valid updated state crash.
    return all_wallets - moralis_complete - birdeye_complete


def scan_discovery_round_robin(
    client: MoralisClient,
    priority: set[str],
    x_mints_by_wallet: dict[str, set[str]],
    progress: dict[str, Any],
    meta: dict[str, Any],
    today_new: set[str],
) -> None:
    daily_pages: dict[str, int] = defaultdict(int)

    while client.budget.can_call():
        eligible = [
            wallet
            for wallet in priority
            if wallet not in meta["qualified_wallets"]
            and progress["wallets"][wallet].get("status") != "ok"
        ]
        if not eligible:
            return

        # Round-robin:
        # 1) each untouched wallet gets coverage before second pages,
        # 2) the three existing 100-page heavy partial wallets are last,
        # 3) lower daily and historical page counts first.
        eligible.sort(
            key=lambda wallet: (
                daily_pages[wallet],
                1
                if fint(progress["wallets"][wallet].get("api_pages")) >= 100
                else 0,
                fint(progress["wallets"][wallet].get("api_pages")),
                wallet,
            )
        )

        wallet = eligible[0]
        item = progress["wallets"][wallet]
        payload = client.get_swaps(
            wallet,
            cursor=str(item.get("cursor") or ""),
        )
        if payload is None:
            return

        raw_items = [
            value for value in payload["result"] if isinstance(value, dict)
        ]
        added = append_events(wallet, parse_swaps(raw_items, wallet))
        daily_pages[wallet] += 1
        item["api_pages"] = fint(item.get("api_pages")) + 1
        item["api_raw_item_count"] = (
            fint(item.get("api_raw_item_count")) + len(raw_items)
        )
        item["last_error"] = ""

        timestamps = [
            str(value.get("blockTimestamp") or "")
            for value in raw_items
            if value.get("blockTimestamp")
        ]
        if timestamps:
            if not item.get("first_swap_timestamp"):
                item["first_swap_timestamp"] = min(timestamps)
            item["last_swap_timestamp"] = max(timestamps)

        next_cursor = str(payload.get("cursor") or "")
        if next_cursor:
            item["cursor"] = next_cursor
            item["status"] = "partial"
        else:
            item["cursor"] = ""
            item["status"] = "ok"

        save_state(progress, meta)
        print(
            f"[{client.budget.calls}/{client.budget.max_calls}] "
            f"{wallet} page={item['api_pages']} raw={len(raw_items)} "
            f"new_events={added} status={item['status']}",
            flush=True,
        )

        candidates = provisional_candidate_tokens(
            wallet,
            x_mints_by_wallet[wallet],
        )
        for candidate in candidates:
            if not client.budget.can_call():
                return
            result = verify_token_to_completion(
                client,
                wallet,
                candidate["token_mint"],
                candidate.get("token_symbol", ""),
                x_mints_by_wallet[wallet],
                progress,
                meta,
                "moralis_wallet_discovery_then_token_complete",
            )
            if result is None:
                return
            if (
                fnum(result["realized_profit_usd"]) or -math.inf
            ) >= SEARCH_THRESHOLD_USD:
                mark_qualified(
                    wallet,
                    result,
                    meta,
                    "moralis_wallet_discovery_then_token_complete",
                    today_new,
                )
                save_state(progress, meta)
                print(
                    f"QUALIFIED {wallet} {result['token_symbol']} "
                    f"${result['realized_profit_usd']:,.2f}; "
                    "this wallet stops, next wallet continues.",
                    flush=True,
                )
                break

        # A fully completed wallet can qualify without token re-fetch because
        # all wallet swap pages are present.
        if (
            item.get("status") == "ok"
            and wallet not in meta["qualified_wallets"]
        ):
            final_rows = qualified_y_rows(
                wallet,
                x_mints_by_wallet[wallet],
                SEARCH_THRESHOLD_USD,
            )
            if final_rows:
                best = max(
                    final_rows,
                    key=lambda row: fnum(row["realized_profit_usd"])
                    or -math.inf,
                )
                mark_qualified(
                    wallet,
                    best,
                    meta,
                    "moralis_full_wallet_complete_today",
                    today_new,
                )
                save_state(progress, meta)


def all_completed_token_rows(
    x_mints_by_wallet: dict[str, set[str]],
    progress: dict[str, Any],
    meta: dict[str, Any],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    verified_tokens = {
        (
            str(item.get("wallet")),
            str(item.get("token_mint")),
        )
        for item in meta["token_verifications"].values()
        if item.get("status") == "complete"
    }

    for wallet, state in progress["wallets"].items():
        wallet_complete = state.get("status") == "ok"
        calculated = calculate_wallet_token_pnl(
            wallet,
            x_mints_by_wallet[wallet],
        )
        for row in calculated:
            if row["is_x_token"] or row["is_base_or_stable"]:
                continue
            token_complete = (
                wallet_complete
                or (wallet, row["token_mint"]) in verified_tokens
            )
            if not token_complete:
                continue
            row["coverage_basis"] = (
                "full_wallet_history"
                if wallet_complete
                else "complete_wallet_token_filter_history"
            )
            rows.append(row)
    return rows


def build_outputs(
    x_mints_by_wallet: dict[str, set[str]],
    x_best: dict[str, dict[str, Any]],
    progress: dict[str, Any],
    meta: dict[str, Any],
    priority: set[str],
    today_new: set[str],
    budget: Budget,
    run_started: str,
) -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    completed_rows = all_completed_token_rows(
        x_mints_by_wallet,
        progress,
        meta,
    )
    write_csv(OUTPUT_DIR / "all_completed_y_token_pnl.csv", completed_rows)

    wallet_rankings_by_threshold: dict[int, list[dict[str, Any]]] = {}
    for threshold in THRESHOLDS:
        qualifying = [
            row
            for row in completed_rows
            if (fnum(row["realized_profit_usd"]) or -math.inf) >= threshold
        ]
        by_wallet: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for row in qualifying:
            by_wallet[row["wallet"]].append(row)

        wallet_rows: list[dict[str, Any]] = []
        for wallet, rows in by_wallet.items():
            best_y = max(
                rows,
                key=lambda row: fnum(row["realized_profit_usd"])
                or -math.inf,
            )
            x = x_best[wallet]
            wallet_rows.append(
                {
                    "wallet": wallet,
                    "trader_type": "nice",
                    "x_best_token_mint": x.get("token_mint", ""),
                    "x_best_token_symbol": x.get("token_symbol", ""),
                    "x_best_realized_multiple": fnum(
                        x.get("max_realized_lot_multiple")
                    ),
                    "x_best_entry_mcap_usd": fnum(
                        x.get("first_actual_buy_market_cap_usd")
                    ),
                    "best_y_token_mint": best_y["token_mint"],
                    "best_y_token_symbol": best_y["token_symbol"],
                    "best_y_realized_profit_usd": best_y[
                        "realized_profit_usd"
                    ],
                    "best_y_coverage_basis": best_y["coverage_basis"],
                    "qualifying_y_token_count": len(rows),
                    "qualifying_y_tokens": "|".join(
                        f"{row['token_symbol']}:{row['token_mint']}:"
                        f"{row['realized_profit_usd']:.2f}"
                        for row in sorted(
                            rows,
                            key=lambda value: fnum(
                                value["realized_profit_usd"]
                            )
                            or -math.inf,
                            reverse=True,
                        )
                    ),
                    "new_in_this_run": wallet in today_new,
                    "threshold_usd": threshold,
                }
            )

        wallet_rows.sort(
            key=lambda row: fnum(row["x_best_realized_multiple"])
            or -math.inf,
            reverse=True,
        )
        for index, row in enumerate(wallet_rows, start=1):
            row["label"] = f"trader_nice_{index}"

        wallet_rankings_by_threshold[int(threshold)] = wallet_rows
        write_csv(
            OUTPUT_DIR / f"all_wallet_ranking_{int(threshold/1000)}k.csv",
            wallet_rows,
        )
        write_csv(
            OUTPUT_DIR / f"all_token_results_{int(threshold/1000)}k.csv",
            qualifying,
        )

    today_rows = [
        row
        for row in wallet_rankings_by_threshold[int(SEARCH_THRESHOLD_USD)]
        if row["wallet"] in today_new
    ]
    write_csv(OUTPUT_DIR / "today_new_qualified_25k_wallets.csv", today_rows)

    progress_rows: list[dict[str, Any]] = []
    for wallet, item in progress["wallets"].items():
        progress_rows.append(
            {
                "wallet": wallet,
                "moralis_status": item.get("status", ""),
                "api_pages_total": item.get("api_pages", 0),
                "api_raw_item_count_total": item.get(
                    "api_raw_item_count", 0
                ),
                "is_discovery_priority_wallet": wallet in priority,
                "qualified_25k": wallet in meta["qualified_wallets"],
                "qualified_token_mint": meta["qualified_wallets"].get(
                    wallet, {}
                ).get("token_mint", ""),
                "qualified_token_symbol": meta["qualified_wallets"].get(
                    wallet, {}
                ).get("token_symbol", ""),
                "qualified_realized_profit_usd": meta[
                    "qualified_wallets"
                ].get(wallet, {}).get("realized_profit_usd", ""),
                "last_error": item.get("last_error", ""),
            }
        )
    write_csv(OUTPUT_DIR / "wallet_progress_after_run.csv", progress_rows)

    priority_incomplete = [
        row
        for row in progress_rows
        if row["is_discovery_priority_wallet"]
        and not row["qualified_25k"]
        and row["moralis_status"] != "ok"
    ]
    write_csv(
        OUTPUT_DIR / "remaining_priority_wallets.csv",
        priority_incomplete,
    )

    status_counts = defaultdict(int)
    for item in progress["wallets"].values():
        status_counts[str(item.get("status") or "unknown")] += 1

    report = {
        "version": VERSION,
        "run_started_utc": run_started,
        "run_finished_utc": now_utc(),
        "api_calls_this_run": budget.calls,
        "expected_cu_this_run": budget.calls * 50,
        "max_calls_budget": budget.max_calls,
        "stop_reason": budget.stop_reason or "budget_or_work_completed",
        "moralis_status_counts": dict(status_counts),
        "priority_wallet_count": len(priority),
        "remaining_priority_wallet_count": len(priority_incomplete),
        "qualified_25k_total": len(
            wallet_rankings_by_threshold[int(SEARCH_THRESHOLD_USD)]
        ),
        "qualified_25k_new_this_run": len(today_rows),
        "threshold_wallet_counts": {
            str(threshold): len(rows)
            for threshold, rows in wallet_rankings_by_threshold.items()
        },
        "rules": {
            "x_already_passed_25x_for_227_wallets": True,
            "transfer_in_out_not_elimination": True,
            "excess_sells_ignored_not_zero_cost_profit": True,
            "active_y_search_threshold_usd": SEARCH_THRESHOLD_USD,
            "stop_only_the_wallet_after_one_complete_y_token_ge_25k": True,
            "continue_other_wallets_until_daily_budget": True,
            "ranking_by_x_best_realized_multiple_desc": True,
            "trader_nosell_not_in_current_source": True,
        },
    }
    atomic_json(OUTPUT_DIR / "run_report.json", report)

    meta["run_history"].append(report)
    save_state(progress, meta)

    updated_zip = OUTPUT_DIR / "moralis_state_updated.zip"
    if updated_zip.exists():
        updated_zip.unlink()
    with zipfile.ZipFile(
        updated_zip,
        "w",
        compression=zipfile.ZIP_DEFLATED,
    ) as archive:
        for path in sorted(STATE_DIR.rglob("*")):
            if path.is_file():
                archive.write(path, path.relative_to(STATE_DIR))

    print(json.dumps(report, ensure_ascii=False, indent=2), flush=True)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--version", action="store_true")
    parser.add_argument(
        "--max-api-calls",
        type=int,
        default=760,
        help="760 calls = 38,000 CU; leaves 2,000 CU reserve.",
    )
    parser.add_argument(
        "--request-interval-seconds",
        type=float,
        default=0.20,
    )
    args = parser.parse_args()

    if args.version:
        print(VERSION)
        return 0
    if not 1 <= args.max_api_calls <= 800:
        raise SystemExit("max-api-calls must be between 1 and 800")

    api_key = os.environ.get("MORALIS_API_KEY", "").strip()
    if not api_key:
        raise SystemExit("MORALIS_API_KEY is missing")

    initialize_state()
    progress_path = STATE_DIR / "progress.json"
    progress = json.loads(progress_path.read_text(encoding="utf-8"))
    meta = load_scan_meta()
    x_mints_by_wallet, x_best, x_rows = load_x_seed()
    all_wallets = set(x_mints_by_wallet)
    complete_birdeye = birdeye_complete_wallets()
    known_candidates = birdeye_known_candidates()
    priority = priority_wallets(
        all_wallets,
        progress,
        complete_birdeye,
    )

    budget = Budget(args.max_api_calls)
    client = MoralisClient(
        api_key,
        budget,
        args.request_interval_seconds,
    )
    today_new: set[str] = set()
    run_started = now_utc()

    # Offline first: no API usage.
    analyze_existing_state(
        x_mints_by_wallet,
        progress,
        meta,
        today_new,
    )
    save_state(progress, meta)

    # If a previous run stopped during token verification, finish that first.
    resume_incomplete_token_verifications(
        client,
        x_mints_by_wallet,
        progress,
        meta,
        today_new,
    )

    # Directly verify Birdeye's known 50K candidates with tokenAddress.
    verify_known_birdeye_candidates(
        client,
        known_candidates,
        x_mints_by_wallet,
        progress,
        meta,
        today_new,
    )

    # Then scan the 157 not-complete-in-either-provider wallets round-robin.
    scan_discovery_round_robin(
        client,
        priority,
        x_mints_by_wallet,
        progress,
        meta,
        today_new,
    )

    build_outputs(
        x_mints_by_wallet,
        x_best,
        progress,
        meta,
        priority,
        today_new,
        budget,
        run_started,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
