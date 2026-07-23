#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import gzip
import json
import os
import time
from pathlib import Path
from typing import Any

import requests

VERSION = "2026-07-23-helius-moralis-probe-v1"
BASE = "https://api.helius.xyz/v1/wallet"


def read_csv(path: str) -> list[dict[str, str]]:
    with open(path, newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def write_csv(path: Path, rows: list[dict[str, Any]], fields: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        w.writeheader()
        w.writerows(rows)


def fetch_wallet(api_key: str, wallet: str, rps: float, max_pages: int) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    headers = {"X-Api-Key": api_key}
    before = ""
    txs: list[dict[str, Any]] = []
    pages: list[dict[str, Any]] = []
    interval = 1.0 / max(rps, 0.1)

    for page_no in range(1, max_pages + 1):
        params: dict[str, Any] = {
            "limit": 100,
            "type": "SWAP",
            "tokenAccounts": "balanceChanged",
        }
        if before:
            params["before"] = before

        response = requests.get(
            f"{BASE}/{wallet}/history",
            headers=headers,
            params=params,
            timeout=60,
        )
        if response.status_code != 200:
            raise RuntimeError(
                f"wallet={wallet} page={page_no} HTTP {response.status_code}: {response.text[:1000]}"
            )
        payload = response.json()
        data = payload.get("data") or []
        pagination = payload.get("pagination") or {}
        pages.append({
            "wallet": wallet,
            "page": page_no,
            "item_count": len(data),
            "has_more": bool(pagination.get("hasMore")),
            "next_cursor": str(pagination.get("nextCursor") or ""),
        })
        txs.extend(x for x in data if isinstance(x, dict))

        if not pagination.get("hasMore"):
            break
        before = str(pagination.get("nextCursor") or "")
        if not before:
            break
        time.sleep(interval)

    return txs, pages


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--version", action="store_true")
    p.add_argument("--wallets", default="data/helius_probe_wallets_5.csv")
    p.add_argument("--baseline", default="data/moralis_probe_baseline_5_wallets.csv")
    p.add_argument("--output-dir", default="output-helius-probe")
    p.add_argument("--requests-per-second", type=float, default=1.0)
    p.add_argument("--max-pages-per-wallet", type=int, default=25)
    args = p.parse_args()

    if args.version:
        print(VERSION)
        return

    api_key = os.environ.get("HELIUS_API_KEY", "").strip()
    if not api_key:
        raise SystemExit("HELIUS_API_KEY is missing")

    out = Path(args.output_dir)
    out.mkdir(parents=True, exist_ok=True)

    wallets = [r["wallet"] for r in read_csv(args.wallets)]
    baseline = read_csv(args.baseline)
    baseline_by_wallet: dict[str, set[str]] = {}
    for wallet in wallets:
        baseline_by_wallet[wallet] = {
            str(r["transaction_hash"])
            for r in baseline
            if r["wallet"] == wallet and r.get("transaction_hash")
        }

    all_txs: list[dict[str, Any]] = []
    page_rows: list[dict[str, Any]] = []
    compare_rows: list[dict[str, Any]] = []

    for index, wallet in enumerate(wallets, 1):
        print(f"[{index}/{len(wallets)}] {wallet}", flush=True)
        txs, pages = fetch_wallet(
            api_key,
            wallet,
            args.requests_per_second,
            args.max_pages_per_wallet,
        )
        page_rows.extend(pages)
        helius_sigs = {str(tx.get("signature") or "") for tx in txs if tx.get("signature")}
        moralis_sigs = baseline_by_wallet[wallet]
        overlap = helius_sigs & moralis_sigs
        union = helius_sigs | moralis_sigs

        compare_rows.append({
            "wallet": wallet,
            "helius_swap_transactions": len(helius_sigs),
            "moralis_swap_transactions": len(moralis_sigs),
            "signature_overlap": len(overlap),
            "helius_only": len(helius_sigs - moralis_sigs),
            "moralis_only": len(moralis_sigs - helius_sigs),
            "jaccard_signature_coverage": (len(overlap) / len(union)) if union else 1.0,
            "moralis_signature_recall_in_helius": (len(overlap) / len(moralis_sigs)) if moralis_sigs else 1.0,
            "helius_pages": len(pages),
            "helius_history_complete": bool(pages and not pages[-1]["has_more"]),
        })

        for tx in txs:
            all_txs.append({
                "wallet": wallet,
                "signature": tx.get("signature"),
                "slot": tx.get("slot"),
                "timestamp": tx.get("timestamp"),
                "fee": tx.get("fee"),
                "fee_payer": tx.get("feePayer"),
                "error": tx.get("error"),
                "balance_changes_json": json.dumps(tx.get("balanceChanges") or [], separators=(",", ":")),
            })

    write_csv(
        out / "helius_moralis_wallet_comparison.csv",
        compare_rows,
        [
            "wallet", "helius_swap_transactions", "moralis_swap_transactions",
            "signature_overlap", "helius_only", "moralis_only",
            "jaccard_signature_coverage", "moralis_signature_recall_in_helius",
            "helius_pages", "helius_history_complete",
        ],
    )
    write_csv(
        out / "helius_probe_pages.csv",
        page_rows,
        ["wallet", "page", "item_count", "has_more", "next_cursor"],
    )
    with gzip.open(out / "helius_probe_transactions.jsonl.gz", "wt", encoding="utf-8") as f:
        for row in all_txs:
            f.write(json.dumps(row, ensure_ascii=False) + "\n")

    avg_recall = (
        sum(float(r["moralis_signature_recall_in_helius"]) for r in compare_rows) / len(compare_rows)
        if compare_rows else 0.0
    )
    complete_wallets = sum(bool(r["helius_history_complete"]) for r in compare_rows)
    report = {
        "script_version": VERSION,
        "wallet_count": len(wallets),
        "helius_api_calls": len(page_rows),
        "helius_credit_estimate": len(page_rows) * 100,
        "complete_wallets": complete_wallets,
        "average_moralis_signature_recall_in_helius": avg_recall,
        "pass_for_full_scan": complete_wallets == len(wallets) and avg_recall >= 0.90,
        "important_note": "This probe validates API access and swap-signature coverage. It does not yet claim final USD PnL equivalence.",
    }
    (out / "helius_moralis_probe_report.json").write_text(
        json.dumps(report, indent=2), encoding="utf-8"
    )
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
