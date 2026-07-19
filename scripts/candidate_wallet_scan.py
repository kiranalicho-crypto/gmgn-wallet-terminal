"""Screen early Pump.fun buyers against the fixed X/Y wallet criteria."""

from __future__ import annotations

import argparse
import csv
import json
import sys
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

try:
    from gmgn_client import (
        GmgnClient,
        GmgnError,
        as_number,
        token_address,
    )
except ImportError:  # pytest/package import
    from scripts.gmgn_client import (
        GmgnClient,
        GmgnError,
        as_number,
        token_address,
    )


def arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--eligible-tokens", required=True)
    parser.add_argument("--first-buys", required=True)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--min-ath-mc", type=float, default=10_000_000)
    parser.add_argument("--max-early-buy-mc", type=float, default=50_000)
    parser.add_argument("--min-x-realized-multiple", type=float, default=25)
    parser.add_argument("--min-y-realized-profit", type=float, default=75_000)
    parser.add_argument("--strict", action="store_true")
    return parser.parse_args()


def read_csv(path: Path) -> list[dict[str, str]]:
    with path.open(encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def write_csv(path: Path, rows: list[dict[str, Any]], fields: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def event_type(event: dict[str, Any]) -> str:
    return str(
        event.get("event_type")
        or event.get("type")
        or event.get("activity_type")
        or ""
    ).lower()


def tx_hash(event: dict[str, Any]) -> str:
    return str(
        event.get("tx_hash")
        or event.get("transaction_hash")
        or event.get("signature")
        or ""
    )


def timestamp(event: dict[str, Any]) -> float:
    return as_number(event.get("timestamp")) or 0.0


def event_market_cap(event: dict[str, Any]) -> float | None:
    token = event.get("token")
    if not isinstance(token, dict):
        return None
    supply = as_number(token.get("total_supply"))
    price_usd = as_number(event.get("price_usd"))
    if price_usd is None:
        price_usd = as_number(event.get("price"))
    if supply is None or price_usd is None:
        return None
    return supply * price_usd


def realized_metrics(holding: dict[str, Any]) -> dict[str, float | None]:
    sold_income = as_number(holding.get("history_sold_income"))
    realized_profit = as_number(holding.get("realized_profit"))
    bought_cost = as_number(holding.get("history_bought_cost"))
    multiple: float | None = None
    realized_cost_basis: float | None = None

    if sold_income is not None and realized_profit is not None:
        realized_cost_basis = sold_income - realized_profit
        if realized_cost_basis > 0:
            multiple = sold_income / realized_cost_basis

    return {
        "history_sold_income": sold_income,
        "realized_profit": realized_profit,
        "history_bought_cost": bought_cost,
        "realized_cost_basis": realized_cost_basis,
        "realized_multiple": multiple,
    }


def main() -> int:
    args = arguments()
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    cache_dir = output_dir / "wallet_cache"
    cache_dir.mkdir(exist_ok=True)

    eligible_rows = read_csv(Path(args.eligible_tokens))
    eligible = {row["mint"]: row for row in eligible_rows}
    first_buys = [
        row for row in read_csv(Path(args.first_buys))
        if row.get("mint") in eligible and row.get("wallet")
    ]
    by_wallet: dict[str, list[dict[str, str]]] = defaultdict(list)
    for row in first_buys:
        by_wallet[row["wallet"]].append(row)

    client = GmgnClient()
    finalists: list[dict[str, Any]] = []
    x_screened: list[dict[str, Any]] = []
    unresolved: list[dict[str, Any]] = []

    for wallet_index, (wallet, pairs) in enumerate(sorted(by_wallet.items()), 1):
        print(f"WALLET {wallet_index}/{len(by_wallet)} {wallet}", flush=True)
        holdings_path = cache_dir / f"{wallet}_holdings.json"
        try:
            if holdings_path.exists():
                holdings = json.loads(holdings_path.read_text(encoding="utf-8"))
            else:
                holdings = client.holdings_all(wallet)
                holdings_path.write_text(
                    json.dumps(holdings, ensure_ascii=False, indent=2),
                    encoding="utf-8",
                )
        except (GmgnError, json.JSONDecodeError) as exc:
            unresolved.append({
                "wallet": wallet,
                "reason": "holdings_query_failed",
                "detail": str(exc),
            })
            continue

        holding_map = {
            token_address(item): item
            for item in holdings
            if token_address(item)
        }
        y_candidates: list[tuple[str, dict[str, float | None]]] = []
        for item in holdings:
            mint = token_address(item)
            metrics = realized_metrics(item)
            profit = metrics["realized_profit"]
            if mint and profit is not None and profit >= args.min_y_realized_profit:
                y_candidates.append((mint, metrics))
        y_candidates.sort(
            key=lambda pair: pair[1]["realized_profit"] or 0,
            reverse=True,
        )

        for pair in pairs:
            x_mint = pair["mint"]
            holding = holding_map.get(x_mint)
            if holding is None:
                unresolved.append({
                    "wallet": wallet,
                    "x_mint": x_mint,
                    "reason": "x_holding_not_returned",
                })
                continue

            metrics = realized_metrics(holding)
            multiple = metrics["realized_multiple"]
            x_record: dict[str, Any] = {
                "wallet": wallet,
                "x_mint": x_mint,
                "x_ath_market_cap_usd": eligible[x_mint].get(
                    "ath_market_cap_usd", ""
                ),
                "bigquery_first_buy_tx": pair.get("first_buy_tx", ""),
                "bigquery_first_buy_at_utc": pair.get("first_buy_at_utc", ""),
                **{f"x_{key}": value for key, value in metrics.items()},
            }

            if multiple is None or multiple < args.min_x_realized_multiple:
                x_record["x_pass"] = False
                x_record["reason"] = "x_realized_multiple_below_threshold"
                x_screened.append(x_record)
                continue

            activity_path = cache_dir / f"{wallet}_{x_mint}_activity.json"
            try:
                if activity_path.exists():
                    activities = json.loads(activity_path.read_text(encoding="utf-8"))
                else:
                    activities = client.activity_all(wallet, x_mint)
                    activity_path.write_text(
                        json.dumps(activities, ensure_ascii=False, indent=2),
                        encoding="utf-8",
                    )
            except (GmgnError, json.JSONDecodeError) as exc:
                unresolved.append({
                    "wallet": wallet,
                    "x_mint": x_mint,
                    "reason": "x_activity_query_failed",
                    "detail": str(exc),
                })
                continue

            buys = sorted(
                (
                    item for item in activities
                    if event_type(item) == "buy"
                    and token_address(item) == x_mint
                ),
                key=timestamp,
            )
            if not buys:
                unresolved.append({
                    "wallet": wallet,
                    "x_mint": x_mint,
                    "reason": "real_buy_not_found",
                })
                continue

            first_buy = buys[0]
            first_buy_mc = event_market_cap(first_buy)
            if first_buy_mc is None:
                unresolved.append({
                    "wallet": wallet,
                    "x_mint": x_mint,
                    "reason": "first_buy_market_cap_unavailable",
                })
                continue

            gmgn_first_tx = tx_hash(first_buy)
            expected_tx = pair.get("first_buy_tx", "")
            if expected_tx and gmgn_first_tx and expected_tx != gmgn_first_tx:
                unresolved.append({
                    "wallet": wallet,
                    "x_mint": x_mint,
                    "reason": "bigquery_gmgn_first_buy_mismatch",
                    "detail": f"bigquery={expected_tx}; gmgn={gmgn_first_tx}",
                })
                continue

            x_record.update({
                "gmgn_first_buy_tx": gmgn_first_tx,
                "gmgn_first_buy_timestamp": timestamp(first_buy),
                "first_buy_market_cap_usd": first_buy_mc,
                "x_pass": first_buy_mc < args.max_early_buy_mc,
                "reason": (
                    "pass" if first_buy_mc < args.max_early_buy_mc
                    else "first_buy_market_cap_above_threshold"
                ),
            })
            x_screened.append(x_record)
            if not x_record["x_pass"]:
                continue

            valid_y = [pair for pair in y_candidates if pair[0] != x_mint]
            if not valid_y:
                continue
            y_mint, y_metrics = valid_y[0]
            finalists.append({
                **x_record,
                "y_mint": y_mint,
                "y_realized_profit": y_metrics["realized_profit"],
                "y_history_sold_income": y_metrics["history_sold_income"],
                "y_history_bought_cost": y_metrics["history_bought_cost"],
                "criteria_version": "ATH10M_MC50K_X25_Y75K_v1",
                "verification_status": "gmgn_screened_bigquery_buy_matched",
            })

    finalist_fields = [
        "wallet", "x_mint", "x_ath_market_cap_usd",
        "bigquery_first_buy_tx", "gmgn_first_buy_tx",
        "first_buy_market_cap_usd", "x_realized_multiple",
        "x_realized_profit", "x_history_sold_income",
        "x_realized_cost_basis", "y_mint", "y_realized_profit",
        "y_history_sold_income", "y_history_bought_cost",
        "criteria_version", "verification_status",
    ]
    write_csv(output_dir / "wallet_finalists.csv", finalists, finalist_fields)
    write_csv(
        output_dir / "x_screening.csv",
        x_screened,
        list(dict.fromkeys(finalist_fields + ["x_pass", "reason"])),
    )
    write_csv(
        output_dir / "unresolved_wallet_tokens.csv",
        unresolved,
        ["wallet", "x_mint", "reason", "detail"],
    )

    manifest = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "criteria": {
            "min_ath_market_cap_usd": args.min_ath_mc,
            "max_first_buy_market_cap_usd": args.max_early_buy_mc,
            "min_x_realized_multiple": args.min_x_realized_multiple,
            "min_y_realized_profit_usd": args.min_y_realized_profit,
            "x_and_y_must_differ": True,
        },
        "eligible_token_count": len(eligible),
        "candidate_wallet_token_count": len(first_buys),
        "candidate_wallet_count": len(by_wallet),
        "x_screened_count": len(x_screened),
        "finalist_count": len(finalists),
        "unresolved_count": len(unresolved),
        "strict": args.strict,
        "complete": len(unresolved) == 0,
        "note": (
            "Finalistler GMGN PnL ekranından ve BigQuery ile eşleşen gerçek "
            "buy eventinden süzülür. Nihai zincir muhasebesi ayrı doğrulama "
            "katmanıdır."
        ),
    }
    (output_dir / "candidate_manifest.json").write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    print(json.dumps(manifest, ensure_ascii=False, indent=2))

    if args.strict and unresolved:
        print("CANDIDATE_UNRESOLVED: detaylar CSV'de.", file=sys.stderr)
        return 2
    print("CANDIDATE_SCAN_OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
