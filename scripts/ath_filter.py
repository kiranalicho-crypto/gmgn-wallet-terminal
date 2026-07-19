"""Enrich migrated Pump.fun mints with GMGN ATH market-cap data."""

from __future__ import annotations

import argparse
import csv
import json
import os
import sys
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

try:
    from gmgn_client import GmgnClient, GmgnError, as_number
except ImportError:  # pytest/package import
    from scripts.gmgn_client import GmgnClient, GmgnError, as_number


def arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--creates", required=True)
    parser.add_argument("--migrations", required=True)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--min-ath-mc", type=float, default=10_000_000)
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


def main() -> int:
    args = arguments()
    creates_path = Path(args.creates)
    migrations_path = Path(args.migrations)
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    cache_dir = output_dir / "creator_cache"
    cache_dir.mkdir(exist_ok=True)

    creates = {row["mint"]: row for row in read_csv(creates_path)}
    migration_rows = read_csv(migrations_path)
    migrations = {row["mint"]: row for row in migration_rows if row.get("mint")}

    targets: dict[str, dict[str, str]] = {}
    for mint, migration in migrations.items():
        create = creates.get(mint)
        if create:
            targets[mint] = {**create, **{
                "migrated_at_utc": migration.get("migrated_at_utc", ""),
                "migration_tx": migration.get("tx_signature", ""),
            }}

    by_creator: dict[str, list[str]] = defaultdict(list)
    unresolved: list[dict[str, Any]] = []
    for mint, row in targets.items():
        creator = row.get("creator", "")
        if creator:
            by_creator[creator].append(mint)
        else:
            unresolved.append({"mint": mint, "reason": "missing_creator"})

    client = GmgnClient()
    enriched: list[dict[str, Any]] = []

    for index, (creator, mints) in enumerate(sorted(by_creator.items()), 1):
        print(f"ATH_CREATOR {index}/{len(by_creator)} {creator}", flush=True)
        cache_path = cache_dir / f"{creator}.json"
        try:
            if cache_path.exists():
                data = json.loads(cache_path.read_text(encoding="utf-8"))
            else:
                data = client.created_tokens(creator)
                cache_path.write_text(
                    json.dumps(data, ensure_ascii=False, indent=2),
                    encoding="utf-8",
                )
        except (GmgnError, json.JSONDecodeError) as exc:
            for mint in mints:
                unresolved.append({
                    "mint": mint,
                    "creator": creator,
                    "reason": "creator_query_failed",
                    "detail": str(exc),
                })
            continue

        tokens = data.get("tokens")
        if not isinstance(tokens, list):
            for mint in mints:
                unresolved.append({
                    "mint": mint,
                    "creator": creator,
                    "reason": "tokens_array_missing",
                })
            continue

        token_map = {
            str(item.get("token_address")): item
            for item in tokens
            if isinstance(item, dict) and item.get("token_address")
        }
        open_count = int(as_number(data.get("open_count")) or 0)
        truncated = open_count > len(tokens) or len(tokens) >= 100
        returned_ath_values = [
            value
            for value in (
                as_number(item.get("token_ath_mc"))
                for item in tokens
                if isinstance(item, dict)
            )
            if value is not None
        ]
        lowest_returned_ath = (
            min(returned_ath_values) if returned_ath_values else None
        )

        for mint in mints:
            item = token_map.get(mint)
            if item is None:
                # The command is explicitly sorted by ATH descending. When the
                # returned rank cutoff is already below our threshold, omitted
                # lower-ranked mints cannot pass the threshold and are safely
                # classified as non-eligible without inventing an exact ATH.
                if (
                    truncated
                    and lowest_returned_ath is not None
                    and lowest_returned_ath < args.min_ath_mc
                ):
                    source = targets[mint]
                    enriched.append({
                        "mint": mint,
                        "creator": creator,
                        "created_at_utc": source.get("created_at_utc", ""),
                        "migrated_at_utc": source.get("migrated_at_utc", ""),
                        "migration_tx": source.get("migration_tx", ""),
                        "symbol": "",
                        "ath_market_cap_usd": "",
                        "current_market_cap_usd": "",
                        "is_open": True,
                        "launchpad_platform": "Pump.fun",
                        "is_pump": True,
                        "ath_pass": False,
                        "ath_source": "gmgn_creator_rank_cutoff",
                        "ath_upper_bound_usd": lowest_returned_ath,
                    })
                    continue

                unresolved.append({
                    "mint": mint,
                    "creator": creator,
                    "reason": (
                        "creator_tokens_truncated_above_threshold"
                        if truncated
                        else "mint_not_returned"
                    ),
                    "creator_open_count": open_count,
                    "returned_token_count": len(tokens),
                    "lowest_returned_ath": lowest_returned_ath,
                })
                continue

            ath_mc = as_number(item.get("token_ath_mc"))
            if ath_mc is None:
                unresolved.append({
                    "mint": mint,
                    "creator": creator,
                    "reason": "ath_mc_missing",
                })
                continue

            source = targets[mint]
            enriched.append({
                "mint": mint,
                "creator": creator,
                "created_at_utc": source.get("created_at_utc", ""),
                "migrated_at_utc": source.get("migrated_at_utc", ""),
                "migration_tx": source.get("migration_tx", ""),
                "symbol": item.get("symbol", ""),
                "ath_market_cap_usd": ath_mc,
                "current_market_cap_usd": as_number(item.get("market_cap")),
                "is_open": item.get("is_open"),
                "launchpad_platform": item.get("launchpad_platform", ""),
                "is_pump": item.get("is_pump"),
                "ath_pass": ath_mc >= args.min_ath_mc,
                "ath_source": "gmgn_created_tokens",
            })

    eligible = [row for row in enriched if row["ath_pass"]]
    fields = [
        "mint", "creator", "created_at_utc", "migrated_at_utc",
        "migration_tx", "symbol", "ath_market_cap_usd",
        "current_market_cap_usd", "is_open", "launchpad_platform",
        "is_pump", "ath_pass", "ath_source", "ath_upper_bound_usd",
    ]
    write_csv(output_dir / "all_migrated_token_ath.csv", enriched, fields)
    write_csv(output_dir / "eligible_ath_tokens.csv", eligible, fields)
    write_csv(
        output_dir / "unresolved_ath_tokens.csv",
        unresolved,
        [
            "mint", "creator", "reason", "detail",
            "creator_open_count", "returned_token_count",
            "lowest_returned_ath",
        ],
    )

    manifest = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "min_ath_market_cap_usd": args.min_ath_mc,
        "create_count": len(creates),
        "migration_event_count": len(migration_rows),
        "candidate_migrated_mint_count": len(targets),
        "creator_query_count": len(by_creator),
        "ath_resolved_count": len(enriched),
        "ath_eligible_count": len(eligible),
        "ath_unresolved_count": len(unresolved),
        "strict": args.strict,
        "complete": len(unresolved) == 0,
    }
    (output_dir / "ath_manifest.json").write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    print(json.dumps(manifest, ensure_ascii=False, indent=2))

    if args.strict and unresolved:
        print("ATH_UNRESOLVED: eksik tokenlar ayrı CSV'ye yazıldı.", file=sys.stderr)
        return 2
    print("ATH_FILTER_OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
