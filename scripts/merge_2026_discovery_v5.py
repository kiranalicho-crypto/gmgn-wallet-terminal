"""Merge monthly Pump.fun discovery chunks into one 2026 universe."""

from __future__ import annotations

import argparse
import csv
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Sequence

SCRIPT_VERSION = "2026-07-19-discovery-merge-v5"
EXPECTED_DEFAULT = [
    "2026-01",
    "2026-02",
    "2026-03",
    "2026-04",
    "2026-05",
    "2026-06",
    "2026-07-partial",
]

FIELDS = [
    "mint",
    "created_at_utc",
    "create_tx",
    "creator",
    "creator_source",
    "metadata_creator_count",
    "name",
    "symbol",
    "uri",
    "update_authority",
    "migrated_at_utc",
    "migration_block_slot",
    "migration_tx",
    "migration_instruction_index",
    "migration_type",
    "migrator",
    "transaction_status",
    "transaction_error",
    "transaction_success",
    "in_scope_create",
    "scope_exclusion_reason",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--version", action="store_true")
    parser.add_argument("--chunks-root", default="artifacts/chunks")
    parser.add_argument("--output-dir", default="artifacts/discovery-merged")
    parser.add_argument(
        "--expected-chunks",
        default=",".join(EXPECTED_DEFAULT),
        help="Comma-separated exact chunk ids.",
    )
    parser.add_argument("--strict", action="store_true")
    return parser.parse_args()


def read_csv(path: Path) -> list[dict[str, str]]:
    if not path.is_file():
        return []
    with path.open(encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def write_csv(
    path: Path, rows: Iterable[dict[str, Any]], fields: Sequence[str]
) -> int:
    materialized = list(rows)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle, fieldnames=list(fields), extrasaction="ignore"
        )
        writer.writeheader()
        for row in materialized:
            writer.writerow({field: row.get(field, "") for field in fields})
    return len(materialized)


def bool_text(value: Any) -> bool:
    return str(value).strip().lower() in {"true", "1", "yes"}


def merge_chunks(
    chunks_root: Path, expected_chunks: Sequence[str]
) -> tuple[dict[str, Any], dict[str, list[dict[str, Any]]]]:
    manifests_by_chunk: dict[str, tuple[Path, dict[str, Any]]] = {}
    duplicate_manifest_chunks: list[str] = []

    for manifest_path in sorted(chunks_root.rglob("discovery_manifest.json")):
        data = json.loads(manifest_path.read_text(encoding="utf-8"))
        chunk_id = str(data.get("chunk_id") or "")
        if not chunk_id:
            continue
        if chunk_id in manifests_by_chunk:
            duplicate_manifest_chunks.append(chunk_id)
            continue
        manifests_by_chunk[chunk_id] = (manifest_path.parent, data)

    expected = list(expected_chunks)
    missing_chunks = [chunk for chunk in expected if chunk not in manifests_by_chunk]
    unexpected_chunks = sorted(set(manifests_by_chunk) - set(expected))
    incomplete_chunks = [
        chunk
        for chunk in expected
        if chunk in manifests_by_chunk
        and manifests_by_chunk[chunk][1].get("complete") is not True
    ]

    scope_events: list[dict[str, Any]] = []
    out_of_scope: list[dict[str, Any]] = []
    failed_events: list[dict[str, Any]] = []
    chunk_duplicates: list[dict[str, Any]] = []
    unresolved: list[dict[str, Any]] = []
    chunk_summaries: list[dict[str, Any]] = []

    total_cost = {
        "estimated_main_query_bytes": 0,
        "actual_main_query_bytes": 0,
        "status_query_bytes": 0,
        "fallback_query_bytes": 0,
        "total_reported_bytes": 0,
    }

    for chunk in expected:
        item = manifests_by_chunk.get(chunk)
        if item is None:
            continue
        directory, manifest = item
        counts = manifest.get("counts") or {}
        cost = manifest.get("cost") or {}
        chunk_summaries.append(
            {
                "chunk_id": chunk,
                "migration_start_date": manifest.get("migration_start_date", ""),
                "migration_end_date": manifest.get("migration_end_date", ""),
                "complete": manifest.get("complete") is True,
                "raw_migration_event_count": counts.get(
                    "raw_migration_event_count", 0
                ),
                "in_scope_2026_mint_count": counts.get(
                    "in_scope_2026_mint_count", 0
                ),
                "unique_creator_count": counts.get("unique_creator_count", 0),
                "unresolved_count": counts.get("unresolved_count", 0),
                "total_reported_bytes": cost.get("total_reported_bytes", 0),
            }
        )
        for key in total_cost:
            total_cost[key] += int(cost.get(key, 0) or 0)

        for row in read_csv(directory / "scope_2026_migrated_tokens.csv"):
            row["source_chunk"] = chunk
            scope_events.append(row)
        for row in read_csv(directory / "out_of_scope_migrations.csv"):
            row["source_chunk"] = chunk
            out_of_scope.append(row)
        for row in read_csv(directory / "failed_migration_events.csv"):
            row["source_chunk"] = chunk
            failed_events.append(row)
        for row in read_csv(directory / "duplicate_migration_events.csv"):
            row["source_chunk"] = chunk
            chunk_duplicates.append(row)
        for row in read_csv(directory / "unresolved_discovery.csv"):
            row["source_chunk"] = chunk
            unresolved.append(row)

    # A permissionless/idempotent migrate can appear in more than one month.
    # Keep the earliest successful migration globally and report the rest.
    unique_scope: dict[str, dict[str, Any]] = {}
    cross_chunk_duplicates: list[dict[str, Any]] = []
    for row in sorted(
        scope_events,
        key=lambda item: (
            str(item.get("migrated_at_utc") or ""),
            str(item.get("migration_tx") or ""),
        ),
    ):
        mint = str(row.get("mint") or "")
        if not mint:
            unresolved.append(
                {
                    "mint": "",
                    "reason": "merged_scope_mint_missing",
                    "detail": str(row.get("migration_tx") or ""),
                    "source_chunk": str(row.get("source_chunk") or ""),
                }
            )
            continue
        if not bool_text(row.get("transaction_success")):
            unresolved.append(
                {
                    "mint": mint,
                    "reason": "merged_scope_not_successful",
                    "detail": str(row.get("migration_tx") or ""),
                    "source_chunk": str(row.get("source_chunk") or ""),
                }
            )
            continue
        if mint in unique_scope:
            cross_chunk_duplicates.append(row)
            continue
        unique_scope[mint] = row

    unique_rows = list(unique_scope.values())
    unique_creators = {
        str(row.get("creator") or "") for row in unique_rows if row.get("creator")
    }

    complete = (
        not missing_chunks
        and not unexpected_chunks
        and not duplicate_manifest_chunks
        and not incomplete_chunks
        and not unresolved
        and bool(unique_rows)
    )

    manifest = {
        "script_version": SCRIPT_VERSION,
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "expected_chunks": expected,
        "found_chunks": sorted(manifests_by_chunk),
        "missing_chunks": missing_chunks,
        "unexpected_chunks": unexpected_chunks,
        "duplicate_manifest_chunks": sorted(set(duplicate_manifest_chunks)),
        "incomplete_chunks": incomplete_chunks,
        "chunk_summaries": chunk_summaries,
        "cost": total_cost,
        "counts": {
            "merged_scope_event_count": len(scope_events),
            "unique_2026_migrated_mint_count": len(unique_rows),
            "unique_creator_count": len(unique_creators),
            "within_chunk_duplicate_event_count": len(chunk_duplicates),
            "cross_chunk_duplicate_event_count": len(cross_chunk_duplicates),
            "out_of_scope_event_count": len(out_of_scope),
            "failed_migration_event_count": len(failed_events),
            "unresolved_count": len(unresolved),
        },
        "coverage": {
            "scope_start_date": "2026-01-01",
            "scope_end_date": "2026-07-18",
            "migration_universe": "canonical_migrate_plus_historical_withdraw",
            "buy_sell_rows_downloaded": False,
            "global_ath_completeness_claimed": False,
        },
        "complete": complete,
        "next_stage": (
            "Size creator-level GMGN ATH shards from the measured unique_creator_count; "
            "do not choose concurrency before this count and a small API benchmark."
        ),
    }
    outputs = {
        "unique_scope": unique_rows,
        "out_of_scope": out_of_scope,
        "failed_events": failed_events,
        "within_chunk_duplicates": chunk_duplicates,
        "cross_chunk_duplicates": cross_chunk_duplicates,
        "unresolved": unresolved,
        "chunk_summaries": chunk_summaries,
    }
    return manifest, outputs


def main() -> int:
    args = parse_args()
    if args.version:
        print(SCRIPT_VERSION)
        return 0

    expected = [value.strip() for value in args.expected_chunks.split(",") if value.strip()]
    chunks_root = Path(args.chunks_root)
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    manifest, outputs = merge_chunks(chunks_root, expected)
    write_csv(
        output_dir / "scope_2026_migrated_tokens.csv",
        outputs["unique_scope"],
        [*FIELDS, "source_chunk"],
    )
    write_csv(
        output_dir / "out_of_scope_migrations.csv",
        outputs["out_of_scope"],
        [*FIELDS, "source_chunk"],
    )
    write_csv(
        output_dir / "failed_migration_events.csv",
        outputs["failed_events"],
        [*FIELDS, "source_chunk"],
    )
    write_csv(
        output_dir / "duplicate_migration_events.csv",
        [
            *outputs["within_chunk_duplicates"],
            *outputs["cross_chunk_duplicates"],
        ],
        [*FIELDS, "source_chunk"],
    )
    write_csv(
        output_dir / "unresolved_discovery.csv",
        outputs["unresolved"],
        ["mint", "reason", "detail", "source_chunk"],
    )
    write_csv(
        output_dir / "chunk_summary.csv",
        outputs["chunk_summaries"],
        [
            "chunk_id",
            "migration_start_date",
            "migration_end_date",
            "complete",
            "raw_migration_event_count",
            "in_scope_2026_mint_count",
            "unique_creator_count",
            "unresolved_count",
            "total_reported_bytes",
        ],
    )
    (output_dir / "discovery_manifest.json").write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    print(json.dumps(manifest, ensure_ascii=False, indent=2))

    if args.strict and manifest["complete"] is not True:
        print("DISCOVERY_2026_MERGE_INCOMPLETE")
        return 2
    print("DISCOVERY_2026_MERGE_OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
