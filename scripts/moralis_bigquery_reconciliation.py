"""Compare Moralis Pump.fun graduated mints with on-chain migrations in BigQuery.

Modes:
- --dry-run-only: estimates the bytes for the seven-month migration query.
- normal mode: downloads migration instructions, verifies transaction success,
  deduplicates mints, and compares them with the Moralis CSV.GZ committed in data/.
"""

from __future__ import annotations

import argparse
import csv
import gzip
import hashlib
import json
import os
import sys
import time
from collections import defaultdict
from datetime import date, datetime, time as dt_time, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable

from google.api_core import exceptions as gexc
from google.cloud import bigquery

SCRIPT_VERSION = "2026-07-20-moralis-bigquery-reconciliation-v2"

PUMP_PROGRAM_ID = "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P"
MIGRATE_DATA_B58 = "Mjb79tJwDb7"
WITHDRAW_DATA_B58 = "Xd2GMpFXgQ1"

INSTRUCTIONS_TABLE = "bigquery-public-data.crypto_solana_mainnet_us.Instructions"
TRANSACTIONS_TABLE = "bigquery-public-data.crypto_solana_mainnet_us.Transactions"

TRANSIENT_ERRORS = (
    gexc.TooManyRequests,
    gexc.ServiceUnavailable,
    gexc.InternalServerError,
    gexc.BadGateway,
    gexc.GatewayTimeout,
    gexc.DeadlineExceeded,
)


class ReconciliationError(RuntimeError):
    pass


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--version", action="store_true")
    parser.add_argument("--project")
    parser.add_argument("--start-date", default="2026-01-01")
    parser.add_argument("--end-date", default="2026-07-18")
    parser.add_argument(
        "--moralis-file",
        default="data/moralis_graduated_mints_2026.csv",
    )
    parser.add_argument(
        "--output-dir",
        default="artifacts/moralis-bigquery-reconciliation",
    )
    parser.add_argument(
        "--maximum-bytes-billed",
        type=int,
        default=700_000_000_000,
    )
    parser.add_argument(
        "--status-maximum-bytes-billed-per-query",
        type=int,
        default=10_000_000_000,
    )
    parser.add_argument("--dry-run-only", action="store_true")
    parser.add_argument("--strict", action="store_true")
    return parser.parse_args()


def utc_bounds(start_text: str, end_text: str) -> tuple[datetime, datetime]:
    start_day = date.fromisoformat(start_text)
    end_day = date.fromisoformat(end_text)
    if end_day < start_day:
        raise ValueError("Bitiş tarihi başlangıç tarihinden önce olamaz.")
    start = datetime.combine(start_day, dt_time.min, tzinfo=timezone.utc)
    end = datetime.combine(
        end_day + timedelta(days=1),
        dt_time.min,
        tzinfo=timezone.utc,
    )
    return start, end


def dump_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def write_csv(path: Path, rows: Iterable[dict[str, Any]], fields: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def retry(operation, label: str, attempts: int = 7):
    delay = 4.0
    for attempt in range(1, attempts + 1):
        try:
            return operation()
        except TRANSIENT_ERRORS as exc:
            if attempt == attempts:
                raise
            print(
                f"RETRY {label} {attempt}/{attempts}: {type(exc).__name__}: {exc}",
                file=sys.stderr,
                flush=True,
            )
            time.sleep(delay)
            delay = min(60.0, delay * 2)


def migration_sql() -> str:
    return f"""
    SELECT
      block_timestamp AS migrated_at_utc,
      block_slot AS migration_block_slot,
      tx_signature AS migration_tx,
      index AS migration_instruction_index,
      data AS migration_data,
      CASE
        WHEN data = @migrate_data THEN accounts[SAFE_OFFSET(2)]
        WHEN data = @withdraw_data THEN accounts[SAFE_OFFSET(1)]
      END AS mint,
      CASE
        WHEN data = @migrate_data THEN 'migrate'
        WHEN data = @withdraw_data THEN 'withdraw'
      END AS migration_type
    FROM `{INSTRUCTIONS_TABLE}`
    WHERE block_timestamp >= @start_timestamp
      AND block_timestamp < @end_timestamp
      AND program_id = @program_id
      AND data IN UNNEST(@migration_values)
    """


def migration_config(
    start: datetime,
    end: datetime,
    *,
    dry_run: bool,
    maximum_bytes_billed: int,
) -> bigquery.QueryJobConfig:
    config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter(
                "start_timestamp", "TIMESTAMP", start
            ),
            bigquery.ScalarQueryParameter(
                "end_timestamp", "TIMESTAMP", end
            ),
            bigquery.ScalarQueryParameter(
                "program_id", "STRING", PUMP_PROGRAM_ID
            ),
            bigquery.ScalarQueryParameter(
                "migrate_data", "STRING", MIGRATE_DATA_B58
            ),
            bigquery.ScalarQueryParameter(
                "withdraw_data", "STRING", WITHDRAW_DATA_B58
            ),
            bigquery.ArrayQueryParameter(
                "migration_values",
                "STRING",
                [MIGRATE_DATA_B58, WITHDRAW_DATA_B58],
            ),
        ],
        dry_run=dry_run,
        use_query_cache=not dry_run,
    )
    if not dry_run:
        config.maximum_bytes_billed = maximum_bytes_billed
    return config


def row_to_dict(row: Any) -> dict[str, Any]:
    migrated = row["migrated_at_utc"]
    if isinstance(migrated, datetime):
        if migrated.tzinfo is None:
            migrated = migrated.replace(tzinfo=timezone.utc)
        migrated_text = migrated.astimezone(timezone.utc).isoformat()
    else:
        migrated_text = str(migrated or "")
    return {
        "migrated_at_utc": migrated_text,
        "migration_block_slot": str(row["migration_block_slot"] or ""),
        "migration_tx": str(row["migration_tx"] or ""),
        "migration_instruction_index": str(
            row["migration_instruction_index"] or ""
        ),
        "migration_data": str(row["migration_data"] or ""),
        "mint": str(row["mint"] or "").strip(),
        "migration_type": str(row["migration_type"] or ""),
    }


def transaction_success(status: Any, err: Any) -> bool:
    err_text = str(err).strip().lower() if err is not None else ""
    if err_text not in {"", "none", "null"}:
        return False
    status_text = str(status).strip().lower() if status is not None else ""
    if status_text in {"failed", "failure", "error", "err"}:
        return False
    return True


def query_statuses(
    client: bigquery.Client,
    migration_rows: list[dict[str, Any]],
    maximum_bytes_billed_per_query: int,
) -> tuple[dict[str, bool], int]:
    signatures_by_day: dict[str, set[str]] = defaultdict(set)
    for row in migration_rows:
        timestamp = str(row["migrated_at_utc"])
        signature = str(row["migration_tx"])
        if timestamp and signature:
            signatures_by_day[timestamp[:10]].add(signature)

    sql = f"""
    SELECT signature, status, err
    FROM `{TRANSACTIONS_TABLE}`
    WHERE block_timestamp >= @start_timestamp
      AND block_timestamp < @end_timestamp
      AND signature IN UNNEST(@signatures)
    """

    results: dict[str, bool] = {}
    total_bytes = 0

    for day_text, signatures_set in sorted(signatures_by_day.items()):
        day = date.fromisoformat(day_text)
        day_start = datetime.combine(day, dt_time.min, tzinfo=timezone.utc)
        day_end = day_start + timedelta(days=1)
        signatures = sorted(signatures_set)

        for offset in range(0, len(signatures), 2_000):
            chunk = signatures[offset : offset + 2_000]
            config = bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter(
                        "start_timestamp", "TIMESTAMP", day_start
                    ),
                    bigquery.ScalarQueryParameter(
                        "end_timestamp", "TIMESTAMP", day_end
                    ),
                    bigquery.ArrayQueryParameter(
                        "signatures", "STRING", chunk
                    ),
                ],
                maximum_bytes_billed=maximum_bytes_billed_per_query,
                use_query_cache=True,
            )
            job = retry(
                lambda: client.query(sql, job_config=config, location="US"),
                f"status-submit-{day_text}-{offset}",
            )
            rows = retry(
                lambda: list(job.result()),
                f"status-result-{day_text}-{offset}",
            )
            total_bytes += int(job.total_bytes_processed or 0)
            for row in rows:
                signature = str(row["signature"] or "")
                if signature:
                    results[signature] = transaction_success(
                        row["status"], row["err"]
                    )

    return results, total_bytes


def load_moralis(path: Path) -> list[dict[str, str]]:
    if not path.is_file():
        raise ReconciliationError(f"Moralis dosyası bulunamadı: {path}")

    # Tarayıcı bazı sistemlerde .csv.gz dosyasını otomatik açıp .csv olarak
    # kaydedebilir. Dosya adından değil, ilk iki bayttan gzip olup olmadığını
    # tespit ederek hem düz CSV hem gerçek GZIP kabul ediyoruz.
    with path.open("rb") as raw:
        is_gzip = raw.read(2) == b"\\x1f\\x8b"

    opener = gzip.open if is_gzip else open
    with opener(path, "rt", encoding="utf-8", newline="") as handle:
        rows = [
            {
                "token_address": str(row.get("token_address") or "").strip(),
                "graduated_at_utc": str(
                    row.get("graduated_at_utc") or ""
                ).strip(),
            }
            for row in csv.DictReader(handle)
        ]

    invalid = [
        row for row in rows
        if not row["token_address"] or not row["graduated_at_utc"]
    ]
    if invalid:
        raise ReconciliationError(
            f"Moralis dosyasında geçersiz satır var: {len(invalid)}"
        )

    addresses = [row["token_address"] for row in rows]
    if len(addresses) != len(set(addresses)):
        raise ReconciliationError("Moralis dosyasında duplicate mint var.")

    return rows


def month_key(timestamp: str) -> str:
    return timestamp[:7] if len(timestamp) >= 7 else "unknown"


def main() -> int:
    args = parse_args()
    if args.version:
        print(SCRIPT_VERSION)
        return 0
    if not args.project:
        raise SystemExit("--project zorunlu.")

    start, end = utc_bounds(args.start_date, args.end_date)
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    moralis_rows = load_moralis(Path(args.moralis_file))
    moralis_sha256 = hashlib.sha256(
        Path(args.moralis_file).read_bytes()
    ).hexdigest()

    client = bigquery.Client(project=args.project)
    sql = migration_sql()

    dry_job = retry(
        lambda: client.query(
            sql,
            job_config=migration_config(
                start,
                end,
                dry_run=True,
                maximum_bytes_billed=args.maximum_bytes_billed,
            ),
            location="US",
        ),
        "migration-dry-run",
    )
    estimated_bytes = int(dry_job.total_bytes_processed or 0)

    preflight = {
        "script_version": SCRIPT_VERSION,
        "scope": {
            "start_utc": start.isoformat(),
            "end_exclusive_utc": end.isoformat(),
        },
        "moralis": {
            "file": args.moralis_file,
            "row_count": len(moralis_rows),
            "sha256": moralis_sha256,
        },
        "bigquery": {
            "estimated_migration_query_bytes": estimated_bytes,
            "maximum_bytes_billed": args.maximum_bytes_billed,
            "within_hard_limit": estimated_bytes <= args.maximum_bytes_billed,
        },
        "dry_run_only": args.dry_run_only,
        "complete": estimated_bytes <= args.maximum_bytes_billed,
    }
    dump_json(output_dir / "preflight.json", preflight)
    print(json.dumps(preflight, ensure_ascii=False, indent=2), flush=True)

    if estimated_bytes > args.maximum_bytes_billed:
        print("BIGQUERY_DRY_RUN_LIMIT_EXCEEDED", file=sys.stderr)
        return 2

    if args.dry_run_only:
        print("MORALIS_BIGQUERY_RECONCILIATION_DRY_RUN_OK")
        return 0

    config = migration_config(
        start,
        end,
        dry_run=False,
        maximum_bytes_billed=args.maximum_bytes_billed,
    )
    query_job = retry(
        lambda: client.query(sql, job_config=config, location="US"),
        "migration-submit",
    )
    raw_rows = retry(
        lambda: [row_to_dict(row) for row in query_job.result()],
        "migration-result",
    )
    migration_bytes = int(query_job.total_bytes_processed or 0)

    statuses, status_bytes = query_statuses(
        client,
        raw_rows,
        args.status_maximum_bytes_billed_per_query,
    )

    verified_rows: list[dict[str, Any]] = []
    unresolved_signatures: set[str] = set()
    failed_rows: list[dict[str, Any]] = []

    for row in raw_rows:
        signature = row["migration_tx"]
        state = statuses.get(signature)
        if state is True:
            verified_rows.append(row)
        elif state is False:
            failed_rows.append(row)
        else:
            unresolved_signatures.add(signature)

    canonical_by_mint: dict[str, dict[str, Any]] = {}
    for row in sorted(
        verified_rows,
        key=lambda value: (
            value["migrated_at_utc"],
            value["migration_tx"],
            value["migration_instruction_index"],
        ),
    ):
        mint = str(row["mint"])
        if mint and mint not in canonical_by_mint:
            canonical_by_mint[mint] = row

    onchain_rows = list(canonical_by_mint.values())
    moralis_by_mint = {
        row["token_address"]: row for row in moralis_rows
    }
    onchain_by_mint = {
        row["mint"]: row for row in onchain_rows
    }

    moralis_set = set(moralis_by_mint)
    onchain_set = set(onchain_by_mint)
    only_moralis = sorted(moralis_set - onchain_set)
    only_onchain = sorted(onchain_set - moralis_set)
    intersection = sorted(moralis_set & onchain_set)

    month_rows: list[dict[str, Any]] = []
    all_months = sorted(
        {
            month_key(row["graduated_at_utc"])
            for row in moralis_rows
        }
        | {
            month_key(row["migrated_at_utc"])
            for row in onchain_rows
        }
    )
    for month in all_months:
        moralis_count = sum(
            month_key(row["graduated_at_utc"]) == month
            for row in moralis_rows
        )
        onchain_count = sum(
            month_key(row["migrated_at_utc"]) == month
            for row in onchain_rows
        )
        month_rows.append(
            {
                "month": month,
                "moralis_count": moralis_count,
                "onchain_count": onchain_count,
                "difference_onchain_minus_moralis": (
                    onchain_count - moralis_count
                ),
            }
        )

    write_csv(
        output_dir / "onchain_verified_migrations.csv",
        onchain_rows,
        [
            "mint",
            "migrated_at_utc",
            "migration_type",
            "migration_tx",
            "migration_block_slot",
            "migration_instruction_index",
            "migration_data",
        ],
    )
    write_csv(
        output_dir / "only_in_moralis.csv",
        [
            {
                **moralis_by_mint[mint],
                "reason": "missing_from_verified_onchain_migration_set",
            }
            for mint in only_moralis
        ],
        ["token_address", "graduated_at_utc", "reason"],
    )
    write_csv(
        output_dir / "only_onchain.csv",
        [
            {
                **onchain_by_mint[mint],
                "reason": "missing_from_moralis_graduated_set",
            }
            for mint in only_onchain
        ],
        [
            "mint",
            "migrated_at_utc",
            "migration_type",
            "migration_tx",
            "migration_block_slot",
            "migration_instruction_index",
            "migration_data",
            "reason",
        ],
    )
    write_csv(
        output_dir / "monthly_counts.csv",
        month_rows,
        [
            "month",
            "moralis_count",
            "onchain_count",
            "difference_onchain_minus_moralis",
        ],
    )
    write_csv(
        output_dir / "failed_migration_transactions.csv",
        failed_rows,
        [
            "mint",
            "migrated_at_utc",
            "migration_type",
            "migration_tx",
            "migration_block_slot",
            "migration_instruction_index",
            "migration_data",
        ],
    )

    report = {
        "script_version": SCRIPT_VERSION,
        "scope": preflight["scope"],
        "moralis": preflight["moralis"],
        "bigquery": {
            "estimated_migration_query_bytes": estimated_bytes,
            "actual_migration_query_bytes": migration_bytes,
            "actual_status_query_bytes": status_bytes,
            "actual_total_bytes": migration_bytes + status_bytes,
        },
        "counts": {
            "raw_migration_instruction_count": len(raw_rows),
            "verified_migration_instruction_count": len(verified_rows),
            "verified_unique_mint_count": len(onchain_set),
            "moralis_unique_mint_count": len(moralis_set),
            "intersection_count": len(intersection),
            "only_in_moralis_count": len(only_moralis),
            "only_onchain_count": len(only_onchain),
            "failed_migration_instruction_count": len(failed_rows),
            "unresolved_transaction_signature_count": len(
                unresolved_signatures
            ),
        },
        "coverage": {
            "moralis_subset_of_onchain": not only_moralis,
            "onchain_subset_of_moralis": not only_onchain,
            "exact_mint_set_match": not only_moralis and not only_onchain,
            "global_completeness_claimed": False,
        },
        "complete": not unresolved_signatures,
    }
    dump_json(output_dir / "reconciliation_report.json", report)
    print(json.dumps(report, ensure_ascii=False, indent=2), flush=True)

    if args.strict and not report["complete"]:
        print("RECONCILIATION_INCOMPLETE", file=sys.stderr)
        return 3

    print("MORALIS_BIGQUERY_RECONCILIATION_OK")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (ReconciliationError, ValueError) as exc:
        print(f"RECONCILIATION_ERROR: {exc}", file=sys.stderr)
        raise SystemExit(4) from exc
