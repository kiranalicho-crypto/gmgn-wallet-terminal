"""Export one month of Pump.fun migration instructions from BigQuery.

The script:
1. Validates the requested date window.
2. Runs a BigQuery dry-run and enforces a hard byte limit.
3. Runs the real query only when the estimate is within the limit.
4. Writes a compressed CSV plus a JSON control report.

It does not verify transaction success. That will be done later through
Solana RPC/Moralis using the exported transaction signatures.
"""

from __future__ import annotations

import argparse
import csv
import gzip
import hashlib
import json
import sys
import time
from datetime import date, datetime, time as dt_time, timezone
from pathlib import Path
from typing import Any

from google.api_core import exceptions as gexc
from google.cloud import bigquery


SCRIPT_VERSION = "2026-07-20-pumpfun-migration-month-export-v1"
BIGQUERY_LOCATION = "us-central1"

INSTRUCTIONS_TABLE = (
    "bigquery-public-data."
    "crypto_solana_mainnet_us."
    "Instructions"
)

PUMP_PROGRAM_ID = "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P"
MIGRATE_DATA_B58 = "Mjb79tJwDb7"
WITHDRAW_DATA_B58 = "Xd2GMpFXgQ1"

TRANSIENT_ERRORS = (
    gexc.TooManyRequests,
    gexc.ServiceUnavailable,
    gexc.InternalServerError,
    gexc.BadGateway,
    gexc.GatewayTimeout,
    gexc.DeadlineExceeded,
)


class ExportError(RuntimeError):
    pass


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--version", action="store_true")
    parser.add_argument("--project")
    parser.add_argument("--start-date")
    parser.add_argument("--end-date-exclusive")
    parser.add_argument("--label")
    parser.add_argument("--maximum-bytes-billed", type=int, default=200_000_000_000)
    parser.add_argument("--output-dir", default="artifacts/pumpfun-migration-export")
    return parser.parse_args()


def retry(operation, label: str, attempts: int = 7):
    delay = 4.0
    for attempt in range(1, attempts + 1):
        try:
            return operation()
        except TRANSIENT_ERRORS as exc:
            if attempt == attempts:
                raise
            print(
                f"RETRY {label} {attempt}/{attempts}: "
                f"{type(exc).__name__}: {exc}",
                file=sys.stderr,
                flush=True,
            )
            time.sleep(delay)
            delay = min(60.0, delay * 2)


def parse_utc_day(value: str, field_name: str) -> datetime:
    try:
        parsed = date.fromisoformat(value)
    except ValueError as exc:
        raise ExportError(f"{field_name} YYYY-MM-DD olmalı: {value}") from exc
    return datetime.combine(parsed, dt_time.min, tzinfo=timezone.utc)


def validate_args(args: argparse.Namespace) -> tuple[datetime, datetime]:
    if not args.project:
        raise ExportError("--project zorunlu.")
    if not args.start_date:
        raise ExportError("--start-date zorunlu.")
    if not args.end_date_exclusive:
        raise ExportError("--end-date-exclusive zorunlu.")
    if not args.label:
        raise ExportError("--label zorunlu.")
    if args.maximum_bytes_billed <= 0:
        raise ExportError("--maximum-bytes-billed pozitif olmalı.")

    start = parse_utc_day(args.start_date, "--start-date")
    end = parse_utc_day(args.end_date_exclusive, "--end-date-exclusive")

    if end <= start:
        raise ExportError("Bitiş tarihi başlangıç tarihinden sonra olmalı.")

    return start, end


def migration_sql() -> str:
    return f"""
    SELECT
      block_timestamp AS migrated_at_utc,
      block_slot AS migration_block_slot,
      tx_signature AS migration_tx,
      `index` AS migration_instruction_index,
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
    ORDER BY block_timestamp, tx_signature, `index`
    """


def query_config(
    start: datetime,
    end: datetime,
    maximum_bytes_billed: int,
    *,
    dry_run: bool,
) -> bigquery.QueryJobConfig:
    return bigquery.QueryJobConfig(
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
        use_query_cache=False,
        maximum_bytes_billed=maximum_bytes_billed,
    )


def normalize_row(row: Any) -> dict[str, str]:
    migrated_at = row["migrated_at_utc"]
    if hasattr(migrated_at, "isoformat"):
        migrated_at_text = migrated_at.isoformat()
    else:
        migrated_at_text = str(migrated_at or "")

    return {
        "mint": str(row["mint"] or "").strip(),
        "migrated_at_utc": migrated_at_text,
        "migration_type": str(row["migration_type"] or ""),
        "migration_tx": str(row["migration_tx"] or ""),
        "migration_block_slot": str(row["migration_block_slot"] or ""),
        "migration_instruction_index": str(
            row["migration_instruction_index"] or ""
        ),
        "migration_data": str(row["migration_data"] or ""),
    }


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def bytes_to_gib(value: int) -> float:
    return round(value / (1024 ** 3), 2)


def main() -> int:
    args = parse_args()

    if args.version:
        print(SCRIPT_VERSION)
        return 0

    start, end = validate_args(args)
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    client = bigquery.Client(project=args.project)
    sql = migration_sql()

    dry_job = retry(
        lambda: client.query(
            sql,
            job_config=query_config(
                start,
                end,
                args.maximum_bytes_billed,
                dry_run=True,
            ),
            location=BIGQUERY_LOCATION,
        ),
        "migration-dry-run",
    )

    estimated_bytes = int(dry_job.total_bytes_processed or 0)
    print(
        f"ESTIMATED_BYTES={estimated_bytes} "
        f"ESTIMATED_GIB={bytes_to_gib(estimated_bytes)}",
        flush=True,
    )

    if estimated_bytes > args.maximum_bytes_billed:
        raise ExportError(
            "Tahmini tarama sert sınırı aşıyor: "
            f"{estimated_bytes} > {args.maximum_bytes_billed}"
        )

    real_job = retry(
        lambda: client.query(
            sql,
            job_config=query_config(
                start,
                end,
                args.maximum_bytes_billed,
                dry_run=False,
            ),
            location=BIGQUERY_LOCATION,
        ),
        "migration-submit",
    )

    result_iterator = retry(
        lambda: real_job.result(page_size=10_000),
        "migration-result",
    )

    csv_path = output_dir / f"pumpfun_migrations_{args.label}.csv.gz"
    fields = [
        "mint",
        "migrated_at_utc",
        "migration_type",
        "migration_tx",
        "migration_block_slot",
        "migration_instruction_index",
        "migration_data",
    ]

    row_count = 0
    valid_row_count = 0
    invalid_row_count = 0
    duplicate_count = 0
    unique_mints: set[str] = set()
    seen_instruction_keys: set[tuple[str, str]] = set()

    with gzip.open(
        csv_path,
        "wt",
        encoding="utf-8",
        newline="",
        compresslevel=9,
    ) as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()

        for source_row in result_iterator:
            row_count += 1
            row = normalize_row(source_row)

            key = (
                row["migration_tx"],
                row["migration_instruction_index"],
            )

            if not row["mint"] or not row["migration_tx"]:
                invalid_row_count += 1
                continue

            if key in seen_instruction_keys:
                duplicate_count += 1
                continue

            seen_instruction_keys.add(key)
            unique_mints.add(row["mint"])
            writer.writerow(row)
            valid_row_count += 1

    actual_bytes = int(real_job.total_bytes_processed or 0)
    billed_bytes = int(real_job.total_bytes_billed or 0)
    csv_sha256 = sha256_file(csv_path)

    report = {
        "script_version": SCRIPT_VERSION,
        "label": args.label,
        "scope": {
            "start_utc": start.isoformat(),
            "end_exclusive_utc": end.isoformat(),
        },
        "bigquery": {
            "location": BIGQUERY_LOCATION,
            "instructions_table": INSTRUCTIONS_TABLE,
            "estimated_bytes": estimated_bytes,
            "estimated_gib": bytes_to_gib(estimated_bytes),
            "actual_bytes_processed": actual_bytes,
            "actual_gib_processed": bytes_to_gib(actual_bytes),
            "actual_bytes_billed": billed_bytes,
            "actual_gib_billed": bytes_to_gib(billed_bytes),
            "maximum_bytes_billed": args.maximum_bytes_billed,
            "job_id": real_job.job_id,
        },
        "output": {
            "file": str(csv_path),
            "sha256": csv_sha256,
            "raw_row_count": row_count,
            "valid_row_count": valid_row_count,
            "invalid_row_count": invalid_row_count,
            "duplicate_instruction_count": duplicate_count,
            "unique_mint_count": len(unique_mints),
        },
        "transaction_success_verified": False,
        "complete": invalid_row_count == 0,
    }

    report_path = output_dir / f"pumpfun_migrations_{args.label}_report.json"
    write_json(report_path, report)

    print(json.dumps(report, ensure_ascii=False, indent=2), flush=True)

    if invalid_row_count:
        print(
            "MIGRATION_EXPORT_COMPLETED_WITH_INVALID_ROWS",
            file=sys.stderr,
        )
        return 3

    print("PUMPFUN_MIGRATION_MONTH_EXPORT_OK")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (ExportError, ValueError) as exc:
        print(f"EXPORT_ERROR: {exc}", file=sys.stderr)
        raise SystemExit(4) from exc
