"""Moralis Pump.fun mezun token listesini BigQuery ile güvenli şekilde test eder.

Bu sürüm yalnızca dry-run yapar ve veri indirmez.
"""

from __future__ import annotations

import argparse
import csv
import gzip
import hashlib
import json
import sys
from datetime import date, datetime, time as dt_time, timedelta, timezone
from pathlib import Path
from typing import Any

from google.cloud import bigquery

SCRIPT_VERSION = "2026-07-20-moralis-bigquery-reconciliation-v2"

BIGQUERY_LOCATION = "us-central1"

INSTRUCTIONS_TABLE = (
    "bigquery-public-data.crypto_solana_mainnet_us.Instructions"
)

PUMP_PROGRAM_ID = (
    "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P"
)

MIGRATE_DATA_B58 = "Mjb79tJwDb7"
WITHDRAW_DATA_B58 = "Xd2GMpFXgQ1"


class ReconciliationError(RuntimeError):
    pass


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--version",
        action="store_true",
    )

    parser.add_argument(
        "--project",
    )

    parser.add_argument(
        "--start-date",
        default="2026-01-01",
    )

    parser.add_argument(
        "--end-date",
        default="2026-07-18",
    )

    parser.add_argument(
        "--moralis-file",
        default="data/moralis_graduated_mints_2026.csv",
    )

    parser.add_argument(
        "--output-dir",
        default=(
            "artifacts/"
            "moralis-bigquery-reconciliation-dry-run"
        ),
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

    parser.add_argument(
        "--dry-run-only",
        action="store_true",
    )

    parser.add_argument(
        "--strict",
        action="store_true",
    )

    return parser.parse_args()


def utc_bounds(
    start_text: str,
    end_text: str,
) -> tuple[datetime, datetime]:

    start_day = date.fromisoformat(start_text)
    end_day = date.fromisoformat(end_text)

    if end_day < start_day:
        raise ReconciliationError(
            "Bitiş tarihi başlangıç tarihinden önce olamaz."
        )

    start = datetime.combine(
        start_day,
        dt_time.min,
        tzinfo=timezone.utc,
    )

    end = datetime.combine(
        end_day + timedelta(days=1),
        dt_time.min,
        tzinfo=timezone.utc,
    )

    return start, end


def load_moralis(
    path: Path,
) -> tuple[int, str]:

    if not path.is_file():
        raise ReconciliationError(
            f"Moralis dosyası bulunamadı: {path}"
        )

    raw_bytes = path.read_bytes()

    is_gzip = raw_bytes[:2] == b"\x1f\x8b"
    opener = gzip.open if is_gzip else open

    with opener(
        path,
        "rt",
        encoding="utf-8",
        newline="",
    ) as handle:

        reader = csv.DictReader(handle)

        required_columns = {
            "token_address",
            "graduated_at_utc",
        }

        if (
            not reader.fieldnames
            or not required_columns.issubset(
                set(reader.fieldnames)
            )
        ):
            raise ReconciliationError(
                "Moralis CSV kolonları eksik: "
                "token_address, graduated_at_utc"
            )

        seen: set[str] = set()
        row_count = 0

        for row_number, row in enumerate(
            reader,
            start=2,
        ):
            token_address = str(
                row.get("token_address") or ""
            ).strip()

            graduated_at = str(
                row.get("graduated_at_utc") or ""
            ).strip()

            if not token_address or not graduated_at:
                raise ReconciliationError(
                    "Moralis CSV geçersiz satır: "
                    f"{row_number}"
                )

            if token_address in seen:
                raise ReconciliationError(
                    "Moralis CSV duplicate mint: "
                    f"{token_address}"
                )

            seen.add(token_address)
            row_count += 1

    sha256 = hashlib.sha256(
        raw_bytes
    ).hexdigest()

    return row_count, sha256


def migration_sql() -> str:
    return f"""
    SELECT
      block_timestamp AS migrated_at_utc,
      block_slot AS migration_block_slot,
      tx_signature AS migration_tx,
      index AS migration_instruction_index,
      data AS migration_data,
      CASE
        WHEN data = @migrate_data
          THEN accounts[SAFE_OFFSET(2)]
        WHEN data = @withdraw_data
          THEN accounts[SAFE_OFFSET(1)]
      END AS mint,
      CASE
        WHEN data = @migrate_data
          THEN 'migrate'
        WHEN data = @withdraw_data
          THEN 'withdraw'
      END AS migration_type
    FROM `{INSTRUCTIONS_TABLE}`
    WHERE block_timestamp >= @start_timestamp
      AND block_timestamp < @end_timestamp
      AND program_id = @program_id
      AND data IN UNNEST(@migration_values)
    """


def dry_run_config(
    start: datetime,
    end: datetime,
    maximum_bytes_billed: int,
) -> bigquery.QueryJobConfig:

    return bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter(
                "start_timestamp",
                "TIMESTAMP",
                start,
            ),
            bigquery.ScalarQueryParameter(
                "end_timestamp",
                "TIMESTAMP",
                end,
            ),
            bigquery.ScalarQueryParameter(
                "program_id",
                "STRING",
                PUMP_PROGRAM_ID,
            ),
            bigquery.ScalarQueryParameter(
                "migrate_data",
                "STRING",
                MIGRATE_DATA_B58,
            ),
            bigquery.ScalarQueryParameter(
                "withdraw_data",
                "STRING",
                WITHDRAW_DATA_B58,
            ),
            bigquery.ArrayQueryParameter(
                "migration_values",
                "STRING",
                [
                    MIGRATE_DATA_B58,
                    WITHDRAW_DATA_B58,
                ],
            ),
        ],
        dry_run=True,
        use_query_cache=False,
        maximum_bytes_billed=maximum_bytes_billed,
    )


def write_json(
    path: Path,
    payload: dict[str, Any],
) -> None:

    path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    path.write_text(
        json.dumps(
            payload,
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )


def main() -> int:
    args = parse_args()

    if args.version:
        print(SCRIPT_VERSION)
        return 0

    if not args.project:
        raise ReconciliationError(
            "--project zorunlu."
        )

    if not args.dry_run_only:
        raise ReconciliationError(
            "Bu sürüm yalnız --dry-run-only ile çalışır."
        )

    start, end = utc_bounds(
        args.start_date,
        args.end_date,
    )

    moralis_path = Path(
        args.moralis_file
    )

    row_count, sha256 = load_moralis(
        moralis_path
    )

    client = bigquery.Client(
        project=args.project
    )

    job = client.query(
        migration_sql(),
        job_config=dry_run_config(
            start,
            end,
            args.maximum_bytes_billed,
        ),
        location=BIGQUERY_LOCATION,
    )

    estimated_bytes = int(
        job.total_bytes_processed or 0
    )

    within_limit = (
        estimated_bytes
        <= args.maximum_bytes_billed
    )

    report = {
        "script_version": SCRIPT_VERSION,
        "scope": {
            "start_utc": start.isoformat(),
            "end_exclusive_utc": end.isoformat(),
        },
        "moralis": {
            "file": str(moralis_path),
            "row_count": row_count,
            "sha256": sha256,
        },
        "bigquery": {
            "location": BIGQUERY_LOCATION,
            "instructions_table": INSTRUCTIONS_TABLE,
            "estimated_migration_query_bytes": (
                estimated_bytes
            ),
            "maximum_bytes_billed": (
                args.maximum_bytes_billed
            ),
            "within_hard_limit": within_limit,
        },
        "dry_run_only": True,
        "complete": within_limit,
    }

    output_dir = Path(
        args.output_dir
    )

    write_json(
        output_dir / "preflight.json",
        report,
    )

    print(
        json.dumps(
            report,
            ensure_ascii=False,
            indent=2,
        ),
        flush=True,
    )

    if not within_limit:
        print(
            "BIGQUERY_DRY_RUN_LIMIT_EXCEEDED",
            file=sys.stderr,
        )
        return 2

    print(
        "MORALIS_BIGQUERY_RECONCILIATION_DRY_RUN_OK"
    )

    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())

    except ReconciliationError as exc:
        print(
            f"RECONCILIATION_ERROR: {exc}",
            file=sys.stderr,
        )

        raise SystemExit(4) from exc
