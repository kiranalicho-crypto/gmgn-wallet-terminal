"""Transactions tablosunun BigQuery tarama boyutunu güvenli dry-run ile ölçer.

Gerçek veri sorgusu çalıştırmaz ve BigQuery kotasından veri tüketmez.
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
from typing import Any, Iterator

from google.cloud import bigquery


SCRIPT_VERSION = "2026-07-20-moralis-bigquery-reconciliation-v2"

BIGQUERY_LOCATION = "us-central1"

TRANSACTIONS_TABLE = (
    "bigquery-public-data."
    "crypto_solana_mainnet_us."
    "Transactions"
)

# Önceki başarılı dry-run sonucunda ölçülen Instructions sorgusu.
KNOWN_MIGRATION_ESTIMATE_BYTES = 770_154_556_161


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

    end_exclusive = datetime.combine(
        end_day + timedelta(days=1),
        dt_time.min,
        tzinfo=timezone.utc,
    )

    return start, end_exclusive


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


def month_windows(
    start: datetime,
    end_exclusive: datetime,
) -> Iterator[tuple[datetime, datetime]]:

    current = start

    while current < end_exclusive:
        if current.month == 12:
            next_month = datetime(
                current.year + 1,
                1,
                1,
                tzinfo=timezone.utc,
            )
        else:
            next_month = datetime(
                current.year,
                current.month + 1,
                1,
                tzinfo=timezone.utc,
            )

        segment_end = min(
            next_month,
            end_exclusive,
        )

        yield current, segment_end

        current = segment_end


def transactions_sql() -> str:
    return f"""
    SELECT
      signature,
      status,
      err
    FROM `{TRANSACTIONS_TABLE}`
    WHERE block_timestamp >= @start_timestamp
      AND block_timestamp < @end_timestamp
    """


def dry_run_config(
    start: datetime,
    end: datetime,
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
        ],
        dry_run=True,
        use_query_cache=False,
    )


def bytes_to_gib(
    value: int,
) -> float:
    return round(
        value / (1024 ** 3),
        2,
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

    start, end_exclusive = utc_bounds(
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

    sql = transactions_sql()

    monthly_estimates: list[dict[str, Any]] = []
    transaction_total_bytes = 0

    for month_start, month_end in month_windows(
        start,
        end_exclusive,
    ):
        job = client.query(
            sql,
            job_config=dry_run_config(
                month_start,
                month_end,
            ),
            location=BIGQUERY_LOCATION,
        )

        estimated_bytes = int(
            job.total_bytes_processed or 0
        )

        transaction_total_bytes += estimated_bytes

        monthly_estimates.append(
            {
                "start_utc": month_start.isoformat(),
                "end_exclusive_utc": month_end.isoformat(),
                "estimated_bytes": estimated_bytes,
                "estimated_gib": bytes_to_gib(
                    estimated_bytes
                ),
            }
        )

        print(
            "TRANSACTIONS_MONTH_DRY_RUN "
            f"{month_start.date()} "
            f"{month_end.date()} "
            f"{estimated_bytes} bytes "
            f"({bytes_to_gib(estimated_bytes)} GiB)",
            flush=True,
        )

    combined_bytes = (
        KNOWN_MIGRATION_ESTIMATE_BYTES
        + transaction_total_bytes
    )

    within_transaction_limit = (
        transaction_total_bytes
        <= args.maximum_bytes_billed
    )

    report = {
        "script_version": SCRIPT_VERSION,
        "scope": {
            "start_utc": start.isoformat(),
            "end_exclusive_utc": (
                end_exclusive.isoformat()
            ),
        },
        "moralis": {
            "file": str(moralis_path),
            "row_count": row_count,
            "sha256": sha256,
        },
        "bigquery": {
            "location": BIGQUERY_LOCATION,
            "transactions_table": TRANSACTIONS_TABLE,
            "estimate_type": (
                "one_pass_selected_columns_upper_bound"
            ),
            "monthly_estimates": monthly_estimates,
            "estimated_transactions_bytes": (
                transaction_total_bytes
            ),
            "estimated_transactions_gib": (
                bytes_to_gib(
                    transaction_total_bytes
                )
            ),
            "known_migration_estimate_bytes": (
                KNOWN_MIGRATION_ESTIMATE_BYTES
            ),
            "known_migration_estimate_gib": (
                bytes_to_gib(
                    KNOWN_MIGRATION_ESTIMATE_BYTES
                )
            ),
            "estimated_combined_bytes": (
                combined_bytes
            ),
            "estimated_combined_gib": (
                bytes_to_gib(
                    combined_bytes
                )
            ),
            "maximum_bytes_billed": (
                args.maximum_bytes_billed
            ),
            "transaction_within_hard_limit": (
                within_transaction_limit
            ),
        },
        "assumptions": {
            "dry_run_only": True,
            "real_data_downloaded": False,
            "quota_consumed_by_dry_run": False,
            "estimate_scans_transactions_once": True,
            "repeated_daily_signature_chunks_included": False,
        },
        "complete": True,
    }

    output_dir = Path(
        args.output_dir
    )

    write_json(
        output_dir / "transactions_preflight.json",
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

    if (
        args.strict
        and not within_transaction_limit
    ):
        print(
            "TRANSACTIONS_DRY_RUN_LIMIT_EXCEEDED",
            file=sys.stderr,
        )
        return 2

    print(
        "MORALIS_BIGQUERY_TRANSACTIONS_DRY_RUN_OK"
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
