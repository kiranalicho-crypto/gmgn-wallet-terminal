from __future__ import annotations

import argparse
import base58
import csv
import json
import sqlite3
import sys
from collections import Counter
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Iterable

from google.cloud import bigquery

from pumpfun_bigquery_export import (
    PUMP_PROGRAM_ID,
    choose_account,
    flatten_account_names,
    load_pump_idl,
)


HISTORICAL_WITHDRAW_DISCRIMINATOR = bytes.fromhex(
    "b712469c946da122"
)

HISTORICAL_WITHDRAW_ACCOUNTS = [
    {"name": "global"},
    {"name": "last_withdraw"},
    {"name": "mint"},
    {"name": "bonding_curve"},
    {"name": "associated_bonding_curve"},
    {"name": "associated_user"},
    {"name": "user"},
    {"name": "system_program"},
    {"name": "token_program"},
    {"name": "rent"},
    {"name": "event_authority"},
    {"name": "program"},
]


SCRIPT_VERSION = "2026-07-19-backfill-v2"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Pump.fun geçmişini tarih aralığında tarar ve "
            "kompakt aday veritabanı üretir."
        )
    )

    parser.add_argument(
        "--start-date",
        required=True,
        help="Başlangıç UTC tarihi: YYYY-MM-DD",
    )

    parser.add_argument(
        "--end-date",
        required=True,
        help="Bitiş UTC tarihi: YYYY-MM-DD, dahil",
    )

    parser.add_argument(
        "--project",
        required=True,
        help="BigQuery sorgu projesi",
    )

    parser.add_argument(
        "--output-dir",
        default="artifacts/pumpfun-backfill",
        help="Çıktı klasörü",
    )

    parser.add_argument(
        "--version",
        action="version",
        version=SCRIPT_VERSION,
    )

    return parser.parse_args()


def iter_dates(
    start_date: date,
    end_date: date,
) -> Iterable[date]:
    current_date = start_date

    while current_date <= end_date:
        yield current_date
        current_date += timedelta(days=1)


def instruction_category(
    instruction_name: str,
) -> str:
    normalized = instruction_name.lower()

    if normalized == "create" or normalized.startswith(
        "create_"
    ):
        return "create"

    if normalized == "buy" or normalized.startswith(
        "buy_"
    ):
        return "buy"

    if normalized == "sell" or normalized.startswith(
        "sell_"
    ):
        return "sell"

    if normalized in {"migrate", "withdraw"}:
        return "migrate"

    if normalized.startswith("migrate_"):
        return "migrate"

    return "other"


def query_day(
    client: bigquery.Client,
    scan_date: date,
) -> tuple[Any, int]:
    start_timestamp = datetime.combine(
        scan_date,
        datetime.min.time(),
    )

    end_timestamp = start_timestamp + timedelta(days=1)

    sql = """
    SELECT
      block_timestamp,
      block_slot,
      tx_signature,
      index AS instruction_index,
      parent_index,
      accounts,
      data
    FROM
      `bigquery-public-data.crypto_solana_mainnet_us.Instructions`
    WHERE
      block_timestamp >= @start_timestamp
      AND block_timestamp < @end_timestamp
      AND program_id = @program_id
    ORDER BY
      block_timestamp,
      tx_signature,
      instruction_index
    """

    parameters = [
        bigquery.ScalarQueryParameter(
            "start_timestamp",
            "TIMESTAMP",
            start_timestamp,
        ),
        bigquery.ScalarQueryParameter(
            "end_timestamp",
            "TIMESTAMP",
            end_timestamp,
        ),
        bigquery.ScalarQueryParameter(
            "program_id",
            "STRING",
            PUMP_PROGRAM_ID,
        ),
    ]

    dry_run_config = bigquery.QueryJobConfig(
        query_parameters=parameters,
        dry_run=True,
        use_query_cache=False,
    )

    dry_run_job = client.query(
        sql,
        job_config=dry_run_config,
    )

    estimated_bytes = int(
        dry_run_job.total_bytes_processed or 0
    )

    run_config = bigquery.QueryJobConfig(
        query_parameters=parameters,
        use_query_cache=True,
    )

    rows = client.query(
        sql,
        job_config=run_config,
    ).result(
        page_size=10_000
    )

    return rows, estimated_bytes


def create_database(
    database_path: Path,
) -> sqlite3.Connection:
    connection = sqlite3.connect(database_path)

    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS creates (
            mint TEXT PRIMARY KEY,
            created_at_utc TEXT NOT NULL,
            tx_signature TEXT NOT NULL,
            creator TEXT NOT NULL,
            bonding_curve TEXT NOT NULL,
            instruction_name TEXT NOT NULL
        )
        """
    )

    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS first_buys (
            mint TEXT NOT NULL,
            wallet TEXT NOT NULL,
            first_buy_at_utc TEXT NOT NULL,
            first_buy_tx TEXT NOT NULL,
            bonding_curve TEXT NOT NULL,
            instruction_name TEXT NOT NULL,
            PRIMARY KEY (mint, wallet)
        )
        """
    )

    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS migrations (
            mint TEXT PRIMARY KEY,
            migrated_at_utc TEXT NOT NULL,
            tx_signature TEXT NOT NULL,
            wallet TEXT NOT NULL,
            instruction_name TEXT NOT NULL
        )
        """
    )

    connection.execute(
        """
        CREATE INDEX IF NOT EXISTS
        idx_first_buys_wallet
        ON first_buys(wallet)
        """
    )

    connection.execute(
        """
        CREATE INDEX IF NOT EXISTS
        idx_first_buys_time
        ON first_buys(first_buy_at_utc)
        """
    )

    connection.commit()

    return connection


def decode_data(
    encoded_data: str,
) -> tuple[bytes | None, str | None, int]:
    try:
        decoded = base58.b58decode(encoded_data)
    except Exception:
        return None, "base58_decode_failed", 0

    decoded_length = len(decoded)

    if decoded_length < 8:
        return None, "short_instruction_data", decoded_length

    return decoded, None, decoded_length


def transaction_success_state(
    status: Any,
    error_value: Any,
) -> bool | None:
    status_text = (
        str(status).strip().lower()
        if status is not None
        else ""
    )

    error_text = (
        str(error_value).strip().lower()
        if error_value is not None
        else ""
    )

    if error_text not in {"", "none", "null"}:
        return False

    if status_text in {
        "failed",
        "failure",
        "error",
        "err",
    }:
        return False

    if status_text in {
        "success",
        "succeeded",
        "ok",
    }:
        return True

    if status_text:
        return True

    if error_value is None:
        return True

    return None


def query_transaction_statuses(
    client: bigquery.Client,
    scan_date: date,
    signatures: set[str],
) -> tuple[dict[str, dict[str, Any]], int]:
    if not signatures:
        return {}, 0

    start_timestamp = datetime.combine(
        scan_date,
        datetime.min.time(),
    )

    end_timestamp = start_timestamp + timedelta(days=1)

    sql = """
    SELECT
      signature,
      status,
      err
    FROM
      `bigquery-public-data.crypto_solana_mainnet_us.Transactions`
    WHERE
      block_timestamp >= @start_timestamp
      AND block_timestamp < @end_timestamp
      AND signature IN UNNEST(@signatures)
    """

    config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter(
                "start_timestamp",
                "TIMESTAMP",
                start_timestamp,
            ),
            bigquery.ScalarQueryParameter(
                "end_timestamp",
                "TIMESTAMP",
                end_timestamp,
            ),
            bigquery.ArrayQueryParameter(
                "signatures",
                "STRING",
                sorted(signatures),
            ),
        ],
        use_query_cache=True,
    )

    job = client.query(sql, job_config=config)
    rows = job.result(page_size=1_000)

    statuses: dict[str, dict[str, Any]] = {}

    for row in rows:
        signature = str(row.signature)
        status = row.status
        error_value = row.err

        statuses[signature] = {
            "status": (
                None
                if status is None
                else str(status)
            ),
            "err": (
                None
                if error_value is None
                else str(error_value)
            ),
            "is_success": transaction_success_state(
                status,
                error_value,
            ),
        }

    return (
        statuses,
        int(job.total_bytes_processed or 0),
    )


def export_anomalies_csv(
    anomalies: list[dict[str, Any]],
    output_path: Path,
) -> int:
    fieldnames = [
        "scan_date_utc",
        "tx_signature",
        "instruction_index",
        "reason",
        "decoded_length",
        "discriminator_hex",
        "transaction_status",
        "transaction_error",
        "is_success",
        "resolution",
    ]

    with output_path.open(
        "w",
        newline="",
        encoding="utf-8",
    ) as file:
        writer = csv.DictWriter(
            file,
            fieldnames=fieldnames,
        )
        writer.writeheader()

        for anomaly in anomalies:
            writer.writerow(
                {
                    key: anomaly.get(key, "")
                    for key in fieldnames
                }
            )

    return len(anomalies)


def export_query_to_csv(
    connection: sqlite3.Connection,
    sql: str,
    output_path: Path,
) -> int:
    cursor = connection.execute(sql)

    column_names = [
        description[0]
        for description in cursor.description
    ]

    row_count = 0

    with output_path.open(
        "w",
        newline="",
        encoding="utf-8",
    ) as file:
        writer = csv.writer(file)
        writer.writerow(column_names)

        for row in cursor:
            writer.writerow(row)
            row_count += 1

    return row_count


def scalar_count(
    connection: sqlite3.Connection,
    sql: str,
) -> int:
    row = connection.execute(sql).fetchone()

    if row is None:
        return 0

    return int(row[0])


def main() -> int:
    args = parse_args()

    start_date = date.fromisoformat(args.start_date)
    end_date = date.fromisoformat(args.end_date)

    if end_date < start_date:
        raise RuntimeError(
            "Bitiş tarihi başlangıç tarihinden önce olamaz."
        )

    output_dir = Path(args.output_dir)

    output_dir.mkdir(
        parents=True,
        exist_ok=True,
    )

    range_name = (
        f"{start_date.isoformat()}_to_"
        f"{end_date.isoformat()}"
    )

    database_path = (
        output_dir
        / f"pumpfun_candidates_{range_name}.sqlite"
    )

    manifest_path = (
        output_dir
        / f"pumpfun_manifest_{range_name}.json"
    )

    creates_csv_path = (
        output_dir
        / f"pumpfun_creates_{range_name}.csv"
    )

    eligible_first_buys_csv_path = (
        output_dir
        / f"pumpfun_eligible_first_buys_{range_name}.csv"
    )

    out_of_scope_first_buys_csv_path = (
        output_dir
        / f"pumpfun_out_of_scope_first_buys_{range_name}.csv"
    )

    migrations_csv_path = (
        output_dir
        / f"pumpfun_migrations_{range_name}.csv"
    )

    anomalies_csv_path = (
        output_dir
        / f"pumpfun_instruction_anomalies_{range_name}.csv"
    )

    instruction_map, idl_sha256 = load_pump_idl()

    # Güncel IDL'de bulunmayan eski Raydium migration
    # instruction'ını tarihsel taramalar için tanıyoruz.
    instruction_map.setdefault(
        HISTORICAL_WITHDRAW_DISCRIMINATOR,
        {
            "name": "withdraw",
            "accounts": HISTORICAL_WITHDRAW_ACCOUNTS,
        },
    )

    client = bigquery.Client(
        project=args.project
    )

    connection = create_database(
        database_path
    )

    total_instruction_counts: Counter[str] = Counter()
    total_category_counts: Counter[str] = Counter()
    unknown_discriminators: Counter[str] = Counter()

    daily_results: list[dict[str, Any]] = []
    empty_days: list[str] = []
    missing_required_account_rows: list[dict[str, Any]] = []
    instruction_anomalies: list[dict[str, Any]] = []
    unresolved_instruction_anomalies: list[dict[str, Any]] = []
    ignored_failed_instruction_anomalies: list[dict[str, Any]] = []

    total_rows = 0
    total_estimated_bytes = 0

    for scan_date in iter_dates(
        start_date,
        end_date,
    ):
        print(
            f"Tarama başlıyor: {scan_date.isoformat()}",
            flush=True,
        )

        rows, estimated_bytes = query_day(
            client,
            scan_date,
        )

        daily_instruction_counts: Counter[str] = Counter()
        daily_category_counts: Counter[str] = Counter()
        daily_unknowns: Counter[str] = Counter()

        daily_row_count = 0
        daily_transactions: set[str] = set()
        daily_anomalies: list[dict[str, Any]] = []

        for row in rows:
            daily_row_count += 1
            total_rows += 1

            tx_signature = str(
                row.tx_signature
            )

            instruction_index = int(
                row.instruction_index
            )

            daily_transactions.add(
                tx_signature
            )

            decoded_data, decode_issue, decoded_length = decode_data(
                encoded_data=str(row.data),
            )

            if decode_issue is not None:
                instruction_name = decode_issue.upper()
                category = "other"

                daily_instruction_counts[instruction_name] += 1
                total_instruction_counts[instruction_name] += 1
                daily_category_counts[category] += 1
                total_category_counts[category] += 1

                daily_anomalies.append(
                    {
                        "scan_date_utc": scan_date.isoformat(),
                        "tx_signature": tx_signature,
                        "instruction_index": instruction_index,
                        "reason": decode_issue,
                        "decoded_length": decoded_length,
                        "discriminator_hex": "",
                    }
                )

                continue

            assert decoded_data is not None

            discriminator = decoded_data[:8]
            discriminator_hex = discriminator.hex()

            definition = instruction_map.get(
                discriminator
            )

            if definition is None:
                instruction_name = "UNKNOWN"
                category = "other"

                daily_unknowns[discriminator_hex] += 1
                unknown_discriminators[discriminator_hex] += 1
                daily_instruction_counts[instruction_name] += 1
                total_instruction_counts[instruction_name] += 1
                daily_category_counts[category] += 1
                total_category_counts[category] += 1

                daily_anomalies.append(
                    {
                        "scan_date_utc": scan_date.isoformat(),
                        "tx_signature": tx_signature,
                        "instruction_index": instruction_index,
                        "reason": "unknown_discriminator",
                        "decoded_length": decoded_length,
                        "discriminator_hex": discriminator_hex,
                    }
                )

                continue

            instruction_name = str(
                definition["name"]
            )

            account_names = flatten_account_names(
                definition.get(
                    "accounts",
                    [],
                )
            )

            category = instruction_category(
                instruction_name
            )

            daily_instruction_counts[
                instruction_name
            ] += 1

            total_instruction_counts[
                instruction_name
            ] += 1

            daily_category_counts[
                category
            ] += 1

            total_category_counts[
                category
            ] += 1

            accounts = [
                str(account)
                for account in (
                    row.accounts or []
                )
            ]

            account_map = {
                account_name: accounts[position]
                for position, account_name
                in enumerate(account_names)
                if position < len(accounts)
            }

            mint = choose_account(
                account_map,
                "mint",
                "base_mint",
            )

            wallet = choose_account(
                account_map,
                "user",
                "buyer",
                "seller",
                "payer",
                "withdraw_authority",
            )

            creator = choose_account(
                account_map,
                "creator",
            )

            if category == "create" and not creator:
                creator = wallet

            bonding_curve = choose_account(
                account_map,
                "bonding_curve",
            )

            timestamp = row.block_timestamp

            if hasattr(timestamp, "isoformat"):
                timestamp_text = timestamp.isoformat()
            else:
                timestamp_text = str(timestamp)

            if category in {
                "create",
                "buy",
                "sell",
                "migrate",
            } and not mint:
                missing_required_account_rows.append(
                    {
                        "scan_date": scan_date.isoformat(),
                        "tx_signature": tx_signature,
                        "instruction_index": instruction_index,
                        "instruction_name": instruction_name,
                        "missing_field": "mint",
                    }
                )

                continue

            if category == "create":
                connection.execute(
                    """
                    INSERT OR IGNORE INTO creates (
                        mint,
                        created_at_utc,
                        tx_signature,
                        creator,
                        bonding_curve,
                        instruction_name
                    )
                    VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    (
                        mint,
                        timestamp_text,
                        tx_signature,
                        creator,
                        bonding_curve,
                        instruction_name,
                    ),
                )

            elif category == "buy" and wallet:
                connection.execute(
                    """
                    INSERT OR IGNORE INTO first_buys (
                        mint,
                        wallet,
                        first_buy_at_utc,
                        first_buy_tx,
                        bonding_curve,
                        instruction_name
                    )
                    VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    (
                        mint,
                        wallet,
                        timestamp_text,
                        tx_signature,
                        bonding_curve,
                        instruction_name,
                    ),
                )

            elif category == "migrate":
                connection.execute(
                    """
                    INSERT OR IGNORE INTO migrations (
                        mint,
                        migrated_at_utc,
                        tx_signature,
                        wallet,
                        instruction_name
                    )
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    (
                        mint,
                        timestamp_text,
                        tx_signature,
                        wallet,
                        instruction_name,
                    ),
                )

        anomaly_status_bytes = 0
        daily_ignored_failed_anomalies = 0
        daily_unresolved_anomalies = 0

        if daily_anomalies:
            status_map, anomaly_status_bytes = (
                query_transaction_statuses(
                    client,
                    scan_date,
                    {
                        anomaly["tx_signature"]
                        for anomaly in daily_anomalies
                    },
                )
            )

            for anomaly in daily_anomalies:
                status_info = status_map.get(
                    anomaly["tx_signature"]
                )

                if status_info is None:
                    anomaly.update(
                        {
                            "transaction_status": None,
                            "transaction_error": None,
                            "is_success": None,
                            "resolution": "unresolved_status_missing",
                        }
                    )
                    unresolved_instruction_anomalies.append(anomaly)
                    daily_unresolved_anomalies += 1
                elif status_info["is_success"] is False:
                    anomaly.update(
                        {
                            "transaction_status": status_info["status"],
                            "transaction_error": status_info["err"],
                            "is_success": False,
                            "resolution": "ignored_failed_transaction",
                        }
                    )
                    ignored_failed_instruction_anomalies.append(anomaly)
                    daily_ignored_failed_anomalies += 1
                else:
                    anomaly.update(
                        {
                            "transaction_status": status_info["status"],
                            "transaction_error": status_info["err"],
                            "is_success": status_info["is_success"],
                            "resolution": "unresolved_success_or_ambiguous",
                        }
                    )
                    unresolved_instruction_anomalies.append(anomaly)
                    daily_unresolved_anomalies += 1

                instruction_anomalies.append(anomaly)

        connection.commit()

        total_estimated_bytes += (
            estimated_bytes + anomaly_status_bytes
        )

        if daily_row_count == 0:
            empty_days.append(
                scan_date.isoformat()
            )

        daily_result = {
            "scan_date_utc": scan_date.isoformat(),
            "row_count": daily_row_count,
            "unique_transactions": len(
                daily_transactions
            ),
            "instruction_counts": dict(
                sorted(
                    daily_instruction_counts.items()
                )
            ),
            "category_counts": dict(
                sorted(
                    daily_category_counts.items()
                )
            ),
            "unknown_discriminators": dict(
                sorted(
                    daily_unknowns.items()
                )
            ),
            "instruction_anomaly_count": len(daily_anomalies),
            "ignored_failed_instruction_anomaly_count": (
                daily_ignored_failed_anomalies
            ),
            "unresolved_instruction_anomaly_count": (
                daily_unresolved_anomalies
            ),
            "estimated_instruction_bytes_processed": estimated_bytes,
            "estimated_anomaly_status_bytes_processed": (
                anomaly_status_bytes
            ),
            "estimated_bytes_processed": (
                estimated_bytes + anomaly_status_bytes
            ),
        }

        daily_results.append(
            daily_result
        )

        print(
            json.dumps(
                daily_result,
                ensure_ascii=False,
            ),
            flush=True,
        )

    creates_count = export_query_to_csv(
        connection,
        """
        SELECT
            mint,
            created_at_utc,
            tx_signature,
            creator,
            bonding_curve,
            instruction_name
        FROM creates
        ORDER BY created_at_utc, mint
        """,
        creates_csv_path,
    )

    raw_first_buys_count = scalar_count(
        connection,
        """
        SELECT COUNT(*)
        FROM first_buys
        """,
    )

    eligible_first_buys_count = export_query_to_csv(
        connection,
        """
        SELECT
            f.mint,
            f.wallet,
            f.first_buy_at_utc,
            f.first_buy_tx,
            f.bonding_curve,
            f.instruction_name
        FROM first_buys AS f
        INNER JOIN creates AS c
            ON c.mint = f.mint
        ORDER BY
            f.first_buy_at_utc,
            f.mint,
            f.wallet
        """,
        eligible_first_buys_csv_path,
    )

    out_of_scope_first_buys_count = export_query_to_csv(
        connection,
        """
        SELECT
            f.mint,
            f.wallet,
            f.first_buy_at_utc,
            f.first_buy_tx,
            f.bonding_curve,
            f.instruction_name
        FROM first_buys AS f
        LEFT JOIN creates AS c
            ON c.mint = f.mint
        WHERE c.mint IS NULL
        ORDER BY
            f.first_buy_at_utc,
            f.mint,
            f.wallet
        """,
        out_of_scope_first_buys_csv_path,
    )

    migrations_count = export_query_to_csv(
        connection,
        """
        SELECT
            mint,
            migrated_at_utc,
            tx_signature,
            wallet,
            instruction_name
        FROM migrations
        ORDER BY migrated_at_utc, mint
        """,
        migrations_csv_path,
    )

    instruction_anomaly_count = export_anomalies_csv(
        instruction_anomalies,
        anomalies_csv_path,
    )

    database_integrity = connection.execute(
        "PRAGMA integrity_check"
    ).fetchone()

    connection.close()

    integrity_status = (
        str(database_integrity[0])
        if database_integrity
        else "unknown"
    )

    manifest = {
        "start_date_utc": start_date.isoformat(),
        "end_date_utc": end_date.isoformat(),
        "program_id": PUMP_PROGRAM_ID,
        "idl_sha256": idl_sha256,
        "database_integrity": integrity_status,
        "total_rows": total_rows,
        "total_instruction_counts": dict(
            sorted(
                total_instruction_counts.items()
            )
        ),
        "total_category_counts": dict(
            sorted(
                total_category_counts.items()
            )
        ),
        "creates_count": creates_count,
        "raw_first_buys_count": raw_first_buys_count,
        "eligible_first_buys_count": (
            eligible_first_buys_count
        ),
        "out_of_scope_first_buys_count": (
            out_of_scope_first_buys_count
        ),
        "migrations_count": migrations_count,
        "unknown_discriminators": dict(
            sorted(
                unknown_discriminators.items()
            )
        ),
        "instruction_anomaly_count": instruction_anomaly_count,
        "ignored_failed_instruction_anomaly_count": len(
            ignored_failed_instruction_anomalies
        ),
        "unresolved_instruction_anomaly_count": len(
            unresolved_instruction_anomalies
        ),
        "unresolved_instruction_anomalies_sample": (
            unresolved_instruction_anomalies[:100]
        ),
        "missing_required_account_row_count": len(
            missing_required_account_rows
        ),
        "missing_required_account_rows_sample": (
            missing_required_account_rows[:100]
        ),
        "empty_days": empty_days,
        "total_estimated_bytes_processed": (
            total_estimated_bytes
        ),
        "total_estimated_gigabytes_processed": round(
            total_estimated_bytes
            / 1_000_000_000,
            3,
        ),
        "daily_results": daily_results,
        "files": {
            "database": database_path.name,
            "creates_csv": creates_csv_path.name,
            "eligible_first_buys_csv": (
                eligible_first_buys_csv_path.name
            ),
            "out_of_scope_first_buys_csv": (
                out_of_scope_first_buys_csv_path.name
            ),
            "migrations_csv": migrations_csv_path.name,
            "instruction_anomalies_csv": anomalies_csv_path.name,
        },
    }

    manifest_path.write_text(
        json.dumps(
            manifest,
            indent=2,
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    print(
        json.dumps(
            {
                key: value
                for key, value in manifest.items()
                if key != "daily_results"
            },
            indent=2,
            ensure_ascii=False,
        )
    )

    if unresolved_instruction_anomalies:
        print(
            "Başarılı veya durumu doğrulanamayan bilinmeyen "
            "instruction bulundu; anomaly CSV'yi incele.",
            file=sys.stderr,
        )

        return 2

    if missing_required_account_rows:
        print(
            "Gerekli mint hesabı çözülemeyen "
            "create/buy/sell/migration kayıtları var.",
            file=sys.stderr,
        )

        return 3

    if empty_days:
        print(
            "Pump.fun instruction bulunmayan "
            "gün veya veri boşluğu var.",
            file=sys.stderr,
        )

        return 4

    if integrity_status != "ok":
        print(
            "SQLite bütünlük kontrolü başarısız.",
            file=sys.stderr,
        )

        return 5

    if (
        raw_first_buys_count
        != eligible_first_buys_count
        + out_of_scope_first_buys_count
    ):
        print(
            "First-buy kapsam sayımları tutarsız.",
            file=sys.stderr,
        )

        return 6

    print("PUMPFUN_RANGE_BACKFILL_OK")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
