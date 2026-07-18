from __future__ import annotations

import argparse
import csv
import hashlib
import json
import sys
import urllib.request
from collections import Counter
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path
from typing import Any

import base58
from google.cloud import bigquery


PUMP_PROGRAM_ID = "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P"

PUMP_IDL_URL = (
    "https://raw.githubusercontent.com/"
    "pump-fun/pump-public-docs/main/idl/pump.json"
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--date",
        required=True,
        help="UTC tarihi: YYYY-MM-DD",
    )
    parser.add_argument(
        "--project",
        required=True,
        help="BigQuery job projesi",
    )
    parser.add_argument(
        "--output-dir",
        default="artifacts/pumpfun",
    )
    return parser.parse_args()


def load_pump_idl() -> tuple[dict[bytes, dict[str, Any]], str]:
    with urllib.request.urlopen(PUMP_IDL_URL, timeout=30) as response:
        raw_idl = response.read()

    idl = json.loads(raw_idl)
    instruction_map: dict[bytes, dict[str, Any]] = {}

    for instruction in idl.get("instructions", []):
        name = instruction.get("name")
        discriminator = instruction.get("discriminator")

        if (
            not name
            or not isinstance(discriminator, list)
            or len(discriminator) != 8
        ):
            continue

        discriminator_bytes = bytes(
            int(value) for value in discriminator
        )
        instruction_map[discriminator_bytes] = instruction

    if not instruction_map:
        raise RuntimeError(
            "Pump IDL içinde instruction discriminator bulunamadı."
        )

    return instruction_map, hashlib.sha256(raw_idl).hexdigest()


def flatten_account_names(
    accounts: list[dict[str, Any]],
) -> list[str]:
    names: list[str] = []

    for account in accounts:
        nested_accounts = account.get("accounts")

        if isinstance(nested_accounts, list):
            names.extend(flatten_account_names(nested_accounts))
        elif account.get("name"):
            names.append(str(account["name"]))

    return names


def choose_account(
    account_map: dict[str, str],
    *candidate_names: str,
) -> str:
    for name in candidate_names:
        value = account_map.get(name)

        if value:
            return value

    return ""


def query_day(
    client: bigquery.Client,
    start_timestamp: datetime,
    end_timestamp: datetime,
) -> tuple[list[Any], int]:
    sql = """
    SELECT
      i.block_timestamp,
      i.block_slot,
      i.tx_signature,
      i.index,
      i.parent_index,
      i.accounts,
      i.data,
      t.fee AS transaction_fee_lamports,
      t.status AS transaction_status
    FROM
      `bigquery-public-data.crypto_solana_mainnet_us.Instructions` AS i
    JOIN
      `bigquery-public-data.crypto_solana_mainnet_us.Transactions` AS t
    ON
      t.signature = i.tx_signature
      AND t.block_timestamp >= @start_timestamp
      AND t.block_timestamp < @end_timestamp
    WHERE
      i.block_timestamp >= @start_timestamp
      AND i.block_timestamp < @end_timestamp
      AND i.program_id = @program_id
    ORDER BY
      i.block_timestamp,
      i.tx_signature,
      i.index
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

    rows = list(
        client.query(
            sql,
            job_config=run_config,
        ).result()
    )

    return rows, estimated_bytes


def main() -> int:
    args = parse_args()

    scan_date = date.fromisoformat(args.date)

    start_timestamp = datetime.combine(
        scan_date,
        time.min,
        tzinfo=timezone.utc,
    )
    end_timestamp = start_timestamp + timedelta(days=1)

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    instruction_map, idl_sha256 = load_pump_idl()

    client = bigquery.Client(project=args.project)

    rows, estimated_bytes = query_day(
        client,
        start_timestamp,
        end_timestamp,
    )

    csv_path = (
        output_dir
        / f"pumpfun_instructions_{scan_date.isoformat()}.csv"
    )
    summary_path = (
        output_dir
        / f"pumpfun_summary_{scan_date.isoformat()}.json"
    )

    instruction_counts: Counter[str] = Counter()
    unknown_discriminators: Counter[str] = Counter()

    columns = [
        "block_timestamp",
        "block_slot",
        "tx_signature",
        "instruction_index",
        "parent_index",
        "instruction_name",
        "discriminator_hex",
        "mint",
        "user",
        "creator",
        "bonding_curve",
        "transaction_fee_lamports",
        "transaction_status",
        "accounts_json",
        "data_base58",
    ]

    with csv_path.open(
        "w",
        newline="",
        encoding="utf-8",
    ) as file:
        writer = csv.DictWriter(
            file,
            fieldnames=columns,
        )
        writer.writeheader()

        for row in rows:
            try:
                decoded_data = base58.b58decode(row.data)
            except Exception as error:
                raise RuntimeError(
                    "Base58 decode başarısız: "
                    f"{row.tx_signature} / {row.index}"
                ) from error

            if len(decoded_data) < 8:
                raise RuntimeError(
                    "Instruction data 8 byte'tan kısa: "
                    f"{row.tx_signature} / {row.index}"
                )

            discriminator = decoded_data[:8]
            discriminator_hex = discriminator.hex()

            instruction_definition = instruction_map.get(
                discriminator
            )

            if instruction_definition is None:
                instruction_name = "UNKNOWN"
                account_names: list[str] = []
                unknown_discriminators[
                    discriminator_hex
                ] += 1
            else:
                instruction_name = str(
                    instruction_definition["name"]
                )
                account_names = flatten_account_names(
                    instruction_definition.get(
                        "accounts",
                        [],
                    )
                )

            instruction_counts[instruction_name] += 1

            accounts = list(row.accounts or [])

            account_map = {
                account_name: accounts[position]
                for position, account_name
                in enumerate(account_names)
                if position < len(accounts)
            }

            writer.writerow(
                {
                    "block_timestamp": (
                        row.block_timestamp.isoformat()
                    ),
                    "block_slot": row.block_slot,
                    "tx_signature": row.tx_signature,
                    "instruction_index": row.index,
                    "parent_index": row.parent_index,
                    "instruction_name": instruction_name,
                    "discriminator_hex": discriminator_hex,
                    "mint": choose_account(
                        account_map,
                        "mint",
                        "base_mint",
                    ),
                    "user": choose_account(
                        account_map,
                        "user",
                    ),
                    "creator": choose_account(
                        account_map,
                        "creator",
                    ),
                    "bonding_curve": choose_account(
                        account_map,
                        "bonding_curve",
                    ),
                    "transaction_fee_lamports": (
                        row.transaction_fee_lamports
                    ),
                    "transaction_status": (
                        row.transaction_status
                    ),
                    "accounts_json": json.dumps(
                        accounts,
                        ensure_ascii=False,
                    ),
                    "data_base58": row.data,
                }
            )

    summary = {
        "scan_date_utc": scan_date.isoformat(),
        "program_id": PUMP_PROGRAM_ID,
        "row_count": len(rows),
        "unique_transactions": len(
            {row.tx_signature for row in rows}
        ),
        "instruction_counts": dict(
            sorted(instruction_counts.items())
        ),
        "unknown_discriminators": dict(
            sorted(unknown_discriminators.items())
        ),
        "estimated_bytes_processed": estimated_bytes,
        "idl_url": PUMP_IDL_URL,
        "idl_sha256": idl_sha256,
        "csv_file": csv_path.name,
    }

    summary_path.write_text(
        json.dumps(
            summary,
            indent=2,
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    print(
        json.dumps(
            summary,
            indent=2,
            ensure_ascii=False,
        )
    )

    if unknown_discriminators:
        print(
            "Bilinmeyen discriminator bulundu. "
            "Kayıtlar CSV'ye yazıldı ama kontrol gerekiyor.",
            file=sys.stderr,
        )
        return 2

    if not rows:
        print(
            "Seçilen günde Pump.fun instruction bulunamadı.",
            file=sys.stderr,
        )
        return 3

    print("PUMPFUN_DAILY_EXPORT_OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
