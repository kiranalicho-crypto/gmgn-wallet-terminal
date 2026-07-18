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


PUMP_PROGRAM_ID = (
    "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P"
)

PUMP_IDL_URL = (
    "https://raw.githubusercontent.com/"
    "pump-fun/pump-public-docs/main/idl/pump.json"
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Pump.fun instructionlarını BigQuery'den "
            "belirli bir UTC günü için çeker."
        )
    )

    parser.add_argument(
        "--date",
        required=True,
        help="Taranacak UTC tarihi: YYYY-MM-DD",
    )

    parser.add_argument(
        "--project",
        required=True,
        help="BigQuery sorgu projesi",
    )

    parser.add_argument(
        "--output-dir",
        default="artifacts/pumpfun",
        help="Çıktı klasörü",
    )

    return parser.parse_args()


def load_pump_idl(
) -> tuple[dict[bytes, dict[str, Any]], str]:
    request = urllib.request.Request(
        PUMP_IDL_URL,
        headers={
            "User-Agent": "gmgn-wallet-terminal/1.0",
        },
    )

    with urllib.request.urlopen(
        request,
        timeout=30,
    ) as response:
        raw_idl = response.read()

    idl = json.loads(raw_idl)

    instruction_map: dict[
        bytes,
        dict[str, Any],
    ] = {}

    for instruction in idl.get("instructions", []):
        name = instruction.get("name")
        discriminator = instruction.get(
            "discriminator"
        )

        if not name:
            continue

        if not isinstance(discriminator, list):
            continue

        if len(discriminator) != 8:
            continue

        discriminator_bytes = bytes(
            int(value)
            for value in discriminator
        )

        instruction_map[
            discriminator_bytes
        ] = instruction

    if not instruction_map:
        raise RuntimeError(
            "Pump IDL içinde instruction "
            "discriminator bulunamadı."
        )

    idl_sha256 = hashlib.sha256(
        raw_idl
    ).hexdigest()

    return instruction_map, idl_sha256


def flatten_account_names(
    accounts: list[dict[str, Any]],
) -> list[str]:
    names: list[str] = []

    for account in accounts:
        nested_accounts = account.get(
            "accounts"
        )

        if isinstance(nested_accounts, list):
            names.extend(
                flatten_account_names(
                    nested_accounts
                )
            )
            continue

        name = account.get("name")

        if name:
            names.append(str(name))

    return names


def choose_account(
    account_map: dict[str, str],
    *candidate_names: str,
) -> str:
    for candidate_name in candidate_names:
        value = account_map.get(
            candidate_name
        )

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

    query_parameters = [
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

    dry_run_config = (
        bigquery.QueryJobConfig(
            query_parameters=query_parameters,
            dry_run=True,
            use_query_cache=False,
        )
    )

    dry_run_job = client.query(
        sql,
        job_config=dry_run_config,
    )

    estimated_bytes = int(
        dry_run_job.total_bytes_processed
        or 0
    )

    run_config = bigquery.QueryJobConfig(
        query_parameters=query_parameters,
        use_query_cache=True,
    )

    query_job = client.query(
        sql,
        job_config=run_config,
    )

    rows = list(query_job.result())

    return rows, estimated_bytes


def decode_instruction_data(
    encoded_data: str,
    tx_signature: str,
    instruction_index: int,
) -> bytes:
    try:
        decoded_data = base58.b58decode(
            encoded_data
        )
    except Exception as error:
        raise RuntimeError(
            "Base58 decode başarısız: "
            f"{tx_signature} / "
            f"{instruction_index}"
        ) from error

    if len(decoded_data) < 8:
        raise RuntimeError(
            "Instruction data 8 byte'tan kısa: "
            f"{tx_signature} / "
            f"{instruction_index}"
        )

    return decoded_data


def main() -> int:
    args = parse_args()

    try:
        scan_date = date.fromisoformat(
            args.date
        )
    except ValueError as error:
        raise RuntimeError(
            "Tarih YYYY-MM-DD biçiminde olmalı."
        ) from error

    start_timestamp = datetime.combine(
        scan_date,
        time.min,
        tzinfo=timezone.utc,
    )

    end_timestamp = (
        start_timestamp
        + timedelta(days=1)
    )

    output_dir = Path(args.output_dir)

    output_dir.mkdir(
        parents=True,
        exist_ok=True,
    )

    instruction_map, idl_sha256 = (
        load_pump_idl()
    )

    client = bigquery.Client(
        project=args.project
    )

    rows, estimated_bytes = query_day(
        client,
        start_timestamp,
        end_timestamp,
    )

    csv_path = (
        output_dir
        / (
            "pumpfun_instructions_"
            f"{scan_date.isoformat()}.csv"
        )
    )

    summary_path = (
        output_dir
        / (
            "pumpfun_summary_"
            f"{scan_date.isoformat()}.json"
        )
    )

    instruction_counts: Counter[
        str
    ] = Counter()

    unknown_discriminators: Counter[
        str
    ] = Counter()

    unique_transactions: set[str] = set()

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
            unique_transactions.add(
                str(row.tx_signature)
            )

            decoded_data = (
                decode_instruction_data(
                    encoded_data=str(row.data),
                    tx_signature=str(
                        row.tx_signature
                    ),
                    instruction_index=int(
                        row.instruction_index
                    ),
                )
            )

            discriminator = (
                decoded_data[:8]
            )

            discriminator_hex = (
                discriminator.hex()
            )

            instruction_definition = (
                instruction_map.get(
                    discriminator
                )
            )

            if instruction_definition is None:
                instruction_name = "UNKNOWN"

                account_names: list[str] = []

                unknown_discriminators[
                    discriminator_hex
                ] += 1
            else:
                instruction_name = str(
                    instruction_definition[
                        "name"
                    ]
                )

                account_names = (
                    flatten_account_names(
                        instruction_definition.get(
                            "accounts",
                            [],
                        )
                    )
                )

            instruction_counts[
                instruction_name
            ] += 1

            accounts = [
                str(account)
                for account in (
                    row.accounts or []
                )
            ]

            account_map = {
                account_name: accounts[
                    position
                ]
                for position, account_name
                in enumerate(account_names)
                if position < len(accounts)
            }

            mint = choose_account(
                account_map,
                "mint",
                "base_mint",
            )

            user = choose_account(
                account_map,
                "user",
                "buyer",
                "seller",
                "payer",
            )

            creator = choose_account(
                account_map,
                "creator",
            )

            bonding_curve = choose_account(
                account_map,
                "bonding_curve",
            )

            block_timestamp = (
                row.block_timestamp
            )

            if hasattr(
                block_timestamp,
                "isoformat",
            ):
                timestamp_text = (
                    block_timestamp.isoformat()
                )
            else:
                timestamp_text = str(
                    block_timestamp
                )

            writer.writerow(
                {
                    "block_timestamp": (
                        timestamp_text
                    ),
                    "block_slot": (
                        row.block_slot
                    ),
                    "tx_signature": (
                        row.tx_signature
                    ),
                    "instruction_index": (
                        row.instruction_index
                    ),
                    "parent_index": (
                        row.parent_index
                    ),
                    "instruction_name": (
                        instruction_name
                    ),
                    "discriminator_hex": (
                        discriminator_hex
                    ),
                    "mint": mint,
                    "user": user,
                    "creator": creator,
                    "bonding_curve": (
                        bonding_curve
                    ),
                    "accounts_json": (
                        json.dumps(
                            accounts,
                            ensure_ascii=False,
                        )
                    ),
                    "data_base58": (
                        row.data
                    ),
                }
            )

    summary = {
        "scan_date_utc": (
            scan_date.isoformat()
        ),
        "program_id": PUMP_PROGRAM_ID,
        "row_count": len(rows),
        "unique_transactions": len(
            unique_transactions
        ),
        "instruction_counts": dict(
            sorted(
                instruction_counts.items()
            )
        ),
        "unknown_discriminators": dict(
            sorted(
                unknown_discriminators.items()
            )
        ),
        "estimated_bytes_processed": (
            estimated_bytes
        ),
        "estimated_megabytes_processed": (
            round(
                estimated_bytes
                / 1_000_000,
                2,
            )
        ),
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
            "Kayıtlar CSV'ye yazıldı ancak "
            "kontrol edilmesi gerekiyor.",
            file=sys.stderr,
        )

        return 2

    if not rows:
        print(
            "Seçilen günde Pump.fun "
            "instruction bulunamadı.",
            file=sys.stderr,
        )

        return 3

    print("PUMPFUN_DAILY_EXPORT_OK")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
