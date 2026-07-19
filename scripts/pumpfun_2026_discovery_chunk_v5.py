"""Discover one monthly chunk of the 2026 Pump.fun migration universe.

The migration window is separate from the X-token creation scope. This is
important: a token created in January may migrate in February and must still
remain inside the 2026 scope.

This stage intentionally reads only:
- canonical Pump ``migrate`` instructions,
- historical Pump ``withdraw`` migration instructions,
- token metadata for the discovered mints,
- targeted ``create`` instructions only when metadata is incomplete.

It does not download Pump.fun buy/sell history.  The output is the compact
mint/creator universe used by the later ATH stage.
"""

from __future__ import annotations

import argparse
import base58
import csv
import hashlib
import json
import os
import random
import struct
import sys
import time
from collections import defaultdict
from dataclasses import dataclass
from datetime import date, datetime, time as dt_time, timedelta, timezone
from pathlib import Path
from typing import Any, Callable, Iterable, Sequence, TypeVar

from google.api_core import exceptions as gexc
from google.cloud import bigquery

try:
    import requests
except ImportError:  # pragma: no cover - requests is a transitive dependency
    requests = None  # type: ignore[assignment]


SCRIPT_VERSION = "2026-07-19-discovery-chunk-v5"
PUMP_PROGRAM_ID = "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P"
MINT_AUTHORITY = "TSLvdd1pWpHVjahSpsvCXUbgwsL3JAcvokwaKt1eokM"

# Official canonical PumpSwap migration, no arguments.
MIGRATE_DISCRIMINATOR = bytes([123, 246, 78, 200, 245, 251, 253, 202])
# Deprecated Raydium migration used historically, no arguments.
WITHDRAW_DISCRIMINATOR = bytes([183, 18, 70, 156, 148, 109, 161, 34])

CREATE_DISCRIMINATOR = bytes([24, 30, 200, 40, 5, 28, 7, 119])
CREATE_V2_DISCRIMINATOR = bytes([214, 144, 76, 236, 95, 139, 49, 180])

MIGRATE_DATA_B58 = base58.b58encode(MIGRATE_DISCRIMINATOR).decode("ascii")
WITHDRAW_DATA_B58 = base58.b58encode(WITHDRAW_DISCRIMINATOR).decode("ascii")

INSTRUCTIONS_TABLE = "bigquery-public-data.crypto_solana_mainnet_us.Instructions"
TOKENS_TABLE = "bigquery-public-data.crypto_solana_mainnet_us.Tokens"
TRANSACTIONS_TABLE = "bigquery-public-data.crypto_solana_mainnet_us.Transactions"
TOKEN_DATASET_START = datetime(2020, 10, 7, tzinfo=timezone.utc)

T = TypeVar("T")
RUN_INSTANCE = os.environ.get("GITHUB_RUN_ID", "local") + "_" + os.environ.get("GITHUB_RUN_ATTEMPT", str(int(time.time())))


@dataclass(frozen=True)
class RetryPolicy:
    attempts: int = 8
    initial_delay_seconds: float = 5.0
    maximum_delay_seconds: float = 90.0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--version", action="store_true")
    parser.add_argument("--project")
    parser.add_argument("--migration-start-date", required=True)
    parser.add_argument("--migration-end-date", required=True)
    parser.add_argument("--scope-start-date", default="2026-01-01")
    parser.add_argument("--scope-end-date", default="2026-07-18")
    parser.add_argument("--chunk-id", required=True)
    parser.add_argument("--output-dir", default="artifacts/discovery")
    parser.add_argument(
        "--maximum-bytes-billed",
        type=int,
        default=600_000_000_000,
        help="Main discovery query hard ceiling. Dry-run exceeds it => stop.",
    )
    parser.add_argument(
        "--status-maximum-bytes-billed",
        type=int,
        default=10_000_000_000,
    )
    parser.add_argument(
        "--fallback-maximum-bytes-billed",
        type=int,
        default=100_000_000_000,
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
    end_exclusive = datetime.combine(
        end_day + timedelta(days=1), dt_time.min, tzinfo=timezone.utc
    )
    return start, end_exclusive


def stable_job_id(label: str, *parts: str) -> str:
    digest = hashlib.sha256((RUN_INSTANCE + "|" + "|".join(parts)).encode("utf-8")).hexdigest()[:20]
    clean = "".join(ch if ch.isalnum() or ch == "_" else "_" for ch in label)
    return f"walletintel_{SCRIPT_VERSION.replace('-', '_')}_{clean}_{digest}"[:1024]


def is_transient(exc: BaseException) -> bool:
    transient_types: tuple[type[BaseException], ...] = (
        gexc.TooManyRequests,
        gexc.ServiceUnavailable,
        gexc.InternalServerError,
        gexc.BadGateway,
        gexc.GatewayTimeout,
        gexc.DeadlineExceeded,
    )
    if isinstance(exc, transient_types):
        return True
    if requests is not None and isinstance(
        exc,
        (requests.exceptions.SSLError, requests.exceptions.ConnectionError),
    ):
        return True
    text = str(exc).upper()
    return any(
        marker in text
        for marker in (
            "SSLEOFERROR",
            "EOF OCCURRED IN VIOLATION OF PROTOCOL",
            "ECONNRESET",
            "CONNECTION RESET",
            "TEMPORARY FAILURE",
            "429",
            "502",
            "503",
            "504",
        )
    )


def with_retry(operation: Callable[[], T], *, label: str, policy: RetryPolicy) -> T:
    delay = policy.initial_delay_seconds
    for attempt in range(1, policy.attempts + 1):
        try:
            return operation()
        except BaseException as exc:
            if attempt >= policy.attempts or not is_transient(exc):
                raise
            jitter = random.uniform(0.0, min(3.0, delay * 0.2))
            wait = min(policy.maximum_delay_seconds, delay + jitter)
            print(
                f"RETRY {label} attempt={attempt}/{policy.attempts} "
                f"wait={wait:.1f}s error={type(exc).__name__}: {exc}",
                file=sys.stderr,
                flush=True,
            )
            time.sleep(wait)
            delay = min(policy.maximum_delay_seconds, delay * 2)
    raise AssertionError("unreachable")


def start_or_resume_query(
    client: bigquery.Client,
    *,
    sql: str,
    config: bigquery.QueryJobConfig,
    job_id: str,
    location: str = "US",
    retry_policy: RetryPolicy = RetryPolicy(),
) -> bigquery.QueryJob:
    def submit() -> bigquery.QueryJob:
        try:
            return client.query(
                sql,
                job_config=config,
                job_id=job_id,
                location=location,
            )
        except gexc.Conflict:
            return client.get_job(job_id, location=location)

    job = with_retry(submit, label=f"submit:{job_id}", policy=retry_policy)

    def wait() -> bigquery.QueryJob:
        job.result()
        return job

    return with_retry(wait, label=f"result:{job_id}", policy=retry_policy)


def row_value(row: Any, key: str, default: Any = None) -> Any:
    try:
        value = row[key]
    except (KeyError, TypeError, IndexError):
        value = getattr(row, key, default)
    return default if value is None else value


def timestamp_text(value: Any) -> str:
    if value in (None, ""):
        return ""
    if isinstance(value, datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc).isoformat()
    return str(value)


def transaction_success_state(status: Any, error_value: Any) -> bool | None:
    status_text = str(status).strip().lower() if status is not None else ""
    error_text = str(error_value).strip().lower() if error_value is not None else ""
    if error_text not in {"", "none", "null"}:
        return False
    if status_text in {"failed", "failure", "error", "err"}:
        return False
    if status_text in {"success", "succeeded", "ok"}:
        return True
    if status_text:
        return True
    if error_value is None:
        return True
    return None


def select_creator(creators: Any) -> tuple[str, str, int]:
    candidates: list[dict[str, Any]] = []
    if creators:
        for item in creators:
            address = str(row_value(item, "address", "") or "")
            if not address:
                continue
            candidates.append(
                {
                    "address": address,
                    "verified": bool(row_value(item, "verified", False)),
                    "share": int(row_value(item, "share", 0) or 0),
                }
            )
    if not candidates:
        return "", "", 0
    candidates.sort(
        key=lambda item: (item["verified"], item["share"]), reverse=True
    )
    chosen = candidates[0]
    source = (
        "tokens_verified_creator"
        if chosen["verified"]
        else "tokens_unverified_creator"
    )
    return str(chosen["address"]), source, len(candidates)


def main_query() -> str:
    return f"""
    WITH migration_rows AS (
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
          WHEN data = @migrate_data THEN accounts[SAFE_OFFSET(10)]
          WHEN data = @withdraw_data THEN accounts[SAFE_OFFSET(5)]
        END AS migrator,
        CASE
          WHEN data = @migrate_data THEN 'migrate'
          WHEN data = @withdraw_data THEN 'withdraw'
        END AS migration_type
      FROM `{INSTRUCTIONS_TABLE}`
      WHERE block_timestamp >= @start_timestamp
        AND block_timestamp < @end_timestamp
        AND program_id = @program_id
        AND data IN UNNEST(@migration_data_values)
    ),
    migrated_mints AS (
      SELECT DISTINCT mint
      FROM migration_rows
      WHERE mint IS NOT NULL
    ),
    token_metadata AS (
      SELECT
        t.mint,
        t.block_timestamp AS created_at_utc,
        t.tx_signature AS create_tx,
        t.name,
        t.symbol,
        t.uri,
        t.update_authority,
        t.creators,
        ROW_NUMBER() OVER (
          PARTITION BY t.mint
          ORDER BY t.block_timestamp ASC, t.tx_signature ASC
        ) AS rn
      FROM `{TOKENS_TABLE}` t
      JOIN migrated_mints m USING (mint)
      WHERE t.block_timestamp >= @metadata_start_timestamp
        AND t.block_timestamp < @end_timestamp
    )
    SELECT
      m.migrated_at_utc,
      m.migration_block_slot,
      m.migration_tx,
      m.migration_instruction_index,
      m.migration_data,
      m.mint,
      m.migrator,
      m.migration_type,
      t.created_at_utc,
      t.create_tx,
      t.name,
      t.symbol,
      t.uri,
      t.update_authority,
      t.creators
    FROM migration_rows m
    LEFT JOIN token_metadata t
      ON m.mint = t.mint AND t.rn = 1
    ORDER BY m.migrated_at_utc, m.migration_tx
    """


def main_query_config(
    start: datetime,
    end_exclusive: datetime,
    maximum_bytes_billed: int,
    metadata_start: datetime | None = None,
    *,
    dry_run: bool,
) -> bigquery.QueryJobConfig:
    if metadata_start is None:
        metadata_start = start
    config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("start_timestamp", "TIMESTAMP", start),
            bigquery.ScalarQueryParameter(
                "end_timestamp", "TIMESTAMP", end_exclusive
            ),
            bigquery.ScalarQueryParameter(
                "metadata_start_timestamp", "TIMESTAMP", metadata_start
            ),
            bigquery.ScalarQueryParameter("program_id", "STRING", PUMP_PROGRAM_ID),
            bigquery.ScalarQueryParameter(
                "migrate_data", "STRING", MIGRATE_DATA_B58
            ),
            bigquery.ScalarQueryParameter(
                "withdraw_data", "STRING", WITHDRAW_DATA_B58
            ),
            bigquery.ArrayQueryParameter(
                "migration_data_values",
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


def query_statuses(
    client: bigquery.Client,
    rows: Sequence[dict[str, Any]],
    *,
    retry_policy: RetryPolicy,
    maximum_bytes_billed: int,
) -> tuple[dict[str, dict[str, Any]], int]:
    by_day: dict[str, set[str]] = defaultdict(set)
    for row in rows:
        migrated = row.get("migrated_at_utc")
        signature = str(row.get("migration_tx") or "")
        if not migrated or not signature:
            continue
        day = str(migrated)[:10]
        by_day[day].add(signature)

    statuses: dict[str, dict[str, Any]] = {}
    total_bytes = 0
    sql = f"""
    SELECT signature, status, err
    FROM `{TRANSACTIONS_TABLE}`
    WHERE block_timestamp >= @start_timestamp
      AND block_timestamp < @end_timestamp
      AND signature IN UNNEST(@signatures)
    """

    for day_text, signature_set in sorted(by_day.items()):
        day = date.fromisoformat(day_text)
        day_start = datetime.combine(day, dt_time.min, tzinfo=timezone.utc)
        day_end = day_start + timedelta(days=1)
        signatures = sorted(signature_set)
        for offset in range(0, len(signatures), 2_000):
            chunk = signatures[offset : offset + 2_000]
            chunk_hash = hashlib.sha256("|".join(chunk).encode()).hexdigest()[:12]
            config = bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter(
                        "start_timestamp", "TIMESTAMP", day_start
                    ),
                    bigquery.ScalarQueryParameter(
                        "end_timestamp", "TIMESTAMP", day_end
                    ),
                    bigquery.ArrayQueryParameter("signatures", "STRING", chunk),
                ],
                use_query_cache=True,
                maximum_bytes_billed=maximum_bytes_billed,
            )
            job_id = stable_job_id(
                "status", day_text, str(offset), chunk_hash
            )
            job = start_or_resume_query(
                client,
                sql=sql,
                config=config,
                job_id=job_id,
                retry_policy=retry_policy,
            )
            total_bytes += int(job.total_bytes_processed or 0)
            result_rows = with_retry(
                lambda: list(job.result(page_size=2_000)),
                label=f"iterate:{job_id}",
                policy=retry_policy,
            )
            for result in result_rows:
                signature = str(row_value(result, "signature", ""))
                status = row_value(result, "status", None)
                err = row_value(result, "err", None)
                statuses[signature] = {
                    "status": "" if status is None else str(status),
                    "err": "" if err is None else str(err),
                    "is_success": transaction_success_state(status, err),
                }
    return statuses, total_bytes


def decode_creator_from_create_data(encoded: str) -> tuple[str, str]:
    try:
        raw = base58.b58decode(encoded)
    except Exception as exc:
        raise ValueError("create_data_base58_decode_failed") from exc
    if len(raw) < 8:
        raise ValueError("create_data_short")
    discriminator = raw[:8]
    if discriminator not in {CREATE_DISCRIMINATOR, CREATE_V2_DISCRIMINATOR}:
        raise ValueError(f"unexpected_create_discriminator:{discriminator.hex()}")
    cursor = 8
    for field_name in ("name", "symbol", "uri"):
        if cursor + 4 > len(raw):
            raise ValueError(f"create_{field_name}_length_missing")
        length = struct.unpack_from("<I", raw, cursor)[0]
        cursor += 4
        if length > 1_000_000 or cursor + length > len(raw):
            raise ValueError(f"create_{field_name}_invalid_length:{length}")
        cursor += length
    if cursor + 32 <= len(raw):
        creator = base58.b58encode(raw[cursor : cursor + 32]).decode("ascii")
        return creator, "create_instruction_creator_arg"
    if discriminator == CREATE_DISCRIMINATOR:
        return "", "legacy_create_without_creator_arg"
    raise ValueError("create_v2_creator_arg_missing")


def targeted_create_fallback(
    client: bigquery.Client,
    *,
    mints: Sequence[str],
    start: datetime,
    end_exclusive: datetime,
    retry_policy: RetryPolicy,
    maximum_bytes_billed: int,
) -> tuple[dict[str, dict[str, Any]], list[dict[str, Any]], int]:
    if not mints:
        return {}, [], 0
    sql = f"""
    SELECT
      block_timestamp,
      block_slot,
      tx_signature,
      index AS instruction_index,
      accounts,
      data
    FROM `{INSTRUCTIONS_TABLE}`
    WHERE block_timestamp >= @start_timestamp
      AND block_timestamp < @end_timestamp
      AND program_id = @program_id
      AND accounts[SAFE_OFFSET(0)] IN UNNEST(@mints)
      AND accounts[SAFE_OFFSET(1)] = @mint_authority
    ORDER BY block_timestamp, tx_signature, instruction_index
    """
    found: dict[str, dict[str, Any]] = {}
    anomalies: list[dict[str, Any]] = []
    total_bytes = 0

    # This is only a metadata-gap fallback.  Keep request payloads bounded and
    # stop rather than re-scan the full date range an unbounded number of times.
    if len(mints) > 10_000:
        raise RuntimeError(
            "Metadata fallback 10,000 minti aştı; toplu veri kaynağı sorunu var. "
            "Aynı tarih partitionını tekrar tekrar taramamak için işlem durduruldu."
        )

    config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("start_timestamp", "TIMESTAMP", start),
            bigquery.ScalarQueryParameter(
                "end_timestamp", "TIMESTAMP", end_exclusive
            ),
            bigquery.ScalarQueryParameter("program_id", "STRING", PUMP_PROGRAM_ID),
            bigquery.ScalarQueryParameter(
                "mint_authority", "STRING", MINT_AUTHORITY
            ),
            bigquery.ArrayQueryParameter("mints", "STRING", sorted(set(mints))),
        ],
        use_query_cache=True,
        maximum_bytes_billed=maximum_bytes_billed,
    )
    mint_hash = hashlib.sha256("|".join(sorted(set(mints))).encode()).hexdigest()
    job_id = stable_job_id(
        "create_fallback", start.isoformat(), end_exclusive.isoformat(), mint_hash
    )
    job = start_or_resume_query(
        client,
        sql=sql,
        config=config,
        job_id=job_id,
        retry_policy=retry_policy,
    )
    total_bytes += int(job.total_bytes_processed or 0)
    query_rows = with_retry(
        lambda: list(job.result(page_size=5_000)),
        label=f"iterate:{job_id}",
        policy=retry_policy,
    )

    raw_rows: list[dict[str, Any]] = []
    for row in query_rows:
        accounts = [str(value) for value in (row_value(row, "accounts", []) or [])]
        mint = accounts[0] if accounts else ""
        record = {
            "mint": mint,
            "created_at_utc": timestamp_text(row_value(row, "block_timestamp", "")),
            "create_tx": str(row_value(row, "tx_signature", "")),
            "instruction_index": int(row_value(row, "instruction_index", 0)),
            "accounts": accounts,
            "data": str(row_value(row, "data", "")),
        }
        try:
            creator, source = decode_creator_from_create_data(record["data"])
            if not creator and source == "legacy_create_without_creator_arg":
                # Legacy create's user is account 7. This fallback is explicit,
                # never silently applied to create_v2.
                creator = accounts[7] if len(accounts) > 7 else ""
                source = "legacy_create_user_account"
            record["creator"] = creator
            record["creator_source"] = source
        except ValueError as exc:
            anomalies.append(
                {
                    "mint": mint,
                    "create_tx": record["create_tx"],
                    "reason": str(exc),
                }
            )
            raw_rows.append(record)
            continue
        raw_rows.append(record)

    status_input = [
        {
            "migrated_at_utc": item["created_at_utc"],
            "migration_tx": item["create_tx"],
        }
        for item in raw_rows
    ]
    statuses, status_bytes = query_statuses(
        client,
        status_input,
        retry_policy=retry_policy,
        maximum_bytes_billed=maximum_bytes_billed,
    )
    total_bytes += status_bytes

    for record in raw_rows:
        status = statuses.get(record["create_tx"])
        if status is None or status.get("is_success") is not True:
            continue
        mint = record["mint"]
        if not mint or not record.get("creator"):
            continue
        previous = found.get(mint)
        if previous is None or record["created_at_utc"] < previous["created_at_utc"]:
            found[mint] = record
    return found, anomalies, total_bytes


def write_csv(path: Path, rows: Iterable[dict[str, Any]], fields: Sequence[str]) -> int:
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


def main() -> int:
    args = parse_args()
    if args.version:
        print(SCRIPT_VERSION)
        return 0
    if not args.project:
        raise SystemExit("--project zorunlu")

    migration_start, migration_end_exclusive = utc_bounds(
        args.migration_start_date, args.migration_end_date
    )
    scope_start, scope_end_exclusive = utc_bounds(
        args.scope_start_date, args.scope_end_date
    )
    if migration_start < scope_start or migration_end_exclusive > scope_end_exclusive:
        raise SystemExit(
            "Migration chunk 2026 scope sınırlarının dışında olamaz."
        )
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    retry_policy = RetryPolicy()
    client = bigquery.Client(project=args.project, location="US")
    sql = main_query()

    dry_config = main_query_config(
        migration_start,
        migration_end_exclusive,
        args.maximum_bytes_billed,
        metadata_start=scope_start,
        dry_run=True,
    )
    dry_job = with_retry(
        lambda: client.query(sql, job_config=dry_config, location="US"),
        label="discovery_dry_run",
        policy=retry_policy,
    )
    estimated_main_bytes = int(dry_job.total_bytes_processed or 0)
    preflight = {
        "script_version": SCRIPT_VERSION,
        "chunk_id": args.chunk_id,
        "migration_start_date": args.migration_start_date,
        "migration_end_date": args.migration_end_date,
        "scope_start_date": args.scope_start_date,
        "scope_end_date": args.scope_end_date,
        "estimated_main_query_bytes": estimated_main_bytes,
        "maximum_bytes_billed": args.maximum_bytes_billed,
        "status_maximum_bytes_billed_per_query": args.status_maximum_bytes_billed,
        "fallback_maximum_bytes_billed": args.fallback_maximum_bytes_billed,
        "migrate_discriminator_hex": MIGRATE_DISCRIMINATOR.hex(),
        "migrate_data_base58": MIGRATE_DATA_B58,
        "withdraw_discriminator_hex": WITHDRAW_DISCRIMINATOR.hex(),
        "withdraw_data_base58": WITHDRAW_DATA_B58,
    }
    (output_dir / "preflight.json").write_text(
        json.dumps(preflight, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    print(json.dumps(preflight, ensure_ascii=False, indent=2), flush=True)

    if estimated_main_bytes > args.maximum_bytes_billed:
        raise SystemExit(
            "Dry-run limiti aştı; sorgu çalıştırılmadı. "
            f"estimated={estimated_main_bytes}, max={args.maximum_bytes_billed}"
        )
    if args.dry_run_only:
        print("DISCOVERY_DRY_RUN_OK")
        return 0

    run_config = main_query_config(
        migration_start,
        migration_end_exclusive,
        args.maximum_bytes_billed,
        metadata_start=scope_start,
        dry_run=False,
    )
    query_hash = hashlib.sha256(sql.encode()).hexdigest()
    main_job_id = stable_job_id(
        "discovery",
        args.chunk_id,
        args.migration_start_date,
        args.migration_end_date,
        args.scope_start_date,
        args.scope_end_date,
        str(args.maximum_bytes_billed),
        query_hash,
    )
    main_job = start_or_resume_query(
        client,
        sql=sql,
        config=run_config,
        job_id=main_job_id,
        retry_policy=retry_policy,
    )
    main_bytes = int(main_job.total_bytes_processed or 0)
    bq_rows = with_retry(
        lambda: list(main_job.result(page_size=10_000)),
        label=f"iterate:{main_job_id}",
        policy=retry_policy,
    )

    raw_rows: list[dict[str, Any]] = []
    for row in bq_rows:
        creator, creator_source, creator_count = select_creator(
            row_value(row, "creators", [])
        )
        raw_rows.append(
            {
                "mint": str(row_value(row, "mint", "") or ""),
                "created_at_utc": timestamp_text(
                    row_value(row, "created_at_utc", "")
                ),
                "create_tx": str(row_value(row, "create_tx", "") or ""),
                "creator": creator,
                "creator_source": creator_source,
                "metadata_creator_count": creator_count,
                "name": str(row_value(row, "name", "") or ""),
                "symbol": str(row_value(row, "symbol", "") or ""),
                "uri": str(row_value(row, "uri", "") or ""),
                "update_authority": str(
                    row_value(row, "update_authority", "") or ""
                ),
                "migrated_at_utc": timestamp_text(
                    row_value(row, "migrated_at_utc", "")
                ),
                "migration_block_slot": int(
                    row_value(row, "migration_block_slot", 0) or 0
                ),
                "migration_tx": str(row_value(row, "migration_tx", "") or ""),
                "migration_instruction_index": int(
                    row_value(row, "migration_instruction_index", 0) or 0
                ),
                "migration_type": str(
                    row_value(row, "migration_type", "") or ""
                ),
                "migrator": str(row_value(row, "migrator", "") or ""),
            }
        )

    if not raw_rows:
        raise SystemExit(
            "Migration sorgusu sıfır satır döndürdü. Discriminator/veri kapsamı "
            "doğrulanmadan devam edilmiyor."
        )

    status_map, status_bytes = query_statuses(
        client,
        raw_rows,
        retry_policy=retry_policy,
        maximum_bytes_billed=args.status_maximum_bytes_billed,
    )
    for row in raw_rows:
        status = status_map.get(row["migration_tx"])
        row["transaction_status"] = "" if status is None else status["status"]
        row["transaction_error"] = "" if status is None else status["err"]
        row["transaction_success"] = (
            None if status is None else status["is_success"]
        )

    successful_rows = [
        row for row in raw_rows if row.get("transaction_success") is True
    ]
    failed_rows = [
        row for row in raw_rows if row.get("transaction_success") is False
    ]
    missing_status_rows = [
        row for row in raw_rows if row.get("transaction_success") is None
    ]

    # Canonical migrate is idempotent. Keep the earliest successful event per mint.
    unique_by_mint: dict[str, dict[str, Any]] = {}
    duplicate_events: list[dict[str, Any]] = []
    for row in sorted(
        successful_rows,
        key=lambda item: (item["migrated_at_utc"], item["migration_tx"]),
    ):
        mint = row["mint"]
        if not mint:
            continue
        if mint in unique_by_mint:
            duplicate_events.append(row)
            continue
        unique_by_mint[mint] = row

    # Metadata may be incomplete for a small number of mints. Use a targeted,
    # mint-filtered create query only for those rows; never re-download all creates.
    needs_fallback = [
        mint
        for mint, row in unique_by_mint.items()
        if not row.get("created_at_utc") or not row.get("creator")
    ]
    fallback, fallback_anomalies, fallback_bytes = targeted_create_fallback(
        client,
        mints=needs_fallback,
        start=scope_start,
        end_exclusive=migration_end_exclusive,
        retry_policy=retry_policy,
        maximum_bytes_billed=args.fallback_maximum_bytes_billed,
    )
    for mint, fallback_row in fallback.items():
        row = unique_by_mint[mint]
        if not row.get("created_at_utc"):
            row["created_at_utc"] = fallback_row["created_at_utc"]
            row["create_tx"] = fallback_row["create_tx"]
        if not row.get("creator"):
            row["creator"] = fallback_row["creator"]
            row["creator_source"] = fallback_row["creator_source"]

    unresolved: list[dict[str, Any]] = []
    scope_rows: list[dict[str, Any]] = []
    out_of_scope_rows: list[dict[str, Any]] = []
    for mint, row in sorted(unique_by_mint.items()):
        created_text = str(row.get("created_at_utc") or "")
        if not created_text:
            fallback_anomaly_mints = {
                str(item.get("mint") or "") for item in fallback_anomalies
            }
            if mint in fallback_anomaly_mints:
                unresolved.append(
                    {
                        "mint": mint,
                        "reason": "creation_record_missing_with_fallback_anomaly",
                        "detail": "",
                    }
                )
            else:
                row["in_scope_create"] = False
                row["scope_exclusion_reason"] = "no_create_metadata_in_2026_scope"
                out_of_scope_rows.append(row)
            continue
        try:
            created = datetime.fromisoformat(created_text.replace("Z", "+00:00"))
            if created.tzinfo is None:
                created = created.replace(tzinfo=timezone.utc)
        except ValueError:
            unresolved.append(
                {
                    "mint": mint,
                    "reason": "creation_timestamp_invalid",
                    "detail": created_text,
                }
            )
            continue
        row["in_scope_create"] = scope_start <= created < scope_end_exclusive
        if not row["in_scope_create"]:
            out_of_scope_rows.append(row)
            continue
        if not row.get("creator"):
            unresolved.append(
                {"mint": mint, "reason": "creator_missing", "detail": ""}
            )
            continue
        scope_rows.append(row)

    for row in missing_status_rows:
        unresolved.append(
            {
                "mint": row.get("mint", ""),
                "reason": "migration_transaction_status_missing",
                "detail": row.get("migration_tx", ""),
            }
        )
    fallback_anomaly_fields = ["mint", "create_tx", "reason"]
    write_csv(
        output_dir / "fallback_create_anomalies.csv",
        fallback_anomalies,
        fallback_anomaly_fields,
    )

    fields = [
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
    write_csv(output_dir / "raw_migration_events.csv", raw_rows, fields)
    write_csv(output_dir / "successful_unique_migrations.csv", unique_by_mint.values(), fields)
    write_csv(output_dir / "scope_2026_migrated_tokens.csv", scope_rows, fields)
    write_csv(output_dir / "out_of_scope_migrations.csv", out_of_scope_rows, fields)
    write_csv(output_dir / "failed_migration_events.csv", failed_rows, fields)
    write_csv(output_dir / "duplicate_migration_events.csv", duplicate_events, fields)
    write_csv(
        output_dir / "unresolved_discovery.csv",
        unresolved,
        ["mint", "reason", "detail"],
    )

    manifest = {
        "script_version": SCRIPT_VERSION,
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "chunk_id": args.chunk_id,
        "migration_start_date": args.migration_start_date,
        "migration_end_date": args.migration_end_date,
        "scope_start_date": args.scope_start_date,
        "scope_end_date": args.scope_end_date,
        "coverage": {
            "pump_program_id": PUMP_PROGRAM_ID,
            "migration_types": ["canonical_migrate", "historical_withdraw"],
            "scope_label": "canonical_pump_migrations_plus_historical_withdraw",
            "does_not_claim_global_ath_source": True,
            "buy_sell_rows_downloaded": False,
        },
        "cost": {
            "estimated_main_query_bytes": estimated_main_bytes,
            "actual_main_query_bytes": main_bytes,
            "status_query_bytes": status_bytes,
            "fallback_query_bytes": fallback_bytes,
            "total_reported_bytes": main_bytes + status_bytes + fallback_bytes,
            "maximum_bytes_billed": args.maximum_bytes_billed,
            "status_maximum_bytes_billed_per_query": args.status_maximum_bytes_billed,
            "fallback_maximum_bytes_billed": args.fallback_maximum_bytes_billed,
        },
        "counts": {
            "raw_migration_event_count": len(raw_rows),
            "successful_migration_event_count": len(successful_rows),
            "failed_migration_event_count": len(failed_rows),
            "missing_status_event_count": len(missing_status_rows),
            "successful_unique_mint_count": len(unique_by_mint),
            "duplicate_success_event_count": len(duplicate_events),
            "in_scope_2026_mint_count": len(scope_rows),
            "out_of_scope_mint_count": len(out_of_scope_rows),
            "metadata_fallback_requested_count": len(needs_fallback),
            "metadata_fallback_resolved_count": len(fallback),
            "unresolved_count": len(unresolved),
            "unique_creator_count": len({row["creator"] for row in scope_rows}),
        },
        "strict": bool(args.strict),
        "complete": len(unresolved) == 0 and len(raw_rows) > 0,
        "next_stage": "Merge all 2026 chunks before sizing GMGN ATH sharding.",
    }
    (output_dir / "discovery_manifest.json").write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    print(json.dumps(manifest, ensure_ascii=False, indent=2), flush=True)

    if args.strict and manifest["complete"] is not True:
        print("DISCOVERY_INCOMPLETE", file=sys.stderr)
        return 2
    print(f"DISCOVERY_2026_CHUNK_OK:{args.chunk_id}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
