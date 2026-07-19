from __future__ import annotations

import csv
import json
import struct
from pathlib import Path

import base58

try:
    from scripts.pumpfun_2026_discovery_chunk_v5 import (
        CREATE_DISCRIMINATOR,
        CREATE_V2_DISCRIMINATOR,
        MIGRATE_DATA_B58,
        WITHDRAW_DATA_B58,
        decode_creator_from_create_data,
        main_query,
        main_query_config,
        select_creator,
        transaction_success_state,
        utc_bounds,
    )
    from scripts.merge_2026_discovery_v5 import merge_chunks
except ImportError:  # standalone local validation
    from pumpfun_2026_discovery_chunk_v5 import (
        CREATE_DISCRIMINATOR,
        CREATE_V2_DISCRIMINATOR,
        MIGRATE_DATA_B58,
        WITHDRAW_DATA_B58,
        decode_creator_from_create_data,
        main_query,
        main_query_config,
        select_creator,
        transaction_success_state,
        utc_bounds,
    )
    from merge_2026_discovery_v5 import merge_chunks


def borsh_string(value: str) -> bytes:
    raw = value.encode()
    return struct.pack("<I", len(raw)) + raw


def write_csv(path: Path, rows: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fields = sorted({key for row in rows for key in row})
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def make_chunk(root: Path, chunk: str, mint: str, migrated: str) -> None:
    directory = root / f"wallet-intelligence-2026-discovery-chunk-{chunk}"
    directory.mkdir(parents=True)
    manifest = {
        "chunk_id": chunk,
        "migration_start_date": migrated[:7] + "-01",
        "migration_end_date": migrated[:10],
        "complete": True,
        "counts": {
            "raw_migration_event_count": 1,
            "in_scope_2026_mint_count": 1,
            "unique_creator_count": 1,
            "unresolved_count": 0,
        },
        "cost": {"total_reported_bytes": 10},
    }
    (directory / "discovery_manifest.json").write_text(json.dumps(manifest))
    write_csv(
        directory / "scope_2026_migrated_tokens.csv",
        [
            {
                "mint": mint,
                "creator": "creator-1",
                "migrated_at_utc": migrated,
                "migration_tx": f"tx-{chunk}",
                "transaction_success": "True",
            }
        ],
    )
    for filename in (
        "out_of_scope_migrations.csv",
        "failed_migration_events.csv",
        "duplicate_migration_events.csv",
        "unresolved_discovery.csv",
    ):
        write_csv(directory / filename, [])


def test_known_migration_base58_values() -> None:
    assert MIGRATE_DATA_B58 == "Mjb79tJwDb7"
    assert WITHDRAW_DATA_B58 == "Xd2GMpFXgQ1"


def test_decode_creator_from_create() -> None:
    creator_bytes = bytes(range(32))
    raw = (
        CREATE_DISCRIMINATOR
        + borsh_string("Name")
        + borsh_string("SYM")
        + borsh_string("https://example.test")
        + creator_bytes
    )
    creator, source = decode_creator_from_create_data(base58.b58encode(raw).decode())
    assert creator == base58.b58encode(creator_bytes).decode()
    assert source == "create_instruction_creator_arg"


def test_decode_creator_from_create_v2_ignores_trailing_flags() -> None:
    creator_bytes = bytes(reversed(range(32)))
    raw = (
        CREATE_V2_DISCRIMINATOR
        + borsh_string("Name")
        + borsh_string("SYM")
        + borsh_string("uri")
        + creator_bytes
        + b"\x01\x00"
    )
    creator, source = decode_creator_from_create_data(base58.b58encode(raw).decode())
    assert creator == base58.b58encode(creator_bytes).decode()
    assert source == "create_instruction_creator_arg"


def test_select_creator_prefers_verified_then_share() -> None:
    creator, source, count = select_creator(
        [
            {"address": "unverified", "verified": False, "share": 100},
            {"address": "verified-low", "verified": True, "share": 10},
            {"address": "verified-high", "verified": True, "share": 90},
        ]
    )
    assert creator == "verified-high"
    assert source == "tokens_verified_creator"
    assert count == 3


def test_transaction_status_logic() -> None:
    assert transaction_success_state("SUCCESS", None) is True
    assert transaction_success_state("FAILED", "some error") is False
    assert transaction_success_state(None, None) is True


def test_date_bounds_are_end_inclusive() -> None:
    start, end = utc_bounds("2026-01-01", "2026-07-18")
    assert start.isoformat().startswith("2026-01-01")
    assert end.isoformat().startswith("2026-07-19")


def test_main_query_is_exact_migration_filtered_and_has_no_trade_scan() -> None:
    sql = main_query()
    assert "data IN UNNEST(@migration_data_values)" in sql
    assert "JOIN migrated_mints m USING (mint)" in sql
    assert "buy" not in sql.lower()
    assert "sell" not in sql.lower()


def test_dry_run_does_not_apply_billing_ceiling_before_estimate() -> None:
    start, end = utc_bounds("2026-01-01", "2026-01-31")
    dry = main_query_config(start, end, 123, dry_run=True)
    run = main_query_config(start, end, 123, dry_run=False)
    assert dry.dry_run is True
    assert dry.maximum_bytes_billed is None
    assert run.dry_run is False
    assert run.maximum_bytes_billed == 123


def test_merge_requires_every_expected_chunk(tmp_path: Path) -> None:
    make_chunk(tmp_path, "2026-01", "mint-1", "2026-01-20T00:00:00+00:00")
    manifest, _ = merge_chunks(tmp_path, ["2026-01", "2026-02"])
    assert manifest["complete"] is False
    assert manifest["missing_chunks"] == ["2026-02"]


def test_merge_deduplicates_idempotent_migration_across_months(tmp_path: Path) -> None:
    make_chunk(tmp_path, "2026-01", "same-mint", "2026-01-20T00:00:00+00:00")
    make_chunk(tmp_path, "2026-02", "same-mint", "2026-02-02T00:00:00+00:00")
    manifest, outputs = merge_chunks(tmp_path, ["2026-01", "2026-02"])
    assert manifest["complete"] is True
    assert manifest["counts"]["unique_2026_migrated_mint_count"] == 1
    assert manifest["counts"]["cross_chunk_duplicate_event_count"] == 1
    assert outputs["unique_scope"][0]["source_chunk"] == "2026-01"
