"""Probe GMGN ATH coverage before running the 2026 production pipeline.

This script does not discover candidates and does not write to Supabase. It only
inspects the raw shape and limits of ``portfolio created-tokens`` and compares
its ``token_ath_mc`` field with ``token info``'s documented
``ath_price * circulating_supply`` calculation on a small sample.
"""

from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path
from typing import Any

try:
    from gmgn_client import GmgnClient, GmgnError, as_number, unwrap_data
except ImportError:  # package/pytest import
    from scripts.gmgn_client import GmgnClient, GmgnError, as_number, unwrap_data


PROBE_VERSION = "2026-07-19-gmgn-ath-capability-probe-v1"


def arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--version", action="version", version=PROBE_VERSION)
    parser.add_argument("--creator-wallet", required=True)
    parser.add_argument("--sample-count", type=int, default=5)
    parser.add_argument("--output-dir", required=True)
    return parser.parse_args()


def save_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True),
        encoding="utf-8",
    )


def nested(data: dict[str, Any], *keys: str) -> Any:
    current: Any = data
    for key in keys:
        if not isinstance(current, dict):
            return None
        current = current.get(key)
    return current


def relative_difference(left: float, right: float) -> float | None:
    denominator = max(abs(left), abs(right))
    if denominator == 0:
        return 0.0
    return abs(left - right) / denominator


def pagination_like_keys(data: dict[str, Any]) -> list[str]:
    markers = ("cursor", "next", "page", "offset")
    return sorted(
        str(key)
        for key in data
        if any(marker in str(key).lower() for marker in markers)
    )


def main() -> int:
    args = arguments()
    if args.sample_count < 1 or args.sample_count > 20:
        raise SystemExit("sample-count 1 ile 20 arasında olmalı.")

    output_dir = Path(args.output_dir)
    raw_dir = output_dir / "raw"
    raw_dir.mkdir(parents=True, exist_ok=True)

    print(f"GMGN_ATH_CAPABILITY_PROBE_VERSION={PROBE_VERSION}", flush=True)
    client = GmgnClient(timeout_seconds=180, max_attempts=4)

    created_result = client.run(
        (
            "portfolio",
            "created-tokens",
            "--chain",
            "sol",
            "--wallet",
            args.creator_wallet,
            "--order-by",
            "token_ath_mc",
            "--direction",
            "desc",
            "--migrate-state",
            "migrated",
        )
    )
    save_json(raw_dir / "created_tokens_envelope.json", created_result.payload)

    created = unwrap_data(created_result.payload)
    if not isinstance(created, dict):
        raise GmgnError("created-tokens cevabında data nesnesi yok.")
    save_json(raw_dir / "created_tokens_data.json", created)

    tokens_value = created.get("tokens")
    tokens = tokens_value if isinstance(tokens_value, list) else []
    token_rows = [item for item in tokens if isinstance(item, dict)]

    open_count = int(as_number(created.get("open_count")) or 0)
    inner_count = int(as_number(created.get("inner_count")) or 0)
    returned_count = len(token_rows)
    truncated = open_count > returned_count or returned_count >= 100

    comparisons: list[dict[str, Any]] = []
    for item in token_rows[: args.sample_count]:
        mint = str(item.get("token_address") or "").strip()
        created_ath_mc = as_number(item.get("token_ath_mc"))
        if not mint:
            continue

        info_result = client.run(
            ("token", "info", "--chain", "sol", "--address", mint)
        )
        save_json(raw_dir / f"token_info_{mint}.json", info_result.payload)
        info = unwrap_data(info_result.payload)
        if not isinstance(info, dict):
            comparisons.append(
                {
                    "mint": mint,
                    "created_tokens_ath_mc": created_ath_mc,
                    "token_info_ath_price": None,
                    "token_info_circulating_supply": None,
                    "calculated_ath_mc": None,
                    "relative_difference": None,
                    "status": "token_info_data_missing",
                }
            )
            continue

        ath_price = as_number(info.get("ath_price"))
        circulating_supply = as_number(info.get("circulating_supply"))
        calculated_ath_mc = (
            ath_price * circulating_supply
            if ath_price is not None and circulating_supply is not None
            else None
        )
        difference = (
            relative_difference(created_ath_mc, calculated_ath_mc)
            if created_ath_mc is not None and calculated_ath_mc is not None
            else None
        )
        status = (
            "match_within_2_percent"
            if difference is not None and difference <= 0.02
            else "mismatch_or_missing"
        )

        comparisons.append(
            {
                "mint": mint,
                "symbol": item.get("symbol"),
                "created_tokens_ath_mc": created_ath_mc,
                "token_info_ath_price": ath_price,
                "token_info_circulating_supply": circulating_supply,
                "token_info_total_supply": as_number(info.get("total_supply")),
                "calculated_ath_mc": calculated_ath_mc,
                "relative_difference": difference,
                "status": status,
                "launchpad_platform": info.get("launchpad_platform"),
                "launchpad_status": info.get("launchpad_status"),
                "creator_address": nested(info, "dev", "creator_address"),
            }
        )

    comparison_path = output_dir / "ath_formula_comparison.csv"
    fieldnames = [
        "mint",
        "symbol",
        "created_tokens_ath_mc",
        "token_info_ath_price",
        "token_info_circulating_supply",
        "token_info_total_supply",
        "calculated_ath_mc",
        "relative_difference",
        "status",
        "launchpad_platform",
        "launchpad_status",
        "creator_address",
    ]
    with comparison_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(comparisons)

    valid_comparisons = [
        row
        for row in comparisons
        if isinstance(row.get("relative_difference"), (int, float))
    ]
    formula_supported = (
        len(valid_comparisons) >= min(3, args.sample_count)
        and all(float(row["relative_difference"]) <= 0.02 for row in valid_comparisons)
    )

    report = {
        "probe_version": PROBE_VERSION,
        "creator_wallet": args.creator_wallet,
        "created_tokens": {
            "open_count": open_count,
            "inner_count": inner_count,
            "reported_total_created": open_count + inner_count,
            "returned_token_count": returned_count,
            "tokens_array_truncated": truncated,
            "pagination_like_top_level_keys": pagination_like_keys(created),
            "top_level_keys": sorted(str(key) for key in created),
            "creator_ath_info": created.get("creator_ath_info"),
        },
        "token_info_formula_test": {
            "requested_sample_count": args.sample_count,
            "completed_comparison_count": len(valid_comparisons),
            "all_completed_matches_within_2_percent": formula_supported,
            "comparisons": comparisons,
        },
        "decision_inputs": {
            "created_tokens_is_known_to_cap_tokens_array_at_100": True,
            "created_tokens_response_exposes_pagination_like_field": bool(
                pagination_like_keys(created)
            ),
            "token_info_formula_can_be_used_as_fallback": formula_supported,
        },
        "complete": True,
    }
    save_json(output_dir / "gmgn_ath_capability_report.json", report)

    print(json.dumps(report, ensure_ascii=False, indent=2), flush=True)
    print("GMGN_ATH_CAPABILITY_PROBE_OK", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
