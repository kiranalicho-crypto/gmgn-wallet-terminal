#!/usr/bin/env python3
"""GMGN activity verisinden deployer/dev wallet davranış profili üretir.

Bu script yalnızca wallet'ın kendi ürettiği tokenları deployer profiline alır.
Wallet'ın başkalarının çıkardığı tokenlardaki işlemleri ana activity verisinde
kalmaya devam eder ve daha sonra trader/insider analizinde kullanılır.

Input:
    results/activity_probe/<run>/all_activities_with_exact_dedup.json

Outputs:
    results/activity_probe/<run>/deployer_profile/deployer_summary.json
    results/activity_probe/<run>/deployer_profile/deployer_tokens.json
    results/activity_probe/<run>/deployer_profile/deployer_tokens.jsonl
    results/activity_probe/<run>/deployer_profile/deployer_tokens.csv
"""

from __future__ import annotations

import argparse
import csv
import json
import sys
from collections import Counter, defaultdict
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Iterable


ROOT = Path(__file__).resolve().parent.parent
DEFAULT_ACTIVITY_ROOT = ROOT / "results" / "activity_probe"
INPUT_FILENAME = "all_activities_with_exact_dedup.json"


def read_json(path: Path) -> Any:
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def write_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)

    with path.open("w", encoding="utf-8") as handle:
        json.dump(
            value,
            handle,
            ensure_ascii=False,
            indent=2,
        )
        handle.write("\n")


def to_decimal(value: Any) -> Decimal:
    if value in (None, ""):
        return Decimal(0)

    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError):
        return Decimal(0)


def decimal_string(value: Decimal) -> str:
    text = format(value, "f")

    if "." in text:
        text = text.rstrip("0").rstrip(".")

    return text or "0"


def to_timestamp(value: Any) -> int | None:
    if value in (None, ""):
        return None

    try:
        timestamp = int(float(value))
    except (TypeError, ValueError):
        return None

    if timestamp > 10_000_000_000:
        timestamp //= 1000

    return timestamp


def timestamp_iso(timestamp: int | None) -> str | None:
    if timestamp is None:
        return None

    try:
        return datetime.fromtimestamp(
            timestamp,
            tz=timezone.utc,
        ).isoformat()
    except (ValueError, OverflowError, OSError):
        return None


def event_type(event: dict[str, Any]) -> str:
    return str(
        event.get("event_type") or "unknown"
    ).strip().lower()


def token_object(event: dict[str, Any]) -> dict[str, Any]:
    token = event.get("token")

    if isinstance(token, dict):
        return token

    return {}


def token_address(event: dict[str, Any]) -> str | None:
    token = token_object(event)

    value = (
        token.get("address")
        or token.get("mint")
        or event.get("token_address")
    )

    if value in (None, ""):
        return None

    return str(value)


def token_symbol(event: dict[str, Any]) -> str | None:
    value = token_object(event).get("symbol")

    if value in (None, ""):
        return None

    return str(value)


def token_supply(event: dict[str, Any]) -> Decimal:
    return to_decimal(
        token_object(event).get("total_supply")
    )


def tx_hash(event: dict[str, Any]) -> str | None:
    value = event.get("tx_hash")

    if value in (None, ""):
        return None

    return str(value)


def event_sort_key(
    event: dict[str, Any],
) -> tuple[int, str, str]:
    return (
        to_timestamp(event.get("timestamp")) or 0,
        tx_hash(event) or "",
        event_type(event),
    )


def sum_field(
    events: Iterable[dict[str, Any]],
    field: str,
) -> Decimal:
    return sum(
        (
            to_decimal(event.get(field))
            for event in events
        ),
        Decimal(0),
    )


def first_at_or_after(
    events: Iterable[dict[str, Any]],
    launch_timestamp: int,
) -> dict[str, Any] | None:
    candidates = [
        event
        for event in events
        if (
            to_timestamp(event.get("timestamp")) or -1
        ) >= launch_timestamp
    ]

    if not candidates:
        return None

    return min(
        candidates,
        key=event_sort_key,
    )


def delay_bucket(
    delay_seconds: int | None,
) -> str:
    if delay_seconds is None:
        return "no_sell_observed"

    if delay_seconds <= 5:
        return "0-5s"

    if delay_seconds <= 30:
        return "6-30s"

    if delay_seconds <= 60:
        return "31-60s"

    if delay_seconds <= 300:
        return "1-5m"

    if delay_seconds <= 1800:
        return "5-30m"

    if delay_seconds <= 21600:
        return "30m-6h"

    if delay_seconds <= 86400:
        return "6h-1d"

    return ">1d"


def discover_input(
    explicit_input: str | None,
) -> Path:
    if explicit_input:
        path = Path(
            explicit_input
        ).expanduser().resolve()

        if not path.is_file():
            raise FileNotFoundError(
                f"Input file not found: {path}"
            )

        return path

    if not DEFAULT_ACTIVITY_ROOT.exists():
        raise FileNotFoundError(
            "Activity results directory not found: "
            f"{DEFAULT_ACTIVITY_ROOT}"
        )

    candidates = sorted(
        DEFAULT_ACTIVITY_ROOT.glob(
            f"*/{INPUT_FILENAME}"
        ),
        key=lambda path: path.parent.name,
        reverse=True,
    )

    if not candidates:
        raise FileNotFoundError(
            f"No {INPUT_FILENAME} found under "
            f"{DEFAULT_ACTIVITY_ROOT}"
        )

    return candidates[0]


def build_token_profile(
    launch: dict[str, Any],
    token_events: list[dict[str, Any]],
) -> dict[str, Any]:
    launch_ts = to_timestamp(
        launch.get("timestamp")
    )

    if launch_ts is None:
        raise ValueError(
            "Launch event has no valid timestamp"
        )

    token_events = sorted(
        token_events,
        key=event_sort_key,
    )

    buys = [
        event
        for event in token_events
        if event_type(event) == "buy"
    ]

    sells = [
        event
        for event in token_events
        if event_type(event) == "sell"
    ]

    adds = [
        event
        for event in token_events
        if event_type(event) == "add"
    ]

    removes = [
        event
        for event in token_events
        if event_type(event) == "remove"
    ]

    burns = [
        event
        for event in token_events
        if event_type(event) == "burn"
    ]

    claims = [
        event
        for event in token_events
        if event_type(event) == "claim_fee"
    ]

    launch_tx = tx_hash(launch)

    same_tx_buys = [
        event
        for event in buys
        if tx_hash(event) == launch_tx
    ]

    same_timestamp_buys = [
        event
        for event in buys
        if to_timestamp(
            event.get("timestamp")
        ) == launch_ts
    ]

    first_buy = first_at_or_after(
        buys,
        launch_ts,
    )

    first_sell = first_at_or_after(
        sells,
        launch_ts,
    )

    first_buy_ts = (
        to_timestamp(first_buy.get("timestamp"))
        if first_buy
        else None
    )

    first_sell_ts = (
        to_timestamp(first_sell.get("timestamp"))
        if first_sell
        else None
    )

    first_buy_delay = (
        first_buy_ts - launch_ts
        if first_buy_ts is not None
        else None
    )

    first_sell_delay = (
        first_sell_ts - launch_ts
        if first_sell_ts is not None
        else None
    )

    sixty_second_initial_buys = [
        event
        for event in buys
        if (
            to_timestamp(
                event.get("timestamp")
            ) is not None
            and launch_ts
            <= int(
                to_timestamp(
                    event.get("timestamp")
                )
            )
            <= launch_ts + 60
        )
    ]

    supply = token_supply(launch)

    same_tx_buy_amount = sum_field(
        same_tx_buys,
        "token_amount",
    )

    sixty_second_buy_amount = sum_field(
        sixty_second_initial_buys,
        "token_amount",
    )

    same_tx_supply_pct = (
        same_tx_buy_amount
        / supply
        * Decimal(100)
        if supply > 0
        else None
    )

    sixty_second_supply_pct = (
        sixty_second_buy_amount
        / supply
        * Decimal(100)
        if supply > 0
        else None
    )

    realized_rows = [
        event
        for event in sells
        if event.get("buy_cost_usd")
        not in (None, "")
    ]

    observed_realized_pnl = sum(
        (
            to_decimal(event.get("cost_usd"))
            - to_decimal(
                event.get("buy_cost_usd")
            )
            for event in realized_rows
        ),
        Decimal(0),
    )

    flags: list[str] = []

    if same_tx_buys:
        flags.append(
            "same_tx_initial_buy"
        )

    elif (
        first_buy_delay is not None
        and first_buy_delay <= 60
    ):
        flags.append(
            "initial_buy_within_60s"
        )

    if (
        first_sell_delay is not None
        and first_sell_delay <= 5
    ):
        flags.append(
            "sell_within_5s"
        )

    elif (
        first_sell_delay is not None
        and first_sell_delay <= 30
    ):
        flags.append(
            "sell_within_30s"
        )

    if adds:
        flags.append("liquidity_added")

    if removes:
        flags.append("liquidity_removed")

    if claims:
        flags.append("fees_claimed")

    if burns:
        flags.append("tokens_burned")

    if not sells:
        flags.append("no_sell_observed")

    return {
        "token_address": token_address(
            launch
        ),
        "symbol": token_symbol(launch),
        "total_supply": decimal_string(
            supply
        ),
        "launch": {
            "timestamp": launch_ts,
            "utc": timestamp_iso(
                launch_ts
            ),
            "tx_hash": launch_tx,
            "launchpad": (
                launch.get("launchpad")
                or None
            ),
            "launchpad_platform": (
                launch.get(
                    "launchpad_platform"
                )
                or None
            ),
        },
        "initial_buy": {
            "same_transaction": bool(
                same_tx_buys
            ),
            "same_timestamp": bool(
                same_timestamp_buys
            ),
            "first_buy_delay_seconds": (
                first_buy_delay
            ),
            "first_buy_tx_hash": (
                tx_hash(first_buy)
                if first_buy
                else None
            ),
            "same_tx_event_count": len(
                same_tx_buys
            ),
            "same_tx_token_amount": (
                decimal_string(
                    same_tx_buy_amount
                )
            ),
            "same_tx_quote_amount": (
                decimal_string(
                    sum_field(
                        same_tx_buys,
                        "quote_amount",
                    )
                )
            ),
            "same_tx_cost_usd": (
                decimal_string(
                    sum_field(
                        same_tx_buys,
                        "cost_usd",
                    )
                )
            ),
            "same_tx_supply_percent": (
                decimal_string(
                    same_tx_supply_pct
                )
                if same_tx_supply_pct
                is not None
                else None
            ),
            "within_60s_event_count": len(
                sixty_second_initial_buys
            ),
            "within_60s_token_amount": (
                decimal_string(
                    sixty_second_buy_amount
                )
            ),
            "within_60s_quote_amount": (
                decimal_string(
                    sum_field(
                        sixty_second_initial_buys,
                        "quote_amount",
                    )
                )
            ),
            "within_60s_cost_usd": (
                decimal_string(
                    sum_field(
                        sixty_second_initial_buys,
                        "cost_usd",
                    )
                )
            ),
            "within_60s_supply_percent": (
                decimal_string(
                    sixty_second_supply_pct
                )
                if sixty_second_supply_pct
                is not None
                else None
            ),
        },
        "exit_behavior": {
            "first_sell_timestamp": (
                first_sell_ts
            ),
            "first_sell_utc": timestamp_iso(
                first_sell_ts
            ),
            "first_sell_tx_hash": (
                tx_hash(first_sell)
                if first_sell
                else None
            ),
            "first_sell_delay_seconds": (
                first_sell_delay
            ),
            "first_sell_delay_bucket": (
                delay_bucket(
                    first_sell_delay
                )
            ),
            "first_sell_token_amount": (
                decimal_string(
                    to_decimal(
                        first_sell.get(
                            "token_amount"
                        )
                    )
                    if first_sell
                    else Decimal(0)
                )
            ),
            "first_sell_quote_amount": (
                decimal_string(
                    to_decimal(
                        first_sell.get(
                            "quote_amount"
                        )
                    )
                    if first_sell
                    else Decimal(0)
                )
            ),
            "first_sell_usd": (
                decimal_string(
                    to_decimal(
                        first_sell.get(
                            "cost_usd"
                        )
                    )
                    if first_sell
                    else Decimal(0)
                )
            ),
        },
        "wallet_activity_totals": {
            "event_count": len(
                token_events
            ),
            "transaction_count": len(
                {
                    tx_hash(event)
                    for event in token_events
                    if tx_hash(event)
                }
            ),
            "buy_count": len(buys),
            "sell_count": len(sells),
            "liquidity_add_count": len(
                adds
            ),
            "liquidity_remove_count": len(
                removes
            ),
            "burn_count": len(burns),
            "claim_fee_count": len(
                claims
            ),
            "buy_token_amount": (
                decimal_string(
                    sum_field(
                        buys,
                        "token_amount",
                    )
                )
            ),
            "sell_token_amount": (
                decimal_string(
                    sum_field(
                        sells,
                        "token_amount",
                    )
                )
            ),
            "buy_quote_amount": (
                decimal_string(
                    sum_field(
                        buys,
                        "quote_amount",
                    )
                )
            ),
            "sell_quote_amount": (
                decimal_string(
                    sum_field(
                        sells,
                        "quote_amount",
                    )
                )
            ),
            "buy_cost_usd": (
                decimal_string(
                    sum_field(
                        buys,
                        "cost_usd",
                    )
                )
            ),
            "sell_proceeds_usd": (
                decimal_string(
                    sum_field(
                        sells,
                        "cost_usd",
                    )
                )
            ),
            "liquidity_added_token_amount": (
                decimal_string(
                    sum_field(
                        adds,
                        "token_amount",
                    )
                )
            ),
            "liquidity_added_quote_amount": (
                decimal_string(
                    sum_field(
                        adds,
                        "quote_amount",
                    )
                )
            ),
            "liquidity_removed_token_amount": (
                decimal_string(
                    sum_field(
                        removes,
                        "token_amount",
                    )
                )
            ),
            "liquidity_removed_quote_amount": (
                decimal_string(
                    sum_field(
                        removes,
                        "quote_amount",
                    )
                )
            ),
            "claimed_fee_quote_amount": (
                decimal_string(
                    sum_field(
                        claims,
                        "quote_amount",
                    )
                )
            ),
            "burned_token_amount": (
                decimal_string(
                    sum_field(
                        burns,
                        "token_amount",
                    )
                )
            ),
            "gmgn_realized_pnl_usd_from_sell_rows": (
                decimal_string(
                    observed_realized_pnl
                )
            ),
            "gmgn_realized_pnl_sell_row_coverage": (
                len(realized_rows)
            ),
        },
        "behavior_flags": flags,
        "data_scope": (
            "This wallet's GMGN "
            "activity rows only"
        ),
    }


def write_csv(
    path: Path,
    profiles: list[dict[str, Any]],
) -> None:
    columns = [
        "token_address",
        "symbol",
        "launch_utc",
        "launch_tx_hash",
        "launchpad",
        "launchpad_platform",
        "same_tx_initial_buy",
        "initial_buy_within_60s_usd",
        "initial_buy_within_60s_supply_percent",
        "first_sell_utc",
        "first_sell_delay_seconds",
        "first_sell_delay_bucket",
        "buy_count",
        "sell_count",
        "liquidity_add_count",
        "liquidity_remove_count",
        "claim_fee_count",
        "burn_count",
        "behavior_flags",
    ]

    path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    with path.open(
        "w",
        encoding="utf-8",
        newline="",
    ) as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=columns,
        )

        writer.writeheader()

        for profile in profiles:
            launch = profile["launch"]
            initial = profile[
                "initial_buy"
            ]
            exit_behavior = profile[
                "exit_behavior"
            ]
            totals = profile[
                "wallet_activity_totals"
            ]

            writer.writerow(
                {
                    "token_address": profile[
                        "token_address"
                    ],
                    "symbol": profile[
                        "symbol"
                    ],
                    "launch_utc": launch[
                        "utc"
                    ],
                    "launch_tx_hash": launch[
                        "tx_hash"
                    ],
                    "launchpad": launch[
                        "launchpad"
                    ],
                    "launchpad_platform": (
                        launch[
                            "launchpad_platform"
                        ]
                    ),
                    "same_tx_initial_buy": (
                        initial[
                            "same_transaction"
                        ]
                    ),
                    "initial_buy_within_60s_usd": (
                        initial[
                            "within_60s_cost_usd"
                        ]
                    ),
                    "initial_buy_within_60s_supply_percent": (
                        initial[
                            "within_60s_supply_percent"
                        ]
                    ),
                    "first_sell_utc": (
                        exit_behavior[
                            "first_sell_utc"
                        ]
                    ),
                    "first_sell_delay_seconds": (
                        exit_behavior[
                            "first_sell_delay_seconds"
                        ]
                    ),
                    "first_sell_delay_bucket": (
                        exit_behavior[
                            "first_sell_delay_bucket"
                        ]
                    ),
                    "buy_count": totals[
                        "buy_count"
                    ],
                    "sell_count": totals[
                        "sell_count"
                    ],
                    "liquidity_add_count": (
                        totals[
                            "liquidity_add_count"
                        ]
                    ),
                    "liquidity_remove_count": (
                        totals[
                            "liquidity_remove_count"
                        ]
                    ),
                    "claim_fee_count": (
                        totals[
                            "claim_fee_count"
                        ]
                    ),
                    "burn_count": totals[
                        "burn_count"
                    ],
                    "behavior_flags": "|".join(
                        profile[
                            "behavior_flags"
                        ]
                    ),
                }
            )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=__doc__,
    )

    parser.add_argument(
        "--input",
        help=(
            f"Path to {INPUT_FILENAME}; "
            "latest activity run is used "
            "when omitted."
        ),
    )

    parser.add_argument(
        "--output-dir",
        help=(
            "Output directory; defaults to "
            "<activity-run>/deployer_profile."
        ),
    )

    return parser.parse_args()


def main() -> int:
    args = parse_args()

    input_path = discover_input(
        args.input
    )

    output_dir = (
        Path(
            args.output_dir
        ).expanduser().resolve()
        if args.output_dir
        else input_path.parent
        / "deployer_profile"
    )

    raw = read_json(input_path)

    if not isinstance(raw, list):
        raise TypeError(
            "Activity input must be "
            "a JSON array"
        )

    events = [
        event
        for event in raw
        if isinstance(event, dict)
    ]

    if len(events) != len(raw):
        raise ValueError(
            "Activity input contains "
            "non-object rows"
        )

    wallets = sorted(
        {
            str(event.get("wallet"))
            for event in events
            if event.get("wallet")
            not in (None, "")
        }
    )

    if len(wallets) != 1:
        raise ValueError(
            "Expected exactly one wallet, "
            f"found: {wallets}"
        )

    wallet = wallets[0]

    launches = [
        event
        for event in events
        if event_type(event) == "launch"
    ]

    launch_addresses = [
        token_address(event)
        for event in launches
    ]

    if any(
        address is None
        for address in launch_addresses
    ):
        raise ValueError(
            "At least one launch event has "
            "no token address"
        )

    duplicate_launch_addresses = sorted(
        address
        for address, count in Counter(
            launch_addresses
        ).items()
        if count > 1
    )

    if duplicate_launch_addresses:
        raise ValueError(
            "Duplicate launch token "
            "addresses detected; refusing "
            "to silently merge: "
            + ", ".join(
                duplicate_launch_addresses[
                    :10
                ]
            )
        )

    events_by_token: dict[
        str,
        list[dict[str, Any]],
    ] = defaultdict(list)

    for event in events:
        address = token_address(event)

        if address:
            events_by_token[
                address
            ].append(event)

    profiles = [
        build_token_profile(
            launch,
            events_by_token[
                str(
                    token_address(
                        launch
                    )
                )
            ],
        )
        for launch in sorted(
            launches,
            key=event_sort_key,
        )
    ]

    delay_counts = Counter(
        profile[
            "exit_behavior"
        ][
            "first_sell_delay_bucket"
        ]
        for profile in profiles
    )

    platform_counts = Counter(
        (
            profile["launch"][
                "launchpad_platform"
            ]
            or "unknown"
        )
        for profile in profiles
    )

    launchpad_counts = Counter(
        (
            profile["launch"][
                "launchpad"
            ]
            or "unknown"
        )
        for profile in profiles
    )

    summary = {
        "wallet_address": wallet,
        "profile_generated_utc": (
            datetime.now(
                timezone.utc
            ).isoformat()
        ),
        "source_file": str(
            input_path.relative_to(
                ROOT
            )
        ),
        "source_activity_count": len(
            events
        ),
        "source_unique_token_count": len(
            events_by_token
        ),
        "launched_token_count": len(
            profiles
        ),
        "classification": (
            "deployer"
            if len(profiles) >= 20
            else "deployer_candidate"
        ),
        "launchpad_platform_counts": dict(
            platform_counts.most_common()
        ),
        "launchpad_counts": dict(
            launchpad_counts.most_common()
        ),
        "same_transaction_initial_buy_count": sum(
            profile[
                "initial_buy"
            ][
                "same_transaction"
            ]
            for profile in profiles
        ),
        "initial_buy_within_60s_count": sum(
            profile[
                "initial_buy"
            ][
                "first_buy_delay_seconds"
            ]
            is not None
            and profile[
                "initial_buy"
            ][
                "first_buy_delay_seconds"
            ]
            <= 60
            for profile in profiles
        ),
        "tokens_with_sell_count": sum(
            profile[
                "wallet_activity_totals"
            ][
                "sell_count"
            ]
            > 0
            for profile in profiles
        ),
        "tokens_with_liquidity_add_count": sum(
            profile[
                "wallet_activity_totals"
            ][
                "liquidity_add_count"
            ]
            > 0
            for profile in profiles
        ),
        "tokens_with_liquidity_remove_count": sum(
            profile[
                "wallet_activity_totals"
            ][
                "liquidity_remove_count"
            ]
            > 0
            for profile in profiles
        ),
        "tokens_with_fee_claim_count": sum(
            profile[
                "wallet_activity_totals"
            ][
                "claim_fee_count"
            ]
            > 0
            for profile in profiles
        ),
        "tokens_with_burn_count": sum(
            profile[
                "wallet_activity_totals"
            ][
                "burn_count"
            ]
            > 0
            for profile in profiles
        ),
        "first_sell_delay_bucket_counts": dict(
            delay_counts
        ),
        "first_launch_utc": (
            profiles[0][
                "launch"
            ][
                "utc"
            ]
            if profiles
            else None
        ),
        "last_launch_utc": (
            profiles[-1][
                "launch"
            ][
                "utc"
            ]
            if profiles
            else None
        ),
        "data_quality": {
            "duplicate_launch_token_count": len(
                duplicate_launch_addresses
            ),
            "profile_count_matches_launch_count": (
                len(profiles)
                == len(launches)
            ),
        },
        "scope_and_limits": {
            "included": [
                (
                    "launches and this wallet's "
                    "buy/sell/liquidity/burn/"
                    "fee events"
                ),
                "initial-buy timing",
                "first-sell timing",
                (
                    "wallet-side event totals "
                    "per launched token"
                ),
            ],
            "not_included": [
                (
                    "token ATH market cap "
                    "and price path"
                ),
                (
                    "complete holder "
                    "distribution and transfers"
                ),
                (
                    "related-wallet or insider "
                    "cluster identification"
                ),
                (
                    "current liquidity and "
                    "rug outcome"
                ),
                (
                    "a final deployer "
                    "success score"
                ),
            ],
            "reason": (
                "Those require token-market, "
                "transfer/holder, and related-wallet "
                "data. No success label is inferred "
                "from partial wallet activity."
            ),
        },
    }

    output_dir.mkdir(
        parents=True,
        exist_ok=True,
    )

    write_json(
        output_dir
        / "deployer_summary.json",
        summary,
    )

    write_json(
        output_dir
        / "deployer_tokens.json",
        profiles,
    )

    with (
        output_dir
        / "deployer_tokens.jsonl"
    ).open(
        "w",
        encoding="utf-8",
    ) as handle:
        for profile in profiles:
            handle.write(
                json.dumps(
                    profile,
                    ensure_ascii=False,
                )
                + "\n"
            )

    write_csv(
        output_dir
        / "deployer_tokens.csv",
        profiles,
    )

    print(
        json.dumps(
            summary,
            ensure_ascii=False,
            indent=2,
        )
    )

    print(
        "DEPLOYER_PROFILER_SUCCESS | "
        f"wallet={wallet} | "
        f"launches={len(profiles)} | "
        f"output={output_dir}"
    )

    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())

    except Exception as exc:
        print(
            "DEPLOYER_PROFILER_FAILED | "
            f"{type(exc).__name__}: "
            f"{exc}",
            file=sys.stderr,
        )
        raise
