"""
GMGN portfolio activity pagination ve veri bütünlüğü testi.

Bu sürüm:
- Bütün activity sayfalarını cursor bitene kadar çeker.
- Her ham sayfayı değiştirmeden saklar.
- Aynı transaction içindeki launch, buy, sell ve liquidity eventlerini ayrı tutar.
- Yalnızca içeriği birebir aynı olan kayıtları duplicate kabul eder.
- Gerçek GMGN alanları olan tx_hash ve event_type alanlarını destekler.
- Deployer analizi için bütün eventleri eksiksiz korur.

Gerekli ortam değişkeni:
    GMGN_API_KEY

Watchlist:
    config/watchlist.txt

Çıktılar:
    results/activity_probe/<çalışma_zamanı>/
"""

import hashlib
import json
import os
import re
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parent.parent
WATCHLIST_PATH = ROOT / "config" / "watchlist.txt"
RESULTS_DIR = ROOT / "results" / "activity_probe"

PAGE_LIMIT = 50
MAX_PAGES = 500
PAGE_DELAY_SECONDS = 1.0
COMMAND_TIMEOUT_SECONDS = 180
MAX_REQUEST_ATTEMPTS = 3


def write_json(path: Path, data: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(data, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def load_first_wallet() -> str:
    if not WATCHLIST_PATH.exists():
        raise FileNotFoundError(
            f"Watchlist bulunamadı: {WATCHLIST_PATH}"
        )

    for raw_line in WATCHLIST_PATH.read_text(
        encoding="utf-8"
    ).splitlines():
        line = raw_line.strip()

        if not line or line.startswith("#"):
            continue

        return line

    raise RuntimeError(
        "Watchlist içinde test edilecek wallet bulunamadı."
    )


def extract_reset_timestamp(text: str) -> int | None:
    patterns = [
        r'"reset_at"\s*:\s*(\d+)',
        r"reset_at[=: ]+(\d+)",
        r"X-RateLimit-Reset[=: ]+(\d+)",
    ]

    for pattern in patterns:
        match = re.search(
            pattern,
            text,
            flags=re.IGNORECASE,
        )

        if match:
            try:
                return int(match.group(1))
            except ValueError:
                return None

    return None


def run_activity_command(
    wallet: str,
    cursor: str | None,
) -> dict[str, Any]:
    command = [
        "npx",
        "--yes",
        "gmgn-cli",
        "portfolio",
        "activity",
        "--chain",
        "sol",
        "--wallet",
        wallet,
        "--limit",
        str(PAGE_LIMIT),
        "--raw",
    ]

    if cursor:
        command.extend(
            [
                "--cursor",
                cursor,
            ]
        )

    for attempt in range(1, MAX_REQUEST_ATTEMPTS + 1):
        print(
            (
                "ACTIVITY_REQUEST | "
                f"attempt={attempt}/{MAX_REQUEST_ATTEMPTS} | "
                f"cursor={'FIRST_PAGE' if not cursor else cursor[:40]}"
            ),
            flush=True,
        )

        try:
            result = subprocess.run(
                command,
                capture_output=True,
                text=True,
                timeout=COMMAND_TIMEOUT_SECONDS,
                check=False,
                env=os.environ.copy(),
            )

        except subprocess.TimeoutExpired as exc:
            if attempt == MAX_REQUEST_ATTEMPTS:
                raise RuntimeError(
                    f"GMGN activity sorgusu timeout oldu: {exc}"
                ) from exc

            time.sleep(10)
            continue

        stdout = result.stdout.strip()
        stderr = result.stderr.strip()

        if result.returncode == 0:
            if not stdout:
                raise RuntimeError(
                    "GMGN komutu başarılı göründü fakat boş cevap döndürdü."
                )

            try:
                parsed = json.loads(stdout)
            except json.JSONDecodeError as exc:
                raise RuntimeError(
                    (
                        "GMGN çıktısı geçerli JSON değil. "
                        f"Satır={exc.lineno}, sütun={exc.colno}. "
                        f"Çıktı başlangıcı: {stdout[:500]}"
                    )
                ) from exc

            if not isinstance(parsed, dict):
                raise RuntimeError(
                    "GMGN activity cevabının üst seviyesi JSON nesnesi değil."
                )

            return parsed

        combined_error = f"{stderr}\n{stdout}".strip()

        if (
            "429" in combined_error
            or "RATE_LIMIT" in combined_error.upper()
        ):
            reset_timestamp = extract_reset_timestamp(
                combined_error
            )

            if reset_timestamp:
                wait_seconds = max(
                    1,
                    reset_timestamp - int(time.time()) + 2,
                )
            else:
                wait_seconds = 60

            print(
                (
                    "RATE_LIMIT_WAIT | "
                    f"{wait_seconds} saniye beklenecek."
                ),
                file=sys.stderr,
                flush=True,
            )

            time.sleep(wait_seconds)
            continue

        raise RuntimeError(
            (
                "GMGN activity komutu başarısız oldu. "
                f"exit_code={result.returncode}\n"
                f"stderr={stderr}\n"
                f"stdout={stdout[:1000]}"
            )
        )

    raise RuntimeError(
        "GMGN activity sorgusu tekrar denemelerden sonra tamamlanamadı."
    )


def normalize_response(
    raw_response: dict[str, Any],
) -> tuple[list[dict[str, Any]], str | None]:
    """
    Activity dizisini ve sonraki cursor değerini bulur.

    Hem üst seviye hem de data içindeki olası şemayı destekler.
    Ham veriyi değiştirmez.
    """

    container: dict[str, Any] = raw_response

    nested_data = raw_response.get("data")

    if isinstance(nested_data, dict):
        if (
            "activities" in nested_data
            or "next" in nested_data
        ):
            container = nested_data

    activities = container.get("activities")
    next_cursor = container.get("next")

    if activities is None:
        raise RuntimeError(
            "GMGN cevabında 'activities' alanı bulunamadı."
        )

    if not isinstance(activities, list):
        raise RuntimeError(
            (
                "'activities' alanı liste değil: "
                f"{type(activities).__name__}"
            )
        )

    valid_activities: list[dict[str, Any]] = []

    for activity in activities:
        if isinstance(activity, dict):
            valid_activities.append(activity)

    if next_cursor in ("", None, False):
        normalized_cursor = None
    else:
        normalized_cursor = str(next_cursor)

    return valid_activities, normalized_cursor


def exact_event_identity(
    activity: dict[str, Any],
) -> str:
    """
    Yalnızca tamamen aynı JSON içeriğine sahip eventleri duplicate sayar.

    Böylece aynı tx_hash içindeki:
    - launch
    - buy
    - sell
    - add_liquidity
    gibi farklı eventler kesinlikle birleşmez.
    """

    canonical_json = json.dumps(
        activity,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )

    return hashlib.sha256(
        canonical_json.encode("utf-8")
    ).hexdigest()


def get_event_type(activity: dict[str, Any]) -> str:
    value = (
        activity.get("event_type")
        or activity.get("type")
        or activity.get("activity_type")
        or "unknown"
    )

    return str(value).lower()


def get_tx_hash(activity: dict[str, Any]) -> str | None:
    value = (
        activity.get("tx_hash")
        or activity.get("transaction_hash")
        or activity.get("transaction_signature")
        or activity.get("signature")
    )

    if value in (None, ""):
        return None

    return str(value)


def get_token_address(activity: dict[str, Any]) -> str | None:
    token = activity.get("token")

    if isinstance(token, dict):
        value = (
            token.get("address")
            or token.get("mint")
            or token.get("mint_address")
        )

        if value not in (None, ""):
            return str(value)

    value = (
        activity.get("token_address")
        or activity.get("token_mint")
        or activity.get("mint")
    )

    if value in (None, ""):
        return None

    return str(value)


def get_timestamp(activity: dict[str, Any]) -> float | None:
    candidates = [
        activity.get("timestamp"),
        activity.get("block_timestamp"),
        activity.get("block_time"),
        activity.get("time"),
    ]

    for value in candidates:
        if value in (None, ""):
            continue

        try:
            timestamp = float(value)

            if timestamp > 10_000_000_000:
                timestamp = timestamp / 1000

            return timestamp
        except (TypeError, ValueError):
            continue

    return None


def timestamp_to_iso(
    timestamp: float | None,
) -> str | None:
    if timestamp is None:
        return None

    try:
        return datetime.fromtimestamp(
            timestamp,
            tz=timezone.utc,
        ).isoformat()
    except (ValueError, OverflowError, OSError):
        return None


def main() -> None:
    api_key = os.environ.get(
        "GMGN_API_KEY",
        "",
    ).strip()

    if not api_key:
        print(
            "ACTIVITY_PROBE_FAILED | GMGN_API_KEY bulunamadı.",
            file=sys.stderr,
            flush=True,
        )
        sys.exit(1)

    wallet = load_first_wallet()

    run_timestamp = datetime.now(
        timezone.utc
    ).strftime("%Y-%m-%dT%H-%M-%SZ")

    run_dir = RESULTS_DIR / run_timestamp
    pages_dir = run_dir / "raw_pages"

    run_dir.mkdir(parents=True, exist_ok=True)
    pages_dir.mkdir(parents=True, exist_ok=True)

    print("ACTIVITY_PROBE_START", flush=True)
    print(f"WALLET | {wallet}", flush=True)
    print(f"PAGE_LIMIT | {PAGE_LIMIT}", flush=True)
    print(f"OUTPUT_DIR | {run_dir}", flush=True)

    all_activities: list[dict[str, Any]] = []
    unique_activities: list[dict[str, Any]] = []
    duplicate_records: list[dict[str, Any]] = []

    seen_exact_event_ids: set[str] = set()
    seen_cursors: set[str] = set()

    cursor: str | None = None
    page_number = 0
    pagination_complete = False

    first_timestamp: float | None = None
    last_timestamp: float | None = None

    while page_number < MAX_PAGES:
        page_number += 1

        print(
            (
                "FETCH_PAGE | "
                f"page={page_number} | "
                f"cursor={'FIRST_PAGE' if cursor is None else cursor[:40]}"
            ),
            flush=True,
        )

        raw_response = run_activity_command(
            wallet=wallet,
            cursor=cursor,
        )

        raw_page_path = (
            pages_dir / f"page_{page_number:04d}.json"
        )

        write_json(
            raw_page_path,
            raw_response,
        )

        activities, next_cursor = normalize_response(
            raw_response
        )

        print(
            (
                "PAGE_RESULT | "
                f"page={page_number} | "
                f"record_count={len(activities)} | "
                f"has_next={bool(next_cursor)}"
            ),
            flush=True,
        )

        all_activities.extend(activities)

        for activity in activities:
            timestamp = get_timestamp(activity)

            if timestamp is not None:
                if (
                    first_timestamp is None
                    or timestamp < first_timestamp
                ):
                    first_timestamp = timestamp

                if (
                    last_timestamp is None
                    or timestamp > last_timestamp
                ):
                    last_timestamp = timestamp

            exact_identity = exact_event_identity(
                activity
            )

            if exact_identity in seen_exact_event_ids:
                duplicate_records.append(
                    {
                        "exact_identity": exact_identity,
                        "tx_hash": get_tx_hash(activity),
                        "event_type": get_event_type(activity),
                        "token_address": get_token_address(
                            activity
                        ),
                        "activity": activity,
                    }
                )
                continue

            seen_exact_event_ids.add(exact_identity)
            unique_activities.append(activity)

        if not next_cursor:
            pagination_complete = True
            cursor = None
            break

        if next_cursor in seen_cursors:
            raise RuntimeError(
                (
                    "Aynı pagination cursor tekrar döndü. "
                    "Sonsuz döngüyü önlemek için durduruldu."
                )
            )

        seen_cursors.add(next_cursor)
        cursor = next_cursor

        time.sleep(PAGE_DELAY_SECONDS)

    if not pagination_complete:
        raise RuntimeError(
            (
                "Pagination tamamlanamadı. "
                f"MAX_PAGES={MAX_PAGES} sınırına ulaşıldı."
            )
        )

    all_activities_path = (
        run_dir / "all_activities_with_exact_dedup.json"
    )

    write_json(
        all_activities_path,
        unique_activities,
    )

    raw_all_activities_path = (
        run_dir / "all_activities_raw.json"
    )

    write_json(
        raw_all_activities_path,
        all_activities,
    )

    duplicates_path = (
        run_dir / "exact_duplicate_records.json"
    )

    write_json(
        duplicates_path,
        duplicate_records,
    )

    jsonl_path = (
        run_dir / "all_activities_with_exact_dedup.jsonl"
    )

    with jsonl_path.open(
        "w",
        encoding="utf-8",
    ) as jsonl_file:
        for activity in unique_activities:
            jsonl_file.write(
                json.dumps(
                    activity,
                    ensure_ascii=False,
                )
            )
            jsonl_file.write("\n")

    event_type_counts: dict[str, int] = {}
    transaction_hashes: set[str] = set()
    token_addresses: set[str] = set()

    for activity in unique_activities:
        event_type = get_event_type(activity)

        event_type_counts[event_type] = (
            event_type_counts.get(event_type, 0) + 1
        )

        tx_hash = get_tx_hash(activity)

        if tx_hash:
            transaction_hashes.add(tx_hash)

        token_address = get_token_address(activity)

        if token_address:
            token_addresses.add(token_address)

    report = {
        "wallet_address": wallet,
        "run_timestamp_utc": run_timestamp,
        "page_limit": PAGE_LIMIT,
        "pages_fetched": page_number,
        "records_fetched_raw": len(all_activities),
        "records_after_exact_dedup": len(
            unique_activities
        ),
        "exact_duplicate_count": len(
            duplicate_records
        ),
        "pagination_complete": pagination_complete,
        "final_cursor": cursor,
        "unique_transaction_count": len(
            transaction_hashes
        ),
        "unique_token_count": len(
            token_addresses
        ),
        "first_activity_timestamp": first_timestamp,
        "first_activity_utc": timestamp_to_iso(
            first_timestamp
        ),
        "last_activity_timestamp": last_timestamp,
        "last_activity_utc": timestamp_to_iso(
            last_timestamp
        ),
        "event_type_counts": event_type_counts,
        "raw_pages_directory": str(
            pages_dir.relative_to(ROOT)
        ),
        "all_raw_activities_file": str(
            raw_all_activities_path.relative_to(ROOT)
        ),
        "exact_dedup_activities_file": str(
            all_activities_path.relative_to(ROOT)
        ),
        "exact_duplicates_file": str(
            duplicates_path.relative_to(ROOT)
        ),
        "jsonl_file": str(
            jsonl_path.relative_to(ROOT)
        ),
        "deduplication_method": (
            "Yalnızca canonical tam JSON içeriği birebir aynı olan "
            "eventler duplicate kabul edilir. Aynı tx_hash içindeki "
            "farklı event_type kayıtları ayrı tutulur."
        ),
    }

    report_path = (
        run_dir / "completeness_report.json"
    )

    write_json(
        report_path,
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

    print(
        (
            "ACTIVITY_PROBE_SUCCESS | "
            f"pages={page_number} | "
            f"raw_records={len(all_activities)} | "
            f"records_after_exact_dedup={len(unique_activities)} | "
            f"exact_duplicates={len(duplicate_records)}"
        ),
        flush=True,
    )


if __name__ == "__main__":
    main()
