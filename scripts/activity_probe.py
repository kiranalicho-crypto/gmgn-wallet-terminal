"""
GMGN portfolio activity pagination testi.

Amaç:
- config/watchlist.txt içindeki ilk wallet adresini okur.
- GMGN portfolio activity sorgusunu çalıştırır.
- `next` cursor bitene kadar bütün sayfaları çeker.
- Her ham sayfayı ayrı JSON dosyasında saklar.
- Tüm aktiviteleri birleşik JSONL dosyasına yazar.
- Pagination ve veri tamlığı raporu üretir.

Gerekli ortam değişkeni:
    GMGN_API_KEY

Çıktılar:
    results/activity_probe/<run_timestamp>/
"""

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
        match = re.search(pattern, text, flags=re.IGNORECASE)

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
                f"cursor={'FIRST_PAGE' if not cursor else cursor[:30]}"
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
                return json.loads(stdout)

            except json.JSONDecodeError as exc:
                raise RuntimeError(
                    (
                        "GMGN çıktısı geçerli JSON değil. "
                        f"Satır={exc.lineno}, sütun={exc.colno}. "
                        f"Çıktı başlangıcı: {stdout[:500]}"
                    )
                ) from exc

        combined_error = f"{stderr}\n{stdout}".strip()

        if "429" in combined_error or "RATE_LIMIT" in combined_error.upper():
            reset_timestamp = extract_reset_timestamp(combined_error)

            if reset_timestamp:
                now_timestamp = int(time.time())
                wait_seconds = max(
                    1,
                    reset_timestamp - now_timestamp + 2,
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
    Resmî şema üst seviyede activities ve next alanlarını belirtir.

    CLI farklı bir sürümde bunları data altında döndürürse,
    ham veriyi bozmadan bu ihtimali de destekler.
    """

    response_container: dict[str, Any] = raw_response

    if isinstance(raw_response.get("data"), dict):
        nested_data = raw_response["data"]

        if (
            "activities" in nested_data
            or "next" in nested_data
        ):
            response_container = nested_data

    activities = response_container.get("activities")
    next_cursor = response_container.get("next")

    if activities is None:
        raise RuntimeError(
            (
                "GMGN cevabında 'activities' alanı bulunamadı. "
                "Ham sayfa dosyası incelenmeli."
            )
        )

    if not isinstance(activities, list):
        raise RuntimeError(
            (
                "'activities' alanı liste değil: "
                f"{type(activities).__name__}"
            )
        )

    if next_cursor in ("", None, False):
        next_cursor = None
    elif not isinstance(next_cursor, str):
        next_cursor = str(next_cursor)

    return activities, next_cursor


def activity_identity(
    activity: dict[str, Any],
) -> str:
    token = activity.get("token")

    token_address = None

    if isinstance(token, dict):
        token_address = token.get("address")

    identity_parts = [
        activity.get("transaction_hash"),
        activity.get("type"),
        token_address,
        activity.get("timestamp"),
        activity.get("token_amount"),
    ]

    return "|".join(
        "" if value is None else str(value)
        for value in identity_parts
    )


def main() -> None:
    api_key = os.environ.get("GMGN_API_KEY", "").strip()

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

    seen_activity_ids: set[str] = set()
    seen_cursors: set[str] = set()

    cursor: str | None = None
    page_number = 0
    pagination_complete = False
    duplicate_count = 0
    first_timestamp: int | float | None = None
    last_timestamp: int | float | None = None

    while page_number < MAX_PAGES:
        page_number += 1

        print(
            (
                "FETCH_PAGE | "
                f"page={page_number} | "
                f"cursor={'FIRST_PAGE' if cursor is None else cursor[:30]}"
            ),
            flush=True,
        )

        raw_response = run_activity_command(
            wallet=wallet,
            cursor=cursor,
        )

        raw_page_path = pages_dir / (
            f"page_{page_number:04d}.json"
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
            if not isinstance(activity, dict):
                continue

            timestamp = activity.get("timestamp")

            if isinstance(timestamp, (int, float)):
                if first_timestamp is None:
                    first_timestamp = timestamp

                first_timestamp = min(
                    first_timestamp,
                    timestamp,
                )

                if last_timestamp is None:
                    last_timestamp = timestamp

                last_timestamp = max(
                    last_timestamp,
                    timestamp,
                )

            identity = activity_identity(activity)

            if identity in seen_activity_ids:
                duplicate_count += 1
                continue

            seen_activity_ids.add(identity)
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

    combined_json_path = run_dir / "all_activities.json"

    write_json(
        combined_json_path,
        unique_activities,
    )

    jsonl_path = run_dir / "all_activities.jsonl"

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

    activity_type_counts: dict[str, int] = {}

    for activity in unique_activities:
        activity_type = str(
            activity.get("type") or "unknown"
        )

        activity_type_counts[activity_type] = (
            activity_type_counts.get(activity_type, 0) + 1
        )

    report = {
        "wallet_address": wallet,
        "run_timestamp_utc": run_timestamp,
        "page_limit": PAGE_LIMIT,
        "pages_fetched": page_number,
        "records_fetched_before_dedup": len(all_activities),
        "unique_records": len(unique_activities),
        "duplicate_records": duplicate_count,
        "pagination_complete": pagination_complete,
        "final_cursor": cursor,
        "first_activity_timestamp": first_timestamp,
        "last_activity_timestamp": last_timestamp,
        "activity_type_counts": activity_type_counts,
        "raw_pages_directory": str(
            pages_dir.relative_to(ROOT)
        ),
        "combined_json_file": str(
            combined_json_path.relative_to(ROOT)
        ),
        "combined_jsonl_file": str(
            jsonl_path.relative_to(ROOT)
        ),
    }

    report_path = run_dir / "completeness_report.json"

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
            f"unique_records={len(unique_activities)}"
        ),
        flush=True,
    )


if __name__ == "__main__":
    main()
