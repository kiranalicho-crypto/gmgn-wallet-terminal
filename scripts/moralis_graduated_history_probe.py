"""Resume-capable probe for Moralis Pump.fun graduated-token history.

The script can start fresh or continue from a prior probe artifact without
re-requesting already downloaded cursor pages.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
import shutil
import sys
import time
from datetime import datetime, time as datetime_time, timezone
from pathlib import Path
from typing import Any

import requests

PROBE_VERSION = "2026-07-19-moralis-graduated-history-probe-v2"
DEFAULT_URL = (
    "https://solana-gateway.moralis.io/token/mainnet/"
    "exchange/pumpfun/graduated"
)
TRANSIENT_STATUS_CODES = {429, 500, 502, 503, 504}


class ProbeError(RuntimeError):
    """Raised when the API response cannot be trusted or parsed."""


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--version", action="store_true")
    parser.add_argument("--scope-start-date", default="2026-01-01")
    parser.add_argument("--scope-end-date", default="2026-07-18")
    parser.add_argument(
        "--max-pages",
        type=int,
        default=300,
        help="Fresh run: total page budget. Resume run: additional page budget.",
    )
    parser.add_argument("--page-size", type=int, default=100)
    parser.add_argument(
        "--resume-dir",
        default="",
        help="Directory containing a prior Moralis probe artifact.",
    )
    parser.add_argument(
        "--output-dir",
        default="artifacts/moralis-graduated-history-probe",
    )
    parser.add_argument("--strict", action="store_true")
    return parser.parse_args()


def parse_date_start(value: str) -> datetime:
    parsed = datetime.strptime(value, "%Y-%m-%d").date()
    return datetime.combine(parsed, datetime_time.min, tzinfo=timezone.utc)


def parse_date_end(value: str) -> datetime:
    parsed = datetime.strptime(value, "%Y-%m-%d").date()
    return datetime.combine(parsed, datetime_time.max, tzinfo=timezone.utc)


def parse_timestamp(value: Any) -> datetime | None:
    if not isinstance(value, str) or not value.strip():
        return None
    text = value.strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def json_dump(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def csv_write(path: Path, rows: list[dict[str, Any]], fields: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def csv_read(path: Path) -> list[dict[str, Any]]:
    if not path.is_file():
        return []
    with path.open("r", encoding="utf-8", newline="") as handle:
        return [dict(row) for row in csv.DictReader(handle)]


def safe_headers(
    headers: requests.structures.CaseInsensitiveDict[str],
) -> dict[str, str]:
    allowed_markers = (
        "rate",
        "limit",
        "remaining",
        "reset",
        "compute",
        "request-id",
        "retry-after",
    )
    return {
        str(key): str(value)
        for key, value in headers.items()
        if any(marker in str(key).lower() for marker in allowed_markers)
    }


def request_page(
    session: requests.Session,
    api_key: str,
    cursor: str | None,
    page_size: int,
    max_attempts: int = 5,
) -> tuple[dict[str, Any], dict[str, str]]:
    params: dict[str, Any] = {"limit": page_size}
    if cursor:
        params["cursor"] = cursor

    for attempt in range(1, max_attempts + 1):
        try:
            response = session.get(
                DEFAULT_URL,
                headers={"X-API-Key": api_key, "Accept": "application/json"},
                params=params,
                timeout=60,
            )
        except requests.RequestException as exc:
            if attempt == max_attempts:
                raise ProbeError(f"Moralis bağlantı hatası: {exc}") from exc
            time.sleep(min(60, 5 * attempt))
            continue

        if response.status_code == 200:
            try:
                payload = response.json()
            except ValueError as exc:
                raise ProbeError(
                    "Moralis 200 döndürdü fakat JSON geçersiz: "
                    f"{response.text[:500]}"
                ) from exc
            if not isinstance(payload, dict):
                raise ProbeError("Moralis cevabının kökü JSON nesnesi değil.")
            return payload, safe_headers(response.headers)

        body = response.text[:1000]
        if response.status_code in TRANSIENT_STATUS_CODES and attempt < max_attempts:
            retry_after = response.headers.get("Retry-After")
            try:
                wait_seconds = int(retry_after) if retry_after else 10 * attempt
            except ValueError:
                wait_seconds = 10 * attempt
            time.sleep(min(120, max(1, wait_seconds)))
            continue

        raise ProbeError(
            f"Moralis isteği başarısız: HTTP {response.status_code}; body={body}"
        )

    raise ProbeError("Moralis isteği tekrar denemelerden sonra başarısız.")


def normalized_row(
    item: dict[str, Any],
    page_number: int,
    position: int,
) -> dict[str, Any]:
    timestamp_raw = item.get("graduatedAt")
    timestamp = parse_timestamp(timestamp_raw)
    return {
        "token_address": str(item.get("tokenAddress") or "").strip(),
        "name": str(item.get("name") or ""),
        "symbol": str(item.get("symbol") or ""),
        "graduated_at_utc": timestamp.isoformat() if timestamp else "",
        "graduated_at_raw": str(timestamp_raw or ""),
        "price_usd": str(item.get("priceUsd") or ""),
        "liquidity_usd": str(item.get("liquidity") or ""),
        "fully_diluted_valuation_usd": str(
            item.get("fullyDilutedValuation") or ""
        ),
        "source_page": page_number,
        "source_position": position,
    }


def locate_resume_root(resume_dir: Path) -> Path:
    candidates = list(resume_dir.rglob("moralis_graduated_history_report.json"))
    if len(candidates) != 1:
        raise ProbeError(
            "Önceki artifact içinde tam bir Moralis raporu bulunamadı "
            f"veya birden fazla bulundu: {len(candidates)}"
        )
    return candidates[0].parent


def load_resume_state(
    resume_dir: Path,
    output_raw_dir: Path,
) -> tuple[
    list[dict[str, Any]],
    list[dict[str, Any]],
    list[dict[str, Any]],
    list[dict[str, Any]],
    str | None,
    set[str],
    int,
    datetime | None,
]:
    root = locate_resume_root(resume_dir)
    report = json.loads(
        (root / "moralis_graduated_history_report.json").read_text(
            encoding="utf-8"
        )
    )
    if report.get("complete") is True:
        raise ProbeError("Önceki artifact zaten complete=true; devam gerekmiyor.")

    all_rows = csv_read(root / "all_returned_graduated_tokens.csv")
    invalid_rows = csv_read(root / "invalid_rows.csv")
    ordering_violations = csv_read(root / "ordering_violations.csv")

    summaries_path = root / "page_summaries.json"
    page_summaries = (
        json.loads(summaries_path.read_text(encoding="utf-8"))
        if summaries_path.is_file()
        else []
    )
    if not isinstance(page_summaries, list):
        raise ProbeError("Önceki page_summaries.json liste değil.")

    previous_raw = root / "raw"
    raw_pages = sorted(previous_raw.glob("page_*.json"))
    if not raw_pages:
        raise ProbeError("Önceki artifact içinde raw cursor sayfaları yok.")

    output_raw_dir.mkdir(parents=True, exist_ok=True)
    for source in raw_pages:
        shutil.copy2(source, output_raw_dir / source.name)

    seen_cursors: set[str] = set()
    last_payload: dict[str, Any] | None = None
    for raw_page in raw_pages:
        payload = json.loads(raw_page.read_text(encoding="utf-8"))
        if not isinstance(payload, dict):
            raise ProbeError(f"Geçersiz raw sayfa: {raw_page}")
        cursor_value = payload.get("cursor")
        if cursor_value not in (None, ""):
            seen_cursors.add(str(cursor_value).strip())
        last_payload = payload

    if last_payload is None:
        raise ProbeError("Son raw sayfa okunamadı.")
    cursor_value = last_payload.get("cursor")
    cursor = (
        str(cursor_value).strip()
        if cursor_value not in (None, "")
        else None
    )
    if not cursor:
        raise ProbeError("Önceki artifact son cursor içermiyor.")

    previous_page_count = int(
        report.get("pagination", {}).get("pages_requested")
        or len(raw_pages)
    )
    latest_row_timestamp: datetime | None = None
    for row in reversed(all_rows):
        latest_row_timestamp = parse_timestamp(row.get("graduated_at_utc"))
        if latest_row_timestamp is not None:
            break

    return (
        all_rows,
        page_summaries,
        invalid_rows,
        ordering_violations,
        cursor,
        seen_cursors,
        previous_page_count,
        latest_row_timestamp,
    )


def main() -> int:
    args = parse_args()
    if args.version:
        print(PROBE_VERSION)
        return 0

    if not 1 <= args.page_size <= 100:
        raise SystemExit("page-size 1 ile 100 arasında olmalı.")
    if args.max_pages < 1:
        raise SystemExit("max-pages en az 1 olmalı.")

    api_key = os.environ.get("MORALIS_API_KEY", "").strip()
    if not api_key:
        raise SystemExit("MORALIS_API_KEY ortam değişkeni bulunamadı.")

    scope_start = parse_date_start(args.scope_start_date)
    scope_end = parse_date_end(args.scope_end_date)
    if scope_start > scope_end:
        raise SystemExit("scope-start-date, scope-end-date sonrasına gelemez.")

    output_dir = Path(args.output_dir)
    if output_dir.exists():
        shutil.rmtree(output_dir)
    raw_dir = output_dir / "raw"
    raw_dir.mkdir(parents=True, exist_ok=True)

    all_rows: list[dict[str, Any]] = []
    page_summaries: list[dict[str, Any]] = []
    invalid_rows: list[dict[str, Any]] = []
    ordering_violations: list[dict[str, Any]] = []
    cursor: str | None = None
    seen_cursors: set[str] = set()
    previous_page_count = 0
    previous_timestamp: datetime | None = None
    resumed = False

    if args.resume_dir:
        resumed = True
        (
            all_rows,
            page_summaries,
            invalid_rows,
            ordering_violations,
            cursor,
            seen_cursors,
            previous_page_count,
            previous_timestamp,
        ) = load_resume_state(Path(args.resume_dir), raw_dir)

    reached_scope_start = any(
        (
            timestamp := parse_timestamp(row.get("graduated_at_utc"))
        ) is not None
        and timestamp <= scope_start
        for row in all_rows
    )
    cursor_exhausted = False
    stop_reason = "max_pages_reached"
    new_pages_requested = 0
    session = requests.Session()

    if reached_scope_start and not ordering_violations:
        stop_reason = "scope_start_already_reached"
    else:
        for local_index in range(1, args.max_pages + 1):
            global_page = previous_page_count + local_index
            payload, headers = request_page(
                session=session,
                api_key=api_key,
                cursor=cursor,
                page_size=args.page_size,
            )
            new_pages_requested += 1
            json_dump(raw_dir / f"page_{global_page:04d}.json", payload)

            result = payload.get("result")
            if not isinstance(result, list):
                raise ProbeError(f"Sayfa {global_page}: result listesi yok.")

            page_rows: list[dict[str, Any]] = []
            page_timestamps: list[datetime] = []
            for position, item in enumerate(result, start=1):
                if not isinstance(item, dict):
                    invalid_rows.append(
                        {
                            "source_page": global_page,
                            "source_position": position,
                            "reason": "result_item_not_object",
                            "detail": repr(item)[:500],
                        }
                    )
                    continue

                row = normalized_row(item, global_page, position)
                timestamp = parse_timestamp(item.get("graduatedAt"))
                if not row["token_address"]:
                    invalid_rows.append(
                        {
                            "source_page": global_page,
                            "source_position": position,
                            "reason": "token_address_missing",
                            "detail": json.dumps(
                                item, ensure_ascii=False
                            )[:500],
                        }
                    )
                if timestamp is None:
                    invalid_rows.append(
                        {
                            "source_page": global_page,
                            "source_position": position,
                            "reason": "graduated_at_invalid",
                            "detail": str(item.get("graduatedAt") or ""),
                        }
                    )
                else:
                    page_timestamps.append(timestamp)
                    if (
                        previous_timestamp is not None
                        and timestamp > previous_timestamp
                    ):
                        ordering_violations.append(
                            {
                                "source_page": global_page,
                                "source_position": position,
                                "previous_graduated_at_utc": (
                                    previous_timestamp.isoformat()
                                ),
                                "current_graduated_at_utc": timestamp.isoformat(),
                                "token_address": row["token_address"],
                            }
                        )
                    previous_timestamp = timestamp

                page_rows.append(row)
                all_rows.append(row)

            next_cursor_value = payload.get("cursor")
            next_cursor = (
                str(next_cursor_value).strip()
                if next_cursor_value not in (None, "")
                else None
            )
            page_oldest = min(page_timestamps) if page_timestamps else None
            page_newest = max(page_timestamps) if page_timestamps else None
            page_summaries.append(
                {
                    "request_index": global_page,
                    "response_page": payload.get("page"),
                    "response_page_size": payload.get("pageSize"),
                    "returned_count": len(result),
                    "valid_object_count": len(page_rows),
                    "newest_graduated_at_utc": (
                        page_newest.isoformat() if page_newest else None
                    ),
                    "oldest_graduated_at_utc": (
                        page_oldest.isoformat() if page_oldest else None
                    ),
                    "cursor_present": bool(next_cursor),
                    "safe_response_headers": headers,
                }
            )

            if page_oldest is not None and page_oldest <= scope_start:
                reached_scope_start = True

            if not next_cursor:
                cursor_exhausted = True
                stop_reason = "cursor_exhausted"
                break

            if next_cursor in seen_cursors:
                raise ProbeError("Moralis cursor döngüsü oluştu.")
            seen_cursors.add(next_cursor)
            cursor = next_cursor

            if reached_scope_start and not ordering_violations:
                stop_reason = "scope_start_reached_in_descending_order"
                break

            time.sleep(0.15)

    address_occurrences: dict[str, int] = {}
    for row in all_rows:
        address = str(row.get("token_address") or "")
        if address:
            address_occurrences[address] = (
                address_occurrences.get(address, 0) + 1
            )

    duplicate_addresses = {
        address: count
        for address, count in address_occurrences.items()
        if count > 1
    }
    duplicate_rows = [
        row
        for row in all_rows
        if row.get("token_address") in duplicate_addresses
    ]

    unique_by_address: dict[str, dict[str, Any]] = {}
    for row in all_rows:
        address = str(row.get("token_address") or "")
        if address and address not in unique_by_address:
            unique_by_address[address] = row

    unique_rows = list(unique_by_address.values())
    in_scope_rows: list[dict[str, Any]] = []
    for row in unique_rows:
        timestamp = parse_timestamp(row.get("graduated_at_utc"))
        if timestamp is not None and scope_start <= timestamp <= scope_end:
            in_scope_rows.append(row)

    fields = [
        "token_address",
        "name",
        "symbol",
        "graduated_at_utc",
        "graduated_at_raw",
        "price_usd",
        "liquidity_usd",
        "fully_diluted_valuation_usd",
        "source_page",
        "source_position",
    ]
    csv_write(output_dir / "all_returned_graduated_tokens.csv", all_rows, fields)
    csv_write(output_dir / "unique_returned_graduated_tokens.csv", unique_rows, fields)
    csv_write(output_dir / "graduated_tokens_2026_scope.csv", in_scope_rows, fields)
    csv_write(output_dir / "duplicate_token_addresses.csv", duplicate_rows, fields)
    csv_write(
        output_dir / "invalid_rows.csv",
        invalid_rows,
        ["source_page", "source_position", "reason", "detail"],
    )
    csv_write(
        output_dir / "ordering_violations.csv",
        ordering_violations,
        [
            "source_page",
            "source_position",
            "previous_graduated_at_utc",
            "current_graduated_at_utc",
            "token_address",
        ],
    )
    json_dump(output_dir / "page_summaries.json", page_summaries)

    valid_timestamps = [
        timestamp
        for row in unique_rows
        if (timestamp := parse_timestamp(row.get("graduated_at_utc")))
        is not None
    ]
    earliest = min(valid_timestamps) if valid_timestamps else None
    latest = max(valid_timestamps) if valid_timestamps else None

    historical_boundary_proven = (
        reached_scope_start
        and not ordering_violations
        and not invalid_rows
        and not duplicate_addresses
    )
    report = {
        "probe_version": PROBE_VERSION,
        "endpoint": DEFAULT_URL,
        "scope": {
            "start_utc": scope_start.isoformat(),
            "end_utc": scope_end.isoformat(),
        },
        "resume": {
            "resumed": resumed,
            "previous_page_count": previous_page_count,
            "new_pages_requested": new_pages_requested,
        },
        "request": {
            "page_size": args.page_size,
            "max_new_pages": args.max_pages,
            "page_size_is_per_page_not_total_cap": True,
        },
        "pagination": {
            "pages_requested": len(page_summaries),
            "cursor_exhausted": cursor_exhausted,
            "reached_scope_start": reached_scope_start,
            "stop_reason": stop_reason,
            "ordering_newest_to_oldest": not ordering_violations,
            "ordering_violation_count": len(ordering_violations),
        },
        "counts": {
            "raw_returned_count": len(all_rows),
            "unique_token_address_count": len(unique_rows),
            "in_scope_graduated_token_count": len(in_scope_rows),
            "duplicate_token_address_count": len(duplicate_addresses),
            "invalid_row_count": len(invalid_rows),
        },
        "coverage": {
            "latest_graduated_at_utc": latest.isoformat() if latest else None,
            "earliest_graduated_at_utc": earliest.isoformat() if earliest else None,
            "historical_boundary_proven": historical_boundary_proven,
            "global_completeness_claimed": False,
            "note": (
                "The cursor boundary test must still be reconciled against "
                "on-chain migration counts before production use."
            ),
        },
        "files": {
            "all_rows": "all_returned_graduated_tokens.csv",
            "unique_rows": "unique_returned_graduated_tokens.csv",
            "scope_rows": "graduated_tokens_2026_scope.csv",
            "page_summaries": "page_summaries.json",
            "raw_pages_directory": "raw/",
        },
        "complete": historical_boundary_proven,
    }
    report_bytes = json.dumps(report, sort_keys=True).encode("utf-8")
    report["report_sha256_before_hash_field"] = hashlib.sha256(
        report_bytes
    ).hexdigest()
    json_dump(output_dir / "moralis_graduated_history_report.json", report)
    print(json.dumps(report, ensure_ascii=False, indent=2), flush=True)

    if args.strict and not historical_boundary_proven:
        print("MORALIS_GRADUATED_HISTORY_PROBE_INCOMPLETE", file=sys.stderr)
        return 2

    print("MORALIS_GRADUATED_HISTORY_PROBE_OK")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except ProbeError as exc:
        print(f"MORALIS_GRADUATED_HISTORY_PROBE_ERROR: {exc}", file=sys.stderr)
        raise SystemExit(3) from exc
