#!/usr/bin/env python3
"""Dune SQL sorgusunu çalıştırır ve bütün sonuçları eksiksiz indirir.

Gerekli ortam değişkeni:
    DUNE_API_KEY

Örnek:
    python scripts/dune_sql_runner.py \
        --sql-file sql/00_discover_pumpfun_tables.sql \
        --output-dir results/pumpfun_foundation/table_discovery \
        --name pumpfun_table_discovery
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


API_BASE = "https://api.dune.com/api/v1"
PAGE_SIZE = 1000
HTTP_TIMEOUT_SECONDS = 120
MAX_HTTP_ATTEMPTS = 5

FAILURE_STATES = {
    "QUERY_STATE_FAILED",
    "QUERY_STATE_CANCELED",
    "QUERY_STATE_CANCELLED",
    "QUERY_STATE_EXPIRED",
}

PARTIAL_STATES = {
    "QUERY_STATE_COMPLETED_PARTIAL",
}


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def write_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)

    path.write_text(
        json.dumps(
            value,
            ensure_ascii=False,
            indent=2,
            default=str,
        )
        + "\n",
        encoding="utf-8",
    )


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()

    with path.open("rb") as handle:
        while True:
            chunk = handle.read(1024 * 1024)

            if not chunk:
                break

            digest.update(chunk)

    return digest.hexdigest()


def api_request(
    *,
    api_key: str,
    method: str,
    endpoint: str,
    payload: dict[str, Any] | None = None,
    params: dict[str, Any] | None = None,
) -> dict[str, Any]:
    url = f"{API_BASE}{endpoint}"

    if params:
        url += "?" + urllib.parse.urlencode(params)

    request_body = None

    if payload is not None:
        request_body = json.dumps(payload).encode("utf-8")

    request = urllib.request.Request(
        url=url,
        data=request_body,
        method=method,
        headers={
            "X-Dune-Api-Key": api_key,
            "Accept": "application/json",
            "Content-Type": "application/json",
            "User-Agent": "gmgn-wallet-terminal/1.0",
        },
    )

    for attempt in range(1, MAX_HTTP_ATTEMPTS + 1):
        try:
            with urllib.request.urlopen(
                request,
                timeout=HTTP_TIMEOUT_SECONDS,
            ) as response:
                body = response.read().decode("utf-8")

            if not body.strip():
                return {}

            parsed = json.loads(body)

            if not isinstance(parsed, dict):
                raise RuntimeError(
                    "Dune cevabının üst seviyesi JSON nesnesi değil."
                )

            return parsed

        except urllib.error.HTTPError as exc:
            error_body = exc.read().decode(
                "utf-8",
                errors="replace",
            )

            retryable = (
                exc.code == 429
                or 500 <= exc.code <= 599
            )

            if retryable and attempt < MAX_HTTP_ATTEMPTS:
                retry_after = exc.headers.get(
                    "Retry-After"
                )

                try:
                    wait_seconds = int(
                        retry_after or 0
                    )
                except ValueError:
                    wait_seconds = 0

                if wait_seconds <= 0:
                    wait_seconds = min(
                        60,
                        2 ** attempt,
                    )

                print(
                    "DUNE_HTTP_RETRY | "
                    f"status={exc.code} | "
                    f"attempt={attempt}/"
                    f"{MAX_HTTP_ATTEMPTS} | "
                    f"wait={wait_seconds}s",
                    flush=True,
                )

                time.sleep(wait_seconds)
                continue

            raise RuntimeError(
                "Dune HTTP hatası: "
                f"status={exc.code}, "
                f"body={error_body[:3000]}"
            ) from exc

        except urllib.error.URLError as exc:
            if attempt < MAX_HTTP_ATTEMPTS:
                wait_seconds = min(
                    60,
                    2 ** attempt,
                )

                print(
                    "DUNE_CONNECTION_RETRY | "
                    f"attempt={attempt}/"
                    f"{MAX_HTTP_ATTEMPTS} | "
                    f"wait={wait_seconds}s | "
                    f"error={exc}",
                    flush=True,
                )

                time.sleep(wait_seconds)
                continue

            raise RuntimeError(
                f"Dune bağlantı hatası: {exc}"
            ) from exc

        except json.JSONDecodeError as exc:
            raise RuntimeError(
                "Dune geçerli JSON döndürmedi."
            ) from exc

    raise RuntimeError(
        "Dune isteği tekrar denemelerden sonra başarısız oldu."
    )


def execute_sql(
    *,
    api_key: str,
    sql: str,
    performance: str,
) -> str:
    response = api_request(
        api_key=api_key,
        method="POST",
        endpoint="/sql/execute",
        payload={
            "sql": sql,
            "performance": performance,
        },
    )

    execution_id = response.get("execution_id")

    if not execution_id:
        raise RuntimeError(
            "Dune execution_id döndürmedi: "
            + json.dumps(
                response,
                ensure_ascii=False,
            )[:3000]
        )

    print(
        "DUNE_EXECUTION_CREATED | "
        f"execution_id={execution_id}",
        flush=True,
    )

    return str(execution_id)


def wait_for_completion(
    *,
    api_key: str,
    execution_id: str,
    timeout_seconds: int,
    poll_seconds: int,
) -> dict[str, Any]:
    started = time.monotonic()

    while True:
        status = api_request(
            api_key=api_key,
            method="GET",
            endpoint=(
                f"/execution/{execution_id}/status"
            ),
        )

        state = str(
            status.get("state") or "UNKNOWN"
        )

        print(
            "DUNE_EXECUTION_STATUS | "
            f"execution_id={execution_id} | "
            f"state={state}",
            flush=True,
        )

        if state == "QUERY_STATE_COMPLETED":
            return status

        if state in PARTIAL_STATES:
            raise RuntimeError(
                "Dune sonucu kısmi/truncated tamamlandı. "
                "Eksik veri kabul edilmiyor: "
                + json.dumps(
                    status,
                    ensure_ascii=False,
                )[:4000]
            )

        if state in FAILURE_STATES:
            raise RuntimeError(
                "Dune sorgusu başarısız oldu: "
                + json.dumps(
                    status,
                    ensure_ascii=False,
                )[:4000]
            )

        elapsed = time.monotonic() - started

        if elapsed >= timeout_seconds:
            raise TimeoutError(
                "Dune sorgusu zaman aşımına uğradı: "
                f"execution_id={execution_id}, "
                f"timeout={timeout_seconds}"
            )

        time.sleep(poll_seconds)


def fetch_all_rows(
    *,
    api_key: str,
    execution_id: str,
) -> tuple[
    list[dict[str, Any]],
    dict[str, Any],
    int,
]:
    rows: list[dict[str, Any]] = []
    metadata: dict[str, Any] = {}

    offset = 0
    page_count = 0
    expected_total: int | None = None

    while True:
        page_count += 1

        response = api_request(
            api_key=api_key,
            method="GET",
            endpoint=(
                f"/execution/{execution_id}/results"
            ),
            params={
                "limit": PAGE_SIZE,
                "offset": offset,
                "allow_partial_results": "false",
            },
        )

        state = str(
            response.get("state") or "UNKNOWN"
        )

        if state in PARTIAL_STATES:
            raise RuntimeError(
                "Dune kısmi sonuç döndürdü. "
                "Eksik veri kabul edilmedi."
            )

        if state != "QUERY_STATE_COMPLETED":
            raise RuntimeError(
                "Dune result endpoint'i tamamlanmış "
                f"sonuç döndürmedi: state={state}"
            )

        if response.get("error"):
            raise RuntimeError(
                "Dune sonuç hatası: "
                + json.dumps(
                    response["error"],
                    ensure_ascii=False,
                )[:3000]
            )

        result = response.get("result")

        if not isinstance(result, dict):
            raise RuntimeError(
                "Dune cevabında result nesnesi yok."
            )

        page_rows = result.get("rows")

        if not isinstance(page_rows, list):
            raise RuntimeError(
                "Dune result.rows alanı liste değil."
            )

        for row in page_rows:
            if not isinstance(row, dict):
                raise RuntimeError(
                    "Dune sonucunda JSON nesnesi "
                    "olmayan satır bulundu."
                )

        page_metadata = result.get("metadata")

        if isinstance(page_metadata, dict):
            metadata.update(page_metadata)

            total_value = page_metadata.get(
                "total_row_count"
            )

            if total_value is not None:
                expected_total = int(total_value)

        rows.extend(page_rows)

        print(
            "DUNE_RESULT_PAGE | "
            f"page={page_count} | "
            f"offset={offset} | "
            f"page_rows={len(page_rows)} | "
            f"downloaded={len(rows)} | "
            f"expected={expected_total}",
            flush=True,
        )

        next_offset = response.get(
            "next_offset"
        )

        if next_offset in (None, ""):
            break

        next_offset_int = int(next_offset)

        if next_offset_int <= offset:
            raise RuntimeError(
                "Dune pagination ilerlemedi: "
                f"current={offset}, "
                f"next={next_offset_int}"
            )

        offset = next_offset_int

    if expected_total is None:
        raise RuntimeError(
            "Dune total_row_count döndürmedi; "
            "sonucun tamlığı doğrulanamadı."
        )

    if len(rows) != expected_total:
        raise RuntimeError(
            "Dune sonucu eksik indirildi: "
            f"expected={expected_total}, "
            f"actual={len(rows)}"
        )

    return rows, metadata, page_count


def write_csv(
    path: Path,
    rows: list[dict[str, Any]],
    metadata: dict[str, Any],
) -> None:
    declared_columns = metadata.get(
        "column_names"
    )

    if isinstance(declared_columns, list):
        columns = [
            str(column)
            for column in declared_columns
        ]
    else:
        columns = sorted(
            {
                key
                for row in rows
                for key in row.keys()
            }
        )

    path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    with path.open(
        "w",
        encoding="utf-8",
        newline="",
    ) as handle:
        if not columns:
            handle.write("")
            return

        writer = csv.DictWriter(
            handle,
            fieldnames=columns,
            extrasaction="ignore",
        )

        writer.writeheader()
        writer.writerows(rows)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=__doc__,
    )

    parser.add_argument(
        "--sql-file",
        required=True,
        help="Çalıştırılacak SQL dosyası.",
    )

    parser.add_argument(
        "--output-dir",
        required=True,
        help="Çıktı klasörü.",
    )

    parser.add_argument(
        "--name",
        required=True,
        help="Çalışmanın kısa adı.",
    )

    parser.add_argument(
        "--performance",
        choices=[
            "small",
            "medium",
            "large",
        ],
        default="small",
    )

    parser.add_argument(
        "--timeout-seconds",
        type=int,
        default=1800,
    )

    parser.add_argument(
        "--poll-seconds",
        type=int,
        default=5,
    )

    return parser.parse_args()


def main() -> int:
    args = parse_args()

    api_key = os.environ.get(
        "DUNE_API_KEY",
        "",
    ).strip()

    if not api_key:
        raise RuntimeError(
            "DUNE_API_KEY ortam değişkeni bulunamadı."
        )

    sql_path = Path(
        args.sql_file
    ).expanduser().resolve()

    if not sql_path.is_file():
        raise FileNotFoundError(
            f"SQL dosyası bulunamadı: {sql_path}"
        )

    sql = sql_path.read_text(
        encoding="utf-8"
    ).strip()

    if not sql:
        raise RuntimeError(
            "SQL dosyası boş."
        )

    output_dir = Path(
        args.output_dir
    ).expanduser().resolve()

    output_dir.mkdir(
        parents=True,
        exist_ok=True,
    )

    started_utc = utc_now()

    execution_id = execute_sql(
        api_key=api_key,
        sql=sql,
        performance=args.performance,
    )

    status = wait_for_completion(
        api_key=api_key,
        execution_id=execution_id,
        timeout_seconds=args.timeout_seconds,
        poll_seconds=args.poll_seconds,
    )

    rows, metadata, page_count = (
        fetch_all_rows(
            api_key=api_key,
            execution_id=execution_id,
        )
    )

    rows_json_path = (
        output_dir / "rows.json"
    )
    rows_jsonl_path = (
        output_dir / "rows.jsonl"
    )
    rows_csv_path = (
        output_dir / "rows.csv"
    )
    status_path = (
        output_dir / "execution_status.json"
    )
    manifest_path = (
        output_dir / "manifest.json"
    )

    write_json(
        rows_json_path,
        rows,
    )

    with rows_jsonl_path.open(
        "w",
        encoding="utf-8",
    ) as handle:
        for row in rows:
            handle.write(
                json.dumps(
                    row,
                    ensure_ascii=False,
                    default=str,
                )
                + "\n"
            )

    write_csv(
        rows_csv_path,
        rows,
        metadata,
    )

    write_json(
        status_path,
        status,
    )

    expected_total = int(
        metadata.get("total_row_count", 0)
    )

    manifest = {
        "name": args.name,
        "complete": True,
        "partial_results_allowed": False,
        "started_utc": started_utc,
        "completed_utc": utc_now(),
        "execution_id": execution_id,
        "execution_state": status.get(
            "state"
        ),
        "performance": args.performance,
        "sql_file": str(sql_path),
        "sql_sha256": hashlib.sha256(
            sql.encode("utf-8")
        ).hexdigest(),
        "result_page_size": PAGE_SIZE,
        "result_page_count": page_count,
        "row_count": len(rows),
        "expected_total_row_count": (
            expected_total
        ),
        "row_count_matches": (
            len(rows) == expected_total
        ),
        "column_names": metadata.get(
            "column_names"
        ),
        "rows_json_sha256": sha256_file(
            rows_json_path
        ),
        "data_scope": {
            "chain": "solana",
            "launchpad": "pumpfun",
            "scope_start_utc": (
                "2024-01-19T00:00:00Z"
            ),
        },
    }

    write_json(
        manifest_path,
        manifest,
    )

    print(
        json.dumps(
            manifest,
            ensure_ascii=False,
            indent=2,
        ),
        flush=True,
    )

    print(
        "DUNE_SQL_RUNNER_SUCCESS | "
        f"name={args.name} | "
        f"rows={len(rows)} | "
        f"pages={page_count}",
        flush=True,
    )

    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())

    except Exception as exc:
        print(
            "DUNE_SQL_RUNNER_FAILED | "
            f"{type(exc).__name__}: {exc}",
            file=sys.stderr,
            flush=True,
        )
        raise
