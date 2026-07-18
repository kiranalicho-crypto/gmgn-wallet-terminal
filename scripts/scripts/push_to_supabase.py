"""
Son GMGN tarama sonucunu Supabase veritabanına gönderir.

Gerekli ortam değişkenleri:
    SUPABASE_URL
    SUPABASE_SECRET_KEY

Okunan dosyalar:
    results/summary_*.json
    results/raw/<run_date>/<wallet>.json
"""

import json
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen


ROOT = Path(__file__).resolve().parent.parent
RESULTS_DIR = ROOT / "results"


def require_env(name: str) -> str:
    value = os.environ.get(name, "").strip()

    if not value:
        print(
            f"HATA: {name} ortam değişkeni bulunamadı.",
            file=sys.stderr,
            flush=True,
        )
        sys.exit(1)

    return value.rstrip("/")


SUPABASE_URL = require_env("SUPABASE_URL")
SUPABASE_SECRET_KEY = require_env("SUPABASE_SECRET_KEY")


def api_request(
    method: str,
    table: str,
    payload: Any | None = None,
    query: str = "",
    prefer: str = "return=representation",
    retries: int = 3,
) -> Any:
    url = f"{SUPABASE_URL}/rest/v1/{table}"

    if query:
        url = f"{url}?{query}"

    body = None

    if payload is not None:
        body = json.dumps(
            payload,
            ensure_ascii=False,
        ).encode("utf-8")

    headers = {
        "apikey": SUPABASE_SECRET_KEY,
        "Authorization": f"Bearer {SUPABASE_SECRET_KEY}",
        "Content-Type": "application/json",
        "Accept": "application/json",
        "Prefer": prefer,
    }

    for attempt in range(1, retries + 1):
        request = Request(
            url=url,
            data=body,
            headers=headers,
            method=method,
        )

        try:
            with urlopen(request, timeout=60) as response:
                response_text = response.read().decode("utf-8").strip()

                if not response_text:
                    return None

                return json.loads(response_text)

        except HTTPError as exc:
            error_body = exc.read().decode("utf-8", errors="replace")

            print(
                (
                    f"SUPABASE_HTTP_ERROR | table={table} | "
                    f"status={exc.code} | body={error_body}"
                ),
                file=sys.stderr,
                flush=True,
            )

            if exc.code < 500 or attempt == retries:
                raise

        except URLError as exc:
            print(
                (
                    f"SUPABASE_NETWORK_ERROR | table={table} | "
                    f"attempt={attempt}/{retries} | error={exc}"
                ),
                file=sys.stderr,
                flush=True,
            )

            if attempt == retries:
                raise

        time.sleep(2 ** (attempt - 1))

    raise RuntimeError("Supabase isteği tamamlanamadı.")


def epoch_to_iso(value: Any) -> str | None:
    if value in (None, "", 0, "0"):
        return None

    try:
        timestamp = float(value)

        return datetime.fromtimestamp(
            timestamp,
            tz=timezone.utc,
        ).isoformat()

    except (TypeError, ValueError, OverflowError):
        return None


def to_float(value: Any) -> float | None:
    if value in (None, ""):
        return None

    try:
        return float(value)

    except (TypeError, ValueError):
        return None


def to_int(value: Any) -> int | None:
    if value in (None, ""):
        return None

    try:
        return int(value)

    except (TypeError, ValueError):
        return None


def find_latest_summary() -> Path:
    summary_files = sorted(
        RESULTS_DIR.glob("summary_*.json"),
        key=lambda path: path.stat().st_mtime,
        reverse=True,
    )

    if not summary_files:
        raise FileNotFoundError(
            "results klasöründe summary_*.json bulunamadı."
        )

    return summary_files[0]


def load_json(path: Path) -> dict[str, Any]:
    return json.loads(
        path.read_text(encoding="utf-8")
    )


def create_scan_run(summary: dict[str, Any]) -> str:
    started_at_text = summary.get("run_date_utc")

    started_at = None

    if started_at_text:
        try:
            started_at = datetime.strptime(
                started_at_text,
                "%Y-%m-%dT%H-%M-%SZ",
            ).replace(
                tzinfo=timezone.utc
            ).isoformat()
        except ValueError:
            started_at = None

    payload = {
        "source": "gmgn",
        "scan_type": "wallet_stats",
        "target_type": "wallet_watchlist",
        "target_value": summary.get("watchlist_path"),
        "status": (
            "success"
            if summary.get("failure_count", 0) == 0
            else "partial"
        ),
        "started_at": started_at,
        "finished_at": datetime.now(timezone.utc).isoformat(),
        "records_fetched": summary.get("success_count", 0),
        "pages_fetched": 1,
        "pagination_complete": True,
        "metadata": summary,
    }

    response = api_request(
        method="POST",
        table="scan_runs",
        payload=payload,
    )

    if not response or not response[0].get("id"):
        raise RuntimeError(
            "scan_runs kaydı oluşturulamadı."
        )

    return response[0]["id"]


def upsert_wallet(
    wallet_address: str,
    raw_data: dict[str, Any],
) -> None:
    common = raw_data.get("common") or {}

    wallet_payload = {
        "address": wallet_address,
        "chain": "sol",
        "category": "candidate",
        "status": "active",
        "funding_source": common.get("fund_from"),
        "created_token_count": to_int(
            common.get("created_token_count")
        ),
        "last_seen_at": epoch_to_iso(
            raw_data.get("last_timestamp")
        ),
        "tags": common.get("tags") or [],
        "metadata": {
            "native_balance_sol": raw_data.get("native_balance"),
            "realized_profit_pnl": raw_data.get(
                "realized_profit_pnl"
            ),
            "bought_cost": raw_data.get("bought_cost"),
            "bought_fee": raw_data.get("bought_fee"),
            "sold_income": raw_data.get("sold_income"),
            "sold_fee": raw_data.get("sold_fee"),
            "total_cost": raw_data.get("total_cost"),
            "common": common,
        },
    }

    api_request(
        method="POST",
        table="wallets",
        payload=wallet_payload,
        query="on_conflict=address",
        prefer="resolution=merge-duplicates,return=representation",
    )


def insert_wallet_snapshot(
    wallet_address: str,
    scan_run_id: str,
    raw_data: dict[str, Any],
) -> None:
    pnl_stat = raw_data.get("pnl_stat") or {}
    common = raw_data.get("common") or {}

    snapshot_payload = {
        "wallet_address": wallet_address,
        "scan_run_id": scan_run_id,

        # GMGN komutunda period açıkça belirtilmediği için
        # bunun 7d/30d olduğunu varsaymıyoruz.
        "period": "gmgn_default",

        # native_balance SOL miktarıdır; balance_usd değildir.
        "balance_usd": None,

        "realized_pnl_usd": to_float(
            raw_data.get("realized_profit")
        ),
        "unrealized_pnl_usd": None,
        "total_pnl_usd": None,
        "win_rate": to_float(
            pnl_stat.get("winrate")
        ),
        "buy_count": to_int(
            raw_data.get("buy")
        ),
        "sell_count": to_int(
            raw_data.get("sell")
        ),
        "token_count": to_int(
            pnl_stat.get("token_num")
        ),
        "average_holding_seconds": to_int(
            pnl_stat.get("avg_holding_period")
        ),
        "created_token_count": to_int(
            common.get("created_token_count")
        ),
        "raw_json": raw_data,
    }

    api_request(
        method="POST",
        table="wallet_stats_snapshots",
        payload=snapshot_payload,
    )


def archive_raw_response(
    wallet_address: str,
    scan_run_id: str,
    raw_data: dict[str, Any],
) -> None:
    payload = {
        "scan_run_id": scan_run_id,
        "source": "gmgn",
        "endpoint_or_command": (
            "gmgn-cli portfolio stats "
            "--chain sol --wallet <wallet> --raw"
        ),
        "target": wallet_address,
        "page_cursor": None,
        "response_json": raw_data,
    }

    api_request(
        method="POST",
        table="raw_api_responses",
        payload=payload,
    )


def main() -> None:
    print("SUPABASE_PUSH_START", flush=True)

    summary_path = find_latest_summary()
    summary = load_json(summary_path)

    print(
        f"SUMMARY_FILE | {summary_path}",
        flush=True,
    )

    scan_run_id = create_scan_run(summary)

    print(
        f"SCAN_RUN_CREATED | {scan_run_id}",
        flush=True,
    )

    processed = 0

    for success_record in summary.get("successes", []):
        wallet_address = success_record.get("wallet")
        relative_output_file = success_record.get("output_file")

        if not wallet_address or not relative_output_file:
            print(
                f"UYARI: Eksik success kaydı: {success_record}",
                file=sys.stderr,
                flush=True,
            )
            continue

        raw_path = ROOT / relative_output_file

        if not raw_path.exists():
            raise FileNotFoundError(
                f"Wallet JSON dosyası bulunamadı: {raw_path}"
            )

        raw_data = load_json(raw_path)

        print(
            f"SUPABASE_WALLET_PUSH | {wallet_address}",
            flush=True,
        )

        upsert_wallet(
            wallet_address=wallet_address,
            raw_data=raw_data,
        )

        insert_wallet_snapshot(
            wallet_address=wallet_address,
            scan_run_id=scan_run_id,
            raw_data=raw_data,
        )

        archive_raw_response(
            wallet_address=wallet_address,
            scan_run_id=scan_run_id,
            raw_data=raw_data,
        )

        processed += 1

    if processed == 0:
        raise RuntimeError(
            "Supabase'e gönderilecek başarılı wallet bulunamadı."
        )

    print(
        f"SUPABASE_PUSH_SUCCESS | wallet_count={processed}",
        flush=True,
    )


if __name__ == "__main__":
    main()
