"""
GitHub Actions üzerinde periyodik çalışacak GMGN Solana wallet tarama scripti.

Gerekli ortam değişkeni:
    GMGN_API_KEY

Watchlist:
    config/watchlist.txt

Çıktılar:
    results/raw/<çalışma_zamanı>/
    results/logs/<çalışma_zamanı>/
    results/summary_<çalışma_zamanı>.json
"""

import json
import os
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parent.parent
WATCHLIST_PATH = ROOT / "config" / "watchlist.txt"
RESULTS_DIR = ROOT / "results"


def load_watchlist() -> list[str]:
    """Watchlist içindeki geçerli wallet adreslerini yükler."""

    if not WATCHLIST_PATH.exists():
        raise FileNotFoundError(
            f"Watchlist dosyası bulunamadı: {WATCHLIST_PATH}"
        )

    wallets: list[str] = []

    for raw_line in WATCHLIST_PATH.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()

        if not line:
            continue

        if line.startswith("#"):
            continue

        wallets.append(line)

    return wallets


def run_gmgn_cli(wallet: str) -> dict[str, Any]:
    """Tek bir Solana wallet adresi için GMGN CLI sorgusu çalıştırır."""

    command = [
        "npx",
        "--yes",
        "gmgn-cli",
        "portfolio",
        "stats",
        "--chain",
        "sol",
        "--wallet",
        wallet,
        "--raw",
    ]

    print(
        f"GMGN_COMMAND | wallet={wallet} | command={' '.join(command)}",
        flush=True,
    )

    try:
        result = subprocess.run(
            command,
            capture_output=True,
            text=True,
            timeout=180,
            check=False,
            env=os.environ.copy(),
        )
    except subprocess.TimeoutExpired as exc:
        return {
            "success": False,
            "error": "timeout",
            "exit_code": None,
            "stdout": exc.stdout or "",
            "stderr": exc.stderr or "",
        }
    except FileNotFoundError as exc:
        return {
            "success": False,
            "error": f"command_not_found: {exc}",
            "exit_code": None,
            "stdout": "",
            "stderr": str(exc),
        }
    except Exception as exc:
        return {
            "success": False,
            "error": f"subprocess_error: {type(exc).__name__}: {exc}",
            "exit_code": None,
            "stdout": "",
            "stderr": str(exc),
        }

    stdout = result.stdout.strip()
    stderr = result.stderr.strip()

    if result.returncode != 0:
        return {
            "success": False,
            "error": "gmgn_cli_failed",
            "exit_code": result.returncode,
            "stdout": stdout,
            "stderr": stderr,
        }

    if not stdout:
        return {
            "success": False,
            "error": "empty_response",
            "exit_code": result.returncode,
            "stdout": "",
            "stderr": stderr,
        }

    try:
        parsed_json = json.loads(stdout)
    except json.JSONDecodeError as exc:
        return {
            "success": False,
            "error": (
                f"json_parse_error: line={exc.lineno}, "
                f"column={exc.colno}, message={exc.msg}"
            ),
            "exit_code": result.returncode,
            "stdout": stdout,
            "stderr": stderr,
        }

    return {
        "success": True,
        "error": None,
        "exit_code": result.returncode,
        "stdout": stdout,
        "stderr": stderr,
        "data": parsed_json,
    }


def write_text_file(path: Path, content: str) -> None:
    """Metin dosyasını UTF-8 olarak kaydeder."""

    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def write_json_file(path: Path, data: Any) -> None:
    """JSON dosyasını UTF-8 olarak kaydeder."""

    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(data, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def main() -> None:
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)

    print("SCAN_START", flush=True)
    print(f"ROOT | {ROOT}", flush=True)
    print(f"WATCHLIST_PATH | {WATCHLIST_PATH}", flush=True)
    print(f"RESULTS_DIR | {RESULTS_DIR}", flush=True)

    api_key = os.environ.get("GMGN_API_KEY", "").strip()

    if not api_key:
        print(
            "SCAN_FAILED | GMGN_API_KEY ortam değişkeni bulunamadı.",
            file=sys.stderr,
            flush=True,
        )
        sys.exit(1)

    print("API_KEY_CHECK | GMGN_API_KEY mevcut.", flush=True)

    try:
        wallets = load_watchlist()
    except Exception as exc:
        print(
            f"SCAN_FAILED | Watchlist okunamadı: {exc}",
            file=sys.stderr,
            flush=True,
        )
        sys.exit(1)

    if not wallets:
        print(
            "SCAN_FAILED | Watchlist boş.",
            file=sys.stderr,
            flush=True,
        )
        sys.exit(1)

    print(f"WATCHLIST_COUNT | {len(wallets)}", flush=True)

    run_date = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H-%M-%SZ")

    raw_dir = RESULTS_DIR / "raw" / run_date
    logs_dir = RESULTS_DIR / "logs" / run_date

    raw_dir.mkdir(parents=True, exist_ok=True)
    logs_dir.mkdir(parents=True, exist_ok=True)

    successes: list[dict[str, Any]] = []
    failures: list[dict[str, Any]] = []

    for index, wallet in enumerate(wallets, start=1):
        print(
            f"Taranıyor: {index}/{len(wallets)} | {wallet}",
            flush=True,
        )

        result = run_gmgn_cli(wallet)

        stdout_path = logs_dir / f"{wallet}_stdout.txt"
        stderr_path = logs_dir / f"{wallet}_stderr.txt"

        write_text_file(
            stdout_path,
            str(result.get("stdout", "")),
        )

        write_text_file(
            stderr_path,
            str(result.get("stderr", "")),
        )

        if result["success"]:
            output_path = raw_dir / f"{wallet}.json"

            write_json_file(
                output_path,
                result["data"],
            )

            successes.append(
                {
                    "wallet": wallet,
                    "output_file": str(output_path.relative_to(ROOT)),
                    "stdout_log": str(stdout_path.relative_to(ROOT)),
                    "stderr_log": str(stderr_path.relative_to(ROOT)),
                    "exit_code": result["exit_code"],
                }
            )

            print(
                f"SCAN_OK | wallet={wallet} | output={output_path}",
                flush=True,
            )
        else:
            failure_record = {
                "wallet": wallet,
                "error": result.get("error"),
                "exit_code": result.get("exit_code"),
                "stdout_log": str(stdout_path.relative_to(ROOT)),
                "stderr_log": str(stderr_path.relative_to(ROOT)),
            }

            failures.append(failure_record)

            print(
                (
                    f"SCAN_ERROR | wallet={wallet} | "
                    f"error={result.get('error')} | "
                    f"exit_code={result.get('exit_code')}"
                ),
                file=sys.stderr,
                flush=True,
            )

            if result.get("stderr"):
                print(
                    f"GMGN_STDERR | {result['stderr']}",
                    file=sys.stderr,
                    flush=True,
                )

            if result.get("stdout"):
                print(
                    f"GMGN_STDOUT_PREVIEW | {result['stdout'][:500]}",
                    flush=True,
                )

        time.sleep(1)

    summary = {
        "run_date_utc": run_date,
        "watchlist_path": str(WATCHLIST_PATH.relative_to(ROOT)),
        "wallet_count": len(wallets),
        "success_count": len(successes),
        "failure_count": len(failures),
        "successes": successes,
        "failures": failures,
    }

    summary_path = RESULTS_DIR / f"summary_{run_date}.json"
    write_json_file(summary_path, summary)

    print(
        "SCAN_SUMMARY | "
        f"wallet_count={len(wallets)} | "
        f"success_count={len(successes)} | "
        f"failure_count={len(failures)}",
        flush=True,
    )

    print(f"SUMMARY_FILE | {summary_path}", flush=True)

    if not successes:
        print(
            "SCAN_FAILED | Hiçbir wallet için geçerli JSON üretilemedi.",
            file=sys.stderr,
            flush=True,
        )
        sys.exit(1)

    print("SCAN_SUCCESS", flush=True)


if __name__ == "__main__":
    main()
