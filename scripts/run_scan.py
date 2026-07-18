"""
GitHub Actions üzerinde periyodik çalışacak tarama scripti.
Ortam değişkeni gerekli: GMGN_API_KEY (GitHub Secrets üzerinden gelir)
"""

import json
import os
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
WATCHLIST_PATH = ROOT / "config" / "watchlist.txt"
RESULTS_DIR = ROOT / "results"


def load_watchlist() -> list[str]:
    if not WATCHLIST_PATH.exists():
        print(f"HATA: {WATCHLIST_PATH} bulunamadı.", file=sys.stderr)
        return []
    wallets = []
    for line in WATCHLIST_PATH.read_text().splitlines():
        line = line.strip()
        if line and not line.startswith("#"):
            wallets.append(line)
    return wallets


def run_gmgn_cli(wallet: str):
    cmd = ["npx", "--yes", "gmgn-cli", "portfolio", "stats",
           "--wallet", wallet, "--raw"]
    try:
        result = subprocess.run(
            cmd, capture_output=True, text=True, timeout=60, check=False
        )
    except subprocess.TimeoutExpired:
        return False, "timeout"

    if result.returncode != 0:
        return False, result.stderr.strip() or f"exit_code={result.returncode}"

    stdout = result.stdout.strip()
    if not stdout:
        return False, "empty_response"

    try:
        return True, json.loads(stdout)
    except json.JSONDecodeError:
        return False, f"json_parse_error: {stdout[:200]}"


def main():
    if not os.environ.get("GMGN_API_KEY"):
        print("HATA: GMGN_API_KEY ortam değişkeni yok.", file=sys.stderr)
        sys.exit(1)

    wallets = load_watchlist()
    if not wallets:
        print("Watchlist boş - config/watchlist.txt içine wallet adresi ekle.")
        sys.exit(0)

    run_date = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H-%M-%SZ")
    raw_dir = RESULTS_DIR / "raw" / run_date
    raw_dir.mkdir(parents=True, exist_ok=True)

    successes = []
        failures = []

    print(f"SCAN_START | wallet_sayisi={len(wallets)}", flush=True)
    print(f"Sonuç klasörü: {raw_dir}", flush=True)

    for index, wallet in enumerate(wallets, start=1):
        print(
            f"Taranıyor: {index}/{len(wallets)} | {wallet}",
            flush=True,
        )

        success, response = run_gmgn_cli(wallet)

        if success:
            output_path = raw_dir / f"{wallet}.json"
            output_path.write_text(
                json.dumps(response, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )

            successes.append({
                "wallet": wallet,
                "output_file": str(output_path.relative_to(ROOT)),
            })

            print(
                f"SCAN_OK | {wallet} | {output_path}",
                flush=True,
            )
        else:
            failures.append({
                "wallet": wallet,
                "error": response,
            })

            print(
                f"SCAN_ERROR | {wallet} | {response}",
                file=sys.stderr,
                flush=True,
            )

        time.sleep(1)

    summary = {
        "run_date_utc": run_date,
        "wallet_count": len(wallets),
        "success_count": len(successes),
        "failure_count": len(failures),
        "successes": successes,
        "failures": failures,
    }

    summary_path = RESULTS_DIR / f"summary_{run_date}.json"
    summary_path.write_text(
        json.dumps(summary, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    print(
        json.dumps(summary, ensure_ascii=False, indent=2),
        flush=True,
    )

    if not successes:
        print(
            "SCAN_FAILED | Hiçbir wallet için JSON üretilemedi.",
            file=sys.stderr,
            flush=True,
        )
        sys.exit(1)

    print("SCAN_SUCCESS", flush=True)


if __name__ == "__main__":
    main()
