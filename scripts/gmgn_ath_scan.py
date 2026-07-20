#!/usr/bin/env python3
"""
GMGN ATH scan for the 2026 Pump.fun candidate universe.

Safety rules:
- Missing ATH data is unresolved, never treated as below $10M.
- API errors are retained for retry.
- Only explicit market-cap ATH field names are accepted.
- Raw API responses are hashed and stored in compressed JSONL.
"""

from __future__ import annotations

import argparse
import concurrent.futures
import csv
import gzip
import hashlib
import json
import os
import socket
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid
from collections import Counter
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable

SCRIPT_VERSION = "2026-07-20-gmgn-ath-scan-v1"
DEFAULT_HOST = "https://openapi.gmgn.ai"
ATH_FIELD_PRIORITY = {
    "history_highest_market_cap": 0,
    "token_ath_mc": 1,
    "ath_market_cap": 2,
    "ath_mc": 3,
    "highest_market_cap": 4,
    "all_time_high_market_cap": 5,
    "all_time_high_mc": 6,
}
DIAGNOSTIC_KEY_PARTS = ("ath", "highest", "market_cap", "marketcap")


def force_ipv4() -> None:
    original = socket.getaddrinfo

    def ipv4_getaddrinfo(
        host: str,
        port: int | str,
        family: int = 0,
        type_: int = 0,
        proto: int = 0,
        flags: int = 0,
    ):
        return original(host, port, socket.AF_INET, type_, proto, flags)

    socket.getaddrinfo = ipv4_getaddrinfo  # type: ignore[assignment]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--version", action="store_true")
    parser.add_argument("--mode", choices=("probe", "scan", "merge"), default="scan")
    parser.add_argument("--input")
    parser.add_argument("--output-dir", required=False)
    parser.add_argument("--input-root")
    parser.add_argument("--expected-shards", type=int, default=8)
    parser.add_argument("--shard-index", type=int, default=0)
    parser.add_argument("--shard-count", type=int, default=1)
    parser.add_argument("--probe-size", type=int, default=60)
    parser.add_argument("--requests-per-second", type=float, default=6.0)
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument("--timeout-seconds", type=float, default=25.0)
    parser.add_argument("--max-attempts", type=int, default=5)
    parser.add_argument("--threshold-usd", type=float, default=10_000_000.0)
    args = parser.parse_args()

    if args.version:
        print(SCRIPT_VERSION)
        raise SystemExit(0)

    if not args.output_dir:
        parser.error("--output-dir is required")
    if args.mode in {"probe", "scan"} and not args.input:
        parser.error("--input is required")
    if args.mode == "merge" and not args.input_root:
        parser.error("--input-root is required")
    if args.shard_count < 1:
        parser.error("--shard-count must be >= 1")
    if not 0 <= args.shard_index < args.shard_count:
        parser.error("--shard-index must be between 0 and shard-count - 1")
    if args.requests_per_second <= 0:
        parser.error("--requests-per-second must be > 0")
    if args.workers < 1:
        parser.error("--workers must be >= 1")
    return args


@dataclass(frozen=True)
class Candidate:
    candidate_index: int
    mint: str
    source_class: str
    moralis_graduated_at_utc: str
    first_onchain_migration_at_utc: str


class GlobalRateLimiter:
    def __init__(self, requests_per_second: float) -> None:
        self.interval = 1.0 / requests_per_second
        self.lock = threading.Lock()
        self.next_allowed = 0.0
        self.blocked_until = 0.0

    def wait(self) -> None:
        while True:
            with self.lock:
                now = time.monotonic()
                target = max(self.next_allowed, self.blocked_until)
                delay = target - now
                if delay <= 0:
                    self.next_allowed = now + self.interval
                    return
            time.sleep(min(max(delay, 0.01), 2.0))

    def block_for(self, seconds: float) -> None:
        with self.lock:
            self.blocked_until = max(
                self.blocked_until,
                time.monotonic() + max(seconds, 0.0),
            )


def read_candidates(path: Path) -> list[Candidate]:
    candidates: list[Candidate] = []
    with gzip.open(path, "rt", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        required = {"candidate_index", "mint", "source_class"}
        missing = required - set(reader.fieldnames or [])
        if missing:
            raise RuntimeError(f"Input missing columns: {sorted(missing)}")
        for row in reader:
            candidates.append(
                Candidate(
                    candidate_index=int(row["candidate_index"]),
                    mint=row["mint"].strip(),
                    source_class=row["source_class"].strip(),
                    moralis_graduated_at_utc=row.get(
                        "moralis_graduated_at_utc", ""
                    ),
                    first_onchain_migration_at_utc=row.get(
                        "first_onchain_migration_at_utc", ""
                    ),
                )
            )
    if len({item.mint for item in candidates}) != len(candidates):
        raise RuntimeError("Input contains duplicate mints")
    return candidates


def choose_probe(candidates: list[Candidate], size: int) -> list[Candidate]:
    if size >= len(candidates):
        return candidates
    groups: dict[str, list[Candidate]] = {}
    for candidate in candidates:
        groups.setdefault(candidate.source_class, []).append(candidate)

    selected: list[Candidate] = []
    group_names = sorted(groups)
    per_group = max(1, size // max(len(group_names), 1))

    for group_name in group_names:
        group = groups[group_name]
        take = min(per_group, len(group))
        if take == 1:
            selected.append(group[len(group) // 2])
            continue
        for position in range(take):
            index = round(position * (len(group) - 1) / (take - 1))
            selected.append(group[index])

    if len(selected) < size:
        already = {item.mint for item in selected}
        remaining = [item for item in candidates if item.mint not in already]
        needed = size - len(selected)
        if needed == 1:
            selected.append(remaining[len(remaining) // 2])
        elif needed > 1:
            for position in range(needed):
                index = round(position * (len(remaining) - 1) / (needed - 1))
                selected.append(remaining[index])

    return selected[:size]


def numeric(value: Any) -> float | None:
    if isinstance(value, bool) or value is None:
        return None
    try:
        number = float(str(value).replace(",", "").strip())
    except (TypeError, ValueError):
        return None
    if number < 0 or number != number or number == float("inf"):
        return None
    return number


def walk_json(value: Any, path: str = "$") -> Iterable[tuple[str, str, Any]]:
    if isinstance(value, dict):
        for key, child in value.items():
            child_path = f"{path}.{key}"
            yield child_path, str(key), child
            yield from walk_json(child, child_path)
    elif isinstance(value, list):
        for index, child in enumerate(value):
            yield from walk_json(child, f"{path}[{index}]")


def extract_ath(data: Any) -> tuple[float | None, str, list[dict[str, Any]]]:
    matches: list[tuple[int, float, str, str]] = []
    diagnostics: list[dict[str, Any]] = []

    for path, key, value in walk_json(data):
        normalized = key.strip().lower()
        number = numeric(value)

        if number is not None and normalized in ATH_FIELD_PRIORITY:
            matches.append(
                (ATH_FIELD_PRIORITY[normalized], number, path, normalized)
            )

        if any(part in normalized for part in DIAGNOSTIC_KEY_PARTS):
            if isinstance(value, (str, int, float, bool)) or value is None:
                diagnostics.append({
                    "path": path,
                    "key": key,
                    "value": value,
                })

    if not matches:
        return None, "", diagnostics[:100]

    best_priority = min(item[0] for item in matches)
    priority_matches = [item for item in matches if item[0] == best_priority]
    selected = max(priority_matches, key=lambda item: item[1])
    return selected[1], selected[2], diagnostics[:100]


def first_scalar(data: Any, names: tuple[str, ...]) -> str:
    wanted = {name.lower() for name in names}
    for _, key, value in walk_json(data):
        if key.lower() in wanted and isinstance(value, (str, int, float, bool)):
            return str(value)
    return ""


def parse_reset_delay(
    headers: Any,
    body: dict[str, Any] | None,
    attempt: int,
) -> float:
    raw = headers.get("X-RateLimit-Reset") if headers is not None else None
    if raw:
        try:
            return max(float(raw) - time.time(), 0.0) + 1.0
        except ValueError:
            pass
    if body:
        raw_reset = body.get("reset_at")
        if raw_reset is not None:
            try:
                return max(float(raw_reset) - time.time(), 0.0) + 1.0
            except (TypeError, ValueError):
                pass
    return min(2 ** attempt, 30)


def api_request(
    candidate: Candidate,
    api_key: str,
    host: str,
    limiter: GlobalRateLimiter,
    timeout_seconds: float,
    max_attempts: int,
    threshold_usd: float,
) -> dict[str, Any]:
    last_error = ""

    for attempt in range(1, max_attempts + 1):
        limiter.wait()
        query = urllib.parse.urlencode({
            "chain": "sol",
            "address": candidate.mint,
            "timestamp": int(time.time()),
            "client_id": str(uuid.uuid4()),
        })
        request = urllib.request.Request(
            f"{host.rstrip('/')}/v1/token/info?{query}",
            headers={
                "X-APIKEY": api_key,
                "Content-Type": "application/json",
                "User-Agent": f"gmgn-wallet-research/{SCRIPT_VERSION}",
            },
            method="GET",
        )

        status_code = 0
        response_headers: Any = None
        raw_body = b""

        try:
            with urllib.request.urlopen(
                request,
                timeout=timeout_seconds,
            ) as response:
                status_code = int(response.status)
                response_headers = response.headers
                raw_body = response.read()
        except urllib.error.HTTPError as exc:
            status_code = int(exc.code)
            response_headers = exc.headers
            raw_body = exc.read()
        except Exception as exc:
            last_error = f"{type(exc).__name__}: {exc}"
            if attempt < max_attempts:
                time.sleep(min(2 ** (attempt - 1), 15))
                continue
            return result_error(candidate, last_error, attempt)

        text = raw_body.decode("utf-8", errors="replace")
        try:
            envelope = json.loads(text)
        except json.JSONDecodeError:
            last_error = f"HTTP {status_code}: non-JSON response"
            if attempt < max_attempts:
                time.sleep(min(2 ** (attempt - 1), 15))
                continue
            return result_error(candidate, last_error, attempt)

        api_code = envelope.get("code")
        api_error = str(envelope.get("error") or "")
        api_message = str(envelope.get("message") or "")

        if status_code == 429 or api_error in {
            "RATE_LIMIT_EXCEEDED",
            "RATE_LIMIT_BANNED",
        }:
            delay = parse_reset_delay(response_headers, envelope, attempt)
            limiter.block_for(delay)
            last_error = (
                f"rate_limit status={status_code} error={api_error} "
                f"message={api_message} reset_delay={round(delay, 2)}"
            )
            if attempt < max_attempts:
                continue
            return result_error(candidate, last_error, attempt)

        if status_code >= 500 and attempt < max_attempts:
            time.sleep(min(2 ** (attempt - 1), 15))
            continue

        if str(api_code) != "0":
            message = (
                f"HTTP {status_code} API code={api_code} "
                f"error={api_error} message={api_message}"
            )
            status = (
                "not_found"
                if status_code == 404
                or "not found" in message.lower()
                else "api_error"
            )
            return {
                **candidate_base(candidate),
                "status": status,
                "ath_market_cap_usd": "",
                "ath_field_path": "",
                "is_10m_plus": "",
                "symbol": "",
                "name": "",
                "launchpad_platform": "",
                "is_on_curve": "",
                "http_status": status_code,
                "attempts": attempt,
                "raw_sha256": hashlib.sha256(raw_body).hexdigest(),
                "diagnostic_fields_json": "[]",
                "error": message,
                "_raw_response": envelope,
            }

        data = envelope.get("data")
        ath_value, ath_path, diagnostics = extract_ath(data)
        status = "ok" if ath_value is not None else "missing_ath"
        return {
            **candidate_base(candidate),
            "status": status,
            "ath_market_cap_usd": (
                format(ath_value, ".10g") if ath_value is not None else ""
            ),
            "ath_field_path": ath_path,
            "is_10m_plus": (
                "true"
                if ath_value is not None and ath_value >= threshold_usd
                else "false"
                if ath_value is not None
                else ""
            ),
            "symbol": first_scalar(data, ("symbol", "token_symbol")),
            "name": first_scalar(data, ("name", "token_name")),
            "launchpad_platform": first_scalar(
                data,
                ("launchpad_platform", "launchpad", "platform"),
            ),
            "is_on_curve": first_scalar(data, ("is_on_curve",)),
            "http_status": status_code,
            "attempts": attempt,
            "raw_sha256": hashlib.sha256(raw_body).hexdigest(),
            "diagnostic_fields_json": json.dumps(
                diagnostics,
                ensure_ascii=False,
                separators=(",", ":"),
            ),
            "error": "",
            "_raw_response": envelope,
        }

    return result_error(candidate, last_error or "unknown error", max_attempts)


def candidate_base(candidate: Candidate) -> dict[str, Any]:
    return {
        "candidate_index": candidate.candidate_index,
        "mint": candidate.mint,
        "source_class": candidate.source_class,
        "moralis_graduated_at_utc": candidate.moralis_graduated_at_utc,
        "first_onchain_migration_at_utc":
            candidate.first_onchain_migration_at_utc,
    }


def result_error(
    candidate: Candidate,
    error: str,
    attempts: int,
) -> dict[str, Any]:
    return {
        **candidate_base(candidate),
        "status": "request_error",
        "ath_market_cap_usd": "",
        "ath_field_path": "",
        "is_10m_plus": "",
        "symbol": "",
        "name": "",
        "launchpad_platform": "",
        "is_on_curve": "",
        "http_status": "",
        "attempts": attempts,
        "raw_sha256": "",
        "diagnostic_fields_json": "[]",
        "error": error,
        "_raw_response": None,
    }


OUTPUT_FIELDS = [
    "candidate_index",
    "mint",
    "source_class",
    "moralis_graduated_at_utc",
    "first_onchain_migration_at_utc",
    "status",
    "ath_market_cap_usd",
    "ath_field_path",
    "is_10m_plus",
    "symbol",
    "name",
    "launchpad_platform",
    "is_on_curve",
    "http_status",
    "attempts",
    "raw_sha256",
    "diagnostic_fields_json",
    "error",
]


def write_scan_outputs(
    output_dir: Path,
    label: str,
    results: list[dict[str, Any]],
    args: argparse.Namespace,
    input_count: int,
) -> dict[str, Any]:
    output_dir.mkdir(parents=True, exist_ok=True)
    results.sort(key=lambda item: int(item["candidate_index"]))

    results_path = output_dir / f"ath_results_{label}.csv.gz"
    raw_path = output_dir / f"raw_responses_{label}.jsonl.gz"
    x_path = output_dir / f"x_candidates_10m_plus_{label}.csv"
    unresolved_path = output_dir / f"unresolved_{label}.csv.gz"

    with gzip.open(
        results_path,
        "wt",
        encoding="utf-8",
        newline="",
    ) as handle:
        writer = csv.DictWriter(handle, fieldnames=OUTPUT_FIELDS)
        writer.writeheader()
        for item in results:
            writer.writerow({key: item.get(key, "") for key in OUTPUT_FIELDS})

    with gzip.open(raw_path, "wt", encoding="utf-8") as handle:
        for item in results:
            handle.write(json.dumps({
                "candidate_index": item["candidate_index"],
                "mint": item["mint"],
                "source_class": item["source_class"],
                "status": item["status"],
                "raw_response": item.get("_raw_response"),
            }, ensure_ascii=False, separators=(",", ":")) + "\n")

    with x_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=OUTPUT_FIELDS)
        writer.writeheader()
        for item in results:
            if item.get("is_10m_plus") == "true":
                writer.writerow({
                    key: item.get(key, "") for key in OUTPUT_FIELDS
                })

    with gzip.open(
        unresolved_path,
        "wt",
        encoding="utf-8",
        newline="",
    ) as handle:
        writer = csv.DictWriter(handle, fieldnames=OUTPUT_FIELDS)
        writer.writeheader()
        for item in results:
            if item.get("status") != "ok":
                writer.writerow({
                    key: item.get(key, "") for key in OUTPUT_FIELDS
                })

    status_counts = Counter(str(item["status"]) for item in results)
    source_counts = Counter(str(item["source_class"]) for item in results)
    ath_field_counts = Counter(
        str(item["ath_field_path"])
        for item in results
        if item.get("ath_field_path")
    )
    x_count = sum(
        1 for item in results if item.get("is_10m_plus") == "true"
    )

    report = {
        "script_version": SCRIPT_VERSION,
        "mode": args.mode,
        "label": label,
        "input_total_candidate_count": input_count,
        "selected_count": len(results),
        "shard_index": args.shard_index,
        "shard_count": args.shard_count,
        "requests_per_second": args.requests_per_second,
        "workers": args.workers,
        "threshold_usd": args.threshold_usd,
        "status_counts": dict(sorted(status_counts.items())),
        "source_counts": dict(sorted(source_counts.items())),
        "ath_field_path_counts": dict(sorted(ath_field_counts.items())),
        "recognized_ath_count": sum(
            1 for item in results if item.get("status") == "ok"
        ),
        "x_candidates_10m_plus_count": x_count,
        "missing_or_error_is_not_treated_as_below_threshold": True,
        "complete": len(results) > 0
        and all(item.get("status") == "ok" for item in results),
        "files": {
            "results": results_path.name,
            "raw": raw_path.name,
            "x_candidates": x_path.name,
            "unresolved": unresolved_path.name,
        },
    }

    report_path = output_dir / f"report_{label}.json"
    report_path.write_text(
        json.dumps(report, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    return report


def run_scan(args: argparse.Namespace) -> int:
    api_key = os.environ.get("GMGN_API_KEY", "").strip()
    if not api_key:
        print("GMGN_API_KEY secret is missing", file=sys.stderr)
        return 10

    force_ipv4()
    candidates = read_candidates(Path(args.input))
    input_count = len(candidates)

    if args.mode == "probe":
        selected = choose_probe(candidates, args.probe_size)
        label = "probe"
    else:
        selected = [
            candidate
            for candidate in candidates
            if candidate.candidate_index % args.shard_count
            == args.shard_index
        ]
        label = f"shard_{args.shard_index:02d}_of_{args.shard_count:02d}"

    limiter = GlobalRateLimiter(args.requests_per_second)
    results: list[dict[str, Any]] = []
    completed = 0
    started = time.time()

    with concurrent.futures.ThreadPoolExecutor(
        max_workers=args.workers
    ) as executor:
        future_map = {
            executor.submit(
                api_request,
                candidate,
                api_key,
                DEFAULT_HOST,
                limiter,
                args.timeout_seconds,
                args.max_attempts,
                args.threshold_usd,
            ): candidate
            for candidate in selected
        }

        for future in concurrent.futures.as_completed(future_map):
            candidate = future_map[future]
            try:
                result = future.result()
            except Exception as exc:
                result = result_error(
                    candidate,
                    f"worker_exception {type(exc).__name__}: {exc}",
                    args.max_attempts,
                )
            results.append(result)
            completed += 1
            if completed % 250 == 0 or completed == len(selected):
                elapsed = max(time.time() - started, 0.001)
                print(
                    f"PROGRESS completed={completed}/{len(selected)} "
                    f"rate={completed / elapsed:.2f}_tokens_per_sec",
                    flush=True,
                )

    report = write_scan_outputs(
        Path(args.output_dir),
        label,
        results,
        args,
        input_count,
    )
    print(json.dumps(report, ensure_ascii=False, indent=2))

    if args.mode == "probe":
        recognized = int(report["recognized_ath_count"])
        if recognized < 5:
            print(
                "GMGN_ATH_PROBE_FAILED: fewer than 5 responses contained "
                "an explicit market-cap ATH field.",
                file=sys.stderr,
            )
            return 11
        print("GMGN_ATH_PROBE_OK")
        return 0

    unresolved = len(results) - int(report["recognized_ath_count"])
    if unresolved:
        print(
            f"GMGN_ATH_SHARD_FINISHED_WITH_UNRESOLVED={unresolved}",
            file=sys.stderr,
        )
        return 0

    print("GMGN_ATH_SHARD_OK")
    return 0


def merge_files(args: argparse.Namespace) -> int:
    input_root = Path(args.input_root)
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    result_paths = sorted(input_root.rglob("ath_results_shard_*.csv.gz"))
    raw_paths = sorted(input_root.rglob("raw_responses_shard_*.jsonl.gz"))
    report_paths = sorted(input_root.rglob("report_shard_*.json"))

    if len(result_paths) != args.expected_shards:
        print(
            f"MERGE_ERROR expected {args.expected_shards} result files, "
            f"found {len(result_paths)}",
            file=sys.stderr,
        )
        return 20

    merged: list[dict[str, str]] = []
    seen_indexes: set[int] = set()
    for path in result_paths:
        with gzip.open(path, "rt", encoding="utf-8", newline="") as handle:
            for row in csv.DictReader(handle):
                index = int(row["candidate_index"])
                if index in seen_indexes:
                    raise RuntimeError(f"Duplicate candidate_index: {index}")
                seen_indexes.add(index)
                merged.append(row)

    merged.sort(key=lambda row: int(row["candidate_index"]))
    combined_path = output_dir / "gmgn_ath_results_2026.csv.gz"
    x_path = output_dir / "x_candidates_10m_plus_2026.csv"
    unresolved_path = output_dir / "gmgn_ath_unresolved_2026.csv.gz"
    raw_combined_path = output_dir / "gmgn_ath_raw_responses_2026.jsonl.gz"

    with gzip.open(
        combined_path,
        "wt",
        encoding="utf-8",
        newline="",
    ) as handle:
        writer = csv.DictWriter(handle, fieldnames=OUTPUT_FIELDS)
        writer.writeheader()
        writer.writerows(merged)

    with x_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=OUTPUT_FIELDS)
        writer.writeheader()
        writer.writerows(
            row for row in merged if row["is_10m_plus"] == "true"
        )

    with gzip.open(
        unresolved_path,
        "wt",
        encoding="utf-8",
        newline="",
    ) as handle:
        writer = csv.DictWriter(handle, fieldnames=OUTPUT_FIELDS)
        writer.writeheader()
        writer.writerows(row for row in merged if row["status"] != "ok")

    with gzip.open(raw_combined_path, "wb") as destination:
        for path in raw_paths:
            with gzip.open(path, "rb") as source:
                while True:
                    chunk = source.read(1024 * 1024)
                    if not chunk:
                        break
                    destination.write(chunk)

    shard_reports = [
        json.loads(path.read_text(encoding="utf-8"))
        for path in report_paths
    ]
    status_counts = Counter(row["status"] for row in merged)
    source_counts = Counter(row["source_class"] for row in merged)
    x_count = sum(1 for row in merged if row["is_10m_plus"] == "true")

    report = {
        "script_version": SCRIPT_VERSION,
        "mode": "merge",
        "expected_shards": args.expected_shards,
        "found_shards": len(result_paths),
        "combined_candidate_count": len(merged),
        "unique_candidate_index_count": len(seen_indexes),
        "status_counts": dict(sorted(status_counts.items())),
        "source_counts": dict(sorted(source_counts.items())),
        "x_candidates_10m_plus_count": x_count,
        "unresolved_count": sum(
            1 for row in merged if row["status"] != "ok"
        ),
        "missing_or_error_is_not_treated_as_below_threshold": True,
        "complete": len(result_paths) == args.expected_shards
        and len(seen_indexes) == len(merged)
        and all(row["status"] == "ok" for row in merged),
        "shard_reports": shard_reports,
        "files": {
            "combined_results": combined_path.name,
            "x_candidates": x_path.name,
            "unresolved": unresolved_path.name,
            "raw_responses": raw_combined_path.name,
        },
    }
    (output_dir / "gmgn_ath_report_2026.json").write_text(
        json.dumps(report, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    print(json.dumps(report, ensure_ascii=False, indent=2))
    print("GMGN_ATH_MERGE_OK")
    return 0


def main() -> int:
    args = parse_args()
    if args.mode == "merge":
        return merge_files(args)
    return run_scan(args)


if __name__ == "__main__":
    raise SystemExit(main())
