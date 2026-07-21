#!/usr/bin/env python3
"""
Trace direct SPL-token flows A -> B -> C for the 2026 X-candidate wallet set.

This stage does NOT count transfers as sales. It discovers linked wallets by
following direct transfers of the same X token through standard associated
token accounts. Swap/program transactions are excluded from direct-transfer
edges. There is no maximum sale-delay window: recipient wallets are traced
from receipt time through the latest available chain history.

Outputs are seeds for the next cluster-level swap/PnL verification stage.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
import random
import sys
import time
from collections import defaultdict, deque
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Optional

import pandas as pd
import requests

VERSION = "2026-07-21-x-cluster-trace-v1"

DEFAULT_RPC_URL = "https://api.mainnet-beta.solana.com"
TOKEN_PROGRAM = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"
TOKEN_2022_PROGRAM = "TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb"
ASSOCIATED_TOKEN_PROGRAM = "ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL"
SYSTEM_PROGRAM = "11111111111111111111111111111111"
COMPUTE_BUDGET_PROGRAM = "ComputeBudget111111111111111111111111111111"
MEMO_PROGRAM = "MemoSq4gqABAXKb96qnH8TysNcWxMyWCqXgDLGmfcHr"

SAFE_DIRECT_PROGRAMS = {
    TOKEN_PROGRAM,
    TOKEN_2022_PROGRAM,
    ASSOCIATED_TOKEN_PROGRAM,
    SYSTEM_PROGRAM,
    COMPUTE_BUDGET_PROGRAM,
    MEMO_PROGRAM,
}

CSV_NA = ""


def utc_iso(ts: Optional[int | float]) -> str:
    if ts is None:
        return ""
    try:
        return datetime.fromtimestamp(float(ts), tz=timezone.utc).isoformat().replace("+00:00", "Z")
    except Exception:
        return ""


def parse_timestamp(value: Any, fallback: int = 1767225600) -> int:
    """Return epoch seconds. Default is 2026-01-01 UTC."""
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return fallback
    if isinstance(value, (int, float)):
        v = float(value)
        if v > 10_000_000_000:
            v /= 1000.0
        return int(v)
    s = str(value).strip()
    if not s:
        return fallback
    try:
        v = float(s)
        if v > 10_000_000_000:
            v /= 1000.0
        return int(v)
    except ValueError:
        pass
    try:
        return int(pd.Timestamp(s).timestamp())
    except Exception:
        return fallback


def atomic_write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
    tmp.replace(path)


def write_csv(path: Path, rows: list[dict[str, Any]], fieldnames: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        w.writeheader()
        for row in rows:
            w.writerow({k: (CSV_NA if row.get(k) is None else row.get(k)) for k in fieldnames})


class RateLimiter:
    def __init__(self, requests_per_second: float):
        self.interval = 1.0 / max(float(requests_per_second), 0.05)
        self.last = 0.0

    def wait(self) -> None:
        now = time.monotonic()
        delay = self.interval - (now - self.last)
        if delay > 0:
            time.sleep(delay)
        self.last = time.monotonic()


class RpcClient:
    def __init__(
        self,
        url: str,
        requests_per_second: float,
        timeout: int = 45,
        retries: int = 7,
    ):
        self.url = url
        self.timeout = timeout
        self.retries = retries
        self.rate = RateLimiter(requests_per_second)
        self.session = requests.Session()
        self.calls = 0
        self.retries_used = 0
        self.errors: list[dict[str, Any]] = []

    def call(self, method: str, params: list[Any]) -> Any:
        payload = {
            "jsonrpc": "2.0",
            "id": self.calls + 1,
            "method": method,
            "params": params,
        }
        last_error: Exception | None = None
        for attempt in range(self.retries + 1):
            self.rate.wait()
            self.calls += 1
            try:
                response = self.session.post(self.url, json=payload, timeout=self.timeout)
                if response.status_code in (429, 500, 502, 503, 504):
                    raise RuntimeError(f"RPC HTTP {response.status_code}: {response.text[:300]}")
                response.raise_for_status()
                body = response.json()
                if body.get("error"):
                    code = body["error"].get("code")
                    message = body["error"].get("message", "")
                    if code in (-32005, -32004, -32603) or "rate" in message.lower():
                        raise RuntimeError(f"RPC error {code}: {message}")
                    raise ValueError(f"RPC fatal error {code}: {message}")
                return body.get("result")
            except ValueError:
                raise
            except Exception as exc:
                last_error = exc
                if attempt >= self.retries:
                    break
                self.retries_used += 1
                delay = min(30.0, (2 ** attempt) * 0.75) + random.random() * 0.5
                time.sleep(delay)
        error = {
            "method": method,
            "params_preview": str(params)[:500],
            "error": repr(last_error),
        }
        self.errors.append(error)
        raise RuntimeError(f"RPC failed after retries: {error}")

    def signatures_for_address(
        self,
        address: str,
        min_block_time: int,
        max_pages: int,
        max_signatures: int,
    ) -> tuple[list[dict[str, Any]], bool]:
        rows: list[dict[str, Any]] = []
        before: str | None = None
        complete_to_start = False

        for _ in range(max_pages):
            options: dict[str, Any] = {
                "limit": min(1000, max_signatures - len(rows)),
                "commitment": "finalized",
            }
            if before:
                options["before"] = before
            page = self.call("getSignaturesForAddress", [address, options]) or []
            if not page:
                complete_to_start = True
                break

            for item in page:
                bt = item.get("blockTime")
                if bt is not None and int(bt) < min_block_time:
                    complete_to_start = True
                    break
                if item.get("err") is None:
                    rows.append(item)
                if len(rows) >= max_signatures:
                    break

            if complete_to_start or len(rows) >= max_signatures:
                break
            if len(page) < options["limit"]:
                complete_to_start = True
                break
            before = page[-1].get("signature")
            if not before:
                break

        return rows, complete_to_start

    def transaction(self, signature: str) -> dict[str, Any] | None:
        return self.call(
            "getTransaction",
            [
                signature,
                {
                    "encoding": "jsonParsed",
                    "commitment": "finalized",
                    "maxSupportedTransactionVersion": 0,
                },
            ],
        )


def derive_ata(owner: str, mint: str) -> str:
    from solders.pubkey import Pubkey

    owner_key = Pubkey.from_string(owner)
    mint_key = Pubkey.from_string(mint)
    token_program = Pubkey.from_string(TOKEN_PROGRAM)
    ata_program = Pubkey.from_string(ASSOCIATED_TOKEN_PROGRAM)
    ata, _ = Pubkey.find_program_address(
        [bytes(owner_key), bytes(token_program), bytes(mint_key)],
        ata_program,
    )
    return str(ata)


def account_keys(tx: dict[str, Any]) -> list[str]:
    keys = (
        tx.get("transaction", {})
        .get("message", {})
        .get("accountKeys", [])
    )
    out: list[str] = []
    for item in keys:
        if isinstance(item, str):
            out.append(item)
        elif isinstance(item, dict):
            out.append(str(item.get("pubkey", "")))
        else:
            out.append(str(item))
    return out


def top_level_program_ids(tx: dict[str, Any]) -> set[str]:
    instructions = (
        tx.get("transaction", {})
        .get("message", {})
        .get("instructions", [])
    )
    out: set[str] = set()
    keys = account_keys(tx)
    for ix in instructions:
        if not isinstance(ix, dict):
            continue
        pid = ix.get("programId")
        if pid:
            out.add(str(pid))
            continue
        idx = ix.get("programIdIndex")
        if isinstance(idx, int) and 0 <= idx < len(keys):
            out.add(keys[idx])
    return out


def all_instructions(tx: dict[str, Any]) -> Iterable[dict[str, Any]]:
    top = tx.get("transaction", {}).get("message", {}).get("instructions", [])
    for ix in top:
        if isinstance(ix, dict):
            yield ix
    for group in tx.get("meta", {}).get("innerInstructions", []) or []:
        for ix in group.get("instructions", []) or []:
            if isinstance(ix, dict):
                yield ix


def token_balance_maps(tx: dict[str, Any]) -> tuple[dict[str, dict[str, Any]], dict[str, dict[str, Any]]]:
    keys = account_keys(tx)

    def build(items: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
        result: dict[str, dict[str, Any]] = {}
        for item in items or []:
            idx = item.get("accountIndex")
            if not isinstance(idx, int) or idx < 0 or idx >= len(keys):
                continue
            ui = item.get("uiTokenAmount") or {}
            result[keys[idx]] = {
                "owner": item.get("owner"),
                "mint": item.get("mint"),
                "raw_amount": int(ui.get("amount") or 0),
                "decimals": int(ui.get("decimals") or 0),
            }
        return result

    meta = tx.get("meta", {}) or {}
    return build(meta.get("preTokenBalances") or []), build(meta.get("postTokenBalances") or [])


def extract_raw_amount(info: dict[str, Any]) -> int:
    token_amount = info.get("tokenAmount")
    if isinstance(token_amount, dict):
        try:
            return int(token_amount.get("amount") or 0)
        except Exception:
            return 0
    for key in ("amount", "token_amount"):
        if key in info:
            try:
                return int(info[key])
            except Exception:
                return 0
    return 0


def parse_direct_transfer_edges(
    tx: dict[str, Any],
    signature: str,
    current_owner: str,
    current_ata: str,
    mint: str,
    seed_wallet: str,
    depth: int,
) -> list[dict[str, Any]]:
    meta = tx.get("meta") or {}
    if meta.get("err") is not None:
        return []

    programs = top_level_program_ids(tx)
    direct_only = bool(programs) and programs.issubset(SAFE_DIRECT_PROGRAMS)
    if not direct_only:
        return []

    pre, post = token_balance_maps(tx)
    block_time = tx.get("blockTime")
    rows: list[dict[str, Any]] = []

    for ix in all_instructions(tx):
        parsed = ix.get("parsed")
        if not isinstance(parsed, dict):
            continue
        ix_type = str(parsed.get("type", "")).lower()
        if ix_type not in {"transfer", "transferchecked"}:
            continue
        info = parsed.get("info") or {}
        source = str(info.get("source") or "")
        destination = str(info.get("destination") or "")
        if not source or not destination:
            continue

        source_meta = pre.get(source) or post.get(source) or {}
        dest_meta = post.get(destination) or pre.get(destination) or {}
        source_mint = source_meta.get("mint") or info.get("mint")
        dest_mint = dest_meta.get("mint") or info.get("mint")
        if source_mint != mint and dest_mint != mint:
            continue

        source_owner = source_meta.get("owner")
        if source != current_ata and source_owner != current_owner:
            continue

        destination_owner = dest_meta.get("owner")
        if not destination_owner or destination_owner == current_owner:
            continue

        raw_amount = extract_raw_amount(info)
        if raw_amount <= 0:
            continue

        pre_balance = int(source_meta.get("raw_amount") or 0)
        ratio = (raw_amount / pre_balance) if pre_balance > 0 else None
        if ratio is not None and ratio >= 0.80:
            confidence = "HIGH"
        elif ratio is not None and ratio >= 0.25:
            confidence = "MEDIUM"
        else:
            confidence = "LOW"

        rows.append(
            {
                "token_mint": mint,
                "seed_wallet": seed_wallet,
                "source_wallet": current_owner,
                "destination_wallet": destination_owner,
                "source_token_account": source,
                "destination_token_account": destination,
                "signature": signature,
                "block_time": block_time,
                "block_timestamp": utc_iso(block_time),
                "raw_token_amount": raw_amount,
                "source_pre_balance_raw": pre_balance,
                "transfer_ratio_of_source_pre_balance": ratio,
                "depth_from_seed": depth + 1,
                "direct_only_transaction": True,
                "initial_confidence": confidence,
                "top_level_program_ids": "|".join(sorted(programs)),
            }
        )
    return rows


@dataclass
class NodeStatus:
    token_mint: str
    seed_wallet: str
    wallet: str
    depth: int
    trace_start_time: int
    trace_start_timestamp: str
    ata: str = ""
    signature_count: int = 0
    transaction_count: int = 0
    direct_edge_count: int = 0
    history_complete_to_start: bool = False
    status: str = "pending"
    error: str = ""


def seed_start_times(pairs: pd.DataFrame) -> dict[tuple[str, str], int]:
    out: dict[tuple[str, str], int] = {}
    for row in pairs.to_dict("records"):
        key = (str(row["token_mint"]), str(row["wallet"]))
        if row.get("first_actual_buy_timestamp") not in (None, "") and not pd.isna(row.get("first_actual_buy_timestamp")):
            ts = parse_timestamp(row.get("first_actual_buy_timestamp"))
        elif row.get("start_holding_at") not in (None, "") and not pd.isna(row.get("start_holding_at")):
            ts = parse_timestamp(row.get("start_holding_at"))
        else:
            ts = parse_timestamp("2026-01-01T00:00:00Z")
        out[key] = ts
    return out


def process_seed(
    rpc: RpcClient,
    token_mint: str,
    seed_wallet: str,
    start_time: int,
    max_depth: int,
    max_signature_pages: int,
    max_signatures_per_node: int,
    max_nodes_per_seed: int,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    queue: deque[tuple[str, int, int]] = deque([(seed_wallet, 0, start_time)])
    visited: set[str] = set()
    edges: list[dict[str, Any]] = []
    statuses: list[dict[str, Any]] = []

    while queue and len(visited) < max_nodes_per_seed:
        wallet, depth, node_start = queue.popleft()
        if wallet in visited:
            continue
        visited.add(wallet)

        state = NodeStatus(
            token_mint=token_mint,
            seed_wallet=seed_wallet,
            wallet=wallet,
            depth=depth,
            trace_start_time=node_start,
            trace_start_timestamp=utc_iso(node_start),
        )
        try:
            ata = derive_ata(wallet, token_mint)
            state.ata = ata
            signatures, complete = rpc.signatures_for_address(
                ata,
                min_block_time=node_start,
                max_pages=max_signature_pages,
                max_signatures=max_signatures_per_node,
            )
            state.signature_count = len(signatures)
            state.history_complete_to_start = complete

            node_edges: list[dict[str, Any]] = []
            for sig_row in signatures:
                signature = sig_row.get("signature")
                if not signature:
                    continue
                tx = rpc.transaction(signature)
                if not tx:
                    continue
                state.transaction_count += 1
                parsed = parse_direct_transfer_edges(
                    tx=tx,
                    signature=signature,
                    current_owner=wallet,
                    current_ata=ata,
                    mint=token_mint,
                    seed_wallet=seed_wallet,
                    depth=depth,
                )
                node_edges.extend(parsed)

            # Deduplicate multiple parsed instruction views of the same transfer.
            dedup: dict[tuple[Any, ...], dict[str, Any]] = {}
            for edge in node_edges:
                key = (
                    edge["signature"],
                    edge["source_token_account"],
                    edge["destination_token_account"],
                    edge["raw_token_amount"],
                )
                dedup[key] = edge
            node_edges = list(dedup.values())
            edges.extend(node_edges)
            state.direct_edge_count = len(node_edges)
            state.status = "ok"

            if depth < max_depth:
                for edge in node_edges:
                    recipient = str(edge["destination_wallet"])
                    received_at = int(edge.get("block_time") or node_start)
                    if recipient not in visited:
                        queue.append((recipient, depth + 1, received_at))

        except Exception as exc:
            state.status = "error"
            state.error = repr(exc)[:1000]

        statuses.append(asdict(state))

    return edges, statuses


EDGE_FIELDS = [
    "token_mint",
    "seed_wallet",
    "source_wallet",
    "destination_wallet",
    "source_token_account",
    "destination_token_account",
    "signature",
    "block_time",
    "block_timestamp",
    "raw_token_amount",
    "source_pre_balance_raw",
    "transfer_ratio_of_source_pre_balance",
    "depth_from_seed",
    "direct_only_transaction",
    "initial_confidence",
    "top_level_program_ids",
]

STATUS_FIELDS = [
    "token_mint",
    "seed_wallet",
    "wallet",
    "depth",
    "trace_start_time",
    "trace_start_timestamp",
    "ata",
    "signature_count",
    "transaction_count",
    "direct_edge_count",
    "history_complete_to_start",
    "status",
    "error",
]


def run_scan(args: argparse.Namespace) -> None:
    seeds = pd.read_csv(args.seeds)
    pairs = pd.read_csv(args.pairs)
    starts = seed_start_times(pairs)

    required_seeds = {"token_mint", "wallet"}
    required_pairs = {"token_mint", "wallet"}
    if not required_seeds.issubset(seeds.columns):
        raise SystemExit(f"Missing seed columns: {sorted(required_seeds - set(seeds.columns))}")
    if not required_pairs.issubset(pairs.columns):
        raise SystemExit(f"Missing pair columns: {sorted(required_pairs - set(pairs.columns))}")

    seeds = seeds.drop_duplicates(["token_mint", "wallet"]).reset_index(drop=True)
    if args.shard_count > 1:
        seeds = seeds.iloc[args.shard_index :: args.shard_count].reset_index(drop=True)
    if args.limit_pairs:
        seeds = seeds.head(args.limit_pairs).copy()

    label = "probe" if args.mode == "probe" else f"shard_{args.shard_index:02d}_of_{args.shard_count:02d}"
    out = Path(args.output_dir)
    out.mkdir(parents=True, exist_ok=True)

    rpc_url = args.rpc_url or os.environ.get("SOLANA_RPC_URL") or DEFAULT_RPC_URL
    rpc = RpcClient(
        rpc_url,
        requests_per_second=args.requests_per_second,
        timeout=args.rpc_timeout,
        retries=args.rpc_retries,
    )

    all_edges: list[dict[str, Any]] = []
    all_statuses: list[dict[str, Any]] = []

    for idx, row in enumerate(seeds.to_dict("records"), start=1):
        mint = str(row["token_mint"])
        wallet = str(row["wallet"])
        start = starts.get((mint, wallet), parse_timestamp("2026-01-01T00:00:00Z"))
        print(f"[{idx}/{len(seeds)}] seed={wallet} mint={mint} start={utc_iso(start)}", flush=True)
        edges, statuses = process_seed(
            rpc=rpc,
            token_mint=mint,
            seed_wallet=wallet,
            start_time=start,
            max_depth=args.max_depth,
            max_signature_pages=args.max_signature_pages,
            max_signatures_per_node=args.max_signatures_per_node,
            max_nodes_per_seed=args.max_nodes_per_seed,
        )
        all_edges.extend(edges)
        all_statuses.extend(statuses)

    write_csv(out / f"x_cluster_edges_{label}.csv", all_edges, EDGE_FIELDS)
    write_csv(out / f"x_cluster_node_status_{label}.csv", all_statuses, STATUS_FIELDS)

    discovered_rows = sorted(
        {
            (e["token_mint"], e["seed_wallet"], e["destination_wallet"], e["depth_from_seed"], e["initial_confidence"])
            for e in all_edges
        }
    )
    discovered = [
        {
            "token_mint": r[0],
            "seed_wallet": r[1],
            "linked_wallet": r[2],
            "depth_from_seed": r[3],
            "initial_confidence": r[4],
        }
        for r in discovered_rows
    ]
    write_csv(
        out / f"x_cluster_discovered_wallets_{label}.csv",
        discovered,
        ["token_mint", "seed_wallet", "linked_wallet", "depth_from_seed", "initial_confidence"],
    )

    status_counts = pd.Series([s["status"] for s in all_statuses]).value_counts().to_dict() if all_statuses else {}
    report = {
        "script_version": VERSION,
        "mode": args.mode,
        "label": label,
        "seed_pair_count": int(len(seeds)),
        "node_count": len(all_statuses),
        "direct_edge_count": len(all_edges),
        "unique_linked_wallet_count": len({e["destination_wallet"] for e in all_edges}),
        "status_counts": status_counts,
        "rpc_calls": rpc.calls,
        "rpc_retries_used": rpc.retries_used,
        "rpc_error_count": len(rpc.errors),
        "configured_rules": {
            "no_sale_delay_window": True,
            "transfer_is_not_sale": True,
            "direct_only_program_filter": True,
            "max_depth": args.max_depth,
            "max_signature_pages": args.max_signature_pages,
            "max_signatures_per_node": args.max_signatures_per_node,
            "max_nodes_per_seed": args.max_nodes_per_seed,
        },
        "next_stage": "Query Moralis swaps for accepted linked wallets and recompute X FIFO PnL at cluster level.",
    }
    atomic_write_json(out / f"x_cluster_trace_report_{label}.json", report)
    if rpc.errors:
        with (out / f"x_cluster_rpc_errors_{label}.jsonl").open("w", encoding="utf-8") as f:
            for error in rpc.errors:
                f.write(json.dumps(error, ensure_ascii=False) + "\n")
    print(json.dumps(report, indent=2), flush=True)


class UnionFind:
    def __init__(self) -> None:
        self.parent: dict[str, str] = {}

    def find(self, x: str) -> str:
        self.parent.setdefault(x, x)
        if self.parent[x] != x:
            self.parent[x] = self.find(self.parent[x])
        return self.parent[x]

    def union(self, a: str, b: str) -> None:
        ra, rb = self.find(a), self.find(b)
        if ra != rb:
            self.parent[rb] = ra


def downgrade(conf: str) -> str:
    return {"HIGH": "MEDIUM", "MEDIUM": "LOW", "LOW": "LOW"}.get(conf, "LOW")


def run_merge(args: argparse.Namespace) -> None:
    src = Path(args.input_dir)
    edge_files = sorted(src.rglob("x_cluster_edges_shard_*.csv"))
    status_files = sorted(src.rglob("x_cluster_node_status_shard_*.csv"))
    report_files = sorted(src.rglob("x_cluster_trace_report_shard_*.json"))

    if not edge_files or not status_files:
        raise SystemExit(f"No shard files found in {src}")

    edges = pd.concat([pd.read_csv(p) for p in edge_files], ignore_index=True)
    statuses = pd.concat([pd.read_csv(p) for p in status_files], ignore_index=True)
    edges = edges.drop_duplicates(
        ["token_mint", "seed_wallet", "source_wallet", "destination_wallet", "signature", "raw_token_amount"]
    ).reset_index(drop=True)

    # Global recipient fan-in is a service/CEX risk signal, not an automatic deletion.
    fanin = (
        edges.groupby("destination_wallet")
        .agg(
            unique_source_wallets=("source_wallet", "nunique"),
            unique_seed_wallets=("seed_wallet", "nunique"),
            unique_tokens=("token_mint", "nunique"),
            edge_count=("signature", "count"),
        )
        .reset_index()
    )
    edges = edges.merge(fanin, on="destination_wallet", how="left")
    edges["service_or_aggregator_risk"] = (
        (edges["unique_source_wallets"] >= args.service_risk_source_threshold)
        & (edges["unique_tokens"] >= args.service_risk_token_threshold)
    )
    edges["adjusted_confidence"] = edges["initial_confidence"]
    mask = edges["service_or_aggregator_risk"]
    edges.loc[mask, "adjusted_confidence"] = edges.loc[mask, "adjusted_confidence"].map(downgrade)
    edges["accepted_for_cluster"] = (
        edges["adjusted_confidence"].isin(["HIGH", "MEDIUM"])
        & ~edges["service_or_aggregator_risk"]
    )

    seeds = pd.read_csv(args.seeds).drop_duplicates(["token_mint", "wallet"])
    membership_rows: list[dict[str, Any]] = []
    expansion_rows: list[dict[str, Any]] = []

    for mint, seed_group in seeds.groupby("token_mint"):
        uf = UnionFind()
        for wallet in seed_group["wallet"].astype(str):
            uf.find(wallet)
        token_edges = edges[(edges["token_mint"] == mint) & edges["accepted_for_cluster"]]
        for row in token_edges.itertuples(index=False):
            uf.union(str(row.source_wallet), str(row.destination_wallet))

        all_wallets = set(seed_group["wallet"].astype(str))
        all_wallets.update(token_edges["source_wallet"].astype(str))
        all_wallets.update(token_edges["destination_wallet"].astype(str))
        roots = {w: uf.find(w) for w in all_wallets}
        grouped: dict[str, list[str]] = defaultdict(list)
        for wallet, root in roots.items():
            grouped[root].append(wallet)

        cluster_ids: dict[str, str] = {}
        for root, members in grouped.items():
            digest = hashlib.sha256((mint + "|" + "|".join(sorted(members))).encode()).hexdigest()[:16]
            cluster_ids[root] = f"XCL-{digest}"

        seed_wallets = set(seed_group["wallet"].astype(str))
        for wallet in sorted(all_wallets):
            root = roots[wallet]
            cluster_id = cluster_ids[root]
            is_seed = wallet in seed_wallets
            membership_rows.append(
                {
                    "cluster_id": cluster_id,
                    "token_mint": mint,
                    "wallet": wallet,
                    "is_original_seed": is_seed,
                    "cluster_member_count": len(grouped[root]),
                }
            )
            expansion_rows.append(
                {
                    "cluster_id": cluster_id,
                    "token_mint": mint,
                    "wallet": wallet,
                    "is_original_seed": is_seed,
                    "reason": "original_seed" if is_seed else "accepted_direct_token_flow",
                }
            )

    out = Path(args.output_dir)
    out.mkdir(parents=True, exist_ok=True)
    edges.to_csv(out / "x_cluster_edges_2026.csv", index=False)
    statuses.to_csv(out / "x_cluster_node_status_2026.csv", index=False)
    pd.DataFrame(membership_rows).drop_duplicates().to_csv(
        out / "x_cluster_membership_2026.csv", index=False
    )
    expansion = pd.DataFrame(expansion_rows).drop_duplicates(["token_mint", "wallet"])
    expansion.to_csv(out / "x_cluster_swap_scan_input_2026.csv", index=False)
    fanin.to_csv(out / "x_cluster_recipient_risk_2026.csv", index=False)

    shard_reports = [json.loads(p.read_text(encoding="utf-8")) for p in report_files]
    report = {
        "script_version": VERSION,
        "mode": "merge",
        "found_shards": len(report_files),
        "original_seed_pair_count": int(len(seeds)),
        "node_status_count": int(len(statuses)),
        "direct_edge_count": int(len(edges)),
        "accepted_edge_count": int(edges["accepted_for_cluster"].sum()),
        "review_edge_count": int((~edges["accepted_for_cluster"]).sum()),
        "service_risk_edge_count": int(edges["service_or_aggregator_risk"].sum()),
        "unique_linked_wallet_count": int(edges["destination_wallet"].nunique()),
        "cluster_swap_scan_pair_count": int(len(expansion)),
        "new_linked_pair_count": int((~expansion["is_original_seed"]).sum()),
        "no_sale_delay_window": True,
        "transfer_is_not_sale": True,
        "next_stage": "Run cluster-level Moralis swap scan for x_cluster_swap_scan_input_2026.csv, then FIFO-match A buys against B/C sells.",
        "shard_reports": shard_reports,
    }
    atomic_write_json(out / "x_cluster_trace_report_2026.json", report)
    print(json.dumps(report, indent=2), flush=True)


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser()
    p.add_argument("--version", action="store_true")
    p.add_argument("--mode", choices=["probe", "scan", "merge"])
    p.add_argument("--seeds", default="data/x_cluster_trace_seeds_2026.csv")
    p.add_argument("--pairs", default="data/x_chain_pair_results_2026.csv")
    p.add_argument("--output-dir", default="output")
    p.add_argument("--input-dir", default="")
    p.add_argument("--rpc-url", default="")
    p.add_argument("--requests-per-second", type=float, default=1.0)
    p.add_argument("--rpc-timeout", type=int, default=45)
    p.add_argument("--rpc-retries", type=int, default=7)
    p.add_argument("--max-depth", type=int, default=3)
    p.add_argument("--max-signature-pages", type=int, default=8)
    p.add_argument("--max-signatures-per-node", type=int, default=4000)
    p.add_argument("--max-nodes-per-seed", type=int, default=20)
    p.add_argument("--limit-pairs", type=int, default=0)
    p.add_argument("--shard-index", type=int, default=0)
    p.add_argument("--shard-count", type=int, default=1)
    p.add_argument("--service-risk-source-threshold", type=int, default=5)
    p.add_argument("--service-risk-token-threshold", type=int, default=2)
    return p


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    if args.version:
        print(VERSION)
        return
    if args.mode in {"probe", "scan"}:
        run_scan(args)
    elif args.mode == "merge":
        if not args.input_dir:
            raise SystemExit("--input-dir is required for merge")
        run_merge(args)
    else:
        parser.error("--mode is required")


if __name__ == "__main__":
    main()
