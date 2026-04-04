import argparse
import json
import re
import subprocess
import uuid
from datetime import datetime, timezone
from pathlib import Path

import yaml


def load_json(path: Path):
    return json.loads(path.read_text(encoding="utf-8"))


def load_jsonl_last_record(path: Path):
    last = None
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            last = json.loads(line)
    if last is None:
        raise ValueError(f"No JSONL records found in {path}")
    return last


def load_registry(registry_path: Path):
    if not registry_path.exists():
        return {"subscriptions": []}
    return yaml.safe_load(registry_path.read_text(encoding="utf-8")) or {"subscriptions": []}


def now_iso():
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def to_canonical_failing_field(column_name: str) -> str:
    if not column_name:
        return ""
    s = str(column_name)
    if ">=" in s:
        return s.split(">=")[-1].strip()
    if "." in s and s.startswith("aggregate_id."):
        return s.split(".", 1)[1]
    m = re.match(r"^fact_(.+)$", s)
    if m:
        return f"extracted_facts.{m.group(1)}"
    if s.startswith("extracted_facts."):
        return s
    return s


def registry_blast_radius(registry: dict, contract_id: str, failing_field: str):
    """Subscriber blast radius from registry only (no lineage)."""
    affected = []
    for sub in registry.get("subscriptions", []) or []:
        if sub.get("contract_id") != contract_id:
            continue
        for bf in sub.get("breaking_fields", []) or []:
            field = bf.get("field", "")
            if not field:
                continue
            if failing_field == field or failing_field.startswith(field + "."):
                affected.append(
                    {
                        "subscriber_id": sub.get("subscriber_id"),
                        "subscriber_team": sub.get("subscriber_team"),
                        "contact": sub.get("contact"),
                        "reason": bf.get("reason"),
                    }
                )
                break
    return affected


def find_week7_consumer_node(nodes: list) -> str | None:
    for n in nodes or []:
        nid = str(n.get("node_id", ""))
        if n.get("type") == "PIPELINE" and "week7" in nid.lower():
            return n.get("node_id")
    return None


def upstream_producer_files(edges: list, consumer_id: str | None, contract_id: str) -> tuple[list[str], int]:
    """
    Traverse upstream from Week 7 consumer: edges FILE -CONSUMES-> PIPELINE (target=consumer).
    Returns (producer_file_node_ids, hop_count=1 when direct link exists).
    """
    if not consumer_id:
        return [], 0
    cid = contract_id.lower()
    producers = []
    for e in edges or []:
        if e.get("target") != consumer_id:
            continue
        if str(e.get("relationship", "")).upper() != "CONSUMES":
            continue
        src = e.get("source")
        if not src:
            continue
        sl = str(src).lower()
        if "week3" in cid and ("week3" in sl or "extraction" in sl):
            producers.append(src)
        elif "week5" in cid and ("week5" in sl or "event" in sl):
            producers.append(src)
    if not producers:
        for e in edges or []:
            if e.get("target") == consumer_id and str(e.get("relationship", "")).upper() == "CONSUMES":
                s = e.get("source")
                if s:
                    producers.append(s)
    producers = list(dict.fromkeys(producers))
    hops = 1 if producers else 0
    return producers, hops


def forward_bfs_max_depth(start: str | None, edges: list, node_by_id: dict, max_depth: int = 4) -> tuple[int, list[str]]:
    """Downstream from producer file: max hop depth and visited node ids (pipelines + files)."""
    if not start or not edges:
        return 0, []
    depth_map = {start: 0}
    frontier = {start}
    for d in range(1, max_depth + 1):
        nxt = set()
        for n in frontier:
            for e in edges:
                if e.get("source") != n:
                    continue
                t = e.get("target")
                if t and t not in depth_map:
                    depth_map[t] = d
                    nxt.add(t)
        frontier = nxt
        if not frontier:
            break
    max_d = max(depth_map.values()) if depth_map else 0
    return max_d, list(depth_map.keys())


def find_git_root(start: Path) -> Path | None:
    cur = start
    if cur.is_file():
        cur = cur.parent
    for _ in range(20):
        if (cur / ".git").exists():
            return cur
        if cur.parent == cur:
            break
        cur = cur.parent
    return None


def get_recent_commits(repo_root: Path, file_hint: str | None = None, days: int = 90, limit: int = 5):
    repo_root = repo_root or Path.cwd()
    cmd = [
        "git",
        "log",
        f"--since={days} days ago",
        f"--max-count={limit}",
        "--format=%H|%an|%ai|%s",
    ]
    if file_hint:
        try:
            rel = str(Path(file_hint).resolve().relative_to(repo_root.resolve()))
            cmd += ["--", rel]
        except Exception:
            pass
    try:
        p = subprocess.run(cmd, cwd=str(repo_root), capture_output=True, text=True, check=False)
        commits = []
        for line in p.stdout.strip().splitlines():
            if "|" not in line:
                continue
            h, author, ts, msg = line.split("|", 3)
            commits.append(
                {"commit_hash": h, "author": author, "commit_timestamp": ts, "commit_message": msg}
            )
        return commits
    except Exception:
        return []


def score_candidates(commits: list, violation_timestamp: str, lineage_hops: int):
    """
    Rubric: base = 1.0 − (days_since_commit × 0.1), reduced by 0.2 per lineage hop.
    """
    try:
        vt = datetime.fromisoformat(str(violation_timestamp).replace("Z", "+00:00"))
    except Exception:
        vt = datetime.now(timezone.utc)

    scored = []
    for rank, c in enumerate(commits[:5], start=1):
        try:
            ct = datetime.fromisoformat(str(c["commit_timestamp"]).replace("Z", "+00:00"))
        except Exception:
            ct = vt
        days_since = abs((vt - ct).total_seconds()) / 86400.0
        base = 1.0 - (days_since * 0.1)
        score = max(0.0, base - 0.2 * lineage_hops)
        scored.append({**c, "rank": rank, "confidence_score": round(score, 3)})
    return sorted(scored, key=lambda x: x["confidence_score"], reverse=True)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--violation", required=True)
    parser.add_argument("--lineage", required=True)
    parser.add_argument("--contract", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--write-injection-comment", action="store_true")
    args = parser.parse_args()

    violation_report = load_json(Path(args.violation))
    contract_yaml = yaml.safe_load(Path(args.contract).read_text(encoding="utf-8"))
    contract_id = violation_report.get("contract_id") or contract_yaml.get("id")

    registry_path = Path("contract_registry") / "subscriptions.yaml"
    # 1) Registry first — subscriber blast radius before any lineage traversal
    registry = load_registry(registry_path)

    failing_results = [r for r in violation_report.get("results", []) or [] if r.get("status") in ("FAIL", "ERROR")]
    if not failing_results:
        print("No failing results found; nothing to attribute.")
        return

    # 2) Lineage graph (after registry is loaded)
    lineage_snapshot = load_jsonl_last_record(Path(args.lineage))
    nodes_list = lineage_snapshot.get("nodes", []) or []
    edges = lineage_snapshot.get("edges", []) or []
    node_by_id = {n.get("node_id"): n for n in nodes_list if n.get("node_id")}

    consumer_id = find_week7_consumer_node(nodes_list)
    producer_ids, upstream_hops = upstream_producer_files(edges, consumer_id, contract_id)
    primary_producer = producer_ids[0] if producer_ids else None
    forward_depth, affected_nodes = forward_bfs_max_depth(primary_producer, edges, node_by_id)
    affected_pipelines = [
        nid for nid in affected_nodes if str(node_by_id.get(nid, {}).get("type", "")).upper() == "PIPELINE"
    ]

    file_hint = None
    if primary_producer:
        pn = node_by_id.get(primary_producer, {}) or {}
        file_hint = (pn.get("metadata") or {}).get("path")

    git_root = find_git_root(Path(file_hint)) if file_hint else None
    if not git_root:
        git_root = Path.cwd()

    out_path = Path(args.output)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    if args.write_injection_comment and not out_path.exists():
        with out_path.open("w", encoding="utf-8") as f:
            f.write("# injection_note: true, type: scale_change\n")

    for res in failing_results:
        failing_field = to_canonical_failing_field(str(res.get("column_name") or ""))
        registry_blast = registry_blast_radius(registry, contract_id=contract_id, failing_field=failing_field)

        # Additive contamination per rubric: registry matches + lineage upstream + forward reach
        contamination_depth = len(registry_blast) + upstream_hops + forward_depth

        commits = get_recent_commits(git_root, file_hint=file_hint, days=120, limit=5)
        if not commits:
            head = subprocess.run(
                ["git", "rev-parse", "HEAD"], cwd=str(git_root), capture_output=True, text=True, check=False
            ).stdout.strip()
            commits = [
                {
                    "commit_hash": head or "unknown",
                    "author": "unknown",
                    "commit_timestamp": now_iso(),
                    "commit_message": "unknown",
                }
            ]

        lineage_hops_for_score = upstream_hops if upstream_hops else 1
        scored = score_candidates(
            commits, violation_report.get("run_timestamp", now_iso()), lineage_hops=lineage_hops_for_score
        )

        entry = {
            "violation_id": str(uuid.uuid4()),
            "check_id": res.get("check_id"),
            "detected_at": now_iso(),
            "blame_chain": [
                {
                    "commit_hash": c.get("commit_hash"),
                    "author": c.get("author"),
                    "commit_timestamp": c.get("commit_timestamp"),
                    "commit_message": c.get("commit_message"),
                    "confidence_score": c.get("confidence_score"),
                    "rank": c.get("rank"),
                    "file_path": file_hint or primary_producer or "unknown",
                }
                for c in scored
            ],
            "blast_radius": {
                "registry_subscribers": registry_blast,
                "affected_nodes": affected_nodes,
                "affected_pipelines": affected_pipelines,
                "contamination_depth": contamination_depth,
                "lineage_upstream_hops": upstream_hops,
                "lineage_forward_max_depth": forward_depth,
                "producer_file_node_id": primary_producer,
                "consumer_pipeline_node_id": consumer_id,
            },
            "records_failing": res.get("records_failing", 0),
            "failing_field": failing_field,
        }

        with out_path.open("a", encoding="utf-8") as f:
            f.write(json.dumps(entry) + "\n")

    print(f"Wrote violations to {out_path}")


if __name__ == "__main__":
    main()
