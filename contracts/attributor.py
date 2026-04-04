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
    """
    Map flattened contract column names to the canonical field names used in subscriptions.yaml.
    Examples:
      fact_confidence      -> extracted_facts.confidence
      fact_entity_refs    -> extracted_facts.entity_refs
      extracted_facts.*   -> unchanged
      aggregate_id.sequence_number -> sequence_number (best-effort)
    """
    if not column_name:
        return ""
    # Runner sometimes uses composite names like "aggregate_id.sequence_number" for constraints.
    if "." in column_name and column_name.startswith("aggregate_id."):
        return column_name.split(".", 1)[1]

    m = re.match(r"^fact_(.+)$", column_name)
    if m:
        return f"extracted_facts.{m.group(1)}"

    # If already in extracted_facts dotted form, keep it.
    if column_name.startswith("extracted_facts."):
        return column_name

    return column_name


def registry_blast_radius(registry: dict, contract_id: str, failing_field: str):
    affected = []
    for sub in registry.get("subscriptions", []) or []:
        if sub.get("contract_id") != contract_id:
            continue
        for bf in sub.get("breaking_fields", []) or []:
            field = bf.get("field", "")
            if not field:
                continue
            # Accept exact match or prefix match.
            if failing_field == field or failing_field.startswith(field):
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


def compute_transitive_depth(producer_node_id: str, lineage_snapshot: dict, max_depth: int = 3):
    edges = lineage_snapshot.get("edges", []) or []
    nodes = lineage_snapshot.get("nodes", []) or []
    if not producer_node_id or not edges:
        return {"direct": [], "transitive": [], "max_depth": 0}

    allowed_rel = {"PRODUCES", "WRITES", "CONSUMES"}
    visited = {producer_node_id}
    frontier = {producer_node_id}
    depth_map = {}

    for depth in range(1, max_depth + 1):
        next_frontier = set()
        for node in frontier:
            for e in edges:
                if e.get("source") != node:
                    continue
                if e.get("relationship") not in allowed_rel:
                    continue
                tgt = e.get("target")
                if not tgt or tgt in visited:
                    continue
                visited.add(tgt)
                depth_map[tgt] = depth
                next_frontier.add(tgt)
        frontier = next_frontier
        if not frontier:
            break

    direct = [n for n, d in depth_map.items() if d == 1]
    transitive = [n for n, d in depth_map.items() if d > 1]
    max_d = max(depth_map.values()) if depth_map else 0
    return {"direct": direct, "transitive": transitive, "max_depth": max_d}


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


def get_recent_commits(repo_root: Path, file_hint: str | None = None, days: int = 30, limit: int = 5):
    """
    Best-effort: return up to `limit` recent commits. We do not depend on git blame lines existing.
    """
    repo_root = repo_root or Path.cwd()
    cmd = [
        "git",
        "log",
        f"--since={days} days ago",
        f"--max-count={limit}",
        "--format=%H|%an|%ai|%s",
    ]
    # If file_hint looks like a path, try to narrow history.
    if file_hint:
        # Use repo-relative path when possible.
        try:
            rel = str(Path(file_hint).resolve().relative_to(repo_root.resolve()))
            cmd += ["--", rel]
        except Exception:
            pass

    try:
        p = subprocess.run(cmd, cwd=str(repo_root), capture_output=True, text=True, check=False)
        out = p.stdout.strip().splitlines()
        commits = []
        for line in out:
            if "|" not in line:
                continue
            h, author, ts, msg = line.split("|", 3)
            commits.append(
                {
                    "commit_hash": h,
                    "author": author,
                    "commit_timestamp": ts,
                    "commit_message": msg,
                }
            )
        return commits
    except Exception:
        return []


def score_candidates(commits: list, violation_timestamp: str, lineage_distance: int):
    """
    Confidence formula (best-effort):
      base = 1.0 - days_since_commit*0.1 - lineage_distance*0.2
    Clamped to >= 0.0.
    """
    vt = None
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
        days_diff = abs((vt - ct).days)
        score = max(0.0, 1.0 - (days_diff * 0.1) - (lineage_distance * 0.2))
        scored.append({**c, "rank": rank, "confidence_score": round(score, 3)})
    return sorted(scored, key=lambda x: x["confidence_score"], reverse=True)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--violation", required=True, help="ValidationRunner JSON report containing FAIL rows")
    parser.add_argument("--lineage", required=True, help="Week 4 lineage_snapshot JSONL")
    parser.add_argument("--contract", required=True, help="Generated contract YAML (for contract_id mapping)")
    parser.add_argument("--output", required=True, help="violation_log/violations.jsonl")
    parser.add_argument("--write-injection-comment", action="store_true")
    args = parser.parse_args()

    violation_report = load_json(Path(args.violation))
    contract_yaml = yaml.safe_load(Path(args.contract).read_text(encoding="utf-8"))
    registry = load_registry(Path("contract_registry") / "subscriptions.yaml")

    lineage_snapshot = load_jsonl_last_record(Path(args.lineage))
    contract_id = violation_report.get("contract_id") or contract_yaml.get("id")

    failing_results = [r for r in violation_report.get("results", []) or [] if r.get("status") in ("FAIL", "ERROR")]
    if not failing_results:
        print("No failing results found; nothing to attribute.")
        return

    producer_node_id = ""
    if "week3" in str(contract_id):
        for n in lineage_snapshot.get("nodes", []) or []:
            if "outputs/week3" in str(n.get("node_id", "")):
                producer_node_id = n.get("node_id")
                break
    if "week5" in str(contract_id):
        for n in lineage_snapshot.get("nodes", []) or []:
            if "outputs/week5" in str(n.get("node_id", "")):
                producer_node_id = n.get("node_id")
                break

    transitive = compute_transitive_depth(producer_node_id, lineage_snapshot, max_depth=3)

    out_path = Path(args.output)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    if args.write_injection_comment and not out_path.exists():
        with out_path.open("w", encoding="utf-8") as f:
            f.write("# injection_note: true, type: scale_change\n")

    # Pick a producer "file hint" to ground git logs, using lineage snapshot node metadata.
    file_hint = None
    if producer_node_id:
        for n in lineage_snapshot.get("nodes", []) or []:
            if n.get("node_id") == producer_node_id:
                md = n.get("metadata", {}) or {}
                file_hint = md.get("path")
                break

    # Resolve git root for blame candidate history.
    git_root = None
    if file_hint:
        git_root = find_git_root(Path(file_hint))
    if not git_root:
        git_root = Path.cwd()

    for res in failing_results:
        failing_column = res.get("column_name") or ""
        failing_field = to_canonical_failing_field(str(failing_column))
        registry_blast = registry_blast_radius(registry, contract_id=contract_id, failing_field=failing_field)

        # Commit candidates best-effort.
        commits = get_recent_commits(git_root, file_hint=file_hint, days=60, limit=5)
        if not commits:
            # Fallback to current HEAD.
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

        scored = score_candidates(commits, violation_report.get("run_timestamp", now_iso()), lineage_distance=transitive["max_depth"])

        entry = {
            "violation_id": str(uuid.uuid4()),
            "check_id": res.get("check_id"),
            "detected_at": now_iso(),
            "blame_chain": [
                {
                    **c,
                    "file_path": file_hint or producer_node_id or "unknown",
                }
                for c in scored
            ],
            "blast_radius": {
                "direct_subscribers": registry_blast,
                "transitive_nodes": transitive.get("transitive", []),
                "contamination_depth": transitive.get("max_depth", 0),
                "note": "direct_subscribers from contract registry; transitive_nodes enriched from lineage snapshot",
            },
            "records_failing": res.get("records_failing", 0),
            "failing_field": failing_field,
        }

        with out_path.open("a", encoding="utf-8") as f:
            # Ignore comment/header line in JSONL by prefixing it with '#', not JSON.
            f.write(json.dumps(entry) + "\n")

    print(f"Wrote violations to {out_path}")


if __name__ == "__main__":
    main()

