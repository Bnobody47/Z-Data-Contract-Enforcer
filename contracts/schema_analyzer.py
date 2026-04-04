import argparse
import json
import re
from datetime import datetime, timezone
from pathlib import Path

import yaml


def parse_snapshot_ts(path: Path):
    """Filename pattern YYYYMMDD_HHMMSS.yaml"""
    m = re.match(r"^(\d{8}_\d{6})\.yaml$", path.name)
    if not m:
        return None
    try:
        return datetime.strptime(m.group(1), "%Y%m%d_%H%M%S").replace(tzinfo=timezone.utc)
    except ValueError:
        return None


def load_snapshot_pair(contract_dir: Path, since_iso: str | None):
    files = sorted([p for p in contract_dir.glob("*.yaml") if p.is_file()], key=lambda p: p.name)
    if since_iso:
        try:
            cutoff = datetime.fromisoformat(since_iso.replace("Z", "+00:00"))
        except ValueError:
            cutoff = None
        if cutoff:
            filtered = []
            for p in files:
                ts = parse_snapshot_ts(p)
                if ts is None or ts >= cutoff:
                    filtered.append(p)
            files = filtered
    if len(files) < 2:
        raise ValueError(f"Need at least 2 snapshots in {contract_dir} (after --since filter), found {len(files)}")
    return files[-2], files[-1]


def load_yaml(path: Path):
    return yaml.safe_load(path.read_text(encoding="utf-8"))


def load_registry(path: Path):
    if not path.exists():
        return {"subscriptions": []}
    return yaml.safe_load(path.read_text(encoding="utf-8")) or {"subscriptions": []}


def numeric_bounds(clause: dict | None):
    if not clause:
        return None, None
    lo = clause.get("minimum")
    hi = clause.get("maximum")
    if lo is not None:
        lo = float(lo)
    if hi is not None:
        hi = float(hi)
    return lo, hi


def classify_change(field_name: str, old_clause: dict | None, new_clause: dict | None) -> dict | None:
    if old_clause is None and new_clause is None:
        return None

    if old_clause is None and new_clause is not None:
        req = bool(new_clause.get("required", False))
        tax = "ADD_REQUIRED_FIELD" if req else "ADD_OPTIONAL_FIELD"
        ver = "BREAKING" if req else "COMPATIBLE"
        return {
            "field": field_name,
            "verdict": ver,
            "taxonomy": tax,
            "severity": "CRITICAL" if req else "LOW",
            "reason": f"Added field {field_name} (required={req}).",
        }

    if new_clause is None and old_clause is not None:
        req = bool(old_clause.get("required", False))
        return {
            "field": field_name,
            "verdict": "BREAKING",
            "taxonomy": "REMOVE_FIELD",
            "severity": "CRITICAL",
            "reason": f"Removed field {field_name} (was required={req}).",
        }

    assert old_clause is not None and new_clause is not None
    old_t = old_clause.get("type")
    new_t = new_clause.get("type")
    olo, ohi = numeric_bounds(old_clause)
    nlo, nhi = numeric_bounds(new_clause)

    # CRITICAL: narrow type float 0–1 → int 0–100 (scale / representation change)
    if old_t == "number" and new_t == "integer":
        if ohi is not None and ohi <= 1.0 and nhi is not None and nhi >= 10:
            return {
                "field": field_name,
                "verdict": "BREAKING",
                "taxonomy": "NARROW_TYPE_SCALE_FLOAT_TO_INT",
                "severity": "CRITICAL",
                "reason": (
                    f"CRITICAL breaking: {field_name} narrowed from float scale ~[0,1] to integer scale "
                    f"with max {nhi} (e.g. 0.0–1.0 → int 0–100)."
                ),
            }
        return {
            "field": field_name,
            "verdict": "BREAKING",
            "taxonomy": "NARROW_TYPE",
            "severity": "HIGH",
            "reason": f"Type narrowed: {old_t} -> {new_t} for {field_name}.",
        }

    if old_t != new_t and not (old_t == "integer" and new_t == "number"):
        return {
            "field": field_name,
            "verdict": "BREAKING",
            "taxonomy": "TYPE_CHANGE",
            "severity": "HIGH",
            "reason": f"Type change for {field_name}: {old_t} -> {new_t}.",
        }

    if old_t == "integer" and new_t == "number":
        return {
            "field": field_name,
            "verdict": "COMPATIBLE",
            "taxonomy": "WIDEN_TYPE",
            "severity": "LOW",
            "reason": f"Widened type integer -> number for {field_name}.",
        }

    old_enum = set(old_clause.get("enum", []) or [])
    new_enum = set(new_clause.get("enum", []) or [])
    if old_enum and (old_enum - new_enum):
        return {
            "field": field_name,
            "verdict": "BREAKING",
            "taxonomy": "REMOVE_ENUM_VALUE",
            "severity": "CRITICAL",
            "reason": f"Removed enum values for {field_name}: {sorted(old_enum - new_enum)}.",
        }
    if new_enum and (new_enum - old_enum):
        return {
            "field": field_name,
            "verdict": "COMPATIBLE",
            "taxonomy": "ADD_ENUM_VALUE",
            "severity": "LOW",
            "reason": f"Added enum values for {field_name}: {sorted(new_enum - old_enum)}.",
        }

    if olo != nlo or ohi != nhi:
        if "confidence" in field_name:
            old_stats = old_clause.get("stats", {}) or {}
            new_stats = new_clause.get("stats", {}) or {}
            om, nm = old_stats.get("max"), new_stats.get("max")
            if om is not None and nm is not None and float(om) <= 1.0 and float(nm) > 1.0:
                return {
                    "field": field_name,
                    "verdict": "BREAKING",
                    "taxonomy": "CONFIDENCE_SCALE_DRIFT",
                    "severity": "CRITICAL",
                    "reason": f"Confidence scale drift for {field_name}: max {om} -> {nm} (0–1 -> 0–100).",
                }
        tighter = (nlo is not None and olo is not None and nlo > olo) or (nhi is not None and ohi is not None and nhi < ohi)
        return {
            "field": field_name,
            "verdict": "BREAKING" if tighter else "COMPATIBLE",
            "taxonomy": "RANGE_TIGHTENED" if tighter else "RANGE_WIDENED",
            "severity": "CRITICAL" if tighter else "LOW",
            "reason": f"Range/bounds change for {field_name} (min/max).",
        }

    if not old_clause.get("required") and new_clause.get("required"):
        return {
            "field": field_name,
            "verdict": "BREAKING",
            "taxonomy": "ADD_REQUIRED_FIELD",
            "severity": "CRITICAL",
            "reason": f"Field {field_name} became required.",
        }

    if old_clause.get("required") and not new_clause.get("required"):
        return {
            "field": field_name,
            "verdict": "COMPATIBLE",
            "taxonomy": "FIELD_BECAME_OPTIONAL",
            "severity": "LOW",
            "reason": f"Field {field_name} became optional (relaxation).",
        }

    if "confidence" in field_name:
        old_stats = old_clause.get("stats") or {}
        new_stats = new_clause.get("stats") or {}
        om, nm = old_stats.get("max"), new_stats.get("max")
        if om is not None and nm is not None and float(om) <= 1.0 and float(nm) > 1.0:
            return {
                "field": field_name,
                "verdict": "BREAKING",
                "taxonomy": "CONFIDENCE_SCALE_DRIFT",
                "severity": "CRITICAL",
                "reason": f"Profiled max shifted for {field_name}: {om} -> {nm} (0–1 vs 0–100 semantic drift).",
            }

    return {
        "field": field_name,
        "verdict": "COMPATIBLE",
        "taxonomy": "UNCHANGED_MATERIAL",
        "severity": "LOW",
        "reason": f"No breaking taxonomy match for {field_name}.",
    }


def per_consumer_failure_modes(registry: dict, contract_id: str, breaking_fields: list) -> list:
    modes = []
    bf_set = set(breaking_fields)
    for sub in registry.get("subscriptions", []) or []:
        if sub.get("contract_id") != contract_id:
            continue
        impacted = []
        for bfe in sub.get("breaking_fields", []) or []:
            fn = bfe.get("field", "")
            if any(b == fn or b.startswith(fn + ".") for b in bf_set):
                impacted.append(bfe)
        if impacted:
            modes.append(
                {
                    "subscriber_id": sub.get("subscriber_id"),
                    "subscriber_team": sub.get("subscriber_team"),
                    "contact": sub.get("contact"),
                    "failure_modes": impacted,
                }
            )
    return modes


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--contract-id", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument(
        "--since",
        default=None,
        help="ISO timestamp (e.g. 2026-04-01T00:00:00Z); only snapshots at or after this time are considered",
    )
    args = parser.parse_args()

    contract_dir = Path("schema_snapshots") / args.contract_id
    old_path, new_path = load_snapshot_pair(contract_dir, args.since)
    old_snapshot = load_yaml(old_path)
    new_snapshot = load_yaml(new_path)
    old_schema = (old_snapshot or {}).get("schema", {}) or {}
    new_schema = (new_snapshot or {}).get("schema", {}) or {}
    all_fields = sorted(set(old_schema.keys()) | set(new_schema.keys()))

    changes = []
    breaking_fields = []
    for f in all_fields:
        row = classify_change(f, old_schema.get(f), new_schema.get(f))
        if row is None:
            continue
        changes.append(row)
        if row["verdict"] == "BREAKING":
            breaking_fields.append(f)

    compatibility_verdict = "BREAKING" if any(c["verdict"] == "BREAKING" for c in changes) else "COMPATIBLE"

    registry = load_registry(Path("contract_registry") / "subscriptions.yaml")
    blast_subscribers = [
        s.get("subscriber_id")
        for s in registry.get("subscriptions", [])
        if s.get("contract_id") == args.contract_id
    ]
    consumer_modes = per_consumer_failure_modes(registry, args.contract_id, breaking_fields)

    checklist = []
    if compatibility_verdict == "BREAKING":
        for bf in breaking_fields[:10]:
            checklist.append(
                f"Migrate field '{bf}': notify subscribers in contract_registry/subscriptions.yaml; "
                f"update generated_contracts/{args.contract_id.replace('-', '_')}.yaml and re-run contracts/runner.py --mode AUDIT."
            )
        checklist.append(
            "Refresh statistical baselines: delete or regenerate schema_snapshots/baselines_*.json after producer fix."
        )
    else:
        checklist.append("No breaking diff: tag release and keep monitoring drift on the next snapshot.")

    rollback_plan = []
    if compatibility_verdict == "BREAKING":
        rollback_plan = [
            "Revert producer deployment to the commit that produced the older snapshot YAML in schema_snapshots/.",
            "Restore prior generated_contracts/*.yaml from git and run: python contracts/runner.py --contract generated_contracts/week3_extractions.yaml --data outputs/week3/extractions.jsonl --output validation_reports/rollback_check.json --mode AUDIT",
            "Ping each subscriber_id listed in migration_impact_report.blast_radius_from_registry with rollback notice.",
        ]

    out = {
        "contract_id": args.contract_id,
        "old_snapshot": str(old_path),
        "new_snapshot": str(new_path),
        "since_filter": args.since,
        "compatibility_verdict": compatibility_verdict,
        "breaking_fields": breaking_fields,
        "changes": changes,
        "migration_impact_report": {
            "diff": changes,
            "compatibility_verdict": compatibility_verdict,
            "blast_radius_from_registry": blast_subscribers,
            "per_consumer_failure_modes": consumer_modes,
            "migration_checklist": checklist,
            "rollback_plan": rollback_plan,
        },
    }

    out_path = Path(args.output)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(out, indent=2), encoding="utf-8")
    print(f"Wrote schema evolution report: {out_path}")


if __name__ == "__main__":
    main()
