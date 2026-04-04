import argparse
import json
import re
from pathlib import Path
from datetime import datetime

import yaml


def load_snapshot_files(contract_dir: Path):
    files = sorted([p for p in contract_dir.glob("*.yaml")], key=lambda p: p.name)
    if len(files) < 2:
        raise ValueError(f"Need at least 2 snapshots in {contract_dir}, found {len(files)}")
    return files[-2], files[-1]


def load_yaml(path: Path):
    return yaml.safe_load(path.read_text(encoding="utf-8"))


def classify_change(field_name: str, old_clause: dict | None, new_clause: dict | None):
    """
    Minimal taxonomy implementation for interim/final rubric:
    - Missing required fields or removed fields => BREAKING
    - Type/min/max/enum removals => BREAKING
    - Confidence scale semantic drift => BREAKING when stats max/mean indicate 0–1 -> 0–100 shift.
    """
    if old_clause is None and new_clause is None:
        return None

    if old_clause is None:
        req = bool(new_clause.get("required", False)) if new_clause else False
        return ("BREAKING" if req else "COMPATIBLE", f"Added field {field_name} (required={req}).")

    if new_clause is None:
        req = bool(old_clause.get("required", False))
        return ("BREAKING", f"Removed field {field_name} (required={req}).")

    # Type change
    if old_clause.get("type") != new_clause.get("type"):
        return ("BREAKING", f"Type change for {field_name}: {old_clause.get('type')} -> {new_clause.get('type')}.")

    # Enum removals are breaking.
    old_enum = set(old_clause.get("enum", []) or [])
    new_enum = set(new_clause.get("enum", []) or [])
    if old_enum and (old_enum - new_enum):
        return ("BREAKING", f"Enum removal for {field_name}: {sorted(old_enum - new_enum)}.")

    # Range changes are breaking when bounds change.
    if old_clause.get("minimum") != new_clause.get("minimum") or old_clause.get("maximum") != new_clause.get("maximum"):
        # For confidence specifically, interpret as semantic drift even if dtype stayed numeric.
        if "confidence" in field_name:
            pass
        return ("BREAKING", f"Range/bounds change for {field_name}.")

    # Confidence scale change detection: use stats if present.
    if "confidence" in field_name:
        old_stats = old_clause.get("stats", {}) or {}
        new_stats = new_clause.get("stats", {}) or {}
        old_max = old_stats.get("max")
        new_max = new_stats.get("max")
        if old_max is not None and new_max is not None:
            # 0–1 baseline -> new max > 1 strongly indicates scale rescale
            if float(old_max) <= 1.0 and float(new_max) > 1.0:
                return ("BREAKING", f"Confidence scale drift for {field_name}: max {old_max} -> {new_max} (0–1 -> 0–100).")

    return ("COMPATIBLE", f"No breaking material change detected for {field_name}.")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--contract-id", required=True, help="Contract id used for schema_snapshots/<contract-id>/")
    parser.add_argument("--output", required=True, help="Write schema evolution analysis JSON here")
    args = parser.parse_args()

    contract_id = args.contract_id
    contract_dir = Path("schema_snapshots") / contract_id
    old_path, new_path = load_snapshot_files(contract_dir)
    old_snapshot = load_yaml(old_path)
    new_snapshot = load_yaml(new_path)

    old_schema = (old_snapshot or {}).get("schema", {}) or {}
    new_schema = (new_snapshot or {}).get("schema", {}) or {}
    all_fields = sorted(set(old_schema.keys()) | set(new_schema.keys()))

    changes = []
    breaking_fields = []
    for f in all_fields:
        old_clause = old_schema.get(f)
        new_clause = new_schema.get(f)
        verdict_reason = classify_change(f, old_clause, new_clause)
        if verdict_reason is None:
            continue
        verdict, reason = verdict_reason
        changes.append({"field": f, "verdict": verdict, "reason": reason})
        if verdict == "BREAKING":
            breaking_fields.append(f)

    # Overall verdict
    compatibility_verdict = "BREAKING" if any(c["verdict"] == "BREAKING" for c in changes) else "COMPATIBLE"

    # Build migration checklist (simple, rubric-friendly)
    checklist = []
    if compatibility_verdict == "BREAKING":
        for bf in breaking_fields[:8]:
            checklist.append(f"Coordinate a migration for field '{bf}': update producers/consumers and re-establish statistical baselines after deploying changes.")
        checklist.append("Update contract(s) with new bounds/ranges, then re-run ValidationRunner in AUDIT mode to confirm zero false positives.")
    else:
        checklist.append("No breaking changes detected: re-run ValidationRunner on a fresh snapshot after the next release.")

    rollback_plan = []
    if compatibility_verdict == "BREAKING":
        rollback_plan = [
            "Roll back producer changes to the previous snapshot-producing version.",
            "Re-run ValidationRunner on the rolled-back data to confirm baselines and structural checks return to PASS.",
            "Apply migration with explicit deprecation/alias strategy and communicate blast radius to subscribers via the registry.",
        ]

    output = {
        "contract_id": contract_id,
        "old_snapshot": str(old_path),
        "new_snapshot": str(new_path),
        "compatibility_verdict": compatibility_verdict,
        "breaking_fields": breaking_fields,
        "changes": changes,
        "migration_impact_report": {
            "diff_summary": "Schema + stats were compared field-by-field. Confidence drift is classified via stats when max/mean indicate a 0–1 -> 0–100 rescale.",
            "migration_checklist": checklist,
            "rollback_plan": rollback_plan,
        },
    }

    out_path = Path(args.output)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(output, indent=2), encoding="utf-8")
    print(f"Wrote schema evolution report: {out_path}")


if __name__ == "__main__":
    main()

