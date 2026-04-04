import argparse
import json
import re
from datetime import datetime, timezone
from pathlib import Path


def iso_now():
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def load_json(path: Path):
    return json.loads(path.read_text(encoding="utf-8"))


def load_violations_jsonl(path: Path):
    violations = []
    if not path.exists():
        return violations
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            violations.append(json.loads(line))
    return violations


def load_validation_reports(pattern: str = "validation_reports/*.json"):
    reports = []
    for p in Path(".").glob(pattern):
        try:
            obj = load_json(p)
            # Only include ValidationRunner artifacts (they have `results` as a list of check rows).
            if isinstance(obj, dict) and isinstance(obj.get("results"), list) and obj.get("contract_id"):
                reports.append(obj)
        except Exception:
            continue
    return reports


def severity_weight(status: str):
    if status == "ERROR":
        return 25
    if status == "FAIL":
        return 20
    if status == "WARN":
        return 8
    return 0


def compute_health_score(validation_reports: list):
    # Start from 100 and deduct for each FAIL/ERROR result.
    score = 100.0
    evidence = []
    for rep in validation_reports:
        rid = rep.get("report_id")
        contract_id = rep.get("contract_id")
        snap = rep.get("snapshot_id")
        run_ts = rep.get("run_timestamp")
        evidence.append({"report_id": rid, "contract_id": contract_id, "snapshot_id": snap, "run_timestamp": run_ts})

        for r in rep.get("results", []) or []:
            w = severity_weight(r.get("status"))
            score -= w
    return max(0, round(score, 2)), evidence


def build_plain_language(violation_entry: dict, validation_reports: list):
    check_id = violation_entry.get("check_id", "")
    failing_field = violation_entry.get("failing_field", "")

    # Find matching validation check row if present.
    matched = None
    for rep in validation_reports:
        for rr in rep.get("results", []) or []:
            if rr.get("check_id") == check_id:
                # Prefer the most severe outcome if multiple snapshots share the same check_id.
                if matched is None:
                    matched = rr
                else:
                    order = {"ERROR": 3, "FAIL": 2, "WARN": 1, "PASS": 0}
                    if order.get(rr.get("status"), 0) > order.get(matched.get("status"), 0):
                        matched = rr

    status = matched.get("status") if matched else "FAIL"
    records_failing = matched.get("records_failing") if matched else violation_entry.get("records_failing")
    severity = matched.get("severity") if matched else None
    return {
        "violation_id": violation_entry.get("violation_id"),
        "check_id": check_id,
        "status": status,
        "severity": severity,
        "failing_field": failing_field,
        "records_failing": records_failing,
        "explanation": f"{check_id} failed ({status}). This indicates a contract-breaking meaning/structure change at '{failing_field}' that downstream consumers in the registry blast radius should treat as a risk.",
        "blast_radius": violation_entry.get("blast_radius", {}),
        "blame_chain": violation_entry.get("blame_chain", []),
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", required=True, help="enforcer_report/report_data.json")
    args = parser.parse_args()

    validation_reports = load_validation_reports()
    violations_path = Path("violation_log/violations.jsonl")
    violations = load_violations_jsonl(violations_path)
    score, evidence = compute_health_score(validation_reports)

    # Take top-3 violations by failing records, then stable order.
    violations_sorted = sorted(
        violations,
        key=lambda v: int(v.get("records_failing", 0) or 0),
        reverse=True,
    )
    top = violations_sorted[:3]

    top_plain = [build_plain_language(v, validation_reports) for v in top]

    # Recommendations: short and actionable, derived from blast_radius+check_ids.
    recs = []
    for v in top_plain:
        fc = v.get("failing_field") or "unknown_field"
        recs.append(f"Mitigate '{fc}': update producers/consumers and re-run ValidationRunner in AUDIT mode; then promote to ENFORCE once baselines and expectations are updated.")

    out_path = Path(args.output)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(
        json.dumps(
            {
                "generated_at": iso_now(),
                "data_health_score": score,
                "validated_contracts": sorted(set(r.get("contract_id") for r in validation_reports if r.get("contract_id"))),
                "evidence": evidence,
                "top_violations": top_plain,
                "recommendations": recs[:3],
                "violation_log_source": str(violations_path),
            },
            indent=2,
        ),
        encoding="utf-8",
    )

    print(f"Wrote enforcer report data: {out_path}")


if __name__ == "__main__":
    main()

