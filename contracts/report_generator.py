import argparse
import json
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path


def iso_now():
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def load_json(path: Path):
    return json.loads(path.read_text(encoding="utf-8"))


def load_json_optional(path: Path):
    if not path.exists():
        return None
    try:
        return load_json(path)
    except Exception:
        return None


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
            if isinstance(obj, dict) and isinstance(obj.get("results"), list) and obj.get("contract_id"):
                reports.append(obj)
        except Exception:
            continue
    return reports


def violations_by_severity(reports: list) -> dict:
    """Non-PASS clause results grouped by runner status and declared severity (rubric: violations by severity)."""
    c = Counter()
    for rep in reports:
        for r in rep.get("results", []) or []:
            st = r.get("status")
            if st == "PASS":
                continue
            sev = str(r.get("severity", "UNKNOWN")).upper()
            c[f"{st}/{sev}"] += 1
    return {"breakdown": dict(c), "total_non_pass_checks": sum(c.values())}


def compute_health_score_rubric(reports: list):
    """
    Rubric: (checks_passed / total_checks) * 100 minus 20 points per distinct CRITICAL violation (FAIL/ERROR).
    """
    total_checks = 0
    passed_checks = 0
    critical_keys = set()
    for rep in reports:
        cid = rep.get("contract_id")
        for r in rep.get("results", []) or []:
            total_checks += 1
            if r.get("status") == "PASS":
                passed_checks += 1
            if r.get("status") in ("FAIL", "ERROR") and str(r.get("severity", "")).upper() == "CRITICAL":
                critical_keys.add((cid, r.get("check_id")))
    base = (passed_checks / total_checks) * 100.0 if total_checks else 100.0
    deduction = 20 * len(critical_keys)
    score = max(0.0, base - deduction)
    return round(score, 2), passed_checks, total_checks, len(critical_keys)


def contract_yaml_path_for(contract_id: str) -> str:
    m = {
        "week3-document-refinery-extractions": "generated_contracts/week3_extractions.yaml",
        "week5-event-records": "generated_contracts/week5_events.yaml",
    }
    return m.get(contract_id, f"generated_contracts/{contract_id.replace('-', '_')}.yaml")


def best_match_result(validation_reports: list, check_id: str):
    best = None
    for rep in validation_reports:
        for rr in rep.get("results", []) or []:
            if rr.get("check_id") != check_id:
                continue
            if best is None:
                best = (rep, rr)
            else:
                order = {"ERROR": 3, "FAIL": 2, "WARN": 1, "PASS": 0}
                if order.get(rr.get("status"), 0) > order.get(best[1].get("status"), 0):
                    best = (rep, rr)
    return best


def build_violation_narrative(v: dict, validation_reports: list):
    check_id = v.get("check_id", "")
    failing_field = v.get("failing_field", "")
    rep_rr = best_match_result(validation_reports, check_id)
    rep, rr = rep_rr if rep_rr else (None, None)
    contract_id = (rep or {}).get("contract_id") or "unknown-contract"
    failing_system = "Week-3-Document-Refinery" if "week3" in str(contract_id) else (
        "Week-5-Z-Ledger" if "week5" in str(contract_id) else "upstream-producer"
    )
    status = (rr or {}).get("status", v.get("status", "FAIL"))
    severity = (rr or {}).get("severity", v.get("severity"))
    br = v.get("blast_radius") or {}
    subscribers = br.get("registry_subscribers") or br.get("direct_subscribers") or []
    downstream = ", ".join(str(s.get("subscriber_id")) for s in subscribers[:4]) or "see contract_registry/subscriptions.yaml"

    return {
        "violation_id": v.get("violation_id"),
        "check_id": check_id,
        "status": status,
        "severity": severity,
        "failing_system": failing_system,
        "contract_id": contract_id,
        "failing_field": failing_field,
        "downstream_impact": (
            f"Subscribers at risk: {downstream}. "
            f"Breaking meaning at '{failing_field}' can propagate silent bad joins, ordering, or replay bugs."
        ),
        "records_failing": (rr or {}).get("records_failing", v.get("records_failing")),
        "blast_radius": v.get("blast_radius", {}),
        "blame_chain": v.get("blame_chain", []),
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", required=True)
    parser.add_argument(
        "--violations",
        default="violation_log/violations.jsonl",
        help="Path consistent with violation_log/ (data-driven)",
    )
    parser.add_argument(
        "--validation-glob",
        default="validation_reports/*.json",
        help="Glob consistent with validation_reports/ (excludes non-runner JSON by loader filter)",
    )
    parser.add_argument("--schema-week3", default="validation_reports/schema_evolution_week3.json")
    parser.add_argument("--schema-week5", default="validation_reports/schema_evolution_week5.json")
    parser.add_argument("--ai-extensions", default="validation_reports/ai_extensions_violated.json")
    args = parser.parse_args()

    violations_path = Path(args.violations)
    validation_reports = load_validation_reports(args.validation_glob)
    violations = load_violations_jsonl(violations_path)
    score, passed_checks, total_checks, crit_count = compute_health_score_rubric(validation_reports)

    violations_sorted = sorted(
        violations,
        key=lambda v: int(v.get("records_failing", 0) or 0),
        reverse=True,
    )
    top = violations_sorted[:5]
    violations_section = [build_violation_narrative(v, validation_reports) for v in top]

    schema_section = {
        "week3": load_json_optional(Path(args.schema_week3)),
        "week5": load_json_optional(Path(args.schema_week5)),
    }

    ai_path = Path(args.ai_extensions)
    ai_data = load_json_optional(ai_path)
    if ai_data is None:
        ai_data = load_json_optional(Path("validation_reports/ai_extensions_baseline.json"))
    ai_section = {
        "source_file": str(ai_path) if ai_path.exists() else "validation_reports/ai_extensions_baseline.json",
        "embedding_drift": (ai_data or {}).get("embedding_drift"),
        "prompt_input_validation": (ai_data or {}).get("prompt_input_validation"),
        "output_violation_rate": (ai_data or {}).get("output_violation_rate"),
        "summary": (
            "AI risk is driven by embedding centroid drift vs schema_snapshots/embedding_baselines.npz, "
            "prompt JSON Schema validation with quarantine under outputs/quarantine/trace_prompt_quarantine.jsonl, "
            "and Week 2 verdict enum drift vs schema_snapshots/ai_output_violation_baseline.json."
        ),
    }

    def evidence_data_path(contract_id: str, check_id: str, field: str) -> str:
        if "week5" in str(contract_id):
            return "outputs/week5/events_violated_temporal_and_sequence.jsonl"
        if "entity_refs" in str(check_id) or "entity_refs" in str(field):
            return "outputs/week3/extractions_violated_entity_refs.jsonl"
        if "week3" in str(contract_id):
            return "outputs/week3/extractions_violated_scale_change.jsonl"
        return "outputs/week3/extractions.jsonl"

    recs = []
    for pri, nar in enumerate(violations_section[:3], start=1):
        cid = nar.get("contract_id", "week3-document-refinery-extractions")
        cy = contract_yaml_path_for(cid)
        chk = nar.get("check_id", "")
        field = nar.get("failing_field", "")
        data_path = evidence_data_path(cid, chk, field)
        cmd = (
            f"python contracts/runner.py --contract {cy} --data {data_path} "
            f"--output validation_reports/post_fix.json --mode AUDIT"
        )
        recs.append(
            {
                "priority": pri,
                "data_file": data_path,
                "contract_file": cy,
                "field": field,
                "contract_clause_check_id": chk,
                "action": (
                    f"Fix producer output in `{data_path}` so `{field}` satisfies clause `{chk}` in `{cy}`; "
                    f"verify with `{cmd}`."
                ),
                "verify_command": cmd,
            }
        )
    if not recs:
        recs.append(
            {
                "priority": 1,
                "data_file": "outputs/week3/extractions.jsonl",
                "contract_file": "generated_contracts/week3_extractions.yaml",
                "field": "(none — clean run)",
                "contract_clause_check_id": "(n/a)",
                "action": "Maintain baselines: run runner on clean snapshots periodically.",
                "verify_command": "python contracts/runner.py --contract generated_contracts/week3_extractions.yaml --data outputs/week3/extractions.jsonl --output validation_reports/sanity.json --mode AUDIT",
            }
        )

    evidence = [
        {
            "report_id": r.get("report_id"),
            "contract_id": r.get("contract_id"),
            "snapshot_id": r.get("snapshot_id"),
            "run_timestamp": r.get("run_timestamp"),
        }
        for r in validation_reports
    ]

    report = {
        "report_meta": {
            "classification": "auto_generated_enforcer_report",
            "generator_script": "contracts/report_generator.py",
            "inputs_read": [
                str(violations_path),
                args.validation_glob,
                args.schema_week3,
                args.schema_week5,
                str(ai_path) if ai_path.exists() else "validation_reports/ai_extensions_baseline.json",
            ],
        },
        "generated_at": iso_now(),
        "data_health_score": score,
        "health_score_detail": {
            "formula": "(passed/total)*100 - 20*distinct_critical_failures",
            "formula_plain": "checks_passed / total_checks * 100, minus 20 points per distinct CRITICAL failing check_id",
            "passed_checks": passed_checks,
            "total_checks": total_checks,
            "distinct_critical_violations": crit_count,
        },
        "violations_by_severity": violations_by_severity(validation_reports),
        "violations_this_week": violations_section,
        "schema_changes_detected": schema_section,
        "ai_system_risk_assessment": ai_section,
        "recommended_actions_prioritised": recs,
        "recommended_actions": recs,
        "sources": {
            "violation_log": str(violations_path),
            "validation_reports_glob": args.validation_glob,
        },
        "evidence": evidence,
    }

    out_path = Path(args.output)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(report, indent=2), encoding="utf-8")
    print(f"Wrote enforcer report data: {out_path}")


if __name__ == "__main__":
    main()
