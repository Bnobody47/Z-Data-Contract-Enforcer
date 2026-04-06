import json
import os
import subprocess
from pathlib import Path

import streamlit as st
import yaml


ROOT = Path(__file__).resolve().parent.parent


def load_env_file(path: Path):
    if not path.exists():
        return
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        if key and key not in os.environ:
            os.environ[key] = value


def read_json_file(path: Path):
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None


def read_jsonl_file(path: Path):
    if not path.exists():
        return []
    rows = []
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            rows.append(json.loads(line))
        except Exception:
            continue
    return rows


def read_yaml_file(path: Path):
    if not path.exists():
        return None
    try:
        return yaml.safe_load(path.read_text(encoding="utf-8"))
    except Exception:
        return None


def run_demo_script():
    script = ROOT / "scripts" / "demo_week7_pipeline.ps1"
    if not script.exists():
        return 1, "", f"Missing script: {script}"
    cmd = [
        "powershell",
        "-NoProfile",
        "-ExecutionPolicy",
        "Bypass",
        "-File",
        str(script),
    ]
    proc = subprocess.run(cmd, cwd=str(ROOT), capture_output=True, text=True)
    return proc.returncode, proc.stdout, proc.stderr


def main():
    load_env_file(ROOT / ".env")
    st.set_page_config(page_title="Data Contract Enforcer Dashboard", layout="wide")
    st.title("Data Contract Enforcer Dashboard")
    st.caption("View generated artifacts and run the Week 7 demo pipeline.")

    openrouter_key = os.environ.get("OPENROUTER_API_KEY", "").strip()
    openai_key = os.environ.get("OPENAI_API_KEY", "").strip()
    if openrouter_key:
        st.success("OPENROUTER_API_KEY detected from environment/.env")
    elif openai_key:
        st.info("OPENAI_API_KEY detected (OpenRouter key not found)")
    else:
        st.warning("No OpenRouter/OpenAI key found. LLM annotation calls will be skipped.")

    col_a, col_b = st.columns([1, 1])
    with col_a:
        if st.button("Run full demo pipeline (1→6)"):
            with st.spinner("Running scripts/demo_week7_pipeline.ps1 ..."):
                code, stdout, stderr = run_demo_script()
            if code == 0:
                st.success("Pipeline completed.")
            else:
                st.error(f"Pipeline failed with exit code {code}")
            st.subheader("Pipeline output")
            st.code(stdout or "(no stdout)", language="text")
            if stderr:
                st.subheader("Pipeline errors")
                st.code(stderr, language="text")
    with col_b:
        st.subheader("Useful commands")
        st.code(
            "python contracts\\generator.py --source outputs\\week3\\extractions.jsonl "
            "--contract-id week3-document-refinery-extractions --lineage outputs\\week4\\lineage_snapshots.jsonl "
            "--output generated_contracts",
            language="powershell",
        )
        st.code(
            "python contracts\\runner.py --contract generated_contracts\\week3_extractions.yaml "
            "--data outputs\\week3\\extractions_violated_scale_change.jsonl "
            "--output validation_reports\\demo_violation.json --mode AUDIT",
            language="powershell",
        )

    st.divider()
    st.header("Artifact Explorer")

    contract = read_yaml_file(ROOT / "generated_contracts" / "week3_extractions.yaml")
    violation_report = read_json_file(ROOT / "validation_reports" / "demo_violation.json")
    blame_rows = read_jsonl_file(ROOT / "violation_log" / "demo_violations.jsonl")
    schema_report = read_json_file(ROOT / "validation_reports" / "demo_schema.json")
    ai_report = read_json_file(ROOT / "validation_reports" / "demo_ai.json")
    enforcer_report = read_json_file(ROOT / "enforcer_report" / "report_data.json")

    m1, m2, m3, m4 = st.columns(4)
    m1.metric("Contract Loaded", "Yes" if contract else "No")
    m2.metric("Violation Results", len((violation_report or {}).get("results", [])))
    m3.metric("Blame Rows", len(blame_rows))
    m4.metric(
        "Health Score",
        str((enforcer_report or {}).get("data_health_score", "N/A")),
    )

    tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs(
        [
            "Contract",
            "Violation Report",
            "Blame Log",
            "Schema Evolution",
            "AI Extensions",
            "Enforcer Report",
        ]
    )

    with tab1:
        st.subheader("generated_contracts/week3_extractions.yaml")
        if contract:
            schema = contract.get("schema", {})
            st.write(f"Schema clauses: **{len(schema)}**")
            if "fact_confidence" in schema:
                st.json({"fact_confidence": schema.get("fact_confidence")})
            st.json(contract)
        else:
            st.info("Contract file not found yet.")

    with tab2:
        st.subheader("validation_reports/demo_violation.json")
        if violation_report:
            results = violation_report.get("results", [])
            st.write(f"Checks: **{len(results)}**")
            range_fails = [
                r for r in results if "fact_confidence" in str(r.get("check_id", "")) and r.get("status") == "FAIL"
            ]
            st.write(f"fact_confidence FAIL checks: **{len(range_fails)}**")
            if range_fails:
                st.json(range_fails[0])
            st.json(violation_report)
        else:
            st.info("Violation report not found yet.")

    with tab3:
        st.subheader("violation_log/demo_violations.jsonl")
        if blame_rows:
            latest = blame_rows[-1]
            st.write(f"Rows: **{len(blame_rows)}**")
            st.json(
                {
                    "check_id": latest.get("check_id"),
                    "severity": latest.get("severity"),
                    "blame_chain": latest.get("blame_chain"),
                    "blast_radius": latest.get("blast_radius"),
                }
            )
            st.json(latest)
        else:
            st.info("Blame log not found yet.")

    with tab4:
        st.subheader("validation_reports/demo_schema.json")
        if schema_report:
            st.write(f"Compatibility verdict: **{schema_report.get('compatibility_verdict', 'N/A')}**")
            st.json(
                {
                    "compatibility_verdict": schema_report.get("compatibility_verdict"),
                    "changes": schema_report.get("changes"),
                    "migration_impact_report": schema_report.get("migration_impact_report"),
                }
            )
            st.json(schema_report)
        else:
            st.info("Schema evolution report not found yet.")

    with tab5:
        st.subheader("validation_reports/demo_ai.json")
        if ai_report:
            st.json(ai_report)
        else:
            st.info("AI report not found yet.")

    with tab6:
        st.subheader("enforcer_report/report_data.json")
        if enforcer_report:
            top = enforcer_report.get("violations_this_week", [])[:3]
            st.write(f"Top violations shown: **{len(top)}**")
            st.json({"data_health_score": enforcer_report.get("data_health_score"), "top_3": top})
            st.json(enforcer_report)
        else:
            st.info("Enforcer report not found yet.")


if __name__ == "__main__":
    main()
