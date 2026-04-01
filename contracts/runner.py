import argparse
import hashlib
import json
import re
import uuid
from datetime import datetime
from pathlib import Path

import pandas as pd
import yaml


UUID_RE = re.compile(r"^[0-9a-fA-F-]{36}$")


def load_jsonl(path: Path):
    records = []
    raw_lines = []
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            raw_lines.append(line)
            line = line.strip()
            if line:
                records.append(json.loads(line))
    return records, "".join(raw_lines)


def flatten_records(records):
    rows = []
    for r in records:
        base = {k: v for k, v in r.items() if not isinstance(v, (list, dict))}
        if "extracted_facts" in r and isinstance(r["extracted_facts"], list):
            for fact in r["extracted_facts"] or [{}]:
                row = dict(base)
                if isinstance(fact, dict):
                    for k, v in fact.items():
                        row[f"fact_{k}"] = v
                rows.append(row)
        else:
            rows.append(base)
    return pd.DataFrame(rows)


def status_counters(results):
    passed = sum(1 for r in results if r["status"] == "PASS")
    failed = sum(1 for r in results if r["status"] == "FAIL")
    warned = sum(1 for r in results if r["status"] == "WARN")
    errored = sum(1 for r in results if r["status"] == "ERROR")
    return passed, failed, warned, errored


def check_statistical_drift(column, current_mean, baselines):
    if column not in baselines:
        return None
    b = baselines[column]
    z = abs(current_mean - b["mean"]) / max(b["stddev"], 1e-9)
    if z > 3:
        return "FAIL", round(z, 2), f"{column} mean drifted {z:.1f} stddev from baseline"
    if z > 2:
        return "WARN", round(z, 2), f"{column} mean within warning range ({z:.1f} stddev)"
    return "PASS", round(z, 2), f"{column} drift stable"


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--contract", required=True)
    parser.add_argument("--data", required=True)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()

    contract = yaml.safe_load(Path(args.contract).read_text(encoding="utf-8"))
    records, raw_data = load_jsonl(Path(args.data))
    df = flatten_records(records)

    results = []
    schema = contract.get("schema", {})
    for column, clause in schema.items():
        if column not in df.columns:
            results.append(
                {
                    "check_id": f"{contract['id']}.{column}.exists",
                    "column_name": column,
                    "check_type": "existence",
                    "status": "ERROR",
                    "actual_value": "missing column",
                    "expected": "column exists",
                    "severity": "CRITICAL",
                    "records_failing": len(df),
                    "sample_failing": [],
                    "message": f"{column} missing from dataset",
                }
            )
            continue

        series = df[column]
        if clause.get("required") and series.isna().any():
            results.append(
                {
                    "check_id": f"{contract['id']}.{column}.required",
                    "column_name": column,
                    "check_type": "required",
                    "status": "FAIL",
                    "actual_value": f"nulls={int(series.isna().sum())}",
                    "expected": "no nulls",
                    "severity": "CRITICAL",
                    "records_failing": int(series.isna().sum()),
                    "sample_failing": [],
                    "message": "Required field contains null values.",
                }
            )
        else:
            results.append(
                {
                    "check_id": f"{contract['id']}.{column}.required",
                    "column_name": column,
                    "check_type": "required",
                    "status": "PASS",
                    "actual_value": "ok",
                    "expected": "no nulls when required",
                    "severity": "LOW",
                    "records_failing": 0,
                    "sample_failing": [],
                    "message": "Required check passed.",
                }
            )

        expected_type = clause.get("type")
        if expected_type == "number" and not pd.api.types.is_numeric_dtype(series):
            results.append(
                {
                    "check_id": f"{contract['id']}.{column}.type",
                    "column_name": column,
                    "check_type": "type",
                    "status": "FAIL",
                    "actual_value": str(series.dtype),
                    "expected": "numeric",
                    "severity": "CRITICAL",
                    "records_failing": len(series),
                    "sample_failing": [str(v) for v in series.dropna().head(3).tolist()],
                    "message": "Type mismatch for numeric field.",
                }
            )
        else:
            results.append(
                {
                    "check_id": f"{contract['id']}.{column}.type",
                    "column_name": column,
                    "check_type": "type",
                    "status": "PASS",
                    "actual_value": str(series.dtype),
                    "expected": expected_type,
                    "severity": "LOW",
                    "records_failing": 0,
                    "sample_failing": [],
                    "message": "Type check passed.",
                }
            )

        if "enum" in clause:
            bad = series.dropna()[~series.dropna().astype(str).isin([str(v) for v in clause["enum"]])]
            if len(bad):
                results.append(
                    {
                        "check_id": f"{contract['id']}.{column}.enum",
                        "column_name": column,
                        "check_type": "enum",
                        "status": "FAIL",
                        "actual_value": f"invalid_count={len(bad)}",
                        "expected": f"in {clause['enum']}",
                        "severity": "CRITICAL",
                        "records_failing": int(len(bad)),
                        "sample_failing": [str(v) for v in bad.head(3).tolist()],
                        "message": "Enum conformance check failed.",
                    }
                )

        if clause.get("format") == "uuid":
            non_null = series.dropna().astype(str)
            bad = non_null[~non_null.str.match(UUID_RE)]
            status = "PASS" if len(bad) == 0 else "FAIL"
            results.append(
                {
                    "check_id": f"{contract['id']}.{column}.uuid",
                    "column_name": column,
                    "check_type": "uuid",
                    "status": status,
                    "actual_value": f"invalid_count={len(bad)}",
                    "expected": "uuid pattern",
                    "severity": "CRITICAL" if status == "FAIL" else "LOW",
                    "records_failing": int(len(bad)),
                    "sample_failing": [str(v) for v in bad.head(3).tolist()],
                    "message": "UUID pattern check.",
                }
            )

        if clause.get("format") == "date-time":
            bad_vals = []
            for value in series.dropna().astype(str):
                try:
                    datetime.fromisoformat(value.replace("Z", "+00:00"))
                except Exception:
                    bad_vals.append(value)
            status = "PASS" if not bad_vals else "FAIL"
            results.append(
                {
                    "check_id": f"{contract['id']}.{column}.datetime",
                    "column_name": column,
                    "check_type": "date-time",
                    "status": status,
                    "actual_value": f"invalid_count={len(bad_vals)}",
                    "expected": "ISO 8601 parseable",
                    "severity": "CRITICAL" if status == "FAIL" else "LOW",
                    "records_failing": len(bad_vals),
                    "sample_failing": bad_vals[:3],
                    "message": "Date-time parse check.",
                }
            )

        if "minimum" in clause or "maximum" in clause:
            numeric = pd.to_numeric(series, errors="coerce").dropna()
            if len(numeric):
                min_v = float(numeric.min())
                max_v = float(numeric.max())
                min_ok = "minimum" not in clause or min_v >= float(clause["minimum"])
                max_ok = "maximum" not in clause or max_v <= float(clause["maximum"])
                ok = min_ok and max_ok
                results.append(
                    {
                        "check_id": f"{contract['id']}.{column}.range",
                        "column_name": column,
                        "check_type": "range",
                        "status": "PASS" if ok else "FAIL",
                        "actual_value": f"min={min_v}, max={max_v}",
                        "expected": f"min>={clause.get('minimum')}, max<={clause.get('maximum')}",
                        "severity": "CRITICAL" if not ok else "LOW",
                        "records_failing": int((numeric < clause.get("minimum", min_v)).sum() + (numeric > clause.get("maximum", max_v)).sum()),
                        "sample_failing": [],
                        "message": "Range check.",
                    }
                )

    baseline_file = Path("schema_snapshots/baselines.json")
    baselines = {}
    if baseline_file.exists():
        with baseline_file.open("r", encoding="utf-8") as f:
            baselines = json.load(f).get("columns", {})
    for col in df.select_dtypes(include="number").columns:
        mean_v = float(df[col].mean())
        drift = check_statistical_drift(col, mean_v, baselines)
        if drift:
            status, z, msg = drift
            results.append(
                {
                    "check_id": f"{contract['id']}.{col}.drift",
                    "column_name": col,
                    "check_type": "statistical_drift",
                    "status": status,
                    "actual_value": f"z={z}",
                    "expected": "z<=2 warn threshold, z<=3 fail threshold",
                    "severity": "HIGH" if status == "FAIL" else ("MEDIUM" if status == "WARN" else "LOW"),
                    "records_failing": 0,
                    "sample_failing": [],
                    "message": msg,
                }
            )

    if not baseline_file.exists():
        baseline_file.parent.mkdir(parents=True, exist_ok=True)
        columns = {}
        for col in df.select_dtypes(include="number").columns:
            columns[col] = {"mean": float(df[col].mean()), "stddev": float(df[col].std() if len(df[col].dropna()) > 1 else 0.0)}
        with baseline_file.open("w", encoding="utf-8") as f:
            json.dump({"written_at": datetime.utcnow().isoformat(), "columns": columns}, f, indent=2)

    passed, failed, warned, errored = status_counters(results)
    report = {
        "report_id": str(uuid.uuid4()),
        "contract_id": contract["id"],
        "snapshot_id": hashlib.sha256(raw_data.encode("utf-8")).hexdigest(),
        "run_timestamp": datetime.utcnow().isoformat() + "Z",
        "total_checks": len(results),
        "passed": passed,
        "failed": failed,
        "warned": warned,
        "errored": errored,
        "results": results,
    }

    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    with output.open("w", encoding="utf-8") as f:
        json.dump(report, f, indent=2)
    print(f"Wrote report: {output}")


if __name__ == "__main__":
    main()
