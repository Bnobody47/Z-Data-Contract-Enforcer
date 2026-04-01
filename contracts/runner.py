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
        if "token_count" in r and isinstance(r["token_count"], dict):
            tc = r["token_count"]
            base["token_input"] = tc.get("input")
            base["token_output"] = tc.get("output")
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


def parse_iso(value: str):
    if value is None:
        return None
    s = str(value).replace("Z", "+00:00")
    try:
        return datetime.fromisoformat(s)
    except Exception:
        return None


def get_nested(obj, path: str):
    cur = obj
    for part in path.split("."):
        if cur is None or not isinstance(cur, dict):
            return None
        cur = cur.get(part)
    return cur


def evaluate_constraints(contract: dict, records: list, results: list):
    for c in contract.get("constraints", []) or []:
        rule = c.get("rule")
        sev = c.get("severity", "CRITICAL")
        cid_check = c.get("id", "constraint")

        if rule == "array_min_length":
            path = c.get("path", "")
            minimum = int(c.get("minimum", 1))
            bad = 0
            samples = []
            for rec in records:
                arr = rec.get(path) if path else None
                if not isinstance(arr, list) or len(arr) < minimum:
                    bad += 1
                    if len(samples) < 3:
                        samples.append(rec.get("doc_id") or rec.get("event_id") or str(rec)[:80])
            results.append(
                {
                    "check_id": cid_check,
                    "column_name": path,
                    "check_type": "array_min_length",
                    "status": "FAIL" if bad else "PASS",
                    "actual_value": f"records_failing={bad}",
                    "expected": f"len({path})>={minimum}",
                    "severity": sev if bad else "LOW",
                    "records_failing": bad,
                    "sample_failing": samples,
                    "message": c.get("description", ""),
                }
            )

        elif rule == "entity_refs_in_entities":
            bad = 0
            samples = []
            for rec in records:
                entities = {e.get("entity_id") for e in rec.get("entities", []) if isinstance(e, dict)}
                ok = True
                for fact in rec.get("extracted_facts", []) or []:
                    if not isinstance(fact, dict):
                        continue
                    for ref in fact.get("entity_refs", []) or []:
                        if ref not in entities:
                            ok = False
                            break
                    if not ok:
                        break
                if not ok:
                    bad += 1
                    if len(samples) < 3:
                        samples.append(rec.get("doc_id", ""))
            results.append(
                {
                    "check_id": cid_check,
                    "column_name": "extracted_facts.entity_refs",
                    "check_type": "relationship",
                    "status": "FAIL" if bad else "PASS",
                    "actual_value": f"records_failing={bad}",
                    "expected": "all entity_refs exist in entities[].entity_id",
                    "severity": sev if bad else "LOW",
                    "records_failing": bad,
                    "sample_failing": samples,
                    "message": c.get("description", ""),
                }
            )

        elif rule == "entity_type_enum":
            allowed = set(c.get("allowed", []))
            bad = 0
            samples = []
            for rec in records:
                for ent in rec.get("entities", []) or []:
                    if not isinstance(ent, dict):
                        continue
                    t = ent.get("type")
                    if t not in allowed:
                        bad += 1
                        if len(samples) < 3:
                            samples.append(str(t))
            results.append(
                {
                    "check_id": cid_check,
                    "column_name": "entities.type",
                    "check_type": "enum",
                    "status": "FAIL" if bad else "PASS",
                    "actual_value": f"invalid_entity_rows={bad}",
                    "expected": str(sorted(allowed)),
                    "severity": sev if bad else "LOW",
                    "records_failing": bad,
                    "sample_failing": samples,
                    "message": c.get("description", ""),
                }
            )

        elif rule == "timestamp_order":
            early_k = c.get("early", "")
            late_k = c.get("late", "")
            bad = 0
            samples = []
            for rec in records:
                e = parse_iso(rec.get(early_k))
                l = parse_iso(rec.get(late_k))
                if e is None or l is None:
                    bad += 1
                    continue
                if l < e:
                    bad += 1
                    if len(samples) < 3:
                        samples.append(rec.get("event_id", ""))
            results.append(
                {
                    "check_id": cid_check,
                    "column_name": f"{late_k}>={early_k}",
                    "check_type": "temporal_order",
                    "status": "FAIL" if bad else "PASS",
                    "actual_value": f"records_failing={bad}",
                    "expected": f"{late_k} >= {early_k}",
                    "severity": sev if bad else "LOW",
                    "records_failing": bad,
                    "sample_failing": samples,
                    "message": c.get("description", ""),
                }
            )

        elif rule == "monotonic_sequence":
            group_field = c.get("group_field", "aggregate_id")
            seq_field = c.get("sequence_field", "sequence_number")
            strict = c.get("strict", True)
            bad_groups = 0
            samples = []
            by_group = {}
            for rec in records:
                g = rec.get(group_field)
                by_group.setdefault(g, []).append(rec)
            for g, rows in by_group.items():
                rows_sorted = sorted(rows, key=lambda r: (r.get(seq_field) is None, r.get(seq_field)))
                seqs = [r.get(seq_field) for r in rows_sorted]
                if any(s is None for s in seqs):
                    bad_groups += 1
                    if len(samples) < 3:
                        samples.append(str(g))
                    continue
                prev = None
                ok = True
                for s in seqs:
                    if not isinstance(s, int):
                        ok = False
                        break
                    if prev is None:
                        prev = s
                        continue
                    if strict:
                        if s != prev + 1:
                            ok = False
                            break
                    else:
                        if s <= prev:
                            ok = False
                            break
                    prev = s
                if not ok:
                    bad_groups += 1
                    if len(samples) < 3:
                        samples.append(str(g))
            results.append(
                {
                    "check_id": cid_check,
                    "column_name": f"{group_field}.{seq_field}",
                    "check_type": "sequence_monotonic",
                    "status": "FAIL" if bad_groups else "PASS",
                    "actual_value": f"invalid_aggregate_groups={bad_groups}",
                    "expected": "strict +1 sequence per aggregate" if strict else "monotonic sequence",
                    "severity": sev if bad_groups else "LOW",
                    "records_failing": bad_groups,
                    "sample_failing": samples,
                    "message": c.get("description", ""),
                }
            )

        elif rule == "metadata_string":
            path = c.get("path", "")
            pat = c.get("pattern", "")
            rx = re.compile(pat)
            bad = 0
            samples = []
            for rec in records:
                val = get_nested(rec, path)
                if val is None:
                    continue
                if not rx.match(str(val)):
                    bad += 1
                    if len(samples) < 3:
                        samples.append(str(val)[:120])
            results.append(
                {
                    "check_id": cid_check,
                    "column_name": path,
                    "check_type": "pattern",
                    "status": "FAIL" if bad else "PASS",
                    "actual_value": f"records_failing={bad}",
                    "expected": pat,
                    "severity": sev if bad else "LOW",
                    "records_failing": bad,
                    "sample_failing": samples,
                    "message": c.get("description", ""),
                }
            )


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

    evaluate_constraints(contract, records, results)

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
                    "message": f"{column} missing from flattened dataset",
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
        if expected_type == "number":
            if not pd.api.types.is_numeric_dtype(series):
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
                        "expected": "numeric",
                        "severity": "LOW",
                        "records_failing": 0,
                        "sample_failing": [],
                        "message": "Numeric type check passed.",
                    }
                )
        elif expected_type == "integer":
            int_ok = pd.api.types.is_integer_dtype(series) or (
                pd.api.types.is_float_dtype(series) and (series.dropna() == series.dropna().astype(int)).all()
            )
            if not int_ok:
                results.append(
                    {
                        "check_id": f"{contract['id']}.{column}.type",
                        "column_name": column,
                        "check_type": "type",
                        "status": "FAIL",
                        "actual_value": str(series.dtype),
                        "expected": "integer",
                        "severity": "CRITICAL",
                        "records_failing": len(series),
                        "sample_failing": [],
                        "message": "Type mismatch for integer field.",
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
                        "expected": "integer",
                        "severity": "LOW",
                        "records_failing": 0,
                        "sample_failing": [],
                        "message": "Integer type check passed.",
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
                    "message": "Type check passed (string/boolean).",
                }
            )

        if "enum" in clause:
            bad = series.dropna()[~series.dropna().astype(str).isin([str(v) for v in clause["enum"]])]
            status = "FAIL" if len(bad) else "PASS"
            results.append(
                {
                    "check_id": f"{contract['id']}.{column}.enum",
                    "column_name": column,
                    "check_type": "enum",
                    "status": status,
                    "actual_value": f"invalid_count={len(bad)}" if len(bad) else "all valid",
                    "expected": f"in {clause['enum']}",
                    "severity": "CRITICAL" if status == "FAIL" else "LOW",
                    "records_failing": int(len(bad)),
                    "sample_failing": [str(v) for v in bad.head(3).tolist()],
                    "message": "Enum conformance check.",
                }
            )

        if clause.get("unique"):
            dup = series.duplicated(keep=False) & series.notna()
            status = "FAIL" if dup.any() else "PASS"
            results.append(
                {
                    "check_id": f"{contract['id']}.{column}.unique",
                    "column_name": column,
                    "check_type": "unique",
                    "status": status,
                    "actual_value": f"duplicate_rows={int(dup.sum())}",
                    "expected": "unique values",
                    "severity": "CRITICAL" if status == "FAIL" else "LOW",
                    "records_failing": int(dup.sum()),
                    "sample_failing": series[dup].head(3).astype(str).tolist(),
                    "message": "Uniqueness constraint.",
                }
            )

        if clause.get("minLength") is not None:
            m = int(clause["minLength"])
            bad = series.dropna().astype(str).str.len() < m
            status = "FAIL" if bad.any() else "PASS"
            results.append(
                {
                    "check_id": f"{contract['id']}.{column}.min_length",
                    "column_name": column,
                    "check_type": "min_length",
                    "status": status,
                    "actual_value": f"short_count={int(bad.sum())}",
                    "expected": f"len>={m}",
                    "severity": "CRITICAL" if status == "FAIL" else "LOW",
                    "records_failing": int(bad.sum()),
                    "sample_failing": series[bad].head(3).astype(str).tolist(),
                    "message": "Minimum string length.",
                }
            )

        if clause.get("pattern"):
            rx = re.compile(clause["pattern"])
            non_null = series.dropna().astype(str)
            bad_mask = ~non_null.apply(lambda s: bool(rx.match(s)))
            status = "FAIL" if bad_mask.any() else "PASS"
            results.append(
                {
                    "check_id": f"{contract['id']}.{column}.pattern",
                    "column_name": column,
                    "check_type": "regex",
                    "status": status,
                    "actual_value": f"invalid_count={int(bad_mask.sum())}",
                    "expected": clause["pattern"],
                    "severity": "CRITICAL" if status == "FAIL" else "LOW",
                    "records_failing": int(bad_mask.sum()),
                    "sample_failing": non_null[bad_mask].head(3).tolist(),
                    "message": "Regex pattern check.",
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
                if parse_iso(value) is None:
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
                lo = float(clause["minimum"]) if "minimum" in clause else None
                hi = float(clause["maximum"]) if "maximum" in clause else None
                min_ok = lo is None or min_v >= lo
                max_ok = hi is None or max_v <= hi
                below = (numeric < lo) if lo is not None else pd.Series([False] * len(numeric), index=numeric.index)
                above = (numeric > hi) if hi is not None else pd.Series([False] * len(numeric), index=numeric.index)
                failing = int((below | above).sum())
                ok = min_ok and max_ok
                results.append(
                    {
                        "check_id": f"{contract['id']}.{column}.range",
                        "column_name": column,
                        "check_type": "range",
                        "status": "PASS" if ok else "FAIL",
                        "actual_value": f"min={min_v}, max={max_v}",
                        "expected": f"min>={lo}, max<={hi}",
                        "severity": "CRITICAL" if not ok else "LOW",
                        "records_failing": failing,
                        "sample_failing": [],
                        "message": "Range check (catches 0–1 vs 0–100 semantic drift when bounds apply).",
                    }
                )

    contract_id = contract.get("id", "default")
    safe_id = re.sub(r"[^a-zA-Z0-9_-]+", "_", contract_id)
    baseline_file = Path("schema_snapshots") / f"baselines_{safe_id}.json"
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
            columns[col] = {
                "mean": float(df[col].mean()),
                "stddev": float(df[col].std() if len(df[col].dropna()) > 1 else 0.0),
            }
        with baseline_file.open("w", encoding="utf-8") as f:
            json.dump(
                {"contract_id": contract_id, "written_at": datetime.utcnow().isoformat(), "columns": columns},
                f,
                indent=2,
            )

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
