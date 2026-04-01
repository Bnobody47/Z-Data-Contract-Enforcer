import argparse
import json
from pathlib import Path
from datetime import datetime

import pandas as pd
import yaml


def load_jsonl(path: Path):
    records = []
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                records.append(json.loads(line))
    return records


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


def infer_type(dtype_str: str):
    mapping = {
        "float64": "number",
        "int64": "integer",
        "bool": "boolean",
        "object": "string",
    }
    return mapping.get(dtype_str, "string")


def profile_column(series: pd.Series, col_name: str):
    non_null = series.dropna()
    safe_non_null = non_null.map(lambda v: json.dumps(v, sort_keys=True) if isinstance(v, (dict, list)) else v)
    profile = {
        "name": col_name,
        "dtype": str(series.dtype),
        "null_fraction": float(series.isna().mean()),
        "cardinality_estimate": int(safe_non_null.nunique()) if len(safe_non_null) else 0,
        "sample_values": [str(v) for v in safe_non_null.unique()[:5]],
    }
    if pd.api.types.is_numeric_dtype(series):
        if len(non_null):
            profile["stats"] = {
                "min": float(non_null.min()),
                "max": float(non_null.max()),
                "mean": float(non_null.mean()),
                "p25": float(non_null.quantile(0.25)),
                "p50": float(non_null.quantile(0.5)),
                "p75": float(non_null.quantile(0.75)),
                "p95": float(non_null.quantile(0.95)),
                "p99": float(non_null.quantile(0.99)),
                "stddev": float(non_null.std() if len(non_null) > 1 else 0.0),
            }
    return profile


def column_to_clause(profile: dict):
    clause = {
        "type": infer_type(profile["dtype"]),
        "required": profile["null_fraction"] == 0.0,
        "description": f"Auto-generated clause for {profile['name']}.",
    }
    name = profile["name"]
    if "confidence" in name and clause["type"] == "number":
        clause["minimum"] = 0.0
        clause["maximum"] = 1.0
        clause["description"] = "Confidence score. Must remain 0.0-1.0 float."
    if name.endswith("_id"):
        clause["format"] = "uuid"
        clause["pattern"] = "^[0-9a-fA-F-]{36}$"
    if name.endswith("_at"):
        clause["format"] = "date-time"
    if (
        clause["type"] == "string"
        and profile["cardinality_estimate"] > 0
        and profile["cardinality_estimate"] <= 10
        and len(profile["sample_values"]) == profile["cardinality_estimate"]
    ):
        clause["enum"] = profile["sample_values"]
    if "stats" in profile:
        clause["stats"] = profile["stats"]
    return clause


def sanitize_contract_filename(contract_id: str):
    return contract_id.replace("-", "_")


def inject_lineage(contract: dict, lineage_path: Path):
    if not lineage_path.exists():
        contract["lineage"] = {"upstream": [], "downstream": []}
        return contract
    with lineage_path.open("r", encoding="utf-8") as f:
        lines = [line.strip() for line in f if line.strip()]
    if not lines:
        contract["lineage"] = {"upstream": [], "downstream": []}
        return contract
    snapshot = json.loads(lines[-1])
    consumers = []
    for e in snapshot.get("edges", []):
        source = str(e.get("source", "")).lower()
        if "week3" in source or "extraction" in source or "week5" in source or "event" in source:
            consumers.append(e.get("target"))
    consumers = [c for c in dict.fromkeys(consumers) if c]
    contract["lineage"] = {
        "upstream": [],
        "downstream": [
            {"id": c, "fields_consumed": ["doc_id", "extracted_facts"]} for c in consumers
        ],
    }
    return contract


def build_contract(contract_id: str, source_path: str, column_profiles: dict):
    schema = {}
    for col, prof in column_profiles.items():
        schema[col] = column_to_clause(prof)
    return {
        "kind": "DataContract",
        "apiVersion": "v3.0.0",
        "id": contract_id,
        "info": {
            "title": f"{contract_id} contract",
            "version": "1.0.0",
            "owner": "week7-team",
            "description": "Auto-generated data contract.",
        },
        "servers": {"local": {"type": "local", "path": source_path, "format": "jsonl"}},
        "schema": schema,
    }


def write_dbt_schema(contract: dict, output_file: Path):
    tests_columns = []
    for name, clause in contract.get("schema", {}).items():
        tests = []
        if clause.get("required"):
            tests.append("not_null")
        if clause.get("enum"):
            tests.append({"accepted_values": {"values": clause["enum"]}})
        tests_columns.append({"name": name, "tests": tests})
    dbt_payload = {
        "version": 2,
        "models": [
            {
                "name": output_file.stem,
                "description": f"dbt tests for {contract.get('id')}",
                "columns": tests_columns,
            }
        ],
    }
    dbt_path = output_file.with_name(f"{output_file.stem}_dbt.yml")
    with dbt_path.open("w", encoding="utf-8") as f:
        yaml.safe_dump(dbt_payload, f, sort_keys=False)


def write_snapshot(contract: dict, contract_id: str):
    snap_dir = Path("schema_snapshots") / contract_id
    snap_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    with (snap_dir / f"{ts}.yaml").open("w", encoding="utf-8") as f:
        yaml.safe_dump(contract, f, sort_keys=False)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", required=True)
    parser.add_argument("--contract-id", required=True)
    parser.add_argument("--lineage", required=False, default="")
    parser.add_argument("--output", required=True)
    args = parser.parse_args()

    source = Path(args.source)
    output_dir = Path(args.output)
    output_dir.mkdir(parents=True, exist_ok=True)

    records = load_jsonl(source)
    if not records:
        raise ValueError(f"No records found in {source}")

    df = flatten_records(records)
    column_profiles = {col: profile_column(df[col], col) for col in df.columns}
    contract = build_contract(args.contract_id, args.source, column_profiles)
    if args.lineage:
        contract = inject_lineage(contract, Path(args.lineage))

    file_name = f"{sanitize_contract_filename(args.contract_id)}.yaml"
    output_file = output_dir / file_name
    with output_file.open("w", encoding="utf-8") as f:
        yaml.safe_dump(contract, f, sort_keys=False)

    write_dbt_schema(contract, output_file)
    write_snapshot(contract, args.contract_id)
    print(f"Wrote contract: {output_file}")


if __name__ == "__main__":
    main()
