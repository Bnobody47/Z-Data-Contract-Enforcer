import argparse
import json
import os
import re
import urllib.error
import urllib.request
from pathlib import Path
from datetime import datetime, timezone

import pandas as pd
import yaml


def load_env_file(path: Path):
    """Minimal .env loader (no external dependency)."""
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
        "description": f"Profiled field {profile['name']}.",
    }
    name = profile["name"]
    if "confidence" in name and clause["type"] == "number":
        clause["minimum"] = 0.0
        clause["maximum"] = 1.0
        clause["description"] = (
            "extracted_facts[].confidence: model confidence 0.0–1.0. "
            "BREAKING if rescaled to 0–100 or integer percent."
        )
        stats = profile.get("stats") or {}
        mean_v = stats.get("mean")
        if mean_v is not None and (float(mean_v) > 0.99 or float(mean_v) < 0.01):
            clause["distribution_warning"] = (
                f"Suspicious profiled mean={mean_v:.4f} on [0,1] scale — verify calibration; "
                "values clustered near 0 or 1 often indicate scale misuse or degenerate model output."
            )
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


def detect_kind(contract_id: str, source: Path) -> str:
    cid = contract_id.lower()
    sp = str(source).lower()
    if "week3" in cid or "refinery" in cid or "extractions" in sp:
        return "week3_extractions"
    if "week5" in cid or "event" in cid or "events" in sp:
        return "week5_events"
    return "generic"


def inject_lineage(contract: dict, lineage_path: Path, contract_id: str, kind: str):
    """Populate downstream consumers by querying Week 4 lineage edges (file → pipeline CONSUMES)."""
    empty = {"upstream": [], "downstream": [], "downstream_from_graph": []}
    if not lineage_path.exists():
        contract.setdefault("lineage", {}).update(empty)
        return contract
    with lineage_path.open("r", encoding="utf-8") as f:
        lines = [line.strip() for line in f if line.strip()]
    if not lines:
        contract.setdefault("lineage", {}).update(empty)
        return contract
    snapshot = json.loads(lines[-1])
    nodes = {n.get("node_id"): n for n in snapshot.get("nodes", []) if n.get("node_id")}
    edges = snapshot.get("edges", []) or []

    producer_substr = ""
    fields_default = ["doc_id", "extracted_facts"]
    if kind == "week3_extractions" or "week3" in contract_id.lower():
        producer_substr = "week3"
        fields_default = ["doc_id", "extracted_facts", "entities", "source_hash"]
    elif kind == "week5_events" or "week5" in contract_id.lower():
        producer_substr = "week5"
        fields_default = ["event_id", "aggregate_id", "sequence_number", "occurred_at", "recorded_at", "event_type"]

    producer_node_ids = [
        nid
        for nid, n in nodes.items()
        if n.get("type") == "FILE" and producer_substr and producer_substr in str(nid).lower()
    ]

    downstream_from_graph = []
    for pid in producer_node_ids:
        for e in edges:
            if e.get("source") != pid:
                continue
            if str(e.get("relationship", "")).upper() != "CONSUMES":
                continue
            tgt = e.get("target")
            tn = nodes.get(tgt, {}) or {}
            md = tn.get("metadata") or {}
            downstream_from_graph.append(
                {
                    "producer_node_id": pid,
                    "consumer_node_id": tgt,
                    "consumer_label": tn.get("label"),
                    "consumer_path": md.get("path"),
                    "relationship": e.get("relationship"),
                }
            )

    downstream = [
        {
            "id": row["consumer_node_id"],
            "fields_consumed": list(fields_default),
            "source": "week4_lineage_snapshot",
        }
        for row in downstream_from_graph
    ]

    contract.setdefault("lineage", {})
    contract["lineage"]["upstream"] = contract["lineage"].get("upstream", [])
    contract["lineage"]["downstream"] = downstream
    contract["lineage"]["downstream_from_graph"] = downstream_from_graph
    contract["lineage"]["lineage_snapshot_id"] = snapshot.get("snapshot_id")
    return contract


def safe_contract_id_for_path(contract_id: str) -> str:
    return re.sub(r"[^a-zA-Z0-9_-]+", "_", contract_id)


def write_statistical_baselines(contract_id: str, df: pd.DataFrame):
    """Persist mean/stddev per numeric column (same shape as ValidationRunner baselines)."""
    safe_id = safe_contract_id_for_path(contract_id)
    out = Path("schema_snapshots") / f"baselines_{safe_id}.json"
    out.parent.mkdir(parents=True, exist_ok=True)
    columns = {}
    for col in df.select_dtypes(include="number").columns:
        columns[col] = {
            "mean": float(df[col].mean()),
            "stddev": float(df[col].std() if len(df[col].dropna()) > 1 else 0.0),
        }
    payload = {
        "contract_id": contract_id,
        "written_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "columns": columns,
        "source": "ContractGenerator.profiling",
    }
    out.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    # Aggregate file resembling schema_snapshots/baselines.json (multi-contract)
    agg_path = Path("schema_snapshots") / "baselines.json"
    agg = {}
    if agg_path.exists():
        try:
            agg = json.loads(agg_path.read_text(encoding="utf-8"))
        except Exception:
            agg = {}
    if not isinstance(agg, dict):
        agg = {}
    agg.setdefault("contracts", {})
    agg["contracts"][contract_id] = {"columns": columns, "written_at": payload["written_at"]}
    agg["written_at"] = payload["written_at"]
    agg_path.write_text(json.dumps(agg, indent=2), encoding="utf-8")


def llm_annotate_ambiguous_columns(column_profiles: dict) -> list:
    """
    Optional LLM call for high-cardinality string columns (ambiguous semantics).
    Supports OPENROUTER_API_KEY (preferred) or OPENAI_API_KEY.
    If neither is set, records a skipped call.
    """
    ambiguous = []
    for name, prof in column_profiles.items():
        if infer_type(prof["dtype"]) != "string":
            continue
        if int(prof.get("cardinality_estimate") or 0) <= 10:
            continue
        samples = prof.get("sample_values") or []
        ambiguous.append((name, samples[:8]))

    annotations = []
    openrouter_key = os.environ.get("OPENROUTER_API_KEY", "").strip()
    openai_key = os.environ.get("OPENAI_API_KEY", "").strip()

    if openrouter_key:
        provider = "openrouter"
        api_key = openrouter_key
        model = os.environ.get("OPENROUTER_MODEL", "openai/gpt-4o-mini").strip()
        api_url = "https://openrouter.ai/api/v1/chat/completions"
    else:
        provider = "openai"
        api_key = openai_key
        model = os.environ.get("OPENAI_MODEL", "gpt-4o-mini").strip()
        api_url = "https://api.openai.com/v1/chat/completions"

    for col_name, samples in ambiguous[:12]:
        if not api_key:
            annotations.append(
                {
                    "column": col_name,
                    "llm_annotation": None,
                    "llm_call_status": "skipped_no_api_key",
                    "note": "Set OPENROUTER_API_KEY (or OPENAI_API_KEY) to enable live LLM column semantics annotation.",
                }
            )
            continue
        body = json.dumps(
            {
                "model": model,
                "messages": [
                    {
                        "role": "user",
                        "content": (
                            f"Data contract profiling: column `{col_name}` in a JSONL dataset. "
                            f"Sample string values: {json.dumps(samples)}. "
                            "In one sentence, suggest likely semantic meaning and whether it should be enum, free text, or id. "
                            "Reply as JSON: {{\"summary\": \"...\", \"suggested_contract_type\": \"...\"}}"
                        ),
                    }
                ],
                "temperature": 0.2,
            }
        ).encode("utf-8")
        req = urllib.request.Request(
            api_url,
            data=body,
            headers={"Content-Type": "application/json", "Authorization": f"Bearer {api_key}"},
            method="POST",
        )
        if provider == "openrouter":
            site_url = os.environ.get("OPENROUTER_SITE_URL", "").strip()
            app_name = os.environ.get("OPENROUTER_APP_NAME", "Z Data Contract Enforcer").strip()
            if site_url:
                req.add_header("HTTP-Referer", site_url)
            if app_name:
                req.add_header("X-Title", app_name)
        try:
            with urllib.request.urlopen(req, timeout=60) as resp:
                raw = json.loads(resp.read().decode("utf-8"))
            content = (raw.get("choices") or [{}])[0].get("message", {}).get("content", "")
            annotations.append(
                {
                    "column": col_name,
                    "llm_call_status": "ok",
                    "raw_response_excerpt": content[:500],
                }
            )
        except (urllib.error.URLError, urllib.error.HTTPError, TimeoutError, json.JSONDecodeError) as e:
            annotations.append({"column": col_name, "llm_call_status": "error", "error": str(e)[:300]})

    return annotations


def apply_fact_confidence_warnings(schema: dict, column_profiles: dict):
    """After enrich_week3_schema, attach distribution_warning using profiled stats."""
    fc = schema.get("fact_confidence")
    prof = column_profiles.get("fact_confidence")
    if not fc or not prof:
        return
    stats = prof.get("stats") or {}
    mean_v = stats.get("mean")
    if mean_v is None:
        return
    if float(mean_v) > 0.99 or float(mean_v) < 0.01:
        fc["distribution_warning"] = (
            f"Profiled mean={float(mean_v):.4f} on [0,1] confidence — verify calibration; "
            "clustering near boundaries may indicate wrong scale or saturation."
        )


def enrich_week3_schema(schema: dict, columns: set) -> dict:
    """Merge domain rules for Week 3 Document Refinery — machine-checkable failure modes."""
    out = dict(schema)
    out["doc_id"] = {
        "type": "string",
        "format": "uuid",
        "pattern": "^[0-9a-fA-F-]{36}$",
        "required": True,
        "unique": True,
        "description": "Primary document key (UUIDv4). Must be stable for the same source content.",
    }
    out["source_path"] = {
        "type": "string",
        "required": True,
        "minLength": 1,
        "description": "Source URI or filesystem path. Empty breaks downstream provenance joins.",
    }
    out["source_hash"] = {
        "type": "string",
        "required": True,
        "pattern": "^[a-f0-9]{64}$",
        "description": "SHA-256 of source bytes. Detects silent document swaps.",
    }
    out["extraction_model"] = {
        "type": "string",
        "required": True,
        "pattern": "^(claude|gpt)-",
        "description": "Model family prefix. Unexpected vendor strings indicate pipeline misconfiguration.",
    }
    out["processing_time_ms"] = {
        "type": "integer",
        "required": True,
        "minimum": 1,
        "description": "Wall-clock extraction duration in ms. Zero or negative indicates instrumentation bugs.",
    }
    out["extracted_at"] = {
        "type": "string",
        "format": "date-time",
        "required": True,
        "description": "ISO 8601 extraction timestamp for freshness and ordering.",
    }
    out["token_input"] = {
        "type": "integer",
        "required": False,
        "minimum": 0,
        "description": "Prompt/input token estimate from token_count.input.",
    }
    out["token_output"] = {
        "type": "integer",
        "required": False,
        "minimum": 0,
        "description": "Completion token estimate from token_count.output.",
    }
    if "fact_confidence" in out:
        fc = out["fact_confidence"]
        fc["minimum"] = 0.0
        fc["maximum"] = 1.0
        fc["required"] = True
        fc["description"] = (
            "Per-fact confidence 0.0–1.0. Silent failure mode: rescaling to 0–100 passes type checks but breaks thresholds."
        )
    if "fact_fact_id" in out:
        out["fact_fact_id"]["format"] = "uuid"
        out["fact_fact_id"]["pattern"] = "^[0-9a-fA-F-]{36}$"
        out["fact_fact_id"]["required"] = True
        out["fact_fact_id"]["unique"] = True
        out["fact_fact_id"]["description"] = "Stable id for each extracted fact row."
    if "fact_text" in out:
        out["fact_text"]["required"] = True
        out["fact_text"]["minLength"] = 1
        out["fact_text"]["description"] = "Non-empty extracted fact text."
    if "fact_page_ref" in out:
        out["fact_page_ref"]["type"] = "integer"
        out["fact_page_ref"]["minimum"] = 1
        out["fact_page_ref"]["required"] = True
        out["fact_page_ref"]["description"] = "1-based page reference for citations."
    # Only keep schema entries for columns present in the flattened profile (plus no orphans).
    return {k: v for k, v in out.items() if k in columns}


def week3_constraints() -> list:
    return [
        {
            "id": "week3.extracted_facts.min_items",
            "type": "record_json",
            "rule": "array_min_length",
            "path": "extracted_facts",
            "minimum": 1,
            "severity": "CRITICAL",
            "description": "Each extraction record must contain at least one fact.",
        },
        {
            "id": "week3.entity_refs.resolve_to_entities",
            "type": "record_json",
            "rule": "entity_refs_in_entities",
            "facts_path": "extracted_facts",
            "entities_path": "entities",
            "severity": "CRITICAL",
            "description": "Every entity_refs id must exist in entities[].entity_id in the same record.",
        },
        {
            "id": "week3.entities.type_enum",
            "type": "record_json",
            "rule": "entity_type_enum",
            "entities_path": "entities",
            "allowed": ["PERSON", "ORG", "LOCATION", "DATE", "AMOUNT", "OTHER"],
            "severity": "CRITICAL",
            "description": "Entity types must match the six-value ontology.",
        },
    ]


def enrich_week5_schema(schema: dict, columns: set) -> dict:
    out = dict(schema)
    out["event_id"] = {
        "type": "string",
        "format": "uuid",
        "pattern": "^[0-9a-fA-F-]{36}$",
        "required": True,
        "unique": True,
        "description": "Immutable event identifier.",
    }
    out["event_type"] = {
        "type": "string",
        "required": True,
        "pattern": "^[A-Z][a-zA-Z0-9]*$",
        "description": "PascalCase event name registered in the schema registry.",
    }
    out["aggregate_id"] = {
        "type": "string",
        "format": "uuid",
        "pattern": "^[0-9a-fA-F-]{36}$",
        "required": True,
        "description": "Aggregate root id for optimistic concurrency and replay.",
    }
    out["aggregate_type"] = {
        "type": "string",
        "required": True,
        "enum": ["Document"],
        "description": "Aggregate type label (PascalCase).",
    }
    out["sequence_number"] = {
        "type": "integer",
        "required": True,
        "minimum": 1,
        "description": "Monotonically increasing per aggregate_id (enforced by constraint).",
    }
    out["schema_version"] = {
        "type": "string",
        "required": True,
        "pattern": r"^\d+\.\d+$",
        "description": "Semantic version string for payload upcasting.",
    }
    out["occurred_at"] = {
        "type": "string",
        "format": "date-time",
        "required": True,
        "description": "Domain time of the fact.",
    }
    out["recorded_at"] = {
        "type": "string",
        "format": "date-time",
        "required": True,
        "description": "Append time to the log; must not precede occurred_at.",
    }
    return {k: v for k, v in out.items() if k in columns}


def week5_constraints() -> list:
    return [
        {
            "id": "week5.temporal.recorded_gte_occurred",
            "type": "record_json",
            "rule": "timestamp_order",
            "early": "occurred_at",
            "late": "recorded_at",
            "severity": "CRITICAL",
            "description": "recorded_at must be >= occurred_at for every event.",
        },
        {
            "id": "week5.sequence.monotonic_per_aggregate",
            "type": "dataset_json",
            "rule": "monotonic_sequence",
            "group_field": "aggregate_id",
            "sequence_field": "sequence_number",
            "strict": True,
            "severity": "CRITICAL",
            "description": "sequence_number must strictly increase per aggregate_id with no gaps or duplicates.",
        },
        {
            "id": "week5.metadata.source_service",
            "type": "record_json",
            "rule": "metadata_string",
            "path": "metadata.source_service",
            "pattern": r"^week[0-9]-|^[a-z0-9-]+$",
            "severity": "HIGH",
            "description": "Producer service name for cross-system routing.",
        },
    ]


def week3_quality() -> dict:
    return {
        "type": "SodaChecks",
        "specification": {
            "checks for extractions": [
                "missing_count(doc_id) = 0",
                "duplicate_count(doc_id) = 0",
                "invalid_count(fact_confidence < 0.0 OR fact_confidence > 1.0) = 0",
                "row_count > 0",
            ]
        },
    }


def week5_quality() -> dict:
    return {
        "type": "SodaChecks",
        "specification": {
            "checks for events": [
                "missing_count(event_id) = 0",
                "duplicate_count(event_id) = 0",
                "invalid_count(recorded_at < occurred_at) = 0",
            ]
        },
    }


def build_contract(contract_id: str, source_path: str, column_profiles: dict, kind: str, records: list):
    columns = set(column_profiles.keys())
    schema = {col: column_to_clause(prof) for col, prof in column_profiles.items()}
    contract = {
        "kind": "DataContract",
        "apiVersion": "v3.0.0",
        "id": contract_id,
        "info": {
            "title": "Week 3 Document Refinery — extraction records" if kind == "week3_extractions" else (
                "Week 5 — event store records" if kind == "week5_events" else contract_id
            ),
            "version": "1.0.0",
            "owner": "week7-team",
            "description": (
                "Machine-checkable contract for inter-system JSONL. "
                "Covers structural, statistical, and cross-field rules."
            ),
        },
        "servers": {"local": {"type": "local", "path": source_path, "format": "jsonl"}},
        "schema": schema,
    }
    if kind == "week3_extractions":
        contract["schema"] = enrich_week3_schema(schema, columns)
        apply_fact_confidence_warnings(contract["schema"], column_profiles)
        contract["constraints"] = week3_constraints()
        contract["quality"] = week3_quality()
        contract["lineage"] = contract.get("lineage", {})
        contract["lineage"]["breaking_if_changed"] = [
            "fact_confidence",
            "doc_id",
            "source_hash",
            "extracted_facts",
        ]
    elif kind == "week5_events":
        contract["schema"] = enrich_week5_schema(schema, columns)
        contract["constraints"] = week5_constraints()
        contract["quality"] = week5_quality()
        contract["lineage"] = contract.get("lineage", {})
        contract["lineage"]["breaking_if_changed"] = [
            "sequence_number",
            "aggregate_id",
            "event_type",
            "payload",
        ]
    return contract


def write_dbt_schema(contract: dict, output_file: Path, kind: str):
    """dbt schema.yml: not_null, unique, accepted_values — aligned to contract (no extra packages)."""
    model_name = output_file.stem
    columns = []
    for name, clause in contract.get("schema", {}).items():
        tests = []
        if clause.get("required"):
            tests.append("not_null")
        if clause.get("unique"):
            tests.append("unique")
        if clause.get("enum"):
            tests.append({"accepted_values": {"values": clause["enum"], "quote": True}})
        columns.append(
            {
                "name": name,
                "description": clause.get("description", "")[:500],
                "tests": tests,
            }
        )

    model_description = (
        f"Contract {contract.get('id')}. "
        "Column tests map to machine-checkable clauses. "
        "Cross-row rules (sequence monotonicity, recorded_at >= occurred_at) are enforced in ValidationRunner "
        "and may be added as singular SQL tests under generated_contracts/dbt_singular_tests/."
    )
    dbt_payload = {
        "version": 2,
        "models": [
            {
                "name": model_name,
                "description": model_description,
                "columns": columns,
            }
        ],
    }
    dbt_path = output_file.with_name(f"{output_file.stem}_dbt.yml")
    with dbt_path.open("w", encoding="utf-8") as f:
        yaml.safe_dump(dbt_payload, f, sort_keys=False)


def write_singular_dbt_tests(kind: str, out_dir: Path):
    out_dir.mkdir(parents=True, exist_ok=True)
    if kind == "week5_events":
        sql = """-- Singular test: temporal invariant (dbt). Point ref() at your staging model name.
select event_id
from {{ ref('week5_event_records') }}
where recorded_at < occurred_at
"""
        (out_dir / "singular_week5_recorded_gte_occurred.sql").write_text(sql, encoding="utf-8")
    if kind == "week3_extractions":
        sql = """-- Singular test: per-fact confidence in [0,1]. Align ref() with your dbt model.
select 1 as bad_row
from {{ ref('week3_extractions') }}
where fact_confidence is not null
  and (fact_confidence < 0 or fact_confidence > 1)
"""
        (out_dir / "singular_week3_confidence_range.sql").write_text(sql, encoding="utf-8")


def write_snapshot(contract: dict, contract_id: str):
    snap_dir = Path("schema_snapshots") / contract_id
    snap_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    with (snap_dir / f"{ts}.yaml").open("w", encoding="utf-8") as f:
        yaml.safe_dump(contract, f, sort_keys=False)


def main():
    load_env_file(Path(".env"))
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

    kind = detect_kind(args.contract_id, source)
    df = flatten_records(records)
    column_profiles = {col: profile_column(df[col], col) for col in df.columns}
    contract = build_contract(args.contract_id, args.source, column_profiles, kind, records)
    contract["llm_annotations"] = llm_annotate_ambiguous_columns(column_profiles)
    if args.lineage:
        contract = inject_lineage(contract, Path(args.lineage), args.contract_id, kind)

    write_statistical_baselines(args.contract_id, df)

    file_name = f"{sanitize_contract_filename(args.contract_id)}.yaml"
    output_file = output_dir / file_name
    with output_file.open("w", encoding="utf-8") as f:
        yaml.safe_dump(contract, f, sort_keys=False)

    write_dbt_schema(contract, output_file, kind)
    write_singular_dbt_tests(kind, output_dir / "dbt_singular_tests")
    write_snapshot(contract, args.contract_id)

    # Rubric-friendly filenames (aliases)
    if kind == "week3_extractions":
        (output_dir / "week3_extractions.yaml").write_text(output_file.read_text(encoding="utf-8"), encoding="utf-8")
        dbt_main = output_file.with_name(f"{output_file.stem}_dbt.yml")
        if dbt_main.exists():
            (output_dir / "week3_extractions_dbt.yml").write_text(dbt_main.read_text(encoding="utf-8"), encoding="utf-8")
    if kind == "week5_events":
        (output_dir / "week5_events.yaml").write_text(output_file.read_text(encoding="utf-8"), encoding="utf-8")
        dbt_main = output_file.with_name(f"{output_file.stem}_dbt.yml")
        if dbt_main.exists():
            (output_dir / "week5_events_dbt.yml").write_text(dbt_main.read_text(encoding="utf-8"), encoding="utf-8")

    print(f"Wrote contract: {output_file}")


if __name__ == "__main__":
    main()
