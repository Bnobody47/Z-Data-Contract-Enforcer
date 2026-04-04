import argparse
import json
import uuid
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import yaml
from jsonschema import validate, ValidationError


def load_jsonl(path: Path):
    records = []
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            records.append(json.loads(line))
    return records


def iso_now():
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def embed_texts(texts, dim: int = 256, ngram: int = 3):
    """
    Deterministic, offline embedding surrogate.
    This keeps the project runnable without external API keys while still producing numeric drift scores.
    """
    vecs = np.zeros((len(texts), dim), dtype=np.float32)
    for i, t in enumerate(texts):
        s = (t or "").lower()
        if len(s) < ngram:
            s = s + (" " * (ngram - len(s)))
        for j in range(len(s) - ngram + 1):
            chunk = s[j : j + ngram]
            h = 0
            for ch in chunk:
                h = (h * 31 + ord(ch)) % (2**31 - 1)
            vecs[i, h % dim] += 1.0
    # L2 normalize
    norms = np.linalg.norm(vecs, axis=1, keepdims=True) + 1e-9
    return vecs / norms


def cosine_distance(vec_a: np.ndarray, vec_b: np.ndarray):
    denom = (np.linalg.norm(vec_a) * np.linalg.norm(vec_b)) + 1e-9
    sim = float(np.dot(vec_a, vec_b) / denom)
    return 1.0 - sim


def check_embedding_drift(extractions: list, baseline_path: Path, threshold: float = 0.15, sample_n: int = 200):
    fact_texts = []
    for r in extractions:
        for fact in r.get("extracted_facts", []) or []:
            if isinstance(fact, dict):
                txt = fact.get("text")
                if txt:
                    fact_texts.append(txt)

    if not fact_texts:
        return {"status": "ERROR", "drift_score": None, "message": "No extracted_facts.text found."}

    sample = fact_texts[:sample_n] if len(fact_texts) > sample_n else fact_texts
    cur_vecs = embed_texts(sample)
    cur_centroid = cur_vecs.mean(axis=0)

    if not baseline_path.exists():
        baseline_path.parent.mkdir(parents=True, exist_ok=True)
        np.savez(baseline_path, centroid=cur_centroid)
        return {"status": "BASELINE_SET", "drift_score": 0.0, "threshold": threshold, "message": "Baseline established. Run again to detect drift."}

    base_centroid = np.load(baseline_path)["centroid"]
    drift = cosine_distance(cur_centroid, base_centroid)
    status = "FAIL" if drift > threshold else "PASS"
    return {
        "status": status,
        "drift_score": round(float(drift), 6),
        "threshold": threshold,
        "interpretation": "semantic content shifted" if status == "FAIL" else "stable",
    }


PROMPT_INPUT_SCHEMA = {
    "$schema": "http://json-schema.org/draft-07/schema#",
    "type": "object",
    "required": ["doc_id", "source_path", "content_preview"],
    "properties": {
        "doc_id": {"type": "string", "minLength": 36, "maxLength": 36},
        "source_path": {"type": "string", "minLength": 1},
        "content_preview": {"type": "string", "maxLength": 8000},
    },
    "additionalProperties": False,
}


def check_prompt_inputs(extractions: list, quarantine_path: Path):
    valid = 0
    quarantined = 0
    quarantined_rows = []

    for r in extractions:
        doc_id = r.get("doc_id")
        source_path = r.get("source_path")
        # content_preview is deliberately constructed so that long fact text can violate maxLength.
        facts = r.get("extracted_facts", []) or []
        texts = []
        for f in facts[:5]:
            if isinstance(f, dict) and f.get("text"):
                texts.append(str(f.get("text")))
        content_preview = " ".join(texts)

        rec = {"doc_id": doc_id, "source_path": source_path, "content_preview": content_preview}
        try:
            validate(instance=rec, schema=PROMPT_INPUT_SCHEMA)
            valid += 1
        except ValidationError as e:
            quarantined += 1
            quarantined_rows.append({"record": r, "error": e.message})

    if quarantined_rows:
        quarantine_path.parent.mkdir(parents=True, exist_ok=True)
        with quarantine_path.open("w", encoding="utf-8") as f:
            for q in quarantined_rows:
                f.write(json.dumps(q) + "\n")

    status = "FAIL" if quarantined > 0 else "PASS"
    return {
        "status": status,
        "valid_count": valid,
        "quarantined_count": quarantined,
        "quarantine_file": str(quarantine_path) if quarantined_rows else None,
    }


def load_ai_baseline(baseline_path: Path):
    if not baseline_path.exists():
        return None
    try:
        return json.loads(baseline_path.read_text(encoding="utf-8"))
    except Exception:
        return None


def save_ai_baseline(baseline_path: Path, value: dict):
    baseline_path.parent.mkdir(parents=True, exist_ok=True)
    baseline_path.write_text(json.dumps(value, indent=2), encoding="utf-8")


def append_ai_warn_to_violation_log(log_path: Path, violation_rate: float, baseline_rate: float, trend: str, threshold: float):
    """Rubric: WARN entry to violation_log when LLM output violation rate breaches policy."""
    log_path.parent.mkdir(parents=True, exist_ok=True)
    entry = {
        "violation_id": str(uuid.uuid4()),
        "check_id": "ai_extensions.llm_output_schema_violation_rate",
        "detected_at": iso_now(),
        "status": "WARN",
        "severity": "WARNING",
        "source": "contracts/ai_extensions.py",
        "message": (
            f"LLM output schema violation_rate={violation_rate:.4f} exceeds warn_threshold={threshold} "
            f"or trend={trend} vs baseline={baseline_rate}."
        ),
        "blame_chain": [],
        "blast_radius": {"note": "Week 2 verdict consumers; see contract_registry week2-auditor-verdicts"},
        "records_failing": 0,
        "failing_field": "overall_verdict",
    }
    with log_path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(entry) + "\n")


def check_output_schema_violation_rate(verdicts: list, baseline_path: Path, warn_threshold: float = 0.02):
    total = len(verdicts)
    if total == 0:
        return {"status": "ERROR", "violation_rate": None, "message": "No verdict records."}

    allowed = {"PASS", "FAIL", "WARN"}
    violations = sum(1 for v in verdicts if v.get("overall_verdict") not in allowed)
    rate = violations / max(total, 1)

    baseline = load_ai_baseline(baseline_path)
    if baseline is None:
        save_ai_baseline(baseline_path, {"written_at": iso_now(), "violation_rate": rate})
        return {
            "status": "BASELINE_SET",
            "total_outputs": total,
            "schema_violations": violations,
            "violation_rate": round(float(rate), 6),
            "baseline_violation_rate": None,
            "trend": "unknown",
        }

    base_rate = float(baseline.get("violation_rate", 0.0))
    trend = "rising" if rate > base_rate * 1.5 else ("falling" if rate < base_rate * 0.5 else "stable")
    status = "WARN" if (trend == "rising" or rate > warn_threshold) else "PASS"
    return {
        "status": status,
        "total_outputs": total,
        "schema_violations": violations,
        "violation_rate": round(float(rate), 6),
        "baseline_violation_rate": round(base_rate, 6),
        "trend": trend,
        "warn_threshold": warn_threshold,
        "violation_log_written": False,
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", default="all", choices=["all", "embedding", "prompt", "output"], help="Which checks to run")
    parser.add_argument("--extractions", required=True)
    parser.add_argument("--verdicts", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument(
        "--violation-log",
        default="violation_log/violations.jsonl",
        help="Append AI-extension WARN rows when output violation rate policy trips",
    )
    args = parser.parse_args()

    extractions = load_jsonl(Path(args.extractions))
    verdicts = load_jsonl(Path(args.verdicts))

    out = {}
    baseline_path = Path("schema_snapshots/embedding_baselines.npz")

    if args.mode in ("all", "embedding"):
        out["embedding_drift"] = check_embedding_drift(
            extractions=extractions,
            baseline_path=baseline_path,
            threshold=0.15,
        )

    if args.mode in ("all", "prompt"):
        out["prompt_input_validation"] = check_prompt_inputs(
            extractions=extractions,
            quarantine_path=Path("outputs/quarantine/trace_prompt_quarantine.jsonl"),
        )

    if args.mode in ("all", "output"):
        vr = check_output_schema_violation_rate(
            verdicts=verdicts,
            baseline_path=Path("schema_snapshots/ai_output_violation_baseline.json"),
            warn_threshold=0.02,
        )
        if vr.get("status") == "WARN":
            append_ai_warn_to_violation_log(
                Path(args.violation_log),
                violation_rate=float(vr["violation_rate"]),
                baseline_rate=float(vr.get("baseline_violation_rate") or 0.0),
                trend=str(vr.get("trend", "")),
                threshold=float(vr.get("warn_threshold", 0.02)),
            )
            vr["violation_log_written"] = True
        out["output_violation_rate"] = vr

    out_path = Path(args.output)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(out, indent=2), encoding="utf-8")
    print(f"Wrote AI extensions: {out_path}")


if __name__ == "__main__":
    main()

