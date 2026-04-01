import json
import random
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path


def write_jsonl(path: Path, rows):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        for row in rows:
            f.write(json.dumps(row) + "\n")


def iso(dt: datetime):
    return dt.replace(tzinfo=timezone.utc).isoformat().replace("+00:00", "Z")


def main():
    random.seed(7)
    now = datetime.utcnow().replace(microsecond=0)
    docs_root = Path.home() / "Documents"
    week1_repo = docs_root / "Roo-Code-Beamlak"
    week2_repo = docs_root / "The-Auditor"
    week3_repo = docs_root / "The-Document-Intelligence-Refinery"
    week4_repo = docs_root / "Z-Brownfield-Cartographer"
    week5_repo = docs_root / "Z Ledger"

    week1 = []
    trace_file = week1_repo / ".orchestration" / "agent_trace.jsonl"
    if trace_file.exists():
        with trace_file.open("r", encoding="utf-8") as f:
            for i, line in enumerate(f):
                line = line.strip()
                if not line:
                    continue
                try:
                    t = json.loads(line)
                except json.JSONDecodeError:
                    continue
                refs = []
                for fl in t.get("files", [])[:3]:
                    rel = fl.get("relative_path", "")
                    refs.append(
                        {
                            "file": rel,
                            "line_start": 1,
                            "line_end": 1,
                            "symbol": "unknown",
                            "confidence": round(random.uniform(0.70, 0.98), 3),
                        }
                    )
                week1.append(
                    {
                        "intent_id": t.get("id", str(uuid.uuid4())),
                        "description": f"Intent trace {i} from Roo-Code-Beamlak",
                        "code_refs": refs or [{"file": "src/index.ts", "line_start": 1, "line_end": 1, "symbol": "unknown", "confidence": 0.8}],
                        "governance_tags": ["engineering", "automation"],
                        "created_at": t.get("timestamp", iso(now)),
                    }
                )
                if len(week1) >= 60:
                    break
    if not week1:
        for i in range(60):
            week1.append(
                {
                    "intent_id": str(uuid.uuid4()),
                    "description": f"Generated intent record {i}",
                    "code_refs": [{"file": "src/core.ts", "line_start": 1, "line_end": 1, "symbol": "handler", "confidence": 0.85}],
                    "governance_tags": ["engineering"],
                    "created_at": iso(now - timedelta(minutes=i)),
                }
            )

    week2 = []
    report_file = week2_repo / "audit" / "report_bypeer_received" / "audit_report_20260228_182014.md"
    if report_file.exists():
        text = report_file.read_text(encoding="utf-8", errors="ignore")
        for i in range(60):
            week2.append(
                {
                    "verdict_id": str(uuid.uuid4()),
                    "target_ref": f"{week2_repo.name}/src/graph.py",
                    "rubric_id": "sha256_week2_rubric",
                    "rubric_version": "1.0.0",
                    "scores": {"architecture": {"score": random.randint(2, 5), "evidence": ["state graph"], "notes": "from report synthesis"}},
                    "overall_verdict": random.choice(["PASS", "WARN", "FAIL"]),
                    "overall_score": round(random.uniform(2.0, 4.8), 2),
                    "confidence": round(random.uniform(0.6, 0.98), 2),
                    "evaluated_at": iso(now - timedelta(minutes=3 * i)),
                    "source_excerpt": text[:160],
                }
            )
    else:
        for i in range(60):
            week2.append(
                {
                    "verdict_id": str(uuid.uuid4()),
                    "target_ref": "unknown",
                    "rubric_id": "sha256_unknown",
                    "rubric_version": "1.0.0",
                    "scores": {"architecture": {"score": 3, "evidence": ["placeholder"], "notes": "placeholder"}},
                    "overall_verdict": "WARN",
                    "overall_score": 3.0,
                    "confidence": 0.8,
                    "evaluated_at": iso(now - timedelta(minutes=3 * i)),
                }
            )

    week3 = []
    for i in range(60):
        doc_id = str(uuid.uuid4())
        entity_id = str(uuid.uuid4())
        fact_id = str(uuid.uuid4())
        week3.append(
            {
                "doc_id": doc_id,
                "source_path": f"https://example.com/doc/{i}",
                "source_hash": "".join(random.choice("abcdef0123456789") for _ in range(64)),
                "extracted_facts": [
                    {
                        "fact_id": fact_id,
                        "text": f"Fact {i}",
                        "entity_refs": [entity_id],
                        "confidence": round(random.uniform(0.61, 0.98), 3),
                        "page_ref": random.randint(1, 12),
                        "source_excerpt": f"Excerpt text {i}",
                    }
                ],
                "entities": [
                    {
                        "entity_id": entity_id,
                        "name": f"Entity {i}",
                        "type": random.choice(["PERSON", "ORG", "LOCATION", "DATE", "AMOUNT", "OTHER"]),
                        "canonical_value": f"Entity {i}",
                    }
                ],
                "extraction_model": "claude-3-5-sonnet-20241022",
                "processing_time_ms": random.randint(400, 2200),
                "token_count": {"input": random.randint(500, 2000), "output": random.randint(100, 900)},
                "extracted_at": iso(now - timedelta(minutes=i)),
            }
        )

    # Shared aggregates so sequence_number is monotonic per aggregate (event-sourcing contract).
    aggregate_ids = [str(uuid.uuid4()) for _ in range(5)]
    seq_by_aggregate = {a: 0 for a in aggregate_ids}

    week5 = []
    for i in range(60):
        occurred = now - timedelta(minutes=2 * i)
        recorded = occurred + timedelta(seconds=random.randint(1, 25))
        agg = aggregate_ids[i % len(aggregate_ids)]
        seq_by_aggregate[agg] += 1
        week5.append(
            {
                "event_id": str(uuid.uuid4()),
                "event_type": random.choice(["DocumentProcessed", "ExtractionUpdated", "ContractValidated"]),
                "aggregate_id": agg,
                "aggregate_type": "Document",
                "sequence_number": seq_by_aggregate[agg],
                "payload": {"doc_index": i, "status": "ok"},
                "metadata": {
                    "causation_id": None,
                    "correlation_id": str(uuid.uuid4()),
                    "user_id": "system",
                    "source_service": "week3-document-refinery",
                },
                "schema_version": "1.0",
                "occurred_at": iso(occurred),
                "recorded_at": iso(recorded),
            }
        )

    week4 = [
        {
            "snapshot_id": str(uuid.uuid4()),
            "codebase_root": str(week4_repo),
            "git_commit": "a" * 40,
            "nodes": [
                {"node_id": "file::outputs/week3/extractions.jsonl", "type": "FILE", "label": "week3 extractions", "metadata": {"path": str(week3_repo / ".refinery" / "extractions")}},
                {"node_id": "pipeline::week7-contract-enforcer", "type": "PIPELINE", "label": "week7 enforcer", "metadata": {"path": "contracts/runner.py"}},
                {"node_id": "file::outputs/week5/events.jsonl", "type": "FILE", "label": "week5 events", "metadata": {"path": str(week5_repo / "src" / "models" / "events.py")}},
            ],
            "edges": [
                {"source": "file::outputs/week3/extractions.jsonl", "target": "pipeline::week7-contract-enforcer", "relationship": "CONSUMES", "confidence": 0.95},
                {"source": "file::outputs/week5/events.jsonl", "target": "pipeline::week7-contract-enforcer", "relationship": "CONSUMES", "confidence": 0.95},
            ],
            "captured_at": iso(now),
        }
    ]

    write_jsonl(Path("outputs/week1/intent_records.jsonl"), week1)
    write_jsonl(Path("outputs/week2/verdicts.jsonl"), week2)
    write_jsonl(Path("outputs/week3/extractions.jsonl"), week3)
    write_jsonl(Path("outputs/week5/events.jsonl"), week5)
    write_jsonl(Path("outputs/week4/lineage_snapshots.jsonl"), week4)
    write_jsonl(Path("outputs/traces/runs.jsonl"), week1[:60])
    print("Sample data written under outputs/")


if __name__ == "__main__":
    main()
