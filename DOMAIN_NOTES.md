# DOMAIN_NOTES

This document answers the five required Phase 0 questions for the Data Contract Enforcer using concrete examples from this repository's Week 3 and Week 5 datasets.

## 1) Backward-compatible vs breaking schema changes

A backward-compatible change is one that lets existing downstream consumers continue running without code changes. A breaking change requires downstream consumer changes and can silently corrupt outputs if not coordinated.

### Three backward-compatible examples

1. Add nullable field to Week 5 event records  
   Example: adding `payload.debug_note` as optional text in `outputs/week5/events.jsonl`. Existing consumers that parse known fields can ignore this field.

2. Add enum value without removing old ones (carefully)  
   Example: if `event_type` currently includes `DocumentProcessed`, `ExtractionUpdated`, and `ContractValidated`, adding `ContractRepaired` is typically compatible if consumers do not hard-fail unknown values and only use pattern-level grouping.

3. Widen numeric precision while preserving semantics  
   Example: changing `processing_time_ms` storage from int to number in profiling output can be backward-compatible when values and units stay milliseconds and current consumer calculations still hold.

### Three breaking examples

1. Confidence scale change in Week 3 facts  
   `extracted_facts[].confidence` changing from 0.0-1.0 float to 0-100 causes downstream threshold logic to over-trigger or under-trigger. This is the canonical breaking change for this project.

2. Rename a required key used by consumers  
   Example: `doc_id` renamed to `document_id` in Week 3 records. Any consumer expecting `doc_id` fails lookups and lineage joins.

3. Remove required temporal field from Week 5 events  
   Example: removing `occurred_at` or `recorded_at` breaks ordering and SLA checks (`recorded_at >= occurred_at`), which can invalidate compliance and replay behavior.

## 2) Confidence 0.0-1.0 to 0-100 failure path and contract clause

Failure path (Week 3 -> Week 4/Week 7):

1. Week 3 extractor writes `confidence` as 0-100 instead of 0.0-1.0.
2. Type checks may still pass because values remain numeric.
3. Any consumer using thresholds such as `confidence >= 0.8` now passes nearly all facts incorrectly.
4. Lineage/cartography confidence metrics become meaningless and overconfident.
5. ValidationRunner range checks and drift checks should detect this before propagation.

Bitol-style clause to prevent this:

```yaml
schema:
  fact_confidence:
    type: number
    required: true
    minimum: 0.0
    maximum: 1.0
    description: Confidence score for extracted facts. Breaking if scale changes to 0-100.
```

## 3) How lineage is used for blame chain generation

The Enforcer uses the lineage graph in steps:

1. ValidationRunner emits a failing check with `check_id` and `column_name`.
2. ViolationAttributor maps the failing domain (for example `week3` extraction fields) to producer nodes in the latest `outputs/week4/lineage_snapshots.jsonl`.
3. It traverses upstream candidates and collects producer file paths from node metadata.
4. For each candidate file, it queries git history over a bounded recent window.
5. It ranks candidates by temporal proximity and lineage distance.
6. It writes a ranked blame chain plus blast radius from contract downstream consumers.

In this baseline interim implementation, lineage traversal uses the latest snapshot edges to infer downstream impacted nodes and stores this context in generated contracts for blast radius reporting.

## 4) LangSmith trace contract with structural, statistical, and AI-specific clauses

```yaml
kind: DataContract
apiVersion: v3.0.0
id: langsmith-trace-records
schema:
  id:
    type: string
    format: uuid
    required: true
  run_type:
    type: string
    required: true
    enum: [llm, chain, tool, retriever, embedding]
  start_time:
    type: string
    format: date-time
    required: true
  end_time:
    type: string
    format: date-time
    required: true
  total_cost:
    type: number
    minimum: 0.0
    required: true
quality:
  statistical:
    - rule: total_tokens == prompt_tokens + completion_tokens
  ai_specific:
    - rule: output_schema_violation_rate <= 0.02
      action_on_breach: WARN
```

Structural clause example: `run_type` enum and datetime formatting.  
Statistical clause example: token arithmetic integrity.  
AI-specific clause example: output schema violation rate threshold.

## 5) Why contract systems get stale and how this architecture prevents it

Most contract enforcement systems fail because contracts are written once and then drift away from real producer behavior. Staleness appears when teams ship schema changes faster than contract updates, when ownership is unclear, and when no automated checks are tied to delivery pipelines.

Common staleness failure modes:

- Manual contract docs not connected to runnable validators.
- No snapshot history, so teams cannot detect when changes started.
- No lineage context, so stakeholders cannot see blast radius and therefore under-prioritize fixes.
- No reporting layer translating failures into business risk language.

This architecture counters staleness by design:

1. Contracts are generated programmatically from actual JSONL outputs (not hand-written only).
2. ValidationRunner always produces machine-readable reports and does not crash on partial failure.
3. Baselines are persisted in `schema_snapshots/baselines.json` for drift detection over time.
4. Generated contracts include downstream lineage consumers for impact visibility.
5. Submission workflow requires real validation reports and reproducible script execution.

The key operating principle is to treat contracts as executable artifacts in the same lifecycle as code and data, not static documentation.
