# Data Contract Enforcer (Week 7) — Final Submission Runbook

This repo generates Bitol-style data contracts, validates real JSONL snapshots against those contracts, attributes failures to likely upstream causes, and produces an enforcer report.

## 1. Prerequisites

1. Install Python dependencies:

```powershell
python -m pip install -r requirements.txt
```

2. Ensure you run from the repo root (`Z Data Contract Enforcer`).

## 2. Baseline contracts + validation baselines (clean data)

Contracts and dbt counterparts are generated from your Week 3/5 sample outputs. The generator also writes **statistical baselines** (`schema_snapshots/baselines_<contract>.json` and `schema_snapshots/baselines.json`), optional **LLM column annotations** (set `OPENAI_API_KEY`), and **downstream consumers** from Week 4 lineage when `--lineage` is passed.

```powershell
python contracts\generator.py --source outputs\week3\extractions.jsonl --contract-id week3-document-refinery-extractions --lineage outputs\week4\lineage_snapshots.jsonl --output generated_contracts
python contracts\generator.py --source outputs\week5\events.jsonl --contract-id week5-event-records --lineage outputs\week4\lineage_snapshots.jsonl --output generated_contracts
```

Then create or refresh **runner** statistical drift baselines (first run per contract writes `schema_snapshots/baselines_<contract>.json` if missing):

```powershell
python contracts\runner.py --contract generated_contracts\week3_extractions.yaml --data outputs\week3\extractions.jsonl --output validation_reports\week3_clean_baseline.json --mode AUDIT
python contracts\runner.py --contract generated_contracts\week5_events.yaml --data outputs\week5\events.jsonl --output validation_reports\week5_clean_baseline.json --mode AUDIT
```

Use `--mode WARN` (block on CRITICAL FAIL/ERROR) or `--mode ENFORCE` (block on CRITICAL or HIGH) for pipeline gates.

## 3. Create violated datasets (for final evidence)

```powershell
python scripts\create_violation_scale_change.py --input outputs\week3\extractions.jsonl --output outputs\week3\extractions_violated_scale_change.jsonl
python scripts\create_violation_entity_refs.py --input outputs\week3\extractions.jsonl --output outputs\week3\extractions_violated_entity_refs.jsonl
python scripts\create_violation_week5.py --input outputs\week5\events.jsonl --output outputs\week5\events_violated_temporal_and_sequence.jsonl
python scripts\create_week2_verdicts_violation.py --input outputs\week2\verdicts.jsonl --output outputs\week2\verdicts_violated_output_schema.jsonl
```

## 4. Re-generate contracts to produce evolution snapshots

This appends schema snapshots into `schema_snapshots/<contract-id>/` so `schema_analyzer.py` can diff the two.

```powershell
python contracts\generator.py --source outputs\week3\extractions_violated_scale_change.jsonl --contract-id week3-document-refinery-extractions --lineage outputs\week4\lineage_snapshots.jsonl --output generated_contracts
python contracts\generator.py --source outputs\week5\events_violated_temporal_and_sequence.jsonl --contract-id week5-event-records --lineage outputs\week4\lineage_snapshots.jsonl --output generated_contracts
```

## 5. Validate violated datasets

```powershell
python contracts\runner.py --contract generated_contracts\week3_extractions.yaml --data outputs\week3\extractions_violated_scale_change.jsonl --output validation_reports\violated_week3_scale.json --mode AUDIT
python contracts\runner.py --contract generated_contracts\week3_extractions.yaml --data outputs\week3\extractions_violated_entity_refs.jsonl --output validation_reports\violated_week3_entity_refs.json --mode AUDIT
python contracts\runner.py --contract generated_contracts\week5_events.yaml --data outputs\week5\events_violated_temporal_and_sequence.jsonl --output validation_reports\violated_week5.json --mode AUDIT
```

## 6. Attribute violations (build `violation_log/violations.jsonl`)

```powershell
# Create the file with the documented injection header.
python contracts\attributor.py --violation validation_reports\violated_week3_scale.json --lineage outputs\week4\lineage_snapshots.jsonl --contract generated_contracts\week3_extractions.yaml --output violation_log\violations.jsonl --write-injection-comment

# Append additional (un-documented) violations.
python contracts\attributor.py --violation validation_reports\violated_week3_entity_refs.json --lineage outputs\week4\lineage_snapshots.jsonl --contract generated_contracts\week3_extractions.yaml --output violation_log\violations.jsonl
```

## 7. Detect schema evolution (breaking vs compatible)

```powershell
python contracts\schema_analyzer.py --contract-id week3-document-refinery-extractions --output validation_reports\schema_evolution_week3.json
python contracts\schema_analyzer.py --contract-id week5-event-records --output validation_reports\schema_evolution_week5.json
# Optional: only consider snapshots on/after a date (ISO):
# python contracts\schema_analyzer.py --contract-id week3-document-refinery-extractions --since 2026-04-01T00:00:00Z --output validation_reports\schema_evolution_week3.json
```

## 8. Run AI-specific extensions

This runs:
- embedding drift check (offline embedding surrogate; baseline saved under `schema_snapshots/`)
- prompt input schema validation (quarantines invalid records)
- output schema violation rate on Week 2 verdicts

```powershell
# Baselines
python contracts\ai_extensions.py --mode all --extractions outputs\week3\extractions.jsonl --verdicts outputs\week2\verdicts.jsonl --output validation_reports\ai_extensions_baseline.json

# Violated run (appends a WARN row to violation_log/violations.jsonl when output violation rate policy trips)
python contracts\ai_extensions.py --mode all --extractions outputs\week3\extractions_violated_scale_change.jsonl --verdicts outputs\week2\verdicts_violated_output_schema.jsonl --output validation_reports\ai_extensions_violated.json --violation-log violation_log\violations.jsonl
```

## 9. Generate final enforcer report JSON

```powershell
python contracts\report_generator.py --output enforcer_report\report_data.json
```

The final artifact is `enforcer_report/report_data.json`.

