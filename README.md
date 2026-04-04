# Data Contract Enforcer (Week 7) — Final Submission Runbook

This repo generates Bitol-style data contracts, validates real JSONL snapshots against those contracts, attributes failures to likely upstream causes, and produces an enforcer report.

## 1. Prerequisites

1. Install Python dependencies:

```powershell
python -m pip install -r requirements.txt
```

2. Ensure you run from the repo root (`Z Data Contract Enforcer`).

## 2. Baseline contracts + validation baselines (clean data)

Contracts and dbt counterparts are generated from your Week 3/5 sample outputs:

```powershell
python contracts\generator.py --source outputs\week3\extractions.jsonl --contract-id week3-document-refinery-extractions --output generated_contracts
python contracts\generator.py --source outputs\week5\events.jsonl --contract-id week5-event-records --output generated_contracts
```

Then create statistical drift baselines (runner writes baselines into `schema_snapshots/` the first time it runs per contract):

```powershell
python contracts\runner.py --contract generated_contracts\week3_extractions.yaml --data outputs\week3\extractions.jsonl --output validation_reports\week3_clean_baseline.json
python contracts\runner.py --contract generated_contracts\week5_events.yaml --data outputs\week5\events.jsonl --output validation_reports\week5_clean_baseline.json
```

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
python contracts\generator.py --source outputs\week3\extractions_violated_scale_change.jsonl --contract-id week3-document-refinery-extractions --output generated_contracts
python contracts\generator.py --source outputs\week5\events_violated_temporal_and_sequence.jsonl --contract-id week5-event-records --output generated_contracts
```

## 5. Validate violated datasets

```powershell
python contracts\runner.py --contract generated_contracts\week3_extractions.yaml --data outputs\week3\extractions_violated_scale_change.jsonl --output validation_reports\violated_week3_scale.json
python contracts\runner.py --contract generated_contracts\week3_extractions.yaml --data outputs\week3\extractions_violated_entity_refs.jsonl --output validation_reports\violated_week3_entity_refs.json
python contracts\runner.py --contract generated_contracts\week5_events.yaml --data outputs\week5\events_violated_temporal_and_sequence.jsonl --output validation_reports\violated_week5.json
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
```

## 8. Run AI-specific extensions

This runs:
- embedding drift check (offline embedding surrogate; baseline saved under `schema_snapshots/`)
- prompt input schema validation (quarantines invalid records)
- output schema violation rate on Week 2 verdicts

```powershell
# Baselines
python contracts\ai_extensions.py --mode all --extractions outputs\week3\extractions.jsonl --verdicts outputs\week2\verdicts.jsonl --output validation_reports\ai_extensions_baseline.json

# Violated run
python contracts\ai_extensions.py --mode all --extractions outputs\week3\extractions_violated_scale_change.jsonl --verdicts outputs\week2\verdicts_violated_output_schema.jsonl --output validation_reports\ai_extensions_violated.json
```

## 9. Generate final enforcer report JSON

```powershell
python contracts\report_generator.py --output enforcer_report\report_data.json
```

The final artifact is `enforcer_report/report_data.json`.

