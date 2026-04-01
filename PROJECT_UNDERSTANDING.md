# Week 7 Project Understanding

This Week 7 system enforces contracts across the five prior systems by turning each inter-system interface into a machine-checkable schema plus runtime validation.

## Source Repositories Used

- Week 1: `C:\Users\Bnobody_47\Documents\Roo-Code-Beamlak`
- Week 2: `C:\Users\Bnobody_47\Documents\The-Auditor`
- Week 3: `C:\Users\Bnobody_47\Documents\The-Document-Intelligence-Refinery`
- Week 4: `C:\Users\Bnobody_47\Documents\Z-Brownfield-Cartographer`
- Week 5: `C:\Users\Bnobody_47\Documents\Z Ledger`

## How Week 7 Maps to Week 1-5

1. Contract generation (`contracts/generator.py`)
   - Profiles JSONL interfaces and generates Bitol-style YAML clauses.
   - Injects downstream lineage metadata from Week 4 lineage snapshots.

2. Validation runner (`contracts/runner.py`)
   - Executes structural checks (required, type, enum, UUID, datetime, range).
   - Executes statistical drift checks against stored baselines.
   - Produces structured JSON report for enforcement evidence.

3. Data bootstrapping from actual repos (`scripts/bootstrap_sample_data.py`)
   - Week 1 seed uses Roo trace ledger (`.orchestration/agent_trace.jsonl`) where available.
   - Week 2 seed uses Auditor report artifacts where available.
   - Week 3/5 outputs are generated in canonical Week 7 interface shape for contract testing.
   - Week 4 lineage snapshot includes references to your actual Week 3 and Week 5 repos.

## Trust Boundary Model (Tier 1/2/3)

This repository currently implements a Tier 1-first enforcement model and documents how it extends to Tier 2 and Tier 3.

- Tier 1 (same team, same repo, shared trust boundary)
  - Full lineage graph available.
  - Violation attribution uses graph traversal plus git history.
  - Blast radius can be computed directly from known downstream nodes.

- Tier 2 (different teams, same company)
  - Partial lineage visibility only.
  - Add central contract registry with dependency subscriptions.
  - Blast radius is computed as: known internal lineage impact + subscribed team notifications.

- Tier 3 (different companies)
  - No shared lineage graph.
  - Contract registry and versioned compatibility are the primary control plane.
  - Cross-company blast radius is subscriber-level impact only; each consumer computes internal impact independently.

Design principle: inside a trust boundary, use lineage for causal attribution; across trust boundaries, use contract subscriptions and compatibility gates.

## Interim Submission Status

- `DOMAIN_NOTES.md` present with all five required answers.
- `generated_contracts/` includes Week 3 and Week 5 contracts (+ dbt yaml files).
- `contracts/generator.py` and `contracts/runner.py` are runnable.
- `validation_reports/thursday_baseline.json` generated from real run in this workspace.

## Next Build Steps (Sunday scope)

- Implement `contracts/attributor.py` for blame chain with git traversal.
- Implement `contracts/schema_analyzer.py` for compatibility classification.
- Implement `contracts/ai_extensions.py` for embedding drift + prompt/output schema checks.
- Implement `contracts/report_generator.py` and generate `enforcer_report/report_data.json`.
