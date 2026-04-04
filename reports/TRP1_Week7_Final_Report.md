# TRP1 Week 7 — Data Contract Enforcer  
## Final Submission Report

**Trainee:** Beamlak Adane (Bnobody47)  
**Course:** TRP1 — Data Contract Enforcer  
**Report date:** 4 April 2026  
**Primary Week 7 repository:** *(submit your public GitHub URL on the course form)*  

**Related platform repositories (Weeks 1–5):** [Roo-Code-Beamlak](https://github.com/Bnobody47/Roo-Code-Beamlak), [The-Auditor](https://github.com/Bnobody47/The-Auditor), [The-Document-Intelligence-Refinery](https://github.com/Bnobody47/The-Document-Intelligence-Refinery), [Z-Brownfield-Cartographer](https://github.com/Bnobody47/Z-Brownfield-Cartographer), [Z-Ledger](https://github.com/Bnobody47/Z-Ledger).

---

## How this report is produced

This document is the **human-readable companion** to the **machine-generated** enforcer payload. Numbers and clause IDs below are tied to files in the repo so evaluators can verify them without trusting prose alone.

**Regenerate the JSON report (required for rubric “generation evidence”):**

```text
python contracts/report_generator.py --output enforcer_report/report_data.json
```

To embed the **taxonomy-rich** Week 3 evolution payload in the same JSON (optional):

```text
python contracts/report_generator.py --schema-week3 validation_reports/schema_evolution_week3_rubric.json --output enforcer_report/report_data.json
```

**Primary machine output:** `enforcer_report/report_data.json` (written by `contracts/report_generator.py`, reading `violation_log/violations.jsonl` and `validation_reports/*.json`).

---

## 1. Auto-generated Enforcer Report (machine output)

### 1.1 Generation evidence

| Item | Value |
|------|--------|
| **Classification** | `auto_generated_enforcer_report` (see `report_meta` in `enforcer_report/report_data.json`) |
| **Generator** | `contracts/report_generator.py` |
| **Inputs** | `violation_log/violations.jsonl`, glob `validation_reports/*.json`, `validation_reports/schema_evolution_week3.json`, `validation_reports/schema_evolution_week5.json`, `validation_reports/ai_extensions_violated.json` |
| **Last generated** | `generated_at` in `enforcer_report/report_data.json` (e.g. 2026-04-04T17:50:20Z at time of writing) |

The JSON embeds the same five thematic blocks this narrative expands: health score, violations (including severity breakdown), schema evolution, AI risk, and prioritised actions.

### 1.2 Data health score (0–100) and calculation

From `enforcer_report/report_data.json` → `health_score_detail`:

- **Formula (implemented):** `(passed_checks / total_checks) * 100` **minus** `20 * distinct_critical_violations`, where a **distinct critical violation** is a **FAIL or ERROR** result with **severity CRITICAL**, deduplicated by `(contract_id, check_id)` across all loaded runner reports.
- **Example snapshot (live run):** `passed_checks` = **283**, `total_checks` = **289**, `distinct_critical_violations` = **5** → base ≈ **97.9%**, minus **100** → **`data_health_score` = 0.0**.

A score of **0** here is **not** “the platform is worthless”; it means **aggregating every historical runner JSON in `validation_reports/`** includes **multiple failing runs** (injected/violated evidence) alongside clean baselines, so the **deduped CRITICAL failure set** triggers the full **5 × 20** penalty. For a **clean-only** portfolio score, restrict the glob or archive old violated reports before regenerating.

### 1.3 Required sections in the JSON (all five present)

| # | Section key in JSON | Contents |
|---|---------------------|----------|
| 1 | `data_health_score` + `health_score_detail` | Numeric score and formula inputs |
| 2 | `violations_by_severity` + `violations_this_week` | Counts `FAIL/CRITICAL`, `FAIL/HIGH`, etc.; top violation narratives |
| 3 | `schema_changes_detected` | Week 3 + Week 5 evolution payloads (from `schema_evolution_*.json`) |
| 4 | `ai_system_risk_assessment` | Embedding drift, prompt validation, output violation rate |
| 5 | `recommended_actions` / `recommended_actions_prioritised` | Three structured mitigations (see §1.4) |

**Violations by severity (aggregated across all runner reports in the glob):** see `violations_by_severity.breakdown` in `enforcer_report/report_data.json` (example: **5×** `FAIL/CRITICAL`, **1×** `FAIL/HIGH`, **6** non-pass checks total).

### 1.4 Three prioritised recommended actions (fully specified — file, field, clause)

Each action below names **four concrete anchors**: (1) **input JSONL**, (2) **Bitol contract YAML**, (3) **logical field**, (4) **runner `check_id`** (machine clause). The same triples are emitted as structured objects under `recommended_actions_prioritised` in `enforcer_report/report_data.json`.

| Priority | Input data file (repro) | Contract file | Field | Contract clause (`check_id`) | Related dbt column test (same contract) |
|----------|-------------------------|---------------|-------|------------------------------|----------------------------------------|
| **1** | `outputs/week3/extractions_violated_scale_change.jsonl` | `generated_contracts/week3_extractions.yaml` | `extracted_facts[].confidence` (runner: `fact_confidence`) | **`week3-document-refinery-extractions.fact_confidence.range`** | `fact_confidence` — implied by model + singular SQL `generated_contracts/dbt_singular_tests/singular_week3_confidence_range.sql` |
| **2** | `outputs/week3/extractions_violated_scale_change.jsonl` | `generated_contracts/week3_extractions.yaml` | `source_path` | **`week3-document-refinery-extractions.source_path.min_length`** | `source_path` — `not_null` + `minLength` parity in `generated_contracts/week3_extractions_dbt.yml` |
| **3** | `outputs/week3/extractions_violated_entity_refs.jsonl` | `generated_contracts/week3_extractions.yaml` | `extracted_facts[].entity_refs` | **`week3.entity_refs.resolve_to_entities`** | *(record-graph rule; enforced in runner — add singular test if promoting to warehouse)* |

**Copy-paste verification (after fixing the producer, point `--data` at clean `outputs/week3/extractions.jsonl`):**

```text
python contracts/runner.py --contract generated_contracts/week3_extractions.yaml --data outputs/week3/extractions_violated_scale_change.jsonl --output validation_reports/verify_p1_p2.json --mode AUDIT
python contracts/runner.py --contract generated_contracts/week3_extractions.yaml --data outputs/week3/extractions_violated_entity_refs.jsonl --output validation_reports/verify_p3.json --mode AUDIT
```

These commands re-execute the **exact** clauses named in column **Contract clause** against the **exact** files in column **Input data file**.

---

## 2. Validation run results (clause-level, interpreted)

### 2.1 Week 3 — scale / provenance injection (`outputs/week3/extractions_violated_scale_change.jsonl`)

**Report:** `validation_reports/violated_week3_scale_final.json`  
**Contract:** `generated_contracts/week3_extractions.yaml` (`week3-document-refinery-extractions`)

| Metric | Value |
|--------|--------|
| **report_id** | `e7951445-0c8c-45ca-9239-b35aeb9f7bcf` |
| **snapshot_id** | `eda1977c97f1750c5bdca63cf4fed772b633c60cb7293c0ed0468bc1f40eeb7f` |
| **total_checks** | 52 |
| **passed / failed / warned / errored** | 49 / **3** / 0 / 0 |

**Failure A — structural / CRITICAL — `source_path`**

- **Clause:** `week3-document-refinery-extractions.source_path.min_length`  
- **Field:** `source_path`  
- **Actual vs expected:** `short_count=1` vs **minimum length ≥ 1** (empty string on one injected record).  
- **Why it matters:** provenance joins and prompt packaging assume a non-empty path; empty string is valid JSON but **wrong semantics**.  
- **Downstream:** **week4-cartographer** (registry) consumes `source_path` for graph/provenance; broken paths create **silent bad edges** in lineage-backed UIs.

**Failure B — structural / CRITICAL — confidence range**

- **Clause:** `week3-document-refinery-extractions.fact_confidence.range`  
- **Field:** `fact_confidence` (flattened from `extracted_facts[].confidence`)  
- **Actual vs expected:** **`min=61.1, max=97.6`** vs **`min≥0.0, max≤1.0`**  
- **Why it matters:** this is the classic **0.0–1.0 → 0–100** rescaling: still numeric, still “looks fine” to naive parsers.  

**Downstream consumer impact (named) — Failure B**

- **Consumer:** **`week4-cartographer`**, registered in **`contract_registry/subscriptions.yaml`** under `contract_id: week3-document-refinery-extractions`, with **`breaking_fields`** explicitly listing **`extracted_facts.confidence`** (“used for node ranking; scale change … breaks ordering”).  
- **Consequence if unchecked:** the Cartographer (Week 4) continues to **ingest valid JSON** but **mis-ranks** or **mis-colours** graph nodes that use confidence as a **0–1** signal; dashboards and “top facts” lists become **wrong without any parse error**, until a human notices inconsistent behaviour. Week 7’s **`fact_confidence.range`** FAIL is the tripwire that **stops** that handoff at the consumer boundary when `ValidationRunner` is run on the same path the contract names.

**Failure C — statistical / HIGH — drift (independent path from range)**

- **Clause:** `week3-document-refinery-extractions.fact_confidence.drift`  
- **Field:** `fact_confidence`  
- **Actual vs expected:** **`z=702.64`** vs policy **warn >2σ, fail >3σ** (baseline mean/stddev from `schema_snapshots/baselines_week3-document-refinery-extractions.json`).  
- **Severity:** **HIGH** (runner maps drift FAIL to HIGH; 2σ would surface as **WARNING** severity on the drift check).  
- **Why alongside range:** range catches **bounds**; drift catches **distribution shift** even when future bugs keep values nominally inside bounds — both are needed for defence in depth.  
- **Downstream impact:** the same **`week4-cartographer`** subscription applies: drift on **`extracted_facts.confidence`** signals **semantic movement** even before every row violates `[0,1]`, so projections that **rely on stable ranking statistics** can drift silently; **`…fact_confidence.drift`** is the early-warning path, while **`…fact_confidence.range`** is the hard stop once scale escapes the unit interval.

### 2.2 Week 5 — temporal / sequence injection (`outputs/week5/events_violated_temporal_and_sequence.jsonl`)

**Report:** `validation_reports/violated_week5_final.json` — **contract** `week5-event-records`

| Metric | Value |
|--------|--------|
| **report_id** | `338ce18a-5e52-4b8c-97b0-e0c17f8be3ad` |
| **total_checks** | 31 |
| **passed / failed** | 29 / **2** |

**Failures (summary):**

- **Temporal CRITICAL:** `week5.temporal.recorded_gte_occurred` on **`recorded_at` vs `occurred_at`** — violates **recorded_at ≥ occurred_at** (audit timeline invariant).  
- **Sequence CRITICAL:** `week5.sequence.monotonic_per_aggregate` — **strict +1** `sequence_number` per **`aggregate_id`** broken by duplicate/gapped sequence in the injected slice.  

**Downstream consumer impact (named) — Week 5 failures**

- **`week7-data-contract-enforcer`** is the registered subscriber for **`week5-event-records`** in **`contract_registry/subscriptions.yaml`**. If these clauses fail in production, **replay / projection** code that assumes a **strict monotonic stream** and **valid audit ordering** can **double-apply** events, **skip** events, or build **incorrect aggregate state** while the write path still returns **200 OK** — because the failure is **semantic**, not a transport error.

### 2.3 Clean baseline (contrast)

**Week 3 clean:** `validation_reports/week3_clean_baseline_final.json` — **47** checks, **47** passed, **0** failed (`report_id` `f9dea2ec-8da9-47a3-b734-6ec41a69bcc5`).  
This establishes that failures in §2.1–2.2 are **attributable to the violated snapshots**, not universal noise.

---

## 3. Violation deep-dive — blame chain and blast radius

**Focus check:** `week3-document-refinery-extractions.fact_confidence.range` on **`extracted_facts.confidence`**.

### 3.1 Failing check and field

- **check_id:** `week3-document-refinery-extractions.fact_confidence.range`  
- **Field:** `extracted_facts.confidence` (runner column `fact_confidence`)  
- **Evidence:** `validation_reports/violated_week3_scale_final.json` (actual min/max vs `[0,1]`).

### 3.2 Lineage traversal (step-by-step)

Using `outputs/week4/lineage_snapshots.jsonl` (latest snapshot):

1. **Failing schema element** is enforced at the **Week 7 consumer** boundary on file artifact **`file::outputs/week3/extractions.jsonl`** (logical producer of extraction JSONL in the graph).  
2. **Edge:** `file::outputs/week3/extractions.jsonl` → **`pipeline::week7-contract-enforcer`**, relationship **`CONSUMES`** (pipeline consumes the file).  
3. **Upstream producer file** for git purposes is the **Refinery path** embedded in node metadata (see `violation_log` / report narrative: path under **The-Document-Intelligence-Refinery** `.refinery/extractions`).  
4. **Interpretation:** the “meaning” break originates where **facts and confidence** are materialised **before** Week 7 ingestion.

### 3.3 Blame chain — commits, ranking, formula

**Scoring formula (implemented in `contracts/attributor.py`):**

\[
\text{confidence\_score} = \max\left(0,\ 1.0 - (\text{days\_since\_commit} \times 0.1) - (0.2 \times \text{lineage\_hops})\right)
\]

where **`days_since_commit`** is **fractional** (seconds ÷ 86400) between violation detection time and commit time, and **`lineage_hops`** counts upstream steps from the Week 7 consumer to the producer file node.

**Ranked blame candidates (live values from `enforcer_report/report_data.json` → `violations_this_week` for check `…fact_confidence.range`):**

| Rank | `commit_hash` | Author | `commit_timestamp` | `commit_message` (truncated) | **`confidence_score`** |
|------|----------------|--------|--------------------|------------------------------|------------------------|
| 1 | `a2449ea80e55285f092e725f5d5ad3b9963d6d6a` | Bnobody47 | 2026-03-04 10:35:13 +0300 | first commit | **0.0** |

**Ranks 2–5:** in this environment **`git log -- …/.refinery/extractions`** returned **only one** commit within the search window, so the attributor **does not fabricate** additional rows — the table is complete for the evidence run.

**Attribution confidence (plain language):** a **0.0** score means **low confidence** that this specific commit *introduced* the violation (penalty from age + hops), not that the hash is fake. For **higher-confidence** attribution you would add **line-level `git blame`** on the exporter that writes `extracted_facts[].confidence` and widen **`git log`** depth.

### 3.4 Blast radius — direct subscribers vs transitively contaminated nodes

**Direct (contract registry — authoritative for blast radius in this design)**

- **Subscriber:** **`week4-cartographer`** (`contract_registry/subscriptions.yaml`, subscription on `week3-document-refinery-extractions`, `breaking_fields` includes **`extracted_facts.confidence`**).  
- **Meaning:** this team **must** be notified before shipping a change that alters confidence semantics.

**Transitive (lineage graph — `outputs/week4/lineage_snapshots.jsonl`, latest snapshot)**

- **Producer file node:** `file::outputs/week3/extractions.jsonl` (metadata path points at **The-Document-Intelligence-Refinery** `.refinery/extractions`).  
- **Forward walk from producer (edges where `relationship: CONSUMES`, source → target):** the **only** downstream node in the captured graph is **`pipeline::week7-contract-enforcer`** (Week 7 runner / enforcer). So **transitively contaminated graph nodes** = **`{ pipeline::week7-contract-enforcer }`** for this snapshot — the enforcer **consumes** the bad file before any other node appears in `nodes[]`.  
- **Why `week4-cartographer` is not in `nodes[]`:** the sample lineage file does **not** materialise a separate pipeline node for the cartographer; **registry** still records the **business** subscriber. In a fuller graph you would expect an edge **Refinery file → cartographer pipeline** and would list those node IDs under **transitive** or **direct graph** consumers.

**`contamination_depth` (when using current `contracts/attributor.py`):** additive **registry hit count + upstream hops + forward BFS depth** from producer (see `blast_radius` object in freshly generated `violation_log/violations.jsonl`).

If your submitted `violation_log/violations.jsonl` still shows **`direct_subscribers`** only (older attributor), re-run:

```text
python contracts/attributor.py --violation validation_reports/violated_week3_scale_final.json --lineage outputs/week4/lineage_snapshots.jsonl --contract generated_contracts/week3_extractions.yaml --output violation_log/violations.jsonl
```

…then regenerate `enforcer_report/report_data.json`.

---

## 4. Schema evolution case study

### 4.1 Evidence — timestamped snapshots

- **Old:** `schema_snapshots/week3-document-refinery-extractions/20260403_080206.yaml` (profiled from **clean** extractions)  
- **New:** `schema_snapshots/week3-document-refinery-extractions/20260403_080258.yaml` (profiled after **violated** scale-change dataset regenerated contracts)

**Analyser output:** `validation_reports/schema_evolution_week3_rubric.json` (taxonomy-aware; produced with `contracts/schema_analyzer.py` including `--since` when needed).

### 4.2 Human-readable schema diff (before / after) — `schema.fact_confidence`

The **contract clause** (`type`, `required`, `minimum`, `maximum`, `description`) is **unchanged** between snapshots; what changes is the **profiled `stats`** block embedded by `ContractGenerator`, proving **semantic drift** while the YAML still *claims* `[0,1]`.

**BEFORE** — `schema_snapshots/week3-document-refinery-extractions/20260403_080206.yaml` (clean data):

```yaml
  fact_confidence:
    type: number
    required: true
    description: "Per-fact confidence 0.0–1.0. Silent failure mode: rescaling to 0–100 passes type checks but breaks thresholds."
    minimum: 0.0
    maximum: 1.0
    stats:
      min: 0.611
      max: 0.976
      mean: 0.8021166666666666
      # … percentiles / stddev …
```

**AFTER** — `schema_snapshots/week3-document-refinery-extractions/20260403_080258.yaml` (violated scale-change data):

```yaml
  fact_confidence:
    type: number
    required: true
    description: "Per-fact confidence 0.0–1.0. Silent failure mode: rescaling to 0–100 passes type checks but breaks thresholds."
    minimum: 0.0
    maximum: 1.0
    stats:
      min: 61.1
      max: 97.6
      mean: 80.21166666666667
      # … percentiles / stddev …
```

**Summary table**

| Aspect | Before (clean profile) | After (violated profile) |
|--------|-------------------------|---------------------------|
| **`stats.max`** | **0.976** | **97.6** |
| **`stats.mean`** | **~0.802** | **~80.21** |
| **Clause bounds** | `minimum: 0.0`, `maximum: 1.0` | **unchanged** — **data** no longer fits the **documented** contract |

### 4.3 Taxonomy classification

- **Verdict:** **BREAKING**  
- **Taxonomy label:** **`CONFIDENCE_SCALE_DRIFT`** (see `changes[]` for `fact_confidence` in `schema_evolution_week3_rubric.json`)  
- **Narrow type analogue:** the analyser also classifies **float 0–1 → integer 0–100** style changes as **CRITICAL** **`NARROW_TYPE_SCALE_FLOAT_TO_INT`** when type metadata shifts; here the **dominant signal** is **scale drift** on the **same numeric column** with **CRITICAL** severity.

### 4.4 Migration impact (≥2 concrete steps before ship)

From `migration_impact_report.migration_checklist` in `validation_reports/schema_evolution_week3_rubric.json`:

1. **Notify consumers** listed under **`blast_radius_from_registry`** (e.g. **`week4-cartographer`**) and update **`contract_registry/subscriptions.yaml`** if fields or semantics change.  
2. **Regenerate contracts** from the **new** canonical JSONL (`contracts/generator.py`), then **`contracts/runner.py --mode AUDIT`** on staging data until **FAIL=0** for required clauses.  
3. **Re-establish statistical baselines** after the producer is corrected: remove or regenerate **`schema_snapshots/baselines_week3-document-refinery-extractions.json`** and **`schema_snapshots/baselines.json`** so drift is not compared to an obsolete world.

### 4.5 Rollback plan + **explicit** statistical baselines to re-establish

From `validation_reports/schema_evolution_week3_rubric.json` → **`migration_impact_report.rollback_plan`**, plus the **exact baseline files** this repo uses for **mean/stddev drift**:

1. **Revert producer** to the commit that produced the **older** timestamped snapshot (`20260403_080206.yaml` or equivalent).  
2. **Restore** prior **`generated_contracts/week3_extractions.yaml`** (and alias files) from git.  
3. **Verify:**  
   `python contracts/runner.py --contract generated_contracts/week3_extractions.yaml --data outputs/week3/extractions.jsonl --output validation_reports/rollback_check.json --mode AUDIT`  
4. **Delete or regenerate these statistical baselines** so drift checks do not compare “rolled-back” data to **pre-rollback** moments:  
   - **`schema_snapshots/baselines_week3-document-refinery-extractions.json`** — per-contract **mean/stddev** for `ValidationRunner` drift on Week 3.  
   - **`schema_snapshots/baselines.json`** — aggregate written by **`contracts/generator.py`** (multi-contract rollup).  
   - **Optional:** if you re-ran the generator on clean data, allow **`contracts/generator.py`** to rewrite baselines, **or** delete the files above and let **`contracts/runner.py`** recreate **`baselines_<contract_id>.json`** on the next clean run.

Without step **4**, **`week3-document-refinery-extractions.fact_confidence.drift`** can **false-alarm** or **false-calm** because z-scores target the **wrong** historical distribution.

### 4.6 Production tool comparison

**Confluent Schema Registry** would typically **reject incompatible schema registration** at the boundary (or enforce compatibility mode), blocking downstream consumption **up front**. Our **`schema_analyzer.py`** compares **timestamped YAML snapshots post hoc**: it **detects** the same class of semantic break (confidence scale) **after** data/profile drift appears, which is why **consumer-side `ValidationRunner`** and **registry subscriptions** remain necessary for **fast feedback** in this repo’s Week 7 design.

---

## 5. AI contract extension results

**Sources:** `validation_reports/ai_extensions_baseline.json` (first run / baseline establishment) and `validation_reports/ai_extensions_violated.json` (second run on violated + bad verdicts).

### 5.1 Embedding drift — **cosine distance** method and threshold

**Method (explicit):** `contracts/ai_extensions.py` builds a **fixed-dimension surrogate embedding** per fact text (L2-normalised bag-of-ngram counts), computes the **centroid vector** over the sampled facts, then measures drift as **cosine distance**:

\[
d = 1 - \cos(\theta) = 1 - \frac{\mathbf{c}_{\text{current}} \cdot \mathbf{c}_{\text{baseline}}}{\|\mathbf{c}_{\text{current}}\| \, \|\mathbf{c}_{\text{baseline}}\|}
\]

**Decision rule:** **FAIL** if **`d > 0.15`**, else **PASS** (after baseline is stored in **`schema_snapshots/embedding_baselines.npz`**).

| Run | Input | **Cosine distance** \(d\) | Threshold | Conclusion |
|-----|--------|---------------------------|-----------|------------|
| **Data point 1** (baseline) | `outputs/week3/extractions.jsonl` + first write of `embedding_baselines.npz` | **0.0** (baseline established, no prior centroid) | **0.15** | `BASELINE_SET` |
| **Data point 2** (violated) | `outputs/week3/extractions_violated_scale_change.jsonl` vs stored centroid | **0.000277** | **0.15** | **PASS** — \(d \ll 0.15\) |

**Interpretation:** the **cosine-distance** gate did **not** fire on this injection because the surrogate is driven by **character n-grams**, not raw numeric confidence; a **large paraphrase** of fact text would increase \(d\) toward the **0.15** tripwire even when JSON types still validate.

### 5.2 LLM output schema violation rate — **two data points** and trend

**Metric:** fraction of Week 2 verdict rows where **`overall_verdict ∉ {PASS, FAIL, WARN}`** (see `contracts/ai_extensions.py`).

| Data point | Source file | Total rows | Invalid | **Violation rate** | Compared to |
|------------|-------------|------------|---------|---------------------|-------------|
| **1** | `outputs/week2/verdicts.jsonl` | 60 | 0 | **0.00%** | *(baseline written to `schema_snapshots/ai_output_violation_baseline.json`)* |
| **2** | `outputs/week2/verdicts_violated_output_schema.jsonl` | 60 | 1 | **1.6667%** | **0.00%** baseline → **trend: rising** |

- **Policy:** `warn_threshold` **0.02** (2%) in `contracts/ai_extensions.py`; violated run status **`WARN`** (rising trend from **0%** also contributes).  
- **Plausible cause:** `outputs/week2/verdicts_violated_output_schema.jsonl` sets **`overall_verdict`** to an **out-of-enum** value (**`BROKEN`**), simulating **schema drift in auditor output**.

### 5.3 Prompt input validation (valid vs quarantined)

| Run | Valid | Quarantined | Quarantine file |
|-----|-------|-------------|-----------------|
| Baseline | 60 | 0 | — |
| Violated | **59** | **1** | `outputs/quarantine/trace_prompt_quarantine.jsonl` |

**Plausible cause:** **`content_preview`** built from concatenated fact text exceeded the **8000** character JSON Schema limit after intentional long-text injection on the violated Week 3 dataset.

### 5.4 Are AI outputs “trustworthy” right now?

- **On clean baselines:** all three extensions **PASS** or establish baselines — **trust is higher** for the **specific snapshots** validated.  
- **On violated runs:** **prompt validation FAIL** and **output violation rate WARN** mean **you should not trust** downstream automation that assumes **strict prompt envelope** and **verdict enum stability** without **quarantine review** and **auditor fixes**.  
- **Limitation:** embedding drift uses a **lightweight surrogate**, not OpenAI/Vertex embeddings — treat PASS as **necessary but not sufficient** for semantic safety.

---

## 6. Highest-risk interface analysis

### 6.1 Interface and schema (named)

- **Interface:** **Week 3 Document Refinery → Week 4 Brownfield Cartographer** over artifact **`outputs/week3/extractions.jsonl`** / contract **`week3-document-refinery-extractions`**.  
- **Schema:** `generated_contracts/week3_extractions.yaml` (Bitol-style YAML + dbt counterpart `generated_contracts/week3_extractions_dbt.yml`).

### 6.2 Realistic failure scenario (structural + statistical)

**Scenario:** A deploy changes the refinery exporter so **`extracted_facts[].confidence`** is written as **integer percent (0–100)** instead of **unit probability (0.0–1.0)**. All fields remain **numbers**, JSON validates, and **downstream ETL** still casts to float.

- **Structural class:** values **> 1.0** violate the **documented** `maximum: 1.0` — caught by **`week3-document-refinery-extractions.fact_confidence.range`** (**CRITICAL**).  
- **Statistical class:** if a bug **re-centred** values inside `[0,1]` incorrectly (e.g. dividing by the wrong constant) so **range still passes**, the **mean/stddev** against **`schema_snapshots/baselines_week3-document-refinery-extractions.json`** can still move — caught by **`week3-document-refinery-extractions.fact_confidence.drift`** (**WARNING** at 2σ, **FAIL/HIGH** at 3σ in this implementation).

### 6.3 Highest-risk field

- **Field:** **`extracted_facts[].confidence`** (runner column **`fact_confidence`**).

### 6.4 Enforcement gap — which checks catch vs miss this scenario

| Mechanism | Would **catch** this scenario? | Why |
|-----------|-------------------------------|-----|
| **`week3-document-refinery-extractions.fact_confidence.type`** | **Partial** | Still `number` after rescale — **misses** the semantic bug by design. |
| **`week3-document-refinery-extractions.fact_confidence.range`** | **Yes** (0–100 case) | **Fails** when **max > 1.0** or **min < 0.0**. |
| **`week3-document-refinery-extractions.fact_confidence.drift`** | **Yes** (distribution shift) | **Fails** when mean moves vs baseline; **may miss** if attacker keeps narrow band inside `[0,1]` with no baseline update. |
| **dbt `not_null` on `fact_confidence`** | **No** | Nulls only — not scale. |
| **Singular SQL `singular_week3_confidence_range.sql`** | **Yes** | Same **0–1** bound idea in the warehouse layer. |
| **Embedding drift (`ai_extensions`)** | **Unreliable for this** | Surrogate embeddings track **text**, not **numeric confidence** — **do not** use as substitute for **`fact_confidence`** clauses. |

**Gap summary:** the **highest residual risk** is a change that **preserves** `[0,1]` **bounds** but **breaks calibration** (different mapping of model logits to probability). That requires **tighter distributional clauses**, **golden-file tests**, or **monitoring** beyond basic range.

### 6.5 Blast radius if this failure reached production

- **Registry (contractual):** **`week4-cartographer`** — see `contract_registry/subscriptions.yaml` (`extracted_facts.confidence` in **`breaking_fields`**). **Effect:** wrong confidence **poisons ranking**, **heatmaps**, and any **“explain the graph”** UX that trusts refinery scores.  
- **Lineage graph (as captured):** **`pipeline::week7-contract-enforcer`** consumes **`file::outputs/week3/extractions.jsonl`** — in production, **any CI job** using **`--mode AUDIT` only** would **log** the failure but **not block**; **`--mode WARN` / `ENFORCE`** is required to **stop** the bad file from being treated as **green**.  
- **Secondary:** anything that **reuses** the same JSONL for **RAG** or **prompt context** without re-validation inherits the wrong scale **silently**.

### 6.6 Concrete mitigation (named clause + mode upgrade)

1. **Keep** contract clauses **`week3-document-refinery-extractions.fact_confidence.range`** and **`week3-document-refinery-extractions.fact_confidence.drift`** in `generated_contracts/week3_extractions.yaml`.  
2. **Upgrade enforcement:** run ingestion/CI with **`python contracts/runner.py --contract generated_contracts/week3_extractions.yaml --data outputs/week3/extractions.jsonl --output validation_reports/ci_gate.json --mode ENFORCE`** so **CRITICAL + HIGH** failures **exit non-zero** and block deploy (see `contracts/runner.py`).  
3. **Registry hygiene:** retain **`week4-cartographer`** under **`breaking_fields`** for **`extracted_facts.confidence`** so blast-radius mail/Slack lists stay accurate.

---

## 7. PDF / submission checklist

1. Run **`python contracts/report_generator.py`** so **`enforcer_report/report_data.json`** timestamps match your submission.  
2. Export **this Markdown** to PDF (Cursor / Typora / pandoc).  
3. Submit **GitHub link** + **Drive PDF** per course instructions.  
4. Ensure **`violation_log/violations.jsonl`** and **`validation_reports/*.json`** referenced here are **committed** or attached as instructed.

---

*End of final report.*
