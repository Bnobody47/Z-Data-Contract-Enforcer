# TRP1 Week 7 — Demo Video Script (explainable + runnable scripts)

**Goal:** Walk a grader through **what** each piece does and **why** it matters—not only that it runs. **Same live order:** generate → validate → blame → schema evolution → AI extensions → report.

**Length:** If you use every **Say** line below, plan **~5–7 minutes**. To stay tighter, shorten the **Say** blocks and keep **Why** in one sentence each.

**Repo root:** `Z Data Contract Enforcer`. Large terminal font; zoom JSON/YAML when you open files.

---

## Runnable script (rehearsal or one-shot run)

From repo root:

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\demo_week7_pipeline.ps1
```

That runs all six Python steps with **STEP 1 … STEP 6** banners so the flow is obvious on screen. For the **final recording**, either:

- **Type or paste** each command from this doc (feels more “live”), or  
- **Run the `.ps1` once**, then re-open each output file and narrate using the **Say** lines below.

**Pre-record (off camera):** `pip install -r requirements.txt`, then pre-run AI once so baselines exist:

```powershell
python contracts\ai_extensions.py --mode all --extractions outputs\week3\extractions.jsonl --verdicts outputs\week2\verdicts.jsonl --output validation_reports\demo_ai_precheck.json
```

---

## Big picture (optional 20–30 s after open)

**Say:**  
“A **data contract** is a machine-checkable promise about shape and meaning of data. This repo **infers** a contract from real Week 3 JSONL, **validates** new files against it, **attributes** failures using lineage and git, **diffs** schema snapshots for breaking changes, adds **AI-adjacent** checks on embeddings and prompts, then **rolls everything into one report** for humans.”

---

## Step 1 — Contract generation

**Why this step:** Nobody hand-writes every field. The generator **profiles** actual data and emits YAML the runner can enforce—so the contract stays tied to production-like samples.

**Say:**  
“I’m running **ContractGenerator**. It reads **`outputs/week3/extractions.jsonl`**, infers types and ranges, and writes a Bitol-style contract under **`generated_contracts/`**. I also pass **`lineage_snapshots`** so downstream relationships can be reflected where the generator supports it.”

**Run:**

```powershell
cd "C:\Users\Bnobody_47\Documents\Z Data Contract Enforcer"
python contracts\generator.py --source outputs\week3\extractions.jsonl --contract-id week3-document-refinery-extractions --lineage outputs\week4\lineage_snapshots.jsonl --output generated_contracts
```

**Show:** Open `generated_contracts\week3_extractions.yaml` (or `dir generated_contracts\week3*.yaml` if the name differs).

**Say:**  
“Under **`schema:`** there are **many** clauses—**more than eight** for the rubric. I’m calling out **`fact_confidence`**: **`minimum: 0.0`**, **`maximum: 1.0`**. That documents the promise that **`extracted_facts[].confidence`** stays on a **0–1** scale after the runner flattens facts—so if someone ships **0–100**, we can catch it automatically.”

---

## Step 2 — Violation detection

**Why this step:** The contract is useless without a **ValidationRunner** that returns structured **PASS/FAIL** per check, with **severity** and **how many rows** broke the rule.

**Say:**  
“Step two is **validation**. I’m pointing the **same** contract at a **deliberately violated** file: confidence was rescaled to **0–100** instead of **0–1**. The runner should emit JSON with a clear **FAIL** on the **range** check—not just ‘something went wrong’.”

**Run:**

```powershell
python contracts\runner.py --contract generated_contracts\week3_extractions.yaml --data outputs\week3\extractions_violated_scale_change.jsonl --output validation_reports\demo_violation.json --mode AUDIT
```

**Show:** `validation_reports\demo_violation.json` — search for **`fact_confidence`** / **`range`**.

**Say:**  
“Here’s the machine-readable report: **`report_id`**, **`contract_id`**, **`results`**. This row is **FAIL** for **`week3-document-refinery-extractions.fact_confidence.range`**. **`severity`** is **CRITICAL**, and **`records_failing`** is **greater than zero**—that’s the semantic break between **0–1** and **0–100**.”

---

## Step 3 — Blame chain

**Why this step:** Operators need **who/what broke the promise** and **who downstream is at risk**. The attributor connects the failing check to **lineage**, **git**, and **registry subscribers** (blast radius).

**Say:**  
“Step three is **attribution**. Starting from the JSON from step two, the tool walks **subscriptions** and **lineage** to see how the bad file flows into pipelines, then suggests **git** candidates. **`blast_radius`** is the ‘who else cares’ list—critical for incident response.”

**Run:**

```powershell
python contracts\attributor.py --violation validation_reports\demo_violation.json --lineage outputs\week4\lineage_snapshots.jsonl --contract generated_contracts\week3_extractions.yaml --output violation_log\demo_violations.jsonl
```

**Show:** `violation_log\demo_violations.jsonl` (last object is fine).

**Say:**  
“The failing check was **`fact_confidence.range`**. You can see the **lineage story**: the Week 3 extraction artifact **feeds** the Week 7 path via a **CONSUMES** edge. **Git** gives a **`commit_hash`** and **`author`**. **`blast_radius`** lists downstream subscribers—for example **week4-cartographer**—so we know impact beyond this one file.”

**Optional note if scores look low:**  
“Confidence scores can be modest in a shallow clone; the **hash and author** are still the rubric-relevant signals.”

---

## Step 4 — Schema evolution

**Why this step:** Contracts **change**. We need a **breaking vs non-breaking** verdict, a **taxonomy** of what changed, and a **migration / rollback** narrative—not only a raw diff.

**Say:**  
“Step four compares **timestamped schema snapshots** for this contract ID. The analyzer labels compatibility—for example **BREAKING** when semantics drift—and emits a **migration impact report** graders can read without digging through git.”

**Run:**

```powershell
python contracts\schema_analyzer.py --contract-id week3-document-refinery-extractions --output validation_reports\demo_schema.json
```

**Show:** `validation_reports\demo_schema.json` — `compatibility_verdict`, `changes`, `migration_impact_report`.

**Say:**  
“**`compatibility_verdict`** is **BREAKING**. **`changes`** uses a **taxonomy**—for instance **CONFIDENCE_SCALE_DRIFT** on **`fact_confidence`**. Scrolling to **`migration_impact_report`**: there’s a **checklist**, downstream hints, and a **rollback_plan**—that’s how we’d plan a safe rollout in production.”

---

## Step 5 — AI extensions

**Why this step:** LLM pipelines need guardrails beyond classical schema checks: **embedding drift** (are inputs shifting?), **prompt hygiene** (quarantine bad inputs), and **output violation rate** vs baselines.

**Say:**  
“Step five runs **AI extensions** on **real** paths: Week 3 extractions and Week 2 verdicts. I pre-ran once off camera so **baselines** exist; this run shows **three** metrics in one JSON file.”

**Run:**

```powershell
python contracts\ai_extensions.py --mode all --extractions outputs\week3\extractions.jsonl --verdicts outputs\week2\verdicts.jsonl --output validation_reports\demo_ai.json --violation-log violation_log\demo_ai_warn.jsonl
```

**Show:** `validation_reports\demo_ai.json`.

**Say:**  
“**One — embedding drift:** a numeric **`drift_score`** compared to threshold **0.15** (cosine distance on centroids in code). **Two — prompt input validation:** **`valid_count`** versus **`quarantined_count`**. **Three — output violation rate:** rate as a fraction, with **baseline** and **trend** so we see if quality is sliding.”

---

## Step 6 — Enforcer report

**Why this step:** Executives and on-call engineers want **one artifact**: a **health score** and **plain-language** top issues, synthesized from validation outputs already on disk.

**Say:**  
“Last step aggregates into **`enforcer_report/report_data.json`**: a **0–100 data health score** and human-readable **`violations_this_week`** so the team can triage without opening every JSON report.”

**Run:**

```powershell
python contracts\report_generator.py --output enforcer_report\report_data.json
```

**Show:** `enforcer_report\report_data.json`.

**Say:**  
“**`data_health_score`** is **[read the number]**—that’s the rolled-up signal. From **`violations_this_week`**, I’ll read the **top three** headlines: **[read 1]**, **[read 2]**, **[read 3]**.”

---

## Close

**Say:**  
“That’s the pipeline: **infer a contract, validate data, attribute failures, analyze schema evolution, run AI-adjacent checks, and publish one report**. The runnable script is **`scripts/demo_week7_pipeline.ps1`**; all artifacts are on disk for review. Thanks.”

---

## Rubric quick map

| Step | Must show |
|------|-----------|
| 1 | Live generator; YAML open; **≥8** schema clauses; **`fact_confidence` 0–1** explained |
| 2 | Live runner on violated JSONL; **FAIL** on `…fact_confidence.range`; **severity** + **records_failing > 0** |
| 3 | Live attributor; **commit + author**; **blast radius** / downstream; lineage narrated |
| 4 | Live analyzer; **BREAKING** + taxonomy; **`migration_impact_report`** |
| 5 | Live AI; **Week 3 + Week 2** paths; **three metrics** visible |
| 6 | Live report; **`data_health_score`**; **top 3** plain language |

---

## Quick fixes

- No range FAIL? Use **`extractions_violated_scale_change.jsonl`**.  
- Wrong YAML? `dir generated_contracts\week3*.yaml`.  
- Attributor empty? Regenerate **`validation_reports\demo_violation.json`** from step 2 first.

---

*End.*
