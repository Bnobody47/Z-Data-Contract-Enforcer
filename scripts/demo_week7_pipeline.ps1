# Week 7 demo - runs the full enforcer pipeline in rubric order (1 through 6).
# Usage (from anywhere):
#   powershell -ExecutionPolicy Bypass -File "C:\...\scripts\demo_week7_pipeline.ps1"
# Or: cd repo root, then .\scripts\demo_week7_pipeline.ps1
#
# For the recorded demo you can still type each command from the markdown script;
# use this file to rehearse or to run everything once with clear on-screen labels.

$ErrorActionPreference = "Stop"
$Root = Split-Path -Parent (Split-Path -Parent $MyInvocation.MyCommand.Path)
Set-Location $Root

function Step-Banner($n, $title) {
    Write-Host ""
    Write-Host "========== STEP $n - $title ==========" -ForegroundColor Cyan
}

Step-Banner 1 "Contract generation (ContractGenerator)"
python contracts\generator.py `
    --source outputs\week3\extractions.jsonl `
    --contract-id week3-document-refinery-extractions `
    --lineage outputs\week4\lineage_snapshots.jsonl `
    --output generated_contracts

Step-Banner 2 "Violation detection (ValidationRunner, violated data)"
python contracts\runner.py `
    --contract generated_contracts\week3_extractions.yaml `
    --data outputs\week3\extractions_violated_scale_change.jsonl `
    --output validation_reports\demo_violation.json `
    --mode AUDIT

Step-Banner 3 "Blame chain (ViolationAttributor)"
python contracts\attributor.py `
    --violation validation_reports\demo_violation.json `
    --lineage outputs\week4\lineage_snapshots.jsonl `
    --contract generated_contracts\week3_extractions.yaml `
    --output violation_log\demo_violations.jsonl

Step-Banner 4 "Schema evolution (SchemaEvolutionAnalyzer)"
python contracts\schema_analyzer.py `
    --contract-id week3-document-refinery-extractions `
    --output validation_reports\demo_schema.json

Step-Banner 5 "AI contract extensions (ai_extensions, clean Week 3 + Week 2)"
python contracts\ai_extensions.py `
    --mode all `
    --extractions outputs\week3\extractions.jsonl `
    --verdicts outputs\week2\verdicts.jsonl `
    --output validation_reports\demo_ai.json `
    --violation-log violation_log\demo_ai_warn.jsonl

Step-Banner 6 "Enforcer report (ReportGenerator)"
python contracts\report_generator.py --output enforcer_report\report_data.json

Write-Host ""
Write-Host "Done. Open these to narrate the demo:" -ForegroundColor Green
Write-Host "  generated_contracts\week3_extractions.yaml"
Write-Host "  validation_reports\demo_violation.json"
Write-Host "  violation_log\demo_violations.jsonl"
Write-Host "  validation_reports\demo_schema.json"
Write-Host "  validation_reports\demo_ai.json"
Write-Host "  enforcer_report\report_data.json"
