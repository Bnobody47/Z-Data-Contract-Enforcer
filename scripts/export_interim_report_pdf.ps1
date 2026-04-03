# Optional: export reports/TRP1_Week7_Interim_Report.md to PDF using pandoc.
# Install pandoc: winget install --id JohnMacFarlane.Pandoc
$root = Split-Path -Parent (Split-Path -Parent $MyInvocation.MyCommand.Path)
$md = Join-Path $root "reports\TRP1_Week7_Interim_Report.md"
$pdf = Join-Path $root "reports\TRP1_Week7_Interim_Report.pdf"
if (-not (Test-Path $md)) { Write-Error "Missing $md"; exit 1 }
$pandoc = Get-Command pandoc -ErrorAction SilentlyContinue
if (-not $pandoc) {
    Write-Host "pandoc not found. Install with: winget install JohnMacFarlane.Pandoc"
    Write-Host "Or open the .md in Word/Google Docs and export to PDF."
    exit 1
}
& pandoc $md -o $pdf --from markdown --pdf-engine=wkhtmltopdf 2>$null
if ($LASTEXITCODE -ne 0) {
    & pandoc $md -o $pdf --from markdown
}
if (Test-Path $pdf) { Write-Host "Wrote $pdf" } else { Write-Host "PDF generation may require a LaTeX engine; try opening the .md in VS Code and Print to PDF." }
