param(
    [Parameter(Mandatory)] [string] $Manifest,
    [Parameter(Mandatory)] [string] $RunRoot
)

$ErrorActionPreference = "Stop"

$python = Join-Path $PSScriptRoot "..\.venv\Scripts\python.exe"
$runner = Join-Path $PSScriptRoot "run_intraday_synthetic_certification_v12.py"

foreach ($ordinal in 1, 2) {
    & $python $runner --manifest $Manifest --run-root $RunRoot --run-ordinal $ordinal --exact
    if ($LASTEXITCODE -ne 0) { throw "Synthetic certification run $ordinal failed." }
}
