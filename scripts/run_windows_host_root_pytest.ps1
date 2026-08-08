[CmdletBinding()]
param(
    [switch]$PreflightOnly,
    [Parameter(ValueFromRemainingArguments = $true)]
    [string[]]$PytestArguments = @()
)

$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

$expectedRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$gitRoot = (& git -C $expectedRoot rev-parse --show-toplevel).Trim()
if ($LASTEXITCODE -ne 0 -or
    $gitRoot.Replace("\", "/") -ne $expectedRoot.Replace("\", "/")) {
    throw "Repository identity mismatch; refusing Windows host-root pytest launch."
}

$python = Join-Path $expectedRoot ".venv\Scripts\python.exe"
if (-not (Test-Path -LiteralPath $python -PathType Leaf)) {
    throw "Repository virtual-environment interpreter is absent: $python"
}

foreach ($argument in $PytestArguments) {
    if ($argument -match "^--basetemp(?:=|$)") {
        throw "Explicit --basetemp is forbidden for the Windows host-root full suite."
    }
}

$driveRoot = [IO.Path]::GetPathRoot($expectedRoot)
if ([string]::IsNullOrWhiteSpace($driveRoot)) {
    throw "Cannot determine the repository drive root."
}
$probeName = "fv2p-{0:x}-{1}" -f $PID, ([guid]::NewGuid().ToString("N").Substring(0, 4))
$probePath = Join-Path $driveRoot $probeName

try {
    [void][IO.Directory]::CreateDirectory($probePath)
    if (-not (Test-Path -LiteralPath $probePath -PathType Container)) {
        throw "Drive-root probe was not observable after creation."
    }
}
catch {
    Write-Error (
        "WINDOWS_HOST_ROOT_TEMP_UNAVAILABLE: cannot create the required " +
        "short test directory $probePath. Run this launcher in an execution " +
        "environment with drive-root create/delete access. Pytest was not started."
    )
    exit 86
}
finally {
    if (Test-Path -LiteralPath $probePath -PathType Container) {
        Remove-Item -LiteralPath $probePath -Force
    }
}

Write-Output "WINDOWS_HOST_ROOT_TEMP_VERIFIED"
if ($PreflightOnly) {
    exit 0
}

if ($PytestArguments.Count -eq 0) {
    $PytestArguments = @(
        "-q",
        "--junitxml=.pytest_tmp/full-suite.xml"
    )
}

Push-Location $expectedRoot
try {
    & $python "-m" "pytest" @PytestArguments
    $pytestExitCode = $LASTEXITCODE
}
finally {
    Pop-Location
}
exit $pytestExitCode
