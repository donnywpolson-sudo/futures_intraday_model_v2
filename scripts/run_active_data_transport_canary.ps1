[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)]
    [string]$Plan,

    [Parameter(Mandatory = $true)]
    [string]$Approval
)

$ErrorActionPreference = "Stop"
$expectedRoot = [IO.Path]::GetFullPath(
    "C:\Users\donny\Desktop\futures_intraday_model_v2"
).TrimEnd("\")
$root = [IO.Path]::GetFullPath(
    (& git rev-parse --show-toplevel).Trim()
).TrimEnd("\")
if ($root -ine $expectedRoot) {
    throw "Repository root mismatch: $root"
}
$rootItem = Get-Item -LiteralPath $root -Force
if (($rootItem.Attributes -band [IO.FileAttributes]::ReparsePoint) -ne 0) {
    throw "Repository root is a reparse point."
}
$python = Join-Path $root ".venv\Scripts\python.exe"
$planPath = [IO.Path]::GetFullPath((Join-Path $root $Plan))
$approvalPath = [IO.Path]::GetFullPath((Join-Path $root $Approval))
$jobSource = Join-Path $root "scripts\WindowsKillOnCloseProcess.cs"
$verificationJson = & $python -m `
    futures_rebuild.durable_windows_task_transport `
    verify-approved `
    --repository-root $root `
    --plan $planPath `
    --approval $approvalPath
if ($LASTEXITCODE -ne 0) {
    throw "Transport canary approval revalidation failed."
}
$null = $verificationJson | ConvertFrom-Json
Add-Type -Path $jobSource

$probe = [FuturesRebuild.DurableJobProcess]::ProbeKillOnClose(
    $env:ComSpec,
    $root
)
$arguments = @(
    "-m",
    "futures_rebuild.durable_windows_task_transport",
    "run",
    "--repository-root",
    "`"$root`"",
    "--plan",
    "`"$planPath`"",
    "--approval",
    "`"$approvalPath`"",
    "--containment-owner-process-id",
    [string]$PID,
    "--containment-child-process-id",
    [string]$probe.ChildProcessId,
    "--containment-child-exit-code",
    [string]$probe.ChildExitCode
) -join " "
$exitCode = [FuturesRebuild.DurableJobProcess]::RunContained(
    $python,
    $arguments,
    $root
)
exit $exitCode
