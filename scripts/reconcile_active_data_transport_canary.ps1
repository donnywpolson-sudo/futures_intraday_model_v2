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
$python = Join-Path $root ".venv\Scripts\python.exe"
$planPath = [IO.Path]::GetFullPath((Join-Path $root $Plan))
$approvalPath = [IO.Path]::GetFullPath((Join-Path $root $Approval))
$descriptionJson = & $python -m futures_rebuild.durable_windows_task_transport `
    verify-approved `
    --repository-root $root `
    --plan $planPath `
    --approval $approvalPath
if ($LASTEXITCODE -ne 0) {
    throw "Transport canary reconciliation preflight failed."
}
$description = $descriptionJson | ConvertFrom-Json
$taskName = [string]$description.task_name
$task = Get-ScheduledTask -TaskName $taskName
$taskInfo = Get-ScheduledTaskInfo -TaskName $taskName
$nextRunAbsent = $null -eq $taskInfo.NextRunTime
$lastRunAt = $taskInfo.LastRunTime.ToUniversalTime().ToString(
    "yyyy-MM-ddTHH:mm:ssZ"
)
& $python -m futures_rebuild.durable_windows_task_transport `
    reconcile `
    --repository-root $root `
    --plan $planPath `
    --approval $approvalPath `
    --scheduler-state ([string]$task.State) `
    --last-task-result ([int64]$taskInfo.LastTaskResult) `
    --last-run-at $lastRunAt `
    --next-run-absent ([string]$nextRunAbsent).ToLowerInvariant()
if ($LASTEXITCODE -ne 0) {
    throw "Transport canary reconciliation failed closed."
}
