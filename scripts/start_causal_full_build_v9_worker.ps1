[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)]
    [string]$WorkerScriptRelativePath,
    [Parameter(Mandatory = $true)]
    [ValidatePattern('^[0-9A-Z]{1,16}$')]
    [string]$Market,
    [Parameter(Mandatory = $true)]
    [ValidatePattern('^[0-9a-f]{64}$')]
    [string]$AttemptId
)

$ErrorActionPreference = 'Stop'
$TaskName = 'FuturesIntradayModelV2-CausalFullBuild-V9'
$RepositoryRoot = (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..')).Path
$NormalizedWorker = $WorkerScriptRelativePath.Replace('\', '/')
$ExpectedPattern = '^reports/bounded_2025_full_build_v9_preparation/b25fbv9p_[0-9TZ]+_[0-9a-f]{8}/run_scheduled_worker_v9\.ps1$'
if ($NormalizedWorker -cnotmatch $ExpectedPattern) {
    throw 'V9 worker path is outside the exact packet-bound report layout.'
}
$WorkerPath = (Resolve-Path -LiteralPath (Join-Path $RepositoryRoot $WorkerScriptRelativePath)).Path
if (-not $WorkerPath.StartsWith($RepositoryRoot + [IO.Path]::DirectorySeparatorChar, [StringComparison]::OrdinalIgnoreCase)) {
    throw 'V9 worker path escapes the canonical repository.'
}
if (Get-ScheduledTask -TaskName $TaskName -ErrorAction SilentlyContinue) {
    throw 'The V9 scheduled task already exists; refusing to overwrite or restart it.'
}
$PowerShell = Join-Path $env:SystemRoot 'System32/WindowsPowerShell/v1.0/powershell.exe'
if (-not (Test-Path -LiteralPath $PowerShell -PathType Leaf)) {
    throw 'The stable Windows PowerShell scheduled-task host is absent.'
}
$Arguments = '-NoProfile -NonInteractive -ExecutionPolicy Bypass -File "{0}"' -f $WorkerPath
$Action = New-ScheduledTaskAction -Execute $PowerShell -Argument $Arguments -WorkingDirectory $RepositoryRoot
$ScheduledAt = (Get-Date).AddMinutes(2)
$Trigger = New-ScheduledTaskTrigger -Once -At $ScheduledAt
$Settings = New-ScheduledTaskSettingsSet `
    -AllowStartIfOnBatteries `
    -DontStopIfGoingOnBatteries `
    -ExecutionTimeLimit (New-TimeSpan -Hours 72) `
    -MultipleInstances IgnoreNew `
    -RestartCount 0
$Principal = New-ScheduledTaskPrincipal `
    -UserId ([Security.Principal.WindowsIdentity]::GetCurrent().Name) `
    -LogonType Interactive `
    -RunLevel Limited
$Task = New-ScheduledTask -Action $Action -Trigger $Trigger -Settings $Settings -Principal $Principal
Register-ScheduledTask -TaskName $TaskName -InputObject $Task | Out-Null
[ordered]@{
    status = 'REGISTERED_FOR_SERVICE_TRIGGER_AFTER_LAUNCHER_EXIT'
    task_name = $TaskName
    market = $Market
    attempt_id = $AttemptId
    scheduled_at = $ScheduledAt.ToUniversalTime().ToString('o')
    manual_start = $false
    interactive_parent_independent = $true
} | ConvertTo-Json -Compress
