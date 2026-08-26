[CmdletBinding(DefaultParameterSetName = 'Production')]
param(
    [Parameter(Mandatory = $true, ParameterSetName = 'Production')]
    [string]$WorkerScriptRelativePath,
    [Parameter(Mandatory = $true, ParameterSetName = 'Production')]
    [ValidatePattern('^[0-9A-Z]{1,16}$')]
    [string]$Market,
    [Parameter(Mandatory = $true, ParameterSetName = 'Production')]
    [ValidatePattern('^[0-9a-f]{64}$')]
    [string]$AttemptId,
    [Parameter(Mandatory = $true, ParameterSetName = 'Rehearsal')]
    [switch]$Rehearsal,
    [Parameter(Mandatory = $true, ParameterSetName = 'Rehearsal')]
    [string]$RehearsalRoot,
    [Parameter(Mandatory = $true, ParameterSetName = 'Rehearsal')]
    [string]$RehearsalPythonExecutable
)

$ErrorActionPreference = 'Stop'
$RepositoryRoot = (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..')).Path
if ($Rehearsal) {
    $ResolvedRehearsalRoot = (Resolve-Path -LiteralPath $RehearsalRoot).Path
    $ResolvedPython = (Resolve-Path -LiteralPath $RehearsalPythonExecutable).Path
    if (-not (Test-Path -LiteralPath $ResolvedRehearsalRoot -PathType Container)) {
        throw 'The V10 production rehearsal root is absent.'
    }
    if (-not (Test-Path -LiteralPath $ResolvedPython -PathType Leaf)) {
        throw 'The V10 production rehearsal Python executable is absent.'
    }
    if ($ResolvedRehearsalRoot.StartsWith(
        $RepositoryRoot + [IO.Path]::DirectorySeparatorChar,
        [StringComparison]::OrdinalIgnoreCase
    )) {
        throw 'The V10 production rehearsal root must be outside the canonical repository.'
    }
    $PriorPythonPath = $env:PYTHONPATH
    $env:PYTHONPATH = Join-Path $RepositoryRoot 'src'
    if (-not [string]::IsNullOrWhiteSpace($PriorPythonPath)) {
        $env:PYTHONPATH += [IO.Path]::PathSeparator + $PriorPythonPath
    }
    try {
        & $ResolvedPython -m futures_rebuild.causal_full_build_production_rehearsal `
            --rehearsal-root $ResolvedRehearsalRoot `
            --source-root $RepositoryRoot
        if ($LASTEXITCODE -ne 0) {
            throw "The V10 production rehearsal failed with exit code $LASTEXITCODE."
        }
    }
    finally {
        $env:PYTHONPATH = $PriorPythonPath
    }
    return
}
$TaskName = 'FIMV2-Causal-V10-{0}-{1}' -f $Market, $AttemptId.Substring(0, 8)
$NormalizedWorker = $WorkerScriptRelativePath.Replace('\', '/')
$ExpectedPattern = '^reports/bounded_2025_full_build_v10_preparation/[0-9A-Za-z_\-]+/run_scheduled_worker_v10\.ps1$'
if ($NormalizedWorker -cnotmatch $ExpectedPattern) {
    throw 'V10 worker path is outside the exact preparation layout.'
}
$WorkerPath = (Resolve-Path -LiteralPath (Join-Path $RepositoryRoot $WorkerScriptRelativePath)).Path
if (-not $WorkerPath.StartsWith($RepositoryRoot + [IO.Path]::DirectorySeparatorChar, [StringComparison]::OrdinalIgnoreCase)) {
    throw 'V10 worker path escapes the canonical repository.'
}
if (Get-ScheduledTask -TaskName $TaskName -ErrorAction SilentlyContinue) {
    throw 'The exact V10 scheduled task already exists; refusing to overwrite it.'
}
$PowerShell = Join-Path $env:SystemRoot 'System32/WindowsPowerShell/v1.0/powershell.exe'
if (-not (Test-Path -LiteralPath $PowerShell -PathType Leaf)) {
    throw 'The stable Windows PowerShell scheduled-task host is absent.'
}
$Arguments = '-NoProfile -NonInteractive -ExecutionPolicy Bypass -File "{0}"' -f $WorkerPath
$Action = New-ScheduledTaskAction -Execute $PowerShell -Argument $Arguments -WorkingDirectory $RepositoryRoot
$Trigger = New-ScheduledTaskTrigger -Once -At (Get-Date).AddMinutes(2)
$Settings = New-ScheduledTaskSettingsSet -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries -ExecutionTimeLimit (New-TimeSpan -Hours 72) -MultipleInstances IgnoreNew -RestartCount 0
$Principal = New-ScheduledTaskPrincipal -UserId ([Security.Principal.WindowsIdentity]::GetCurrent().Name) -LogonType Interactive -RunLevel Limited
$Task = New-ScheduledTask -Action $Action -Trigger $Trigger -Settings $Settings -Principal $Principal
Register-ScheduledTask -TaskName $TaskName -InputObject $Task | Out-Null
[ordered]@{
    status = 'REGISTERED_FOR_SERVICE_TRIGGER_AFTER_LAUNCHER_EXIT'
    task_name = $TaskName
    market = $Market
    attempt_id = $AttemptId
    manual_start = $false
    interactive_parent_independent = $true
} | ConvertTo-Json -Compress
