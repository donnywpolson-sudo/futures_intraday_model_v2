[CmdletBinding(SupportsShouldProcess = $true)]
param(
    [Parameter(Mandatory = $true)]
    [string]$Plan,

    [Parameter(Mandatory = $true)]
    [string]$Approval,

    [switch]$ValidateOnly
)

$ErrorActionPreference = 'Stop'
$repositoryRoot = (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..')).Path
$python = Join-Path $repositoryRoot '.venv\Scripts\python.exe'
$planPath = (Resolve-Path -LiteralPath (Join-Path $repositoryRoot $Plan)).Path
$approvalPath = (Resolve-Path -LiteralPath (Join-Path $repositoryRoot $Approval)).Path

$mode = if ($ValidateOnly) { '--describe-plan' } else { '--preflight' }
$descriptionText = & $python -m futures_rebuild.active_data_full_supervisor `
    --repository-root $repositoryRoot `
    --plan $planPath `
    --approval $approvalPath `
    $mode
if ($LASTEXITCODE -ne 0) {
    throw "Durable Stage 6 supervisor preflight failed with exit code $LASTEXITCODE."
}
$description = $descriptionText | ConvertFrom-Json

if ($ValidateOnly) {
    $descriptionText
    return
}

$taskName = [string]$description.task_name
if (Get-ScheduledTask -TaskName $taskName -ErrorAction SilentlyContinue) {
    throw "Refusing to replace existing scheduled task: $taskName"
}

function Quote-TaskArgument {
    param([string]$Value)
    return '"' + $Value.Replace('"', '\"') + '"'
}

$arguments = @(
    '-m',
    'futures_rebuild.active_data_full_supervisor',
    '--repository-root',
    (Quote-TaskArgument $repositoryRoot),
    '--plan',
    (Quote-TaskArgument $planPath),
    '--approval',
    (Quote-TaskArgument $approvalPath)
) -join ' '

$action = New-ScheduledTaskAction `
    -Execute $python `
    -Argument $arguments `
    -WorkingDirectory $repositoryRoot
$principal = New-ScheduledTaskPrincipal `
    -UserId ([Security.Principal.WindowsIdentity]::GetCurrent().Name) `
    -LogonType Interactive `
    -RunLevel Limited
$settings = New-ScheduledTaskSettingsSet `
    -AllowStartIfOnBatteries `
    -DontStopIfGoingOnBatteries `
    -ExecutionTimeLimit (
        New-TimeSpan -Seconds (
            [int]$description.scheduler_execution_time_limit_seconds
        )
    )

if ($PSCmdlet.ShouldProcess($taskName, 'Register and start one-shot Stage 6 supervisor')) {
    Register-ScheduledTask `
        -TaskName $taskName `
        -Action $action `
        -Principal $principal `
        -Settings $settings | Out-Null
    Start-ScheduledTask -TaskName $taskName
}

[pscustomobject]@{
    plan_id = [string]$description.plan_id
    status = 'START_REQUESTED'
    supervision_id = [string]$description.supervision_id
    task_name = $taskName
} | ConvertTo-Json -Compress
