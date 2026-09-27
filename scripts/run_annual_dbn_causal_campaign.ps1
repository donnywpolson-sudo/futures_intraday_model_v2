[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)]
    [ValidateSet('RegisterCampaign', 'RunCampaign', 'RegisterMarket', 'RunMarket')]
    [string]$Mode,
    [Parameter(Mandatory = $true)]
    [string]$PlanPath,
    [string]$ReceiptPath
)

$ErrorActionPreference = 'Stop'
$Root = (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..')).Path
$PlanPath = (Resolve-Path -LiteralPath $PlanPath).Path
$Plan = Get-Content -LiteralPath $PlanPath -Raw | ConvertFrom-Json
$PowerShell = Join-Path $env:SystemRoot 'System32\WindowsPowerShell\v1.0\powershell.exe'
$Python = Join-Path $Root '.venv\Scripts\python.exe'
$Self = $MyInvocation.MyCommand.Path

if ($Mode -eq 'RegisterCampaign') {
    $TaskName = [string]$Plan.controller_host.task_name
    if (Get-ScheduledTask -TaskName $TaskName -ErrorAction SilentlyContinue) {
        throw 'Exact annual Gate-B campaign task already exists.'
    }
    $Arguments = '-NoProfile -NonInteractive -WindowStyle Hidden -ExecutionPolicy Bypass -File "{0}" -Mode RunCampaign -PlanPath "{1}"' -f $Self, $PlanPath
    $Action = New-ScheduledTaskAction -Execute $PowerShell -Argument $Arguments -WorkingDirectory $Root
    $Trigger = New-ScheduledTaskTrigger -Once -At (Get-Date).AddMinutes(2)
    $Settings = New-ScheduledTaskSettingsSet -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries -DontStopOnIdleEnd -DisallowHardTerminate -ExecutionTimeLimit (New-TimeSpan -Days 7) -MultipleInstances IgnoreNew -RestartCount 0
    $Principal = New-ScheduledTaskPrincipal -UserId ([Security.Principal.WindowsIdentity]::GetCurrent().Name) -LogonType Interactive -RunLevel Limited
    Register-ScheduledTask -TaskName $TaskName -InputObject (New-ScheduledTask -Action $Action -Trigger $Trigger -Settings $Settings -Principal $Principal) | Out-Null
    return
}

if ($Mode -eq 'RunCampaign') {
    $env:FUTURES_ANNUAL_DBN_CAMPAIGN_TASK_NAME = [string]$Plan.controller_host.task_name
    & $Python -m futures_rebuild.annual_dbn_causal_campaign campaign --campaign-id ([IO.Path]::GetFileName((Split-Path -Parent $PlanPath)))
    exit $LASTEXITCODE
}

if ([string]::IsNullOrWhiteSpace($ReceiptPath)) {
    throw 'Market mode requires ReceiptPath.'
}
$ReceiptPath = (Resolve-Path -LiteralPath $ReceiptPath).Path
$TaskName = [string]$Plan.durable_host.task_name
if ($Mode -eq 'RegisterMarket') {
    if (Get-ScheduledTask -TaskName $TaskName -ErrorAction SilentlyContinue) {
        throw 'Exact annual Gate-B market task already exists.'
    }
    $Arguments = '-NoProfile -NonInteractive -WindowStyle Hidden -ExecutionPolicy Bypass -File "{0}" -Mode RunMarket -PlanPath "{1}" -ReceiptPath "{2}"' -f $Self, $PlanPath, $ReceiptPath
    $Action = New-ScheduledTaskAction -Execute $PowerShell -Argument $Arguments -WorkingDirectory $Root
    $Trigger = New-ScheduledTaskTrigger -Once -At (Get-Date).AddMinutes(2)
    $Settings = New-ScheduledTaskSettingsSet -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries -DontStopOnIdleEnd -DisallowHardTerminate -ExecutionTimeLimit (New-TimeSpan -Hours 72) -MultipleInstances IgnoreNew -RestartCount 0
    $Principal = New-ScheduledTaskPrincipal -UserId ([Security.Principal.WindowsIdentity]::GetCurrent().Name) -LogonType Interactive -RunLevel Limited
    Register-ScheduledTask -TaskName $TaskName -InputObject (New-ScheduledTask -Action $Action -Trigger $Trigger -Settings $Settings -Principal $Principal) | Out-Null
    return
}

$env:FUTURES_CAUSAL_FULL_BUILD_TASK_NAME = $TaskName
& $Python -m futures_rebuild.annual_dbn_causal_campaign market-worker --plan-path $PlanPath --receipt-path $ReceiptPath
exit $LASTEXITCODE
