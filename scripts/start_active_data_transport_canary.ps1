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
$powershell = Join-Path $env:SystemRoot (
    "System32\WindowsPowerShell\v1.0\powershell.exe"
)
$runner = Join-Path $root "scripts\run_active_data_transport_canary.ps1"
$planPath = [IO.Path]::GetFullPath((Join-Path $root $Plan))
$approvalPath = [IO.Path]::GetFullPath((Join-Path $root $Approval))
$descriptionJson = & $python -m futures_rebuild.durable_windows_task_transport `
    preflight `
    --repository-root $root `
    --plan $planPath `
    --approval $approvalPath
if ($LASTEXITCODE -ne 0) {
    throw "Transport canary preflight failed."
}
$description = $descriptionJson | ConvertFrom-Json
$taskName = [string]$description.task_name
if (Get-ScheduledTask -TaskName $taskName -ErrorAction SilentlyContinue) {
    throw "Transport canary task already exists: $taskName"
}
$arguments = @(
    "-NoLogo",
    "-NoProfile",
    "-NonInteractive",
    "-ExecutionPolicy",
    "Bypass",
    "-File",
    "`"$runner`"",
    "-Plan",
    "`"$planPath`"",
    "-Approval",
    "`"$approvalPath`""
) -join " "
$action = New-ScheduledTaskAction `
    -Execute $powershell `
    -Argument $arguments `
    -WorkingDirectory $root
$principalName = [System.Security.Principal.WindowsIdentity]::GetCurrent().Name
$principal = New-ScheduledTaskPrincipal `
    -UserId $principalName `
    -LogonType S4U `
    -RunLevel Limited
$settings = New-ScheduledTaskSettingsSet `
    -AllowStartIfOnBatteries `
    -DontStopIfGoingOnBatteries `
    -DontStopOnIdleEnd `
    -ExecutionTimeLimit (
        [TimeSpan]::FromSeconds(
            [int]$description.scheduler_duration_seconds
        )
    ) `
    -MultipleInstances IgnoreNew
$task = New-ScheduledTask `
    -Action $action `
    -Principal $principal `
    -Settings $settings

# Failure after registration is preserved fail-closed; no automatic deletion or retry.
Register-ScheduledTask -TaskName $taskName -InputObject $task | Out-Null
$registered = Get-ScheduledTask -TaskName $taskName
if (
    $registered.Principal.LogonType -ne "S4U" -or
    $registered.Principal.RunLevel -ne "Limited" -or
    $registered.Triggers.Count -ne 0 -or
    $registered.Settings.MultipleInstances -ne "IgnoreNew"
) {
    throw "Registered transport canary task differs from the approved shape."
}
$xml = Export-ScheduledTask -TaskName $taskName
$hashAlgorithm = [Security.Cryptography.SHA256]::Create()
try {
    $taskXmlSha256 = (
        $hashAlgorithm.ComputeHash([Text.Encoding]::UTF8.GetBytes($xml)) |
            ForEach-Object { $_.ToString("x2") }
    ) -join ""
}
finally {
    $hashAlgorithm.Dispose()
}
$launchRequestedAt = [DateTime]::UtcNow.ToString("yyyy-MM-ddTHH:mm:ssZ")
Start-ScheduledTask -TaskName $taskName
$launcherReturnedAt = [DateTime]::UtcNow.ToString("yyyy-MM-ddTHH:mm:ssZ")
$launchJson = & $python -m futures_rebuild.durable_windows_task_transport `
    record-launch `
    --repository-root $root `
    --plan $planPath `
    --approval $approvalPath `
    --task-xml-sha256 $taskXmlSha256 `
    --launch-requested-at $launchRequestedAt `
    --launcher-returned-at $launcherReturnedAt
if ($LASTEXITCODE -ne 0) {
    throw "Transport canary launch receipt failed."
}
$launchJson
