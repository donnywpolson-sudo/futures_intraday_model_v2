[CmdletBinding(SupportsShouldProcess = $true)]
param(
    [Parameter(Mandatory = $true)][string]$PreparedInstallPath,
    [Parameter(Mandatory = $true)][string]$LiveSmokeResult,
    [Parameter(Mandatory = $true)][string]$LiveSmokePlan,
    [Parameter(Mandatory = $true)]
    [ValidatePattern('^[0-9a-fA-F]{64}$')]
    [string]$ExpectedExecutableSha256
)

$ErrorActionPreference = 'Stop'
$repoRoot = (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..')).Path
$pythonPath = (Resolve-Path -LiteralPath (
    Join-Path $repoRoot '.venv\Scripts\python.exe'
)).Path
$planPath = (Resolve-Path -LiteralPath $LiveSmokePlan).Path
$installRoot = [IO.Path]::GetFullPath(
    (Join-Path $env:LOCALAPPDATA 'Programs\FuturesLiveCockpit')
)

function Assert-ChildPath {
    param(
        [Parameter(Mandatory = $true)][string]$Parent,
        [Parameter(Mandatory = $true)][string]$Child
    )
    $parentFull = [IO.Path]::GetFullPath($Parent).TrimEnd('\') + '\'
    $childFull = [IO.Path]::GetFullPath($Child)
    if (-not $childFull.StartsWith(
        $parentFull,
        [StringComparison]::OrdinalIgnoreCase
    )) {
        throw "Path escapes the installation root: $childFull"
    }
    return $childFull
}

function Read-ShortcutRecord {
    param(
        [Parameter(Mandatory = $true)][object]$Shell,
        [Parameter(Mandatory = $true)][string]$Path
    )
    $shortcut = $Shell.CreateShortcut($Path)
    return [ordered]@{
        Path = [IO.Path]::GetFullPath($Path)
        TargetPath = $shortcut.TargetPath
        WorkingDirectory = $shortcut.WorkingDirectory
        Description = $shortcut.Description
        Arguments = $shortcut.Arguments
        IconLocation = $shortcut.IconLocation
        WindowStyle = $shortcut.WindowStyle
    }
}

function Restore-ShortcutRecord {
    param(
        [Parameter(Mandatory = $true)][object]$Shell,
        [Parameter(Mandatory = $true)][object]$Record
    )
    $shortcut = $Shell.CreateShortcut([string]$Record.Path)
    $shortcut.TargetPath = [string]$Record.TargetPath
    $shortcut.WorkingDirectory = [string]$Record.WorkingDirectory
    $shortcut.Description = [string]$Record.Description
    $shortcut.Arguments = [string]$Record.Arguments
    $shortcut.IconLocation = [string]$Record.IconLocation
    $shortcut.WindowStyle = [int]$Record.WindowStyle
    $shortcut.Save()
}

function Get-Sha256Hex {
    param([Parameter(Mandatory = $true)][string]$Path)
    return (Get-FileHash -Algorithm SHA256 -LiteralPath $Path).Hash.ToLowerInvariant()
}

$preparedPath = Assert-ChildPath -Parent $installRoot -Child (
    (Resolve-Path -LiteralPath $PreparedInstallPath).Path
)
if ([IO.Path]::GetFileName($preparedPath).StartsWith('.')) {
    throw 'Prepared installation cannot be a staging directory.'
}
$preparedExe = Join-Path $preparedPath 'FuturesLiveCockpit.exe'
$locatorPath = Join-Path $preparedPath 'credential-source.json'
$rollbackPath = Join-Path $preparedPath 'rollback-shortcuts.json'
foreach ($required in @($preparedExe, $locatorPath, $rollbackPath)) {
    if (-not (Test-Path -LiteralPath $required -PathType Leaf)) {
        throw "Prepared installation is incomplete: $required"
    }
}
$expectedHash = $ExpectedExecutableSha256.ToLowerInvariant()
$preparedHash = Get-Sha256Hex -Path $preparedExe
if ($preparedHash -ne $expectedHash) {
    throw "Prepared executable hash mismatch: expected $expectedHash, observed $preparedHash"
}
$resultPath = (Resolve-Path -LiteralPath $LiveSmokeResult).Path

$guardOutput = & $pythonPath -m futures_rebuild.live_cockpit.cutover_guard `
    --plan $planPath --result $resultPath --executable $preparedExe
if ($LASTEXITCODE -ne 0) {
    throw "Cutover guard rejected the prepared installation: $guardOutput"
}

$rollback = Get-Content -LiteralPath $rollbackPath -Raw | ConvertFrom-Json
if (
    $rollback.schema -ne 'futures_live_cockpit_shortcut_rollback/1.0.0' -or
    $rollback.startup_shortcut_absent -ne $true -or
    @($rollback.shortcuts).Count -eq 0
) {
    throw 'Prepared shortcut rollback metadata is invalid.'
}
$startupShortcut = Join-Path (
    $env:APPDATA
) 'Microsoft\Windows\Start Menu\Programs\Startup\Futures Live Cockpit.lnk'
if (Test-Path -LiteralPath $startupShortcut) {
    throw "Refusing to cut over while an auto-start shortcut exists: $startupShortcut"
}

$shell = New-Object -ComObject WScript.Shell
foreach ($record in @($rollback.shortcuts)) {
    if (-not (Test-Path -LiteralPath $record.Path -PathType Leaf)) {
        throw "Required prior shortcut is missing: $($record.Path)"
    }
    $observed = Read-ShortcutRecord -Shell $shell -Path $record.Path
    if (
        -not [string]::Equals(
            [IO.Path]::GetFullPath($observed.TargetPath),
            [IO.Path]::GetFullPath([string]$record.TargetPath),
            [StringComparison]::OrdinalIgnoreCase
        ) -or
        -not [string]::Equals(
            [IO.Path]::GetFullPath($observed.WorkingDirectory),
            [IO.Path]::GetFullPath([string]$record.WorkingDirectory),
            [StringComparison]::OrdinalIgnoreCase
        )
    ) {
        throw "Prior shortcut drifted after preparation: $($record.Path)"
    }
}

if (-not $PSCmdlet.ShouldProcess(
    $preparedPath,
    'Activate verified cockpit and replace preserved shortcuts'
)) {
    return [pscustomobject]@{
        Action = 'WouldActivate'
        InstalledPath = $preparedPath
        LiveSmokeResult = $resultPath
        GuardEvidence = $guardOutput
        ExecutableSha256 = $preparedHash
        AutoStartCreated = $false
    }
}

$updated = [Collections.Generic.List[string]]::new()
try {
    foreach ($record in @($rollback.shortcuts)) {
        $shortcut = $shell.CreateShortcut([string]$record.Path)
        $shortcut.TargetPath = $preparedExe
        $shortcut.WorkingDirectory = $preparedPath
        $shortcut.Description = 'Observation-only futures chart cockpit'
        $shortcut.Arguments = ''
        $shortcut.Save()
        $updated.Add([string]$record.Path)
    }
    foreach ($record in @($rollback.shortcuts)) {
        $verified = Read-ShortcutRecord -Shell $shell -Path $record.Path
        if (
            -not [string]::Equals(
                [IO.Path]::GetFullPath($verified.TargetPath),
                [IO.Path]::GetFullPath($preparedExe),
                [StringComparison]::OrdinalIgnoreCase
            ) -or
            -not [string]::Equals(
                [IO.Path]::GetFullPath($verified.WorkingDirectory),
                [IO.Path]::GetFullPath($preparedPath),
                [StringComparison]::OrdinalIgnoreCase
            )
        ) {
            throw "Shortcut verification failed: $($record.Path)"
        }
    }
    if (Test-Path -LiteralPath $startupShortcut) {
        throw 'Unexpected auto-start shortcut was created.'
    }
    $activatedHash = Get-Sha256Hex -Path $preparedExe
    if ($activatedHash -ne $expectedHash) {
        throw "Activated executable hash mismatch: $activatedHash"
    }
    [pscustomobject]@{
        Action = 'Activated'
        InstalledPath = $preparedPath
        ActivatedExecutable = $preparedExe
        ExecutableSha256 = $activatedHash
        LiveSmokeResult = $resultPath
        GuardEvidence = $guardOutput
        ShortcutsUpdated = @($updated)
        ShortcutTargets = @(
            @($rollback.shortcuts) | ForEach-Object { $preparedExe }
        )
        RollbackVerified = $true
        CredentialCopied = $false
        AutoStartCreated = $false
    }
}
catch {
    foreach ($record in @($rollback.shortcuts)) {
        Restore-ShortcutRecord -Shell $shell -Record $record
    }
    foreach ($record in @($rollback.shortcuts)) {
        $restored = Read-ShortcutRecord -Shell $shell -Path $record.Path
        if (
            -not [string]::Equals(
                [IO.Path]::GetFullPath($restored.TargetPath),
                [IO.Path]::GetFullPath([string]$record.TargetPath),
                [StringComparison]::OrdinalIgnoreCase
            ) -or
            -not [string]::Equals(
                [IO.Path]::GetFullPath($restored.WorkingDirectory),
                [IO.Path]::GetFullPath([string]$record.WorkingDirectory),
                [StringComparison]::OrdinalIgnoreCase
            )
        ) {
            throw "Cutover failed and shortcut rollback verification failed: $($record.Path)"
        }
    }
    throw
}
