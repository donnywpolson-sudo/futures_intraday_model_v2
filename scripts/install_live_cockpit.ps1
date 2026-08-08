[CmdletBinding(SupportsShouldProcess = $true)]
param(
    [string]$BuildRoot = '',
    [string]$CredentialFile = '',
    [switch]$Upgrade
)

$ErrorActionPreference = 'Stop'
$repoRoot = (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..')).Path
if (-not $BuildRoot) {
    $BuildRoot = Join-Path $repoRoot 'FuturesLiveCockpit'
}
if (-not $CredentialFile) {
    $CredentialFile = Join-Path $repoRoot 'api.env'
}

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

function Get-Sha256Hex {
    param(
        [Parameter(Mandatory = $true)][string]$Path
    )
    $stream = [IO.File]::OpenRead($Path)
    $hasher = [Security.Cryptography.SHA256]::Create()
    try {
        $digest = $hasher.ComputeHash($stream)
        return ([BitConverter]::ToString($digest) -replace '-', '').ToLowerInvariant()
    }
    finally {
        $hasher.Dispose()
        $stream.Dispose()
    }
}

$resolvedBuildRoot = (Resolve-Path -LiteralPath $BuildRoot).Path
$resolvedCredentialFile = (Resolve-Path -LiteralPath $CredentialFile).Path
$sourceExe = Join-Path $resolvedBuildRoot 'FuturesLiveCockpit.exe'
if (-not (Test-Path -LiteralPath $sourceExe -PathType Leaf)) {
    throw "Packaged executable not found: $sourceExe"
}
if (-not (Test-Path -LiteralPath (
    Join-Path $resolvedBuildRoot '_internal'
) -PathType Container)) {
    throw "Packaged runtime directory not found under: $resolvedBuildRoot"
}
if (-not (Test-Path -LiteralPath $resolvedCredentialFile -PathType Leaf)) {
    throw 'Credential file not found.'
}

$bundledPlan = Join-Path (
    $resolvedBuildRoot
) '_internal\configs\live_cockpit_smoke_plan.json'
if (-not (Test-Path -LiteralPath $bundledPlan -PathType Leaf)) {
    throw "Packaged live-smoke plan not found: $bundledPlan"
}
$planHash = Get-Sha256Hex -Path $bundledPlan
$version = (
    (Get-Item -LiteralPath $sourceExe).LastWriteTimeUtc.ToString(
        'yyyyMMdd-HHmmss'
    ) + '-' + $planHash.Substring(0, 8).ToLowerInvariant()
)
$installRoot = [IO.Path]::GetFullPath(
    (Join-Path $env:LOCALAPPDATA 'Programs\FuturesLiveCockpit')
)
$installPath = Assert-ChildPath -Parent $installRoot -Child (
    Join-Path $installRoot $version
)
$stagingPath = Assert-ChildPath -Parent $installRoot -Child (
    Join-Path $installRoot (
        ".${version}.staging-$PID-$([Guid]::NewGuid().ToString('N'))"
    )
)
if (Test-Path -LiteralPath $installPath) {
    throw "Version is already installed: $installPath"
}

$startMenuDirectory = Join-Path (
    $env:APPDATA
) 'Microsoft\Windows\Start Menu\Programs'
$shortcutPaths = @(
    (Join-Path $startMenuDirectory 'Futures Live Cockpit.lnk'),
    (Join-Path ([Environment]::GetFolderPath('Desktop')) (
        'Futures Live Cockpit.lnk'
    ))
)
$startupShortcut = Join-Path (
    $env:APPDATA
) 'Microsoft\Windows\Start Menu\Programs\Startup\Futures Live Cockpit.lnk'
if (Test-Path -LiteralPath $startupShortcut) {
    throw "Refusing to proceed while an auto-start shortcut exists: $startupShortcut"
}

$existingShortcuts = @(
    $shortcutPaths | Where-Object { Test-Path -LiteralPath $_ -PathType Leaf }
)
if ($Upgrade -and $existingShortcuts.Count -eq 0) {
    throw 'Upgrade preparation requires at least one existing cockpit shortcut.'
}
if (-not $Upgrade -and $existingShortcuts.Count -gt 0) {
    throw 'Use -Upgrade when cockpit shortcuts already exist.'
}

$shell = New-Object -ComObject WScript.Shell
$shortcutRecords = @()
foreach ($shortcutPath in $existingShortcuts) {
    $record = Read-ShortcutRecord -Shell $shell -Path $shortcutPath
    Assert-ChildPath -Parent $installRoot -Child $record.TargetPath | Out-Null
    if ([IO.Path]::GetFileName($record.TargetPath) -ne 'FuturesLiveCockpit.exe') {
        throw "Existing shortcut is not cockpit-owned: $shortcutPath"
    }
    $shortcutRecords += $record
}

if (-not $PSCmdlet.ShouldProcess(
    $installPath,
    'Prepare isolated Futures Live Cockpit version without changing shortcuts'
)) {
    return [pscustomobject]@{
        Action = 'WouldPrepare'
        InstalledPath = $installPath
        CredentialFile = $resolvedCredentialFile
        ShortcutsChanged = $false
        AutoStartCreated = $false
    }
}

$installRootExisted = Test-Path -LiteralPath $installRoot
$stagingCreated = $false
$finalCreated = $false
try {
    New-Item -ItemType Directory -Path $installRoot -Force | Out-Null
    New-Item -ItemType Directory -Path $stagingPath | Out-Null
    $stagingCreated = $true
    foreach ($item in Get-ChildItem -LiteralPath $resolvedBuildRoot -Force) {
        Copy-Item -LiteralPath $item.FullName -Destination $stagingPath -Recurse
    }

    $forbiddenFiles = @(
        Get-ChildItem -LiteralPath $stagingPath -Recurse -Force -File |
            Where-Object { $_.Name -in @('api.env', 'databento.env') }
    )
    $forbiddenDirectories = @(
        Get-ChildItem -LiteralPath $stagingPath -Recurse -Force -Directory |
            Where-Object { $_.Name -in @('credentials', 'secrets') }
    )
    if ($forbiddenFiles.Count -gt 0 -or $forbiddenDirectories.Count -gt 0) {
        throw 'Packaged build unexpectedly contains credential material.'
    }

    $utf8NoBom = New-Object Text.UTF8Encoding($false)
    $locator = [ordered]@{
        schema = 'futures_live_cockpit_credential_source_v1'
        api_env_path = $resolvedCredentialFile
    }
    [IO.File]::WriteAllText(
        (Join-Path $stagingPath 'credential-source.json'),
        ($locator | ConvertTo-Json -Compress),
        $utf8NoBom
    )
    $rollback = [ordered]@{
        schema = 'futures_live_cockpit_shortcut_rollback/1.0.0'
        captured_at_utc = [DateTime]::UtcNow.ToString('o')
        shortcuts = $shortcutRecords
        startup_shortcut_absent = $true
    }
    [IO.File]::WriteAllText(
        (Join-Path $stagingPath 'rollback-shortcuts.json'),
        ($rollback | ConvertTo-Json -Depth 6 -Compress),
        $utf8NoBom
    )

    $stagedExe = Join-Path $stagingPath 'FuturesLiveCockpit.exe'
    $selfCheck = Start-Process -FilePath $stagedExe `
        -ArgumentList '--self-check' -WindowStyle Hidden -PassThru
    if (-not $selfCheck.WaitForExit(60000)) {
        Stop-Process -Id $selfCheck.Id -Force -ErrorAction SilentlyContinue
        throw 'Packaged self-check exceeded 60 seconds.'
    }
    if ($selfCheck.ExitCode -ne 0) {
        throw "Packaged self-check failed with exit code $($selfCheck.ExitCode)."
    }

    Move-Item -LiteralPath $stagingPath -Destination $installPath
    $stagingCreated = $false
    $finalCreated = $true

    foreach ($record in $shortcutRecords) {
        $observed = Read-ShortcutRecord -Shell $shell -Path $record.Path
        if (
            -not [string]::Equals(
                [IO.Path]::GetFullPath($observed.TargetPath),
                [IO.Path]::GetFullPath($record.TargetPath),
                [StringComparison]::OrdinalIgnoreCase
            ) -or
            -not [string]::Equals(
                [IO.Path]::GetFullPath($observed.WorkingDirectory),
                [IO.Path]::GetFullPath($record.WorkingDirectory),
                [StringComparison]::OrdinalIgnoreCase
            )
        ) {
            throw "Shortcut changed during preparation: $($record.Path)"
        }
    }
    if (Test-Path -LiteralPath $startupShortcut) {
        throw 'Unexpected auto-start shortcut was created.'
    }

    [pscustomobject]@{
        Action = 'Prepared'
        InstalledPath = $installPath
        CredentialLocator = Join-Path $installPath 'credential-source.json'
        RollbackMetadata = Join-Path $installPath 'rollback-shortcuts.json'
        PackagedSelfCheckExitCode = $selfCheck.ExitCode
        CredentialCopied = $false
        ShortcutsChanged = $false
        AutoStartCreated = $false
    }
}
catch {
    if ($finalCreated -and (Test-Path -LiteralPath $installPath)) {
        Assert-ChildPath -Parent $installRoot -Child $installPath | Out-Null
        Remove-Item -LiteralPath $installPath -Recurse -Force
    }
    if ($stagingCreated -and (Test-Path -LiteralPath $stagingPath)) {
        Assert-ChildPath -Parent $installRoot -Child $stagingPath | Out-Null
        Remove-Item -LiteralPath $stagingPath -Recurse -Force
    }
    if (
        -not $installRootExisted -and
        (Test-Path -LiteralPath $installRoot) -and
        @(Get-ChildItem -LiteralPath $installRoot -Force).Count -eq 0
    ) {
        Remove-Item -LiteralPath $installRoot -Force
    }
    throw
}
