[CmdletBinding(SupportsShouldProcess = $true)]
param(
    [string]$BuildRoot = '',
    [string]$CredentialFile = '',
    [switch]$Upgrade
)

$ErrorActionPreference = 'Stop'
if (-not $BuildRoot) {
    $BuildRoot = Join-Path $PSScriptRoot '..\dist\FuturesLiveCockpit'
}
if (-not $CredentialFile) {
    $CredentialFile = Join-Path $PSScriptRoot '..\api.env'
}

function Assert-ChildPath {
    param(
        [Parameter(Mandatory = $true)][string]$Parent,
        [Parameter(Mandatory = $true)][string]$Child
    )
    $parentFull = [IO.Path]::GetFullPath($Parent).TrimEnd('\') + '\'
    $childFull = [IO.Path]::GetFullPath($Child)
    if (-not $childFull.StartsWith($parentFull, [StringComparison]::OrdinalIgnoreCase)) {
        throw "Path escapes the installation root: $childFull"
    }
    return $childFull
}

$resolvedBuildRoot = (Resolve-Path -LiteralPath $BuildRoot).Path
$resolvedCredentialFile = (Resolve-Path -LiteralPath $CredentialFile).Path
$sourceExe = Join-Path $resolvedBuildRoot 'FuturesLiveCockpit.exe'
if (-not (Test-Path -LiteralPath $sourceExe -PathType Leaf)) {
    throw "Packaged executable not found: $sourceExe"
}
if (-not (Test-Path -LiteralPath (Join-Path $resolvedBuildRoot '_internal') -PathType Container)) {
    throw "Packaged runtime directory not found under: $resolvedBuildRoot"
}
if (-not (Test-Path -LiteralPath $resolvedCredentialFile -PathType Leaf)) {
    throw "Credential file not found."
}

$version = (Get-Item -LiteralPath $sourceExe).LastWriteTimeUtc.ToString('yyyyMMdd-HHmmss')
$installRoot = [IO.Path]::GetFullPath(
    (Join-Path $env:LOCALAPPDATA 'Programs\FuturesLiveCockpit')
)
$installPath = Join-Path $installRoot $version
$stagingPath = Join-Path $installRoot (
    ".${version}.staging-$PID-$([Guid]::NewGuid().ToString('N'))"
)
$installPath = Assert-ChildPath -Parent $installRoot -Child $installPath
$stagingPath = Assert-ChildPath -Parent $installRoot -Child $stagingPath
if (Test-Path -LiteralPath $installPath) {
    throw "Version is already installed: $installPath"
}

$startMenuDirectory = Join-Path $env:APPDATA 'Microsoft\Windows\Start Menu\Programs'
$startMenuShortcut = Join-Path $startMenuDirectory 'Futures Live Cockpit.lnk'
$desktopShortcut = Join-Path ([Environment]::GetFolderPath('Desktop')) 'Futures Live Cockpit.lnk'
$startupShortcut = Join-Path $env:APPDATA 'Microsoft\Windows\Start Menu\Programs\Startup\Futures Live Cockpit.lnk'
$shortcutPaths = @($startMenuShortcut, $desktopShortcut)
if (Test-Path -LiteralPath $startupShortcut) {
    throw "Refusing to overwrite existing rollout path: $startupShortcut"
}

$previousShortcuts = @{}
$existingShortcuts = @($shortcutPaths | Where-Object { Test-Path -LiteralPath $_ })
if ($Upgrade) {
    if ($existingShortcuts.Count -ne $shortcutPaths.Count) {
        throw 'Upgrade requires both existing cockpit shortcuts.'
    }
    $existingShell = New-Object -ComObject WScript.Shell
    foreach ($existingPath in $shortcutPaths) {
        $existing = $existingShell.CreateShortcut($existingPath)
        Assert-ChildPath -Parent $installRoot -Child $existing.TargetPath | Out-Null
        if ([IO.Path]::GetFileName($existing.TargetPath) -ne 'FuturesLiveCockpit.exe') {
            throw "Existing shortcut is not cockpit-owned: $existingPath"
        }
        $previousShortcuts[$existingPath] = [ordered]@{
            TargetPath = $existing.TargetPath
            WorkingDirectory = $existing.WorkingDirectory
            Description = $existing.Description
            Arguments = $existing.Arguments
            IconLocation = $existing.IconLocation
            WindowStyle = $existing.WindowStyle
        }
    }
}
elseif ($existingShortcuts.Count -gt 0) {
    foreach ($existingPath in $existingShortcuts) {
        throw "Refusing to overwrite existing rollout path: $existingPath"
    }
}

if (-not $PSCmdlet.ShouldProcess(
    $installPath,
    $(if ($Upgrade) { 'Upgrade Futures Live Cockpit and replace verified shortcuts' } else { 'Install Futures Live Cockpit and create shortcuts' })
)) {
    return [pscustomobject]@{
        Action = $(if ($Upgrade) { 'WouldUpgrade' } else { 'WouldInstall' })
        InstalledPath = $installPath
        CredentialFile = $resolvedCredentialFile
        StartMenuShortcut = $startMenuShortcut
        DesktopShortcut = $desktopShortcut
        AutoStartCreated = $false
    }
}

$installRootExisted = Test-Path -LiteralPath $installRoot
$stagingCreated = $false
$finalCreated = $false
$createdShortcuts = [Collections.Generic.List[string]]::new()
$shortcutsUpdated = $false

try {
    New-Item -ItemType Directory -Path $installRoot -Force | Out-Null
    New-Item -ItemType Directory -Path $stagingPath | Out-Null
    $stagingCreated = $true
    foreach ($item in Get-ChildItem -LiteralPath $resolvedBuildRoot -Force) {
        Copy-Item -LiteralPath $item.FullName -Destination $stagingPath -Recurse -Force
    }

    $forbiddenFiles = @(
        Get-ChildItem -LiteralPath $stagingPath -Recurse -Force -File |
            Where-Object { $_.Name -in @('api.env', 'databento.env') }
    )
    $forbiddenDirectories = @(
        Get-ChildItem -LiteralPath $stagingPath -Recurse -Force -Directory |
            Where-Object { $_.Name -eq 'secrets' }
    )
    if ($forbiddenFiles.Count -gt 0 -or $forbiddenDirectories.Count -gt 0) {
        throw 'Packaged build unexpectedly contains credential files or directories.'
    }

    $locatorPath = Join-Path $stagingPath 'credential-source.json'
    $locatorJson = [ordered]@{
        schema = 'futures_live_cockpit_credential_source_v1'
        api_env_path = $resolvedCredentialFile
    } | ConvertTo-Json -Compress
    $utf8NoBom = New-Object Text.UTF8Encoding($false)
    [IO.File]::WriteAllText($locatorPath, $locatorJson, $utf8NoBom)

    $stagedExe = Join-Path $stagingPath 'FuturesLiveCockpit.exe'
    $selfCheck = Start-Process -FilePath $stagedExe -ArgumentList '--self-check' -WindowStyle Hidden -PassThru
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
    $installedExe = Join-Path $installPath 'FuturesLiveCockpit.exe'
    $installedLocator = Join-Path $installPath 'credential-source.json'

    $shell = New-Object -ComObject WScript.Shell
    foreach ($shortcutPath in $shortcutPaths) {
        $shortcut = $shell.CreateShortcut($shortcutPath)
        $shortcut.TargetPath = $installedExe
        $shortcut.WorkingDirectory = $installPath
        $shortcut.Description = 'Observation-only futures chart cockpit'
        $shortcut.Save()
        if (-not $Upgrade) {
            $createdShortcuts.Add($shortcutPath)
        }
        $verified = $shell.CreateShortcut($shortcutPath)
        if (
            -not [string]::Equals(
                [IO.Path]::GetFullPath($verified.TargetPath),
                [IO.Path]::GetFullPath($installedExe),
                [StringComparison]::OrdinalIgnoreCase
            ) -or
            -not [string]::Equals(
                [IO.Path]::GetFullPath($verified.WorkingDirectory),
                [IO.Path]::GetFullPath($installPath),
                [StringComparison]::OrdinalIgnoreCase
            )
        ) {
            throw "Shortcut verification failed: $shortcutPath"
        }
    }
    $shortcutsUpdated = $Upgrade

    if (Test-Path -LiteralPath $startupShortcut) {
        throw 'Unexpected auto-start shortcut was created.'
    }

    [pscustomobject]@{
        Action = $(if ($Upgrade) { 'Upgraded' } else { 'Installed' })
        InstalledPath = $installPath
        CredentialLocator = $installedLocator
        StartMenuShortcut = $startMenuShortcut
        DesktopShortcut = $desktopShortcut
        PackagedSelfCheckExitCode = $selfCheck.ExitCode
        CredentialCopied = $false
        AutoStartCreated = $false
    }
}
catch {
    if ($Upgrade -and $shortcutsUpdated -or ($Upgrade -and $previousShortcuts.Count -gt 0)) {
        $restoreShell = New-Object -ComObject WScript.Shell
        foreach ($shortcutPath in $shortcutPaths) {
            if ($previousShortcuts.ContainsKey($shortcutPath)) {
                $prior = $previousShortcuts[$shortcutPath]
                $shortcut = $restoreShell.CreateShortcut($shortcutPath)
                $shortcut.TargetPath = $prior.TargetPath
                $shortcut.WorkingDirectory = $prior.WorkingDirectory
                $shortcut.Description = $prior.Description
                $shortcut.Arguments = $prior.Arguments
                $shortcut.IconLocation = $prior.IconLocation
                $shortcut.WindowStyle = $prior.WindowStyle
                $shortcut.Save()
            }
        }
    }
    foreach ($shortcutPath in $createdShortcuts) {
        if (Test-Path -LiteralPath $shortcutPath) {
            Remove-Item -LiteralPath $shortcutPath -Force
        }
    }
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
