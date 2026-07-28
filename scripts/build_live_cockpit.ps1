[CmdletBinding()]
param(
    [string]$PythonExecutable = ''
)

$ErrorActionPreference = 'Stop'
$repoRoot = (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..')).Path
$pinnedPython = Join-Path $repoRoot '.venv\Scripts\python.exe'
$specPath = Join-Path (
    $repoRoot
) 'FuturesLiveCockpit\_internal\FuturesLiveCockpit.spec'
$publishPath = Join-Path $repoRoot 'FuturesLiveCockpit'
$buildRoot = Join-Path $repoRoot 'build'
$runId = "$PID-$([Guid]::NewGuid().ToString('N'))"
$stagingRoot = Join-Path $buildRoot ".live-cockpit-dist-$runId"
$stagedApp = Join-Path $stagingRoot 'FuturesLiveCockpit'
$workPath = Join-Path $stagingRoot 'pyinstaller-work'
$backupPath = Join-Path $buildRoot ".live-cockpit-previous-$runId"
if (-not $PythonExecutable) {
    $PythonExecutable = $pinnedPython
}
$PythonExecutable = (Resolve-Path -LiteralPath $PythonExecutable).Path
if (-not [string]::Equals(
    $PythonExecutable,
    (Resolve-Path -LiteralPath $pinnedPython).Path,
    [StringComparison]::OrdinalIgnoreCase
)) {
    throw 'Cockpit packaging requires the repository-local pinned .venv.'
}
$previousDistutilsMode = $env:SETUPTOOLS_USE_DISTUTILS
$backupCreated = $false

try {
    # This Python 3.11 environment imports stdlib distutils before PyInstaller's
    # setuptools hook. Keep the compatibility setting scoped to this build.
    $env:SETUPTOOLS_USE_DISTUTILS = 'stdlib'
    New-Item -ItemType Directory -Path $buildRoot -Force | Out-Null
    New-Item -ItemType Directory -Path $stagingRoot | Out-Null
    Push-Location $repoRoot
    try {
        & $PythonExecutable -m PyInstaller `
            --noconfirm `
            --distpath $stagingRoot `
            --workpath $workPath `
            $specPath
        if ($LASTEXITCODE -ne 0) {
            throw "PyInstaller failed with exit code $LASTEXITCODE"
        }
    }
    finally {
        Pop-Location
    }

    $stagedExe = Join-Path $stagedApp 'FuturesLiveCockpit.exe'
    $stagedInternal = Join-Path $stagedApp '_internal'
    foreach ($requiredPath in @(
        $stagedExe,
        $stagedInternal,
        (Join-Path $stagedInternal 'FuturesLiveCockpit.spec'),
        (Join-Path $stagedInternal 'futures_live_cockpit.py')
    )) {
        if (-not (Test-Path -LiteralPath $requiredPath)) {
            throw "Packaged cockpit is missing: $requiredPath"
        }
    }

    if (Test-Path -LiteralPath $publishPath) {
        Move-Item -LiteralPath $publishPath -Destination $backupPath
        $backupCreated = $true
    }
    try {
        Move-Item -LiteralPath $stagedApp -Destination $publishPath
    }
    catch {
        if (
            $backupCreated -and
            -not (Test-Path -LiteralPath $publishPath)
        ) {
            Move-Item -LiteralPath $backupPath -Destination $publishPath
            $backupCreated = $false
        }
        throw
    }

    if ($backupCreated) {
        Remove-Item -LiteralPath $backupPath -Recurse -Force
        $backupCreated = $false
    }
}
finally {
    $env:SETUPTOOLS_USE_DISTUTILS = $previousDistutilsMode
    if (Test-Path -LiteralPath $stagingRoot) {
        Remove-Item -LiteralPath $stagingRoot -Recurse -Force
    }
    if (
        $backupCreated -and
        -not (Test-Path -LiteralPath $publishPath)
    ) {
        Move-Item -LiteralPath $backupPath -Destination $publishPath
    }
    if (Test-Path -LiteralPath $buildRoot -PathType Container) {
        $buildRootEntries = @(Get-ChildItem -LiteralPath $buildRoot -Force)
        if ($buildRootEntries.Count -eq 0) {
            Remove-Item -LiteralPath $buildRoot -Force
        }
    }
}
