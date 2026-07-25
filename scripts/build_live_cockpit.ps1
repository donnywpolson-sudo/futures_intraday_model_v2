[CmdletBinding()]
param(
    [string]$PythonExecutable = ''
)

$ErrorActionPreference = 'Stop'
$repoRoot = (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..')).Path
$pinnedPython = Join-Path $repoRoot '.venv\Scripts\python.exe'
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

try {
    # This Python 3.11 environment imports stdlib distutils before PyInstaller's
    # setuptools hook. Keep the compatibility setting scoped to this build.
    $env:SETUPTOOLS_USE_DISTUTILS = 'stdlib'
    Push-Location $repoRoot
    try {
        & $PythonExecutable -m PyInstaller --noconfirm FuturesLiveCockpit.spec
        if ($LASTEXITCODE -ne 0) {
            throw "PyInstaller failed with exit code $LASTEXITCODE"
        }
    }
    finally {
        Pop-Location
    }
}
finally {
    $env:SETUPTOOLS_USE_DISTUTILS = $previousDistutilsMode
}
