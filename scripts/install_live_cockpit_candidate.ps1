[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)]
    [ValidatePattern('^[0-9a-fA-F]{64}$')]
    [string]$ExpectedCandidateSha256,
    [ValidateRange(1, 60)]
    [int]$SelfCheckTimeoutSeconds = 60
)

$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest

$repoRoot = (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..')).Path
$buildRoot = Join-Path $repoRoot 'build'
$candidateRoot = Join-Path $buildRoot 'FuturesLiveCockpit-candidate'
$publishRoot = Join-Path $repoRoot 'FuturesLiveCockpit'
$candidateExe = Join-Path $candidateRoot 'FuturesLiveCockpit.exe'
$publishExe = Join-Path $publishRoot 'FuturesLiveCockpit.exe'
$runId = "$PID-$([Guid]::NewGuid().ToString('N'))"
$backupRoot = Join-Path $buildRoot ".live-cockpit-install-backup-$runId"
$expectedHash = $ExpectedCandidateSha256.ToLowerInvariant()
$backupCreated = $false
$candidateInstalled = $false

function Get-TreeFingerprint {
    param([Parameter(Mandatory = $true)][string]$Root)

    $resolvedRoot = [IO.Path]::GetFullPath($Root).TrimEnd([IO.Path]::DirectorySeparatorChar)
    $rootItem = Get-Item -LiteralPath $resolvedRoot -Force
    if (($rootItem.Attributes -band [IO.FileAttributes]::ReparsePoint) -ne 0) {
        throw "Tree fingerprint root is link-like: $resolvedRoot"
    }
    $linkLikeEntries = @(
        Get-ChildItem -LiteralPath $resolvedRoot -Recurse -Force |
            Where-Object {
                ($_.Attributes -band [IO.FileAttributes]::ReparsePoint) -ne 0
            }
    )
    if ($linkLikeEntries.Count -ne 0) {
        throw "Tree fingerprint contains a link-like entry: $($linkLikeEntries[0].FullName)"
    }
    $entries = @(
        Get-ChildItem -LiteralPath $resolvedRoot -Recurse -File -Force |
            Sort-Object FullName |
            ForEach-Object {
                $relativePath = $_.FullName.Substring($resolvedRoot.Length).TrimStart('\').Replace('\', '/')
                [ordered]@{
                    path = $relativePath
                    size = $_.Length
                    sha256 = (Get-FileHash -Algorithm SHA256 -LiteralPath $_.FullName).Hash.ToLowerInvariant()
                }
            }
    )
    $manifest = ConvertTo-Json -InputObject $entries -Compress -Depth 3
    $bytes = [Text.Encoding]::UTF8.GetBytes($manifest)
    $algorithm = [Security.Cryptography.SHA256]::Create()
    try {
        $digest = [BitConverter]::ToString($algorithm.ComputeHash($bytes)).Replace('-', '').ToLowerInvariant()
    }
    finally {
        $algorithm.Dispose()
    }
    [Int64]$totalBytes = 0
    foreach ($entry in $entries) {
        $totalBytes += [Int64]$entry['size']
    }
    [ordered]@{
        sha256 = $digest
        file_count = $entries.Count
        total_bytes = $totalBytes
    }
}

function Assert-ExactChildPath {
    param(
        [Parameter(Mandatory = $true)][string]$Path,
        [Parameter(Mandatory = $true)][string]$Expected
    )
    $resolvedPath = [IO.Path]::GetFullPath($Path)
    $resolvedExpected = [IO.Path]::GetFullPath($Expected)
    if (-not [string]::Equals($resolvedPath, $resolvedExpected, [StringComparison]::OrdinalIgnoreCase)) {
        throw "Exact path validation failed: $resolvedPath"
    }
}

Assert-ExactChildPath -Path $candidateRoot -Expected (Join-Path $repoRoot 'build\FuturesLiveCockpit-candidate')
Assert-ExactChildPath -Path $publishRoot -Expected (Join-Path $repoRoot 'FuturesLiveCockpit')
Assert-ExactChildPath -Path $backupRoot -Expected (Join-Path $buildRoot ".live-cockpit-install-backup-$runId")

foreach ($requiredPath in @(
    $candidateExe,
    (Join-Path $candidateRoot '_internal'),
    (Join-Path $candidateRoot '_internal\FuturesLiveCockpit.spec'),
    (Join-Path $candidateRoot '_internal\futures_live_cockpit.py')
)) {
    if (-not (Test-Path -LiteralPath $requiredPath)) {
        throw "Validated candidate is incomplete: $requiredPath"
    }
}
if (-not (Test-Path -LiteralPath $publishRoot -PathType Container)) {
    throw "Installed cockpit tree is absent: $publishRoot"
}
if (Test-Path -LiteralPath $backupRoot) {
    throw "Install backup path already exists: $backupRoot"
}

$candidateHash = (Get-FileHash -Algorithm SHA256 -LiteralPath $candidateExe).Hash.ToLowerInvariant()
if ($candidateHash -ne $expectedHash) {
    throw "Candidate hash drifted: expected $expectedHash, observed $candidateHash"
}
$candidateTree = Get-TreeFingerprint -Root $candidateRoot
$installedTree = Get-TreeFingerprint -Root $publishRoot

$running = @(Get-CimInstance Win32_Process -Filter "Name = 'FuturesLiveCockpit.exe'")
if ($running.Count -ne 0) {
    $runningIds = ($running | ForEach-Object { [string]$_.ProcessId }) -join ', '
    throw "Cockpit process is running; close it before installation. PIDs: $runningIds"
}

try {
    Move-Item -LiteralPath $publishRoot -Destination $backupRoot
    $backupCreated = $true
    $rollbackTree = Get-TreeFingerprint -Root $backupRoot
    if (
        $rollbackTree.sha256 -ne $installedTree.sha256 -or
        $rollbackTree.file_count -ne $installedTree.file_count -or
        $rollbackTree.total_bytes -ne $installedTree.total_bytes
    ) {
        throw "Rollback tree verification failed before candidate installation"
    }
    try {
        Move-Item -LiteralPath $candidateRoot -Destination $publishRoot
        $candidateInstalled = $true
    }
    catch {
        if (-not (Test-Path -LiteralPath $publishRoot) -and (Test-Path -LiteralPath $backupRoot)) {
            Move-Item -LiteralPath $backupRoot -Destination $publishRoot
            $backupCreated = $false
        }
        throw
    }

    $installedHash = (Get-FileHash -Algorithm SHA256 -LiteralPath $publishExe).Hash.ToLowerInvariant()
    if ($installedHash -ne $expectedHash) {
        throw "Installed executable hash mismatch: $installedHash"
    }
    $publishedTree = Get-TreeFingerprint -Root $publishRoot
    if (
        $publishedTree.sha256 -ne $candidateTree.sha256 -or
        $publishedTree.file_count -ne $candidateTree.file_count -or
        $publishedTree.total_bytes -ne $candidateTree.total_bytes
    ) {
        throw "Installed tree differs from the validated candidate tree"
    }

    $selfCheck = Start-Process `
        -FilePath $publishExe `
        -ArgumentList '--self-check' `
        -WindowStyle Hidden `
        -PassThru
    if (-not $selfCheck.WaitForExit($SelfCheckTimeoutSeconds * 1000)) {
        $termination = Start-Process `
            -FilePath 'taskkill.exe' `
            -ArgumentList @('/PID', [string]$selfCheck.Id, '/T', '/F') `
            -WindowStyle Hidden `
            -Wait `
            -PassThru
        if ($termination.ExitCode -ne 0) {
            throw "Installed offline self-check exceeded $SelfCheckTimeoutSeconds seconds and its process tree could not be terminated"
        }
        $selfCheck.WaitForExit()
        throw "Installed offline self-check exceeded $SelfCheckTimeoutSeconds seconds"
    }
    if ($selfCheck.ExitCode -ne 0) {
        throw "Installed offline self-check failed with exit code $($selfCheck.ExitCode)"
    }

    [ordered]@{
        status = 'INSTALLED_AND_OFFLINE_VALIDATED_ROLLBACK_RETAINED'
        installed_path = $publishExe
        installed_sha256 = $installedHash
        installed_tree_sha256 = $publishedTree.sha256
        installed_tree_files = $publishedTree.file_count
        installed_tree_bytes = $publishedTree.total_bytes
        provider_requests = 0
        network_smoke = $false
        candidate_removed_by_move = -not (Test-Path -LiteralPath $candidateRoot)
        rollback_path = $backupRoot
        rollback_retained = (Test-Path -LiteralPath $backupRoot)
        rollback_tree_sha256 = $rollbackTree.sha256
        rollback_tree_files = $rollbackTree.file_count
        rollback_tree_bytes = $rollbackTree.total_bytes
    } | ConvertTo-Json -Depth 3
}
catch {
    if ($candidateInstalled -and (Test-Path -LiteralPath $publishRoot)) {
        Remove-Item -LiteralPath $publishRoot -Recurse -Force
        $candidateInstalled = $false
    }
    if ($backupCreated -and (Test-Path -LiteralPath $backupRoot)) {
        Move-Item -LiteralPath $backupRoot -Destination $publishRoot
        $backupCreated = $false
    }
    throw
}
