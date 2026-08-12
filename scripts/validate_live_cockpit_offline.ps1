[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)]
    [ValidateSet('SelfCheck', 'Demo')]
    [string]$Mode,
    [Parameter(Mandatory = $true)]
    [string]$CandidateExecutable,
    [Parameter(Mandatory = $true)]
    [string]$StateRoot,
    [Parameter(Mandatory = $true)]
    [string]$EvidencePath,
    [ValidateRange(1, 300)]
    [int]$ObservationSeconds = 140,
    [ValidateRange(100, 5000)]
    [int]$PollMilliseconds = 250
)

$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest
$script:launchedProcess = $null

trap {
    $failure = $_.Exception.Message
    if ($script:launchedProcess -and -not $script:launchedProcess.HasExited) {
        & taskkill.exe /PID $script:launchedProcess.Id /T /F | Out-Null
        $script:launchedProcess.WaitForExit()
    }
    $diagnostic = [ordered]@{
        schema = 'exact-cockpit-network-observation/1.0.0'
        status = 'HARNESS_ERROR'
        error = $failure
    }
    $parent = Split-Path -Parent $EvidencePath
    if (-not (Test-Path -LiteralPath $parent -PathType Container)) {
        New-Item -ItemType Directory -Path $parent | Out-Null
    }
    [IO.File]::WriteAllText(
        $EvidencePath,
        (ConvertTo-Json -InputObject $diagnostic -Depth 4) + [Environment]::NewLine,
        [Text.UTF8Encoding]::new($false)
    )
    Write-Error $failure
    exit 1
}

function Get-ProcessSnapshot {
    @(
        Get-CimInstance Win32_Process |
            Select-Object ProcessId, ParentProcessId, Name, ExecutablePath,
                CommandLine, CreationDate
    )
}

function Get-CockpitCensus {
    param([object[]]$Processes)
    @(
        $Processes | Where-Object {
            $_.Name -eq 'FuturesLiveCockpit.exe' -or
            ($_.ExecutablePath -and $_.ExecutablePath -match 'FuturesLiveCockpit')
        }
    )
}

function Convert-ProcessIdentity {
    param([Parameter(Mandatory = $true)][object]$Process)
    $pidValue = [int]$Process.ProcessId
    $parentValue = [int]$Process.ParentProcessId
    $nameValue = [string]$Process.Name
    $pathValue = [string]$Process.ExecutablePath
    $commandValue = [string]$Process.CommandLine
    $creationValue = $Process.CreationDate
    $identityDrift = $false
    for ($attempt = 0; $attempt -lt 20; $attempt++) {
        if ($pathValue -and $commandValue -and $creationValue) {
            break
        }
        try {
            $native = Get-Process -Id $pidValue -ErrorAction Stop
            $nativeStart = $native.StartTime
            if (
                $creationValue -and
                $nativeStart -and
                ([DateTime]$nativeStart).ToUniversalTime() -ne
                    ([DateTime]$creationValue).ToUniversalTime()
            ) {
                $identityDrift = $true
                break
            }
            if (-not $pathValue -and $native.Path) {
                $pathValue = $native.Path
            }
            if (-not $creationValue -and $nativeStart) {
                $creationValue = $nativeStart
            }
        }
        catch {
        }
        Start-Sleep -Milliseconds 25
        $refreshed = Get-CimInstance Win32_Process -Filter "ProcessId = $pidValue"
        if ($refreshed) {
            $sameIdentity = (
                [int]$refreshed.ProcessId -eq $pidValue -and
                [int]$refreshed.ParentProcessId -eq $parentValue -and
                [string]::Equals(
                    [string]$refreshed.Name,
                    $nameValue,
                    [StringComparison]::OrdinalIgnoreCase
                )
            )
            if (
                $sameIdentity -and
                $creationValue -and
                $refreshed.CreationDate
            ) {
                $sameIdentity = (
                    ([DateTime]$refreshed.CreationDate).ToUniversalTime() -eq
                    ([DateTime]$creationValue).ToUniversalTime()
                )
            }
            if (-not $sameIdentity) {
                $identityDrift = $true
                break
            }
            if (-not $pathValue -and $refreshed.ExecutablePath) {
                $pathValue = $refreshed.ExecutablePath
            }
            if (-not $commandValue -and $refreshed.CommandLine) {
                $commandValue = $refreshed.CommandLine
            }
            if (-not $creationValue -and $refreshed.CreationDate) {
                $creationValue = $refreshed.CreationDate
            }
        }
    }
    $complete = [bool]($pathValue -and $commandValue -and $creationValue)
    [ordered]@{
        pid = $pidValue
        parent_pid = $parentValue
        name = $nameValue
        executable_path = if ($pathValue) {
            [IO.Path]::GetFullPath($pathValue)
        } else {
            $null
        }
        command_line = $commandValue
        start_time_utc = if ($creationValue) {
            ([DateTime]$creationValue).ToUniversalTime().ToString('o')
        } else {
            $null
        }
        identity_status = if ($complete) {
            'COMPLETE'
        } elseif ($identityDrift) {
            'PID_REUSED_BEFORE_COMPLETE_IDENTITY'
        } else {
            'TRANSIENT_EXIT_BEFORE_COMPLETE_IDENTITY'
        }
    }
}

function Update-ProcessTree {
    param(
        [Parameter(Mandatory = $true)][hashtable]$Known,
        [Parameter(Mandatory = $true)]
        [AllowEmptyCollection()]
        [Collections.Generic.List[object]]$Observed,
        [Parameter(Mandatory = $true)]
        [AllowEmptyCollection()]
        [Collections.Generic.List[object]]$PidReuseEvents,
        [Parameter(Mandatory = $true)][object[]]$Processes
    )
    $snapshot = @{}
    foreach ($item in $Processes) {
        $pidText = [string][int]$item.ProcessId
        $snapshot[$pidText] = $item
    }
    foreach ($pidText in @($Known.Keys)) {
        if (-not $snapshot.ContainsKey($pidText)) {
            $Known.Remove($pidText)
            continue
        }
        $item = $snapshot[$pidText]
        $prior = $Known[$pidText]
        if (-not $item.CreationDate) {
            throw "Active process start time is unknown for PID $pidText"
        }
        $replacementStart = (
            [DateTime]$item.CreationDate
        ).ToUniversalTime().ToString('o')
        if ($replacementStart -ne $prior.start_time_utc) {
            $PidReuseEvents.Add([pscustomobject][ordered]@{
                pid = [int]$pidText
                prior_start_time_utc = $prior.start_time_utc
                replacement_start_time_utc = $replacementStart
                replacement_parent_pid = [int]$item.ParentProcessId
                replacement_name = [string]$item.Name
                detected_at_utc = (Get-Date).ToUniversalTime().ToString('o')
            })
            $Known.Remove($pidText)
            continue
        }
        if (
            [int]$item.ParentProcessId -ne [int]$prior.parent_pid -or
            -not [string]::Equals(
                [string]$item.Name,
                [string]$prior.name,
                [StringComparison]::OrdinalIgnoreCase
            )
        ) {
            throw "Active process identity drift detected for PID $pidText"
        }
        if ($item.ExecutablePath -and $prior.executable_path) {
            $currentPath = [IO.Path]::GetFullPath([string]$item.ExecutablePath)
            if (-not [string]::Equals(
                $currentPath,
                [string]$prior.executable_path,
                [StringComparison]::OrdinalIgnoreCase
            )) {
                throw "Active process path drift detected for PID $pidText"
            }
        }
    }
    $changed = $true
    while ($changed) {
        $changed = $false
        foreach ($item in $Processes) {
            $pidText = [string][int]$item.ProcessId
            $parentText = [string][int]$item.ParentProcessId
            if (-not $Known.ContainsKey($pidText) -and $Known.ContainsKey($parentText)) {
                $identity = Convert-ProcessIdentity $item
                $Known[$pidText] = $identity
                $Observed.Add($identity)
                $changed = $true
            }
        }
    }
}

function Get-AddressClass {
    param([Parameter(Mandatory = $true)][string]$Address)
    $parsed = $null
    if (-not [Net.IPAddress]::TryParse($Address, [ref]$parsed)) {
        return 'unknown'
    }
    if ($parsed.Equals([Net.IPAddress]::Any) -or $parsed.Equals([Net.IPAddress]::IPv6Any)) {
        return 'unspecified'
    }
    if ([Net.IPAddress]::IsLoopback($parsed)) {
        return 'loopback'
    }
    $bytes = $parsed.GetAddressBytes()
    if ($parsed.AddressFamily -eq [Net.Sockets.AddressFamily]::InterNetwork) {
        if ($bytes[0] -eq 10 -or ($bytes[0] -eq 172 -and $bytes[1] -ge 16 -and $bytes[1] -le 31) -or ($bytes[0] -eq 192 -and $bytes[1] -eq 168)) {
            return 'private'
        }
        if ($bytes[0] -eq 169 -and $bytes[1] -eq 254) {
            return 'link_local'
        }
        if ($bytes[0] -ge 224 -and $bytes[0] -le 239) {
            return 'multicast'
        }
        if (($bytes | Where-Object { $_ -ne 255 }).Count -eq 0) {
            return 'broadcast'
        }
        return 'global'
    }
    if ($parsed.IsIPv6LinkLocal) {
        return 'link_local'
    }
    if (($bytes[0] -band 0xfe) -eq 0xfc) {
        return 'private'
    }
    if ($bytes[0] -eq 0xff) {
        return 'multicast'
    }
    return 'global'
}

function Add-SocketObservation {
    param(
        [Parameter(Mandatory = $true)][hashtable]$Known,
        [Parameter(Mandatory = $true)][hashtable]$Unique,
        [Parameter(Mandatory = $true)][hashtable]$Counters,
        [Parameter(Mandatory = $true)][string]$Protocol,
        [Parameter(Mandatory = $true)][object]$Socket,
        [Parameter(Mandatory = $true)][string]$ObservedAt
    )
    $pidText = [string][int]$Socket.OwningProcess
    if (-not $Known.ContainsKey($pidText)) {
        throw "Socket ownership is unknown for PID $pidText"
    }
    $state = if ($Protocol -eq 'tcp') { [string]$Socket.State } else { 'BOUND' }
    $remoteAddress = if ($Protocol -eq 'tcp') { [string]$Socket.RemoteAddress } else { '' }
    $remotePort = if ($Protocol -eq 'tcp') { [int]$Socket.RemotePort } else { 0 }
    $localAddress = [string]$Socket.LocalAddress
    $localPort = [int]$Socket.LocalPort
    $listener = $Protocol -eq 'udp' -or $state -in @('Listen', 'Bound') -or $remotePort -eq 0
    $localClass = Get-AddressClass $localAddress
    $remoteClass = if ($listener) { 'listener' } else { Get-AddressClass $remoteAddress }
    $owner = $Known[$pidText]
    $identity = "$Protocol|$pidText|$($owner.start_time_utc)|$localAddress|$localPort|$remoteAddress|$remotePort"
    $Counters.raw = [int]$Counters.raw + 1
    if ($Unique.ContainsKey($identity)) {
        $record = $Unique[$identity]
        $record.last_seen_utc = $ObservedAt
        $record.observation_count = [int]$record.observation_count + 1
        $record.states = @($record.states + $state | Sort-Object -Unique)
        return
    }
    if ($owner.identity_status -ne 'COMPLETE') {
        throw "Socket owner identity is incomplete for PID $pidText"
    }
    $Unique[$identity] = [pscustomobject][ordered]@{
        identity = $identity
        protocol = $Protocol
        owning_pid = [int]$pidText
        owner_executable_path = $owner.executable_path
        owner_start_time_utc = $owner.start_time_utc
        parent_pid = $owner.parent_pid
        address_family = if ($localAddress -match ':') { 'IPv6' } else { 'IPv4' }
        local_address = $localAddress
        local_port = $localPort
        remote_address = $remoteAddress
        remote_port = $remotePort
        listener = $listener
        local_classification = $localClass
        remote_classification = $remoteClass
        states = @($state)
        first_seen_utc = $ObservedAt
        last_seen_utc = $ObservedAt
        observation_count = 1
    }
}

function Get-HostNetworkBaseline {
    [ordered]@{
        captured_at_utc = (Get-Date).ToUniversalTime().ToString('o')
        tcp_rows = @(Get-NetTCPConnection -ErrorAction Stop).Count
        udp_rows = @(Get-NetUDPEndpoint -ErrorAction Stop).Count
    }
}

function Write-Evidence {
    param([Parameter(Mandatory = $true)][object]$Value)
    $parent = Split-Path -Parent $EvidencePath
    if (-not (Test-Path -LiteralPath $parent -PathType Container)) {
        New-Item -ItemType Directory -Path $parent | Out-Null
    }
    [IO.File]::WriteAllText(
        $EvidencePath,
        (ConvertTo-Json -InputObject $Value -Depth 12) + [Environment]::NewLine,
        [Text.UTF8Encoding]::new($false)
    )
}

$candidate = [IO.Path]::GetFullPath($CandidateExecutable)
if (-not [IO.Path]::IsPathRooted($candidate) -or -not (Test-Path -LiteralPath $candidate -PathType Leaf)) {
    throw 'Candidate executable must be an existing absolute file'
}
if ([IO.Path]::GetFileName($candidate) -ne 'FuturesLiveCockpit.exe') {
    throw 'Candidate executable filename is unexpected'
}
$candidateDirectory = Split-Path -Parent $candidate
$stateDirectory = [IO.Path]::GetFullPath($StateRoot)
if (Test-Path -LiteralPath $stateDirectory) {
    throw "Fresh state root already exists: $stateDirectory"
}
New-Item -ItemType Directory -Path $stateDirectory | Out-Null

$initialProcesses = @(Get-ProcessSnapshot)
$baselineCockpits = @(Get-CockpitCensus $initialProcesses)
if ($baselineCockpits.Count -ne 0) {
    throw 'A cockpit process was already running before launch'
}
$baselineNetwork = Get-HostNetworkBaseline
$argument = if ($Mode -eq 'SelfCheck') { '--self-check' } else { '--demo' }
$requestUtc = (Get-Date).ToUniversalTime()
$psi = [Diagnostics.ProcessStartInfo]::new()
$psi.FileName = $candidate
$psi.WorkingDirectory = $candidateDirectory
$psi.UseShellExecute = $false
$psi.Arguments = $argument
$psi.EnvironmentVariables['LOCALAPPDATA'] = $stateDirectory
$removedVariables = @()
$credentialVariableNames = @(
    $psi.EnvironmentVariables.Keys | Where-Object {
        $_ -match '^(DATABENTO|TRADOVATE|MFF)_'
    }
)
foreach ($name in $credentialVariableNames) {
    if ($psi.EnvironmentVariables.ContainsKey($name)) {
        $psi.EnvironmentVariables.Remove($name)
        $removedVariables += $name
    }
}
if ($psi.EnvironmentVariables.ContainsKey('WEBVIEW2_ADDITIONAL_BROWSER_ARGUMENTS')) {
    $psi.EnvironmentVariables.Remove('WEBVIEW2_ADDITIONAL_BROWSER_ARGUMENTS')
}
$process = [Diagnostics.Process]::Start($psi)
$script:launchedProcess = $process
$rootPid = $process.Id
$rootCim = Get-CimInstance Win32_Process -Filter "ProcessId = $rootPid"
$rootIdentity = Convert-ProcessIdentity $rootCim
if ($rootIdentity.identity_status -ne 'COMPLETE') {
    throw 'Root process identity is incomplete'
}
$mainModulePath = [IO.Path]::GetFullPath($process.MainModule.FileName)
$getProcessPath = [IO.Path]::GetFullPath((Get-Process -Id $rootPid -ErrorAction Stop).Path)
$commandPath = $null
if ($rootIdentity.command_line -match '^"([^"]+)"(?:\s|$)') {
    $commandPath = [IO.Path]::GetFullPath($Matches[1])
}
if (-not $commandPath) {
    throw 'Root command line does not expose an exact quoted executable path'
}
$identityPaths = @($rootIdentity.executable_path, $mainModulePath, $getProcessPath, $commandPath)
if (@($identityPaths | Where-Object { -not [string]::Equals($_, $candidate, [StringComparison]::OrdinalIgnoreCase) }).Count -ne 0) {
    throw 'Spawned process path does not match the exact candidate path'
}
$observedStartUtc = [DateTimeOffset]::Parse($rootIdentity.start_time_utc).UtcDateTime
if ($observedStartUtc -lt $requestUtc.AddSeconds(-5)) {
    throw 'Spawned process start time predates the launch request'
}

$known = @{}
$known[[string]$rootPid] = $rootIdentity
$observedIdentities = [Collections.Generic.List[object]]::new()
$observedIdentities.Add($rootIdentity)
$pidReuseEvents = [Collections.Generic.List[object]]::new()
$unique = @{}
$counters = @{ raw = 0; samples = 0 }
$errors = [Collections.Generic.List[string]]::new()
$observationStarted = (Get-Date).ToUniversalTime()
$stopwatch = [Diagnostics.Stopwatch]::StartNew()
while ($stopwatch.Elapsed.TotalSeconds -lt $ObservationSeconds -and -not $process.HasExited) {
    try {
        $processes = @(Get-ProcessSnapshot)
        Update-ProcessTree -Known $known -Observed $observedIdentities -PidReuseEvents $pidReuseEvents -Processes $processes
        $otherCockpits = @(
            Get-CockpitCensus $processes | Where-Object {
                [int]$_.ProcessId -ne $rootPid
            }
        )
        if ($otherCockpits.Count -ne 0) {
            throw 'Another cockpit process appeared during observation'
        }
        $observedAt = (Get-Date).ToUniversalTime().ToString('o')
        $treeIds = @($known.Keys | ForEach-Object { [int]$_ })
        $tcp = @(
            Get-NetTCPConnection -ErrorAction Stop |
                Where-Object { $treeIds -contains [int]$_.OwningProcess }
        )
        $udp = @(
            Get-NetUDPEndpoint -ErrorAction Stop |
                Where-Object { $treeIds -contains [int]$_.OwningProcess }
        )
        $counters.samples = [int]$counters.samples + 1
        foreach ($socket in $tcp) {
            Add-SocketObservation -Known $known -Unique $unique -Counters $counters -Protocol 'tcp' -Socket $socket -ObservedAt $observedAt
        }
        foreach ($socket in $udp) {
            Add-SocketObservation -Known $known -Unique $unique -Counters $counters -Protocol 'udp' -Socket $socket -ObservedAt $observedAt
        }
    }
    catch {
        $errors.Add($_.Exception.Message)
        break
    }
    Start-Sleep -Milliseconds $PollMilliseconds
    $process.Refresh()
}
$stopwatch.Stop()
$observationEnded = (Get-Date).ToUniversalTime()

$normalCloseRequested = $false
$normalCloseSucceeded = $false
$forcedTermination = $false
if (-not $process.HasExited) {
    $normalCloseRequested = $process.CloseMainWindow()
    if ($normalCloseRequested) {
        $normalCloseSucceeded = $process.WaitForExit(10000)
    }
}
if (-not $process.HasExited) {
    $forcedTermination = $true
    & taskkill.exe /PID $rootPid /T /F | Out-Null
    $process.WaitForExit()
}
$process.WaitForExit()
Start-Sleep -Milliseconds 500
$finalProcesses = @(Get-ProcessSnapshot)
$finalUpdateError = $null
try {
    Update-ProcessTree -Known $known -Observed $observedIdentities -PidReuseEvents $pidReuseEvents -Processes $finalProcesses
}
catch {
    $finalUpdateError = $_.Exception.Message
    $errors.Add($finalUpdateError)
}
$finalCockpits = @(Get-CockpitCensus $finalProcesses)
$remainingTree = @($known.Values)

$sockets = @($unique.Values | Sort-Object protocol, owning_pid, local_address, local_port, remote_address, remote_port)
$external = @($sockets | Where-Object { -not $_.listener -and $_.remote_classification -eq 'global' })
$loopback = @($sockets | Where-Object {
    $_.local_classification -eq 'loopback' -and ($_.listener -or $_.remote_classification -eq 'loopback')
})
$privateLocal = @($sockets | Where-Object {
    $_.remote_classification -in @('private', 'link_local', 'multicast', 'broadcast')
})
$nonLoopbackOutbound = @($sockets | Where-Object {
    -not $_.listener -and $_.remote_classification -ne 'loopback'
})
$listeners = @($sockets | Where-Object { $_.listener })
$parentSockets = @($sockets | Where-Object { $_.owning_pid -eq $rootPid })
$childSockets = @($sockets | Where-Object { $_.owning_pid -ne $rootPid })
$transientIdentities = @(
    $observedIdentities | Where-Object {
        $_.identity_status -ne 'COMPLETE'
    }
)
$durationComplete = (
    $Mode -eq 'SelfCheck' -or
    $stopwatch.Elapsed.TotalSeconds -ge $ObservationSeconds
)
if (-not $durationComplete) {
    $errors.Add('Demo process exited before the requested observation interval')
}
if ($transientIdentities.Count -ne 0) {
    $errors.Add(
        'Process tree contains incomplete or PID-reused descendant identities'
    )
}
$result = [ordered]@{
    schema = 'exact-cockpit-network-observation/1.0.0'
    mode = $Mode
    candidate_executable = $candidate
    candidate_sha256 = (Get-FileHash -LiteralPath $candidate -Algorithm SHA256).Hash.ToLowerInvariant()
    working_directory = $candidateDirectory
    use_shell_execute = $psi.UseShellExecute
    argument_transport = 'ProcessStartInfo.Arguments separate from FileName'
    argument = $argument
    computer_use_initiated_process = $false
    credential_environment_variables_removed = $removedVariables
    state_root = $stateDirectory
    host_network_baseline = $baselineNetwork
    requested_at_utc = $requestUtc.ToString('o')
    observation_started_utc = $observationStarted.ToString('o')
    observation_ended_utc = $observationEnded.ToString('o')
    observation_seconds_requested = $ObservationSeconds
    observation_seconds_actual = [Math]::Round($stopwatch.Elapsed.TotalSeconds, 3)
    observation_duration_complete = $durationComplete
    poll_milliseconds_requested = $PollMilliseconds
    polling_samples = [int]$counters.samples
    raw_socket_observations = [int]$counters.raw
    unique_socket_identities = $sockets.Count
    loopback_only_socket_identities = $loopback.Count
    private_or_local_network_socket_identities = $privateLocal.Count
    globally_routable_external_socket_identities = $external.Count
    listener_socket_identities = $listeners.Count
    outbound_socket_identities = @($sockets | Where-Object { -not $_.listener }).Count
    parent_socket_identities = $parentSockets.Count
    descendant_socket_identities = $childSockets.Count
    dns_observation = 'NOT_AVAILABLE_FROM_GET_NETTCP_CONNECTION_OR_GET_NETUDPENDPOINT'
    root_pid = $rootPid
    root_executable_identity_sources = [ordered]@{
        cim_executable_path = $rootIdentity.executable_path
        process_main_module_path = $mainModulePath
        get_process_path = $getProcessPath
        command_line_executable_path = $commandPath
    }
    process_tree_count = $observedIdentities.Count
    process_tree = @($observedIdentities | Sort-Object start_time_utc, pid)
    pid_reuse_events = @($pidReuseEvents)
    transient_process_identities = $transientIdentities
    sockets = $sockets
    external_sockets = $external
    errors = @($errors)
    normal_close_requested = $normalCloseRequested
    normal_close_succeeded = $normalCloseSucceeded
    forced_termination = $forcedTermination
    exit_code = $process.ExitCode
    final_cockpit_process_count = $finalCockpits.Count
    remaining_process_tree_count = $remainingTree.Count
    status = if ($errors.Count -eq 0 -and $durationComplete -and $transientIdentities.Count -eq 0 -and $nonLoopbackOutbound.Count -eq 0 -and $finalCockpits.Count -eq 0 -and $remainingTree.Count -eq 0) { 'PASS' } else { 'FAIL' }
}
Write-Evidence $result
$result | ConvertTo-Json -Depth 12
if ($result.status -ne 'PASS') {
    exit 1
}
