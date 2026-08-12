from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from futures_rebuild.errors import ContractError
from futures_rebuild.live_cockpit.offline_network import (
    AddressScope,
    DemoLoopbackDenyProxy,
    ProcessRecord,
    SocketRecord,
    classify_address,
    summarize_network_observations,
)


NOW = datetime(2026, 8, 11, tzinfo=UTC)
ROOT_EXE = Path("C:/candidate/FuturesLiveCockpit/FuturesLiveCockpit.exe")
REPOSITORY_ROOT = Path(__file__).resolve().parents[1]


def process(
    pid: int,
    *,
    parent: int | None,
    path: str,
    command: str = "",
) -> ProcessRecord:
    return ProcessRecord(pid, parent, path, command, NOW)


def socket(
    pid: int,
    *,
    local: str = "127.0.0.1",
    local_port: int = 49152,
    remote: str | None = None,
    remote_port: int | None = None,
    protocol: str = "tcp",
    timestamp: datetime = NOW,
) -> SocketRecord:
    return SocketRecord(
        timestamp,
        pid,
        protocol,
        "IPv6" if ":" in local else "IPv4",
        local,
        local_port,
        remote,
        remote_port,
        "Established" if remote else "Listen",
    )


def base_processes() -> tuple[ProcessRecord, ...]:
    return (
        process(100, parent=1, path=str(ROOT_EXE)),
        process(
            101,
            parent=100,
            path="C:/Program Files (x86)/Microsoft/EdgeWebView/msedgewebview2.exe",
            command="--type=utility --utility-sub-type=network.mojom.NetworkService",
        ),
        process(900, parent=1, path="C:/Windows/System32/unrelated.exe"),
    )


@pytest.mark.parametrize(
    ("address", "expected"),
    [
        ("127.0.0.1", AddressScope.LOOPBACK),
        ("::1", AddressScope.LOOPBACK),
        ("10.2.3.4", AddressScope.PRIVATE),
        ("169.254.2.3", AddressScope.LINK_LOCAL),
        ("224.0.0.251", AddressScope.MULTICAST),
        ("255.255.255.255", AddressScope.BROADCAST),
        ("0.0.0.0", AddressScope.UNSPECIFIED),
        ("8.8.8.8", AddressScope.GLOBAL),
    ],
)
def test_address_classification_is_explicit(
    address: str, expected: AddressScope
) -> None:
    assert classify_address(address) == expected


def test_repeated_samples_deduplicate_and_preserve_first_and_last_seen() -> None:
    observations = (
        socket(100),
        socket(100, timestamp=NOW + timedelta(seconds=2)),
    )
    summary = summarize_network_observations(
        processes=base_processes(),
        root_pid=100,
        expected_root_executable=ROOT_EXE,
        observations=observations,
        allow_loopback_ipc=True,
    )
    assert summary.raw_observation_count == 2
    assert summary.unique_socket_count == 1
    assert summary.sockets[0].identity[2] == NOW
    assert summary.sockets[0].observation_count == 2
    assert summary.sockets[0].first_seen == NOW
    assert summary.sockets[0].last_seen == NOW + timedelta(seconds=2)


def test_parent_and_descendant_sockets_are_captured() -> None:
    summary = summarize_network_observations(
        processes=base_processes(),
        root_pid=100,
        expected_root_executable=ROOT_EXE,
        observations=(
            socket(100),
            socket(101, local_port=49153, remote="127.0.0.1", remote_port=49152),
        ),
        allow_loopback_ipc=True,
    )
    assert {item.observation.owning_pid for item in summary.sockets} == {100, 101}
    assert {item.pid for item in summary.process_tree} == {100, 101}


def test_known_unrelated_process_socket_is_excluded() -> None:
    summary = summarize_network_observations(
        processes=base_processes(),
        root_pid=100,
        expected_root_executable=ROOT_EXE,
        observations=(socket(900, remote="8.8.8.8", remote_port=443),),
        allow_loopback_ipc=True,
    )
    assert summary.unique_socket_count == 0
    assert summary.unrelated_observation_count == 1


def test_unknown_socket_ownership_fails_closed() -> None:
    with pytest.raises(ContractError, match="ownership is unknown"):
        summarize_network_observations(
            processes=base_processes(),
            root_pid=100,
            expected_root_executable=ROOT_EXE,
            observations=(socket(777),),
            allow_loopback_ipc=True,
        )


def test_root_process_path_mismatch_fails_closed() -> None:
    with pytest.raises(ContractError, match="path mismatch"):
        summarize_network_observations(
            processes=base_processes(),
            root_pid=100,
            expected_root_executable=Path("C:/wrong/FuturesLiveCockpit.exe"),
            observations=(),
            allow_loopback_ipc=True,
        )


def test_second_appdata_cockpit_process_fails_closed() -> None:
    processes = base_processes() + (
        process(
            200,
            parent=1,
            path="C:/Users/test/AppData/Local/FuturesLiveCockpit/FuturesLiveCockpit.exe",
        ),
    )
    with pytest.raises(ContractError, match="unexpected cockpit process"):
        summarize_network_observations(
            processes=processes,
            root_pid=100,
            expected_root_executable=ROOT_EXE,
            observations=(),
            allow_loopback_ipc=True,
        )


@pytest.mark.parametrize(
    "observation",
    [
        socket(100, remote="8.8.8.8", remote_port=443),
        socket(
            100,
            remote="8.8.8.8",
            remote_port=53,
            protocol="udp",
        ),
        socket(101, remote="8.8.8.8", remote_port=443),
    ],
)
def test_external_parent_udp_and_child_sockets_fail(
    observation: SocketRecord,
) -> None:
    with pytest.raises(ContractError, match="non-permitted outbound socket"):
        summarize_network_observations(
            processes=base_processes(),
            root_pid=100,
            expected_root_executable=ROOT_EXE,
            observations=(observation,),
            allow_loopback_ipc=True,
        )


def test_external_socket_hidden_among_repeated_loopback_still_fails() -> None:
    observations = tuple(socket(100) for _ in range(20)) + (
        socket(101, remote="8.8.8.8", remote_port=443),
    )
    with pytest.raises(ContractError, match="non-permitted outbound socket"):
        summarize_network_observations(
            processes=base_processes(),
            root_pid=100,
            expected_root_executable=ROOT_EXE,
            observations=observations,
            allow_loopback_ipc=True,
        )


def test_local_pywebview_ipc_passes_only_when_explicitly_expected() -> None:
    observation = socket(101, remote="127.0.0.1", remote_port=49152)
    summary = summarize_network_observations(
        processes=base_processes(),
        root_pid=100,
        expected_root_executable=ROOT_EXE,
        observations=(observation,),
        allow_loopback_ipc=True,
    )
    assert summary.outbound_by_scope(AddressScope.LOOPBACK)
    with pytest.raises(ContractError, match="non-permitted outbound socket"):
        summarize_network_observations(
            processes=base_processes(),
            root_pid=100,
            expected_root_executable=ROOT_EXE,
            observations=(observation,),
            allow_loopback_ipc=False,
        )


def test_exact_path_offline_harness_enforces_launch_and_attribution_contract() -> None:
    script = (
        REPOSITORY_ROOT / "scripts" / "validate_live_cockpit_offline.ps1"
    ).read_text(encoding="utf-8")
    assert "[Diagnostics.ProcessStartInfo]::new()" in script
    assert "$psi.FileName = $candidate" in script
    assert "$psi.WorkingDirectory = $candidateDirectory" in script
    assert "$psi.UseShellExecute = $false" in script
    assert "$psi.Arguments = $argument" in script
    assert "Get-NetTCPConnection" in script
    assert "Get-NetUDPEndpoint" in script
    assert "Update-ProcessTree" in script
    assert "root_executable_identity_sources" in script
    assert "process_tree_count = $observedIdentities.Count" in script
    assert "PID_REUSED_BEFORE_COMPLETE_IDENTITY" in script
    assert "pid_reuse_events = @($pidReuseEvents)" in script
    assert "owner_start_time_utc" in script
    assert "$transientIdentities.Count -eq 0" in script
    assert "incomplete or PID-reused descendant identities" in script
    assert "observation_duration_complete = $durationComplete" in script
    assert "Demo process exited before the requested observation interval" in script
    assert "Socket ownership is unknown" in script
    assert "'^(DATABENTO|TRADOVATE|MFF)_'" in script
    assert (
        "$psi.EnvironmentVariables.Remove("
        "'WEBVIEW2_ADDITIONAL_BROWSER_ARGUMENTS')" in script
    )
    assert "Another cockpit process appeared" in script
    assert "computer_use_initiated_process = $false" in script
    assert "Start-Process" not in script
    assert "allowlist" not in script.casefold()


def test_demo_deny_proxy_is_loopback_only_and_rejects_requests() -> None:
    import socket
    from urllib.parse import urlsplit

    with DemoLoopbackDenyProxy() as proxy:
        endpoint = urlsplit(proxy.endpoint)
        assert classify_address(endpoint.hostname) == AddressScope.LOOPBACK
        with socket.create_connection(
            (endpoint.hostname, endpoint.port),
            timeout=1.0,
        ) as connection:
            connection.sendall(
                b"CONNECT external.invalid:443 HTTP/1.1\r\n"
                b"Host: external.invalid:443\r\n\r\n"
            )
            response = connection.recv(256)
        assert response.startswith(b"HTTP/1.1 502 Bad Gateway")
        assert proxy.accepted_connections == 1
