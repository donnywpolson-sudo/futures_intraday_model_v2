"""Deterministic process-tree and socket accounting for offline cockpit checks."""

from __future__ import annotations

import ipaddress
import os
import socket
import threading
from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum
from pathlib import Path
from typing import Iterable

from futures_rebuild.errors import ContractError


class AddressScope(StrEnum):
    NONE = "none"
    LOOPBACK = "loopback"
    PRIVATE = "private"
    LINK_LOCAL = "link_local"
    MULTICAST = "multicast"
    BROADCAST = "broadcast"
    UNSPECIFIED = "unspecified"
    GLOBAL = "global"
    RESERVED = "reserved"


@dataclass(frozen=True)
class ProcessRecord:
    pid: int
    parent_pid: int | None
    executable_path: str
    command_line: str
    start_time: datetime


@dataclass(frozen=True)
class SocketRecord:
    timestamp: datetime
    owning_pid: int
    protocol: str
    address_family: str
    local_address: str
    local_port: int
    remote_address: str | None
    remote_port: int | None
    state: str


@dataclass(frozen=True)
class SocketAggregate:
    identity: tuple[object, ...]
    observation: SocketRecord
    local_scope: AddressScope
    remote_scope: AddressScope
    first_seen: datetime
    last_seen: datetime
    observation_count: int


@dataclass(frozen=True)
class NetworkSummary:
    process_tree: tuple[ProcessRecord, ...]
    raw_observation_count: int
    unrelated_observation_count: int
    sockets: tuple[SocketAggregate, ...]

    @property
    def unique_socket_count(self) -> int:
        return len(self.sockets)

    @property
    def outbound_sockets(self) -> tuple[SocketAggregate, ...]:
        return tuple(item for item in self.sockets if item.observation.remote_address)

    @property
    def listener_sockets(self) -> tuple[SocketAggregate, ...]:
        return tuple(item for item in self.sockets if not item.observation.remote_address)

    def outbound_by_scope(self, scope: AddressScope) -> tuple[SocketAggregate, ...]:
        return tuple(item for item in self.outbound_sockets if item.remote_scope == scope)


class DemoLoopbackDenyProxy:
    """Reject nonlocal WebView2 requests at a private demo-only loopback proxy."""

    def __init__(self) -> None:
        self._listener: socket.socket | None = None
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._accepted_connections = 0
        self.endpoint = ""

    @property
    def accepted_connections(self) -> int:
        return self._accepted_connections

    def __enter__(self) -> "DemoLoopbackDenyProxy":
        candidates = socket.getaddrinfo(
            "localhost",
            0,
            family=socket.AF_INET,
            type=socket.SOCK_STREAM,
        )
        if not candidates:
            raise ContractError("localhost has no IPv4 stream address")
        host = candidates[0][4][0]
        if classify_address(host) != AddressScope.LOOPBACK:
            raise ContractError("localhost did not resolve to loopback")
        listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            exclusive = getattr(socket, "SO_EXCLUSIVEADDRUSE", None)
            if exclusive is not None:
                listener.setsockopt(socket.SOL_SOCKET, exclusive, 1)
            listener.bind((host, 0))
            listener.listen()
            listener.settimeout(0.2)
        except BaseException:
            listener.close()
            raise
        bound_host, bound_port = listener.getsockname()
        if classify_address(bound_host) != AddressScope.LOOPBACK:
            listener.close()
            raise ContractError("demo deny proxy did not bind to loopback")
        self._listener = listener
        self.endpoint = f"http://{bound_host}:{bound_port}"
        self._thread = threading.Thread(
            target=self._serve,
            name="demo-loopback-deny-proxy",
            daemon=True,
        )
        self._thread.start()
        return self

    def _serve(self) -> None:
        listener = self._listener
        if listener is None:
            return
        while not self._stop.is_set():
            try:
                connection, _ = listener.accept()
            except TimeoutError:
                continue
            except OSError:
                break
            self._accepted_connections += 1
            with connection:
                connection.settimeout(0.2)
                try:
                    connection.recv(4096)
                except (OSError, TimeoutError):
                    pass
                try:
                    connection.sendall(
                        b"HTTP/1.1 502 Bad Gateway\r\n"
                        b"Connection: close\r\n"
                        b"Content-Length: 0\r\n\r\n"
                    )
                except OSError:
                    pass

    def __exit__(self, *_: object) -> None:
        self._stop.set()
        if self._listener is not None:
            self._listener.close()
        if self._thread is not None:
            self._thread.join(timeout=2.0)
            if self._thread.is_alive():
                raise ContractError("demo deny proxy did not stop")


def classify_address(address: str | None) -> AddressScope:
    if not address:
        return AddressScope.NONE
    if address == "255.255.255.255":
        return AddressScope.BROADCAST
    try:
        parsed = ipaddress.ip_address(address.split("%", 1)[0])
    except ValueError as exc:
        raise ContractError(f"socket address is invalid: {address!r}") from exc
    if parsed.is_loopback:
        return AddressScope.LOOPBACK
    if parsed.is_link_local:
        return AddressScope.LINK_LOCAL
    if parsed.is_multicast:
        return AddressScope.MULTICAST
    if parsed.is_unspecified:
        return AddressScope.UNSPECIFIED
    if parsed.is_private:
        return AddressScope.PRIVATE
    if parsed.is_global:
        return AddressScope.GLOBAL
    return AddressScope.RESERVED


def _normalized_executable(path: str | Path) -> str:
    return os.path.normcase(os.path.abspath(os.fspath(path)))


def _process_tree(
    processes: dict[int, ProcessRecord],
    *,
    root_pid: int,
    expected_root_executable: Path,
) -> tuple[ProcessRecord, ...]:
    root = processes.get(root_pid)
    if root is None:
        raise ContractError(f"root process ownership is unknown: pid={root_pid}")
    if not root.executable_path:
        raise ContractError(f"root process path is unknown: pid={root_pid}")
    expected = _normalized_executable(expected_root_executable)
    actual = _normalized_executable(root.executable_path)
    if actual != expected:
        raise ContractError(
            f"root process path mismatch: expected={expected} actual={actual}"
        )
    tree_pids = {root_pid}
    changed = True
    while changed:
        changed = False
        for process in processes.values():
            if process.parent_pid in tree_pids and process.pid not in tree_pids:
                tree_pids.add(process.pid)
                changed = True
    tree = tuple(sorted((processes[pid] for pid in tree_pids), key=lambda item: item.pid))
    for process in tree:
        if not process.executable_path:
            raise ContractError(f"process path is unknown: pid={process.pid}")
    return tree


def summarize_network_observations(
    *,
    processes: Iterable[ProcessRecord],
    root_pid: int,
    expected_root_executable: Path,
    observations: Iterable[SocketRecord],
    allow_loopback_ipc: bool,
) -> NetworkSummary:
    process_items = tuple(processes)
    process_map = {process.pid: process for process in process_items}
    if len(process_map) != len(process_items):
        raise ContractError("process census contains duplicate PIDs")
    for process in process_map.values():
        if (
            Path(process.executable_path).name.casefold() == "futureslivecockpit.exe"
            and process.pid != root_pid
        ):
            raise ContractError(
                f"unexpected cockpit process is running: pid={process.pid} "
                f"path={process.executable_path}"
            )
    tree = _process_tree(
        process_map,
        root_pid=root_pid,
        expected_root_executable=expected_root_executable,
    )
    tree_pids = {process.pid for process in tree}
    aggregates: dict[tuple[object, ...], SocketAggregate] = {}
    unrelated_count = 0
    raw_count = 0
    for observation in observations:
        raw_count += 1
        if observation.owning_pid not in process_map:
            raise ContractError(
                f"socket process ownership is unknown: pid={observation.owning_pid}"
            )
        if observation.owning_pid not in tree_pids:
            unrelated_count += 1
            continue
        local_scope = classify_address(observation.local_address)
        remote_scope = classify_address(observation.remote_address)
        identity = (
            observation.protocol.lower(),
            observation.owning_pid,
            process_map[observation.owning_pid].start_time,
            observation.local_address,
            observation.local_port,
            observation.remote_address,
            observation.remote_port,
        )
        existing = aggregates.get(identity)
        if existing is None:
            aggregates[identity] = SocketAggregate(
                identity=identity,
                observation=observation,
                local_scope=local_scope,
                remote_scope=remote_scope,
                first_seen=observation.timestamp,
                last_seen=observation.timestamp,
                observation_count=1,
            )
        else:
            aggregates[identity] = SocketAggregate(
                identity=identity,
                observation=existing.observation,
                local_scope=existing.local_scope,
                remote_scope=existing.remote_scope,
                first_seen=min(existing.first_seen, observation.timestamp),
                last_seen=max(existing.last_seen, observation.timestamp),
                observation_count=existing.observation_count + 1,
            )
    sockets = tuple(sorted(aggregates.values(), key=lambda item: repr(item.identity)))
    for socket in sockets:
        if not socket.observation.remote_address:
            continue
        if socket.remote_scope == AddressScope.LOOPBACK and allow_loopback_ipc:
            continue
        raise ContractError(
            "offline cockpit process tree opened a non-permitted outbound socket: "
            f"pid={socket.observation.owning_pid} "
            f"remote={socket.observation.remote_address}:{socket.observation.remote_port} "
            f"scope={socket.remote_scope}"
        )
    return NetworkSummary(
        process_tree=tree,
        raw_observation_count=raw_count,
        unrelated_observation_count=unrelated_count,
        sockets=sockets,
    )
