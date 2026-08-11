"""Backend-only credential references with a Windows-native store."""

from __future__ import annotations

from dataclasses import dataclass
import ctypes
from ctypes import wintypes
import sys
from typing import Protocol


MAX_SECRET_CHARACTERS = 2048


class CredentialStore(Protocol):
    def available(self) -> bool: ...
    def exists(self, target: str) -> bool: ...
    def read(self, target: str) -> str: ...
    def write(self, target: str, secret: str) -> None: ...
    def delete(self, target: str) -> bool: ...


def _target(value: str) -> str:
    if not value or len(value) > 240 or any(character in value for character in "\r\n\0"):
        raise ValueError("credential target must be bounded")
    return value


def redact(value: object) -> str:
    text = str(value)
    lowered = text.lower()
    for marker in ("bearer ", "accesstoken", "password", "secret", "api key", "apikey"):
        if marker in lowered:
            return "credential operation failed (redacted)"
    return text[:240]


class MemoryCredentialStore:
    """Deterministic test store; never used by normal runtime construction."""

    def __init__(self) -> None:
        self._values: dict[str, str] = {}

    def available(self) -> bool:
        return True

    def exists(self, target: str) -> bool:
        return _target(target) in self._values

    def read(self, target: str) -> str:
        key = _target(target)
        if key not in self._values:
            raise KeyError("credential reference is not configured")
        return self._values[key]

    def write(self, target: str, secret: str) -> None:
        key = _target(target)
        if not isinstance(secret, str) or not secret or len(secret) > MAX_SECRET_CHARACTERS:
            raise ValueError("credential secret must be bounded")
        self._values[key] = secret

    def delete(self, target: str) -> bool:
        return self._values.pop(_target(target), None) is not None


@dataclass(frozen=True)
class CredentialReference:
    mechanism: str
    target: str

    def __post_init__(self) -> None:
        if self.mechanism != "WINDOWS_CREDENTIAL_MANAGER":
            raise ValueError("unsupported credential mechanism")
        _target(self.target)


if sys.platform == "win32":
    class _CredentialW(ctypes.Structure):
        _fields_ = [
            ("Flags", wintypes.DWORD),
            ("Type", wintypes.DWORD),
            ("TargetName", wintypes.LPWSTR),
            ("Comment", wintypes.LPWSTR),
            ("LastWritten", wintypes.FILETIME),
            ("CredentialBlobSize", wintypes.DWORD),
            ("CredentialBlob", ctypes.POINTER(ctypes.c_ubyte)),
            ("Persist", wintypes.DWORD),
            ("AttributeCount", wintypes.DWORD),
            ("Attributes", ctypes.c_void_p),
            ("TargetAlias", wintypes.LPWSTR),
            ("UserName", wintypes.LPWSTR),
        ]


class WindowsCredentialStore:
    """Current-user encrypted generic credentials via Windows Credential Manager."""

    _GENERIC = 1
    _PERSIST_LOCAL_MACHINE = 2
    _NOT_FOUND = 1168

    def __init__(self) -> None:
        self._advapi = ctypes.WinDLL("Advapi32.dll", use_last_error=True) if sys.platform == "win32" else None
        if self._advapi is not None:
            self._advapi.CredReadW.argtypes = [wintypes.LPCWSTR, wintypes.DWORD, wintypes.DWORD, ctypes.POINTER(ctypes.POINTER(_CredentialW))]
            self._advapi.CredReadW.restype = wintypes.BOOL
            self._advapi.CredWriteW.argtypes = [ctypes.POINTER(_CredentialW), wintypes.DWORD]
            self._advapi.CredWriteW.restype = wintypes.BOOL
            self._advapi.CredDeleteW.argtypes = [wintypes.LPCWSTR, wintypes.DWORD, wintypes.DWORD]
            self._advapi.CredDeleteW.restype = wintypes.BOOL
            self._advapi.CredFree.argtypes = [ctypes.c_void_p]

    def available(self) -> bool:
        return self._advapi is not None

    def _require(self) -> object:
        if self._advapi is None:
            raise RuntimeError("Windows Credential Manager is unavailable")
        return self._advapi

    def exists(self, target: str) -> bool:
        try:
            self.read(target)
        except KeyError:
            return False
        return True

    def read(self, target: str) -> str:
        key = _target(target)
        advapi = self._require()
        pointer = ctypes.POINTER(_CredentialW)()
        if not advapi.CredReadW(key, self._GENERIC, 0, ctypes.byref(pointer)):
            error = ctypes.get_last_error()
            if error == self._NOT_FOUND:
                raise KeyError("credential reference is not configured")
            raise OSError(error, "credential read failed")
        try:
            credential = pointer.contents
            blob = ctypes.string_at(credential.CredentialBlob, credential.CredentialBlobSize)
            return blob.decode("utf-16-le")
        finally:
            advapi.CredFree(pointer)

    def write(self, target: str, secret: str) -> None:
        key = _target(target)
        if not isinstance(secret, str) or not secret or len(secret) > MAX_SECRET_CHARACTERS:
            raise ValueError("credential secret must be bounded")
        advapi = self._require()
        encoded = secret.encode("utf-16-le")
        blob = (ctypes.c_ubyte * len(encoded)).from_buffer_copy(encoded)
        credential = _CredentialW()
        credential.Type = self._GENERIC
        credential.TargetName = key
        credential.CredentialBlobSize = len(encoded)
        credential.CredentialBlob = ctypes.cast(blob, ctypes.POINTER(ctypes.c_ubyte))
        credential.Persist = self._PERSIST_LOCAL_MACHINE
        credential.UserName = "futures_intraday_model_v2"
        if not advapi.CredWriteW(ctypes.byref(credential), 0):
            error = ctypes.get_last_error()
            raise OSError(error, "credential write failed")

    def delete(self, target: str) -> bool:
        key = _target(target)
        advapi = self._require()
        if advapi.CredDeleteW(key, self._GENERIC, 0):
            return True
        error = ctypes.get_last_error()
        if error == self._NOT_FOUND:
            return False
        raise OSError(error, "credential deletion failed")
