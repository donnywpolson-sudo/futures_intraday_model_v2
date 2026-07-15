"""Central repository containment and operation-authorization receipts."""

from __future__ import annotations

import json
import hashlib
import hmac
import os
import re
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from enum import Enum
from pathlib import Path
from types import MappingProxyType
from typing import Mapping

from .canonical import (
    assert_no_linklike_ancestors,
    assert_plain_file,
    canonical_bytes,
    fsync_directory,
    sha256_json,
)
from .errors import ContractError, IntegrityError, UnauthorizedOperation


class OperationClassification(str, Enum):
    SYNTHETIC_MECHANICS_ONLY = "SYNTHETIC_MECHANICS_ONLY"
    CONTROLLED_REBUILD_NON_ALPHA = "CONTROLLED_REBUILD_NON_ALPHA"
    EXTERNAL_CANDIDATE_AUTHORIZATION = "EXTERNAL_CANDIDATE_AUTHORIZATION"
    EXTERNAL_REAL_HISTORY_AUTHORIZATION = "EXTERNAL_REAL_HISTORY_AUTHORIZATION"


EXTERNAL_SIGNATURE_ALGORITHM = "RSASSA-PKCS1-v1_5-SHA256"
_SHA256_DIGEST_INFO_PREFIX = bytes.fromhex("3031300d060960864801650304020105000420")


@dataclass(frozen=True)
class AuthorityKeyRecord:
    key_id: str
    modulus: int
    exponent: int
    valid_from: datetime
    valid_until: datetime
    revoked: bool
    allowed_classifications: tuple[OperationClassification, ...]

    def as_dict(self) -> dict[str, object]:
        return {
            "allowed_classifications": [item.value for item in self.allowed_classifications],
            "exponent": self.exponent,
            "key_id": self.key_id,
            "modulus_hex": format(self.modulus, "x"),
            "revoked": self.revoked,
            "valid_from": self.valid_from.isoformat(),
            "valid_until": self.valid_until.isoformat(),
        }


EXTERNAL_AUTHORITY_KEYS: Mapping[str, AuthorityKeyRecord] = MappingProxyType({
    # Only the public key is in-repository. The corresponding private key is not
    # present in code, configs, tests, or the worktree, so repository code cannot
    # mint a candidate/real-history authorization by recomputing a content hash.
    "USER_GATED_REBUILD_AUTHORITY_V1": AuthorityKeyRecord(
        "USER_GATED_REBUILD_AUTHORITY_V1",
        int(
            "b03d865e53238ff71a2a782f76b5d50191296325f8e0e01e7c6c05036a909369"
            "6e6e8e411969f45df47db7c879f7a28c3ef884d58844db53b16e4cd3644797df"
            "615a5459a20f45f2507588f5d219e7b7aeba02491528ae824ca28068a6e5a1e0"
            "4c33db6b932c61d5df1d69527eae86a0ca18c5ebe39ce74c69f95c48af298084"
            "ab53f8d93e317c68e41be12688996f70a6e11009be394cdb73344cd562753b0dd"
            "0d8799a3dd29ab5541d1287b77dd7b612412081d70cde79bc07a1eb79b85b659"
            "7558350054d281f5330c9f8e87aa77dfeabfd48da5de4e5b2fb92d97fa13efbd"
            "4de09a10fbd7ea26521419b0146f23787c96c25b10b0da1c4ff17a423d51761",
            16,
        ),
        65537,
        datetime(2026, 1, 1, tzinfo=timezone.utc),
        datetime(2035, 1, 1, tzinfo=timezone.utc),
        False,
        (
            OperationClassification.EXTERNAL_CANDIDATE_AUTHORIZATION,
            OperationClassification.EXTERNAL_REAL_HISTORY_AUTHORIZATION,
        ),
    )
})
EXTERNAL_AUTHORITY_REGISTRY_HASH = sha256_json(
    [EXTERNAL_AUTHORITY_KEYS[key].as_dict() for key in sorted(EXTERNAL_AUTHORITY_KEYS)]
)


def _verify_external_signature(
    *, key: AuthorityKeyRecord, signature_hex: str, message: bytes
) -> bool:
    modulus, exponent = key.modulus, key.exponent
    width = (modulus.bit_length() + 7) // 8
    if re.fullmatch(rf"[0-9a-f]{{{width * 2}}}", signature_hex) is None:
        return False
    signature = int(signature_hex, 16)
    if signature >= modulus:
        return False
    digest_info = _SHA256_DIGEST_INFO_PREFIX + hashlib.sha256(message).digest()
    padding_length = width - len(digest_info) - 3
    if padding_length < 8:
        return False
    expected = b"\x00\x01" + b"\xff" * padding_length + b"\x00" + digest_info
    observed = pow(signature, exponent, modulus).to_bytes(width, "big")
    return hmac.compare_digest(observed, expected)


def _normalized(path: Path) -> str:
    return os.path.normcase(str(path.resolve(strict=False)))


def _scope_tuple(scope: Mapping[str, str] | None) -> tuple[tuple[str, str], ...]:
    if scope is None:
        return ()
    if not isinstance(scope, Mapping) or any(
        type(key) is not str
        or not key
        or type(value) is not str
        or not value
        for key, value in scope.items()
    ):
        raise ContractError("operation scope keys and values must be exact nonempty strings")
    result = tuple(sorted(scope.items()))
    if len({key for key, _ in result}) != len(result):
        raise ContractError("operation scope contains duplicate keys")
    return result


def _utc(value: datetime, *, name: str) -> datetime:
    if type(value) is not datetime or value.tzinfo is None or value.utcoffset() != timedelta(0):
        raise ContractError(f"{name} must be an exact timezone-aware UTC datetime")
    return value.astimezone(timezone.utc)


@dataclass(frozen=True)
class RepoBoundary:
    """One active repository plus exact read-only and forbidden peer roots."""

    active_root: Path
    legacy_roots: tuple[Path, ...] = ()
    foreign_roots: tuple[Path, ...] = ()

    def __post_init__(self) -> None:
        assert_no_linklike_ancestors(self.active_root.absolute())
        for path in (*self.legacy_roots, *self.foreign_roots):
            assert_no_linklike_ancestors(path.absolute())
        active = self.active_root.resolve(strict=False)
        if not active.is_absolute():
            raise ContractError("active repository root must be absolute")
        legacy = tuple(path.resolve(strict=False) for path in self.legacy_roots)
        foreign = tuple(path.resolve(strict=False) for path in self.foreign_roots)
        all_roots = (active, *legacy, *foreign)
        normalized = [_normalized(path) for path in all_roots]
        if len(set(normalized)) != len(normalized):
            raise ContractError("active, legacy, and foreign repository roots must be distinct")
        for left_index, left in enumerate(all_roots):
            for right in all_roots[left_index + 1 :]:
                try:
                    left.resolve(strict=False).relative_to(right.resolve(strict=False))
                except ValueError:
                    pass
                else:
                    raise ContractError("repository boundaries cannot be nested")
                try:
                    right.resolve(strict=False).relative_to(left.resolve(strict=False))
                except ValueError:
                    pass
                else:
                    raise ContractError("repository boundaries cannot be nested")
        object.__setattr__(self, "active_root", active)
        object.__setattr__(self, "legacy_roots", legacy)
        object.__setattr__(self, "foreign_roots", foreign)

    @property
    def repository_id(self) -> str:
        return sha256_json(
            {
                "active_root": _normalized(self.active_root),
                "boundary_version": "1.0.0",
            }
        )

    def _contained(self, candidate: Path, root: Path) -> bool:
        try:
            candidate.resolve(strict=False).relative_to(root.resolve(strict=False))
        except ValueError:
            return False
        return True

    def assert_active_path(
        self, path: Path, *, purpose: str, subtree: str | None = None
    ) -> Path:
        """Return a resolved active path or fail before any writer creates it."""

        if not path.is_absolute():
            raise ContractError(f"{purpose} path must be absolute")
        assert_no_linklike_ancestors(path.absolute())
        candidate = path.resolve(strict=False)
        root = self.active_root
        if subtree is not None:
            relative = Path(subtree)
            if relative.is_absolute() or ".." in relative.parts:
                raise ContractError("boundary subtree must be relative")
            root = (self.active_root / relative).resolve(strict=False)
        if candidate == root or not self._contained(candidate, root):
            raise UnauthorizedOperation(
                f"{purpose} path is outside its active repository subtree"
            )
        for forbidden in (*self.legacy_roots, *self.foreign_roots):
            if self._contained(candidate, forbidden):
                raise UnauthorizedOperation(f"{purpose} path enters a forbidden repository")
        return candidate

    def assert_active_root(self, path: Path) -> Path:
        assert_no_linklike_ancestors(path.absolute())
        candidate = path.resolve(strict=False)
        if _normalized(candidate) != _normalized(self.active_root):
            raise UnauthorizedOperation("operation is not bound to the active repository root")
        return candidate

    def assert_legacy_read_root(self, path: Path) -> Path:
        assert_no_linklike_ancestors(path.absolute())
        candidate = path.resolve(strict=False)
        if _normalized(candidate) not in {_normalized(root) for root in self.legacy_roots}:
            raise UnauthorizedOperation("read root is not an exact allowlisted legacy repository")
        return candidate

    def assert_snapshot_path(self, path: Path) -> Path:
        candidate = self.assert_active_path(
            path, purpose="source snapshot", subtree="data/vault/source_snapshots"
        )
        relative = candidate.relative_to(
            (self.active_root / "data" / "vault" / "source_snapshots").resolve(
                strict=False
            )
        )
        if len(relative.parts) != 1 or re.fullmatch(r"[0-9a-f]{64}", relative.name) is None:
            raise UnauthorizedOperation(
                "source snapshot must be one content-addressed directory beneath the active vault"
            )
        return candidate


@dataclass(frozen=True)
class OperationReceipt:
    """Hash-bound operation scope. External candidate/history receipts are load-only."""

    operation: str
    repository_id: str
    classification: OperationClassification
    scope: tuple[tuple[str, str], ...]
    externally_authorized: bool
    authority_key_id: str | None
    signature_algorithm: str | None
    signature_hex: str | None
    issued_at: datetime
    not_before: datetime
    expires_at: datetime
    nonce: str
    authority_registry_hash: str
    single_use: bool
    receipt_id: str

    @staticmethod
    def _core(
        operation: str,
        repository_id: str,
        classification: OperationClassification,
        scope: tuple[tuple[str, str], ...],
        externally_authorized: bool,
        authority_key_id: str | None,
        signature_algorithm: str | None,
        issued_at: datetime,
        not_before: datetime,
        expires_at: datetime,
        nonce: str,
        authority_registry_hash: str,
        single_use: bool,
    ) -> dict[str, object]:
        return {
            "authority_key_id": authority_key_id,
            "classification": classification.value,
            "externally_authorized": externally_authorized,
            "expires_at": expires_at.isoformat(),
            "issued_at": issued_at.isoformat(),
            "not_before": not_before.isoformat(),
            "nonce": nonce,
            "operation": operation,
            "repository_id": repository_id,
            "scope": [list(item) for item in scope],
            "signature_algorithm": signature_algorithm,
            "authority_registry_hash": authority_registry_hash,
            "single_use": single_use,
        }

    @classmethod
    def issue_local(
        cls,
        boundary: RepoBoundary,
        *,
        operation: str,
        classification: OperationClassification,
        scope: Mapping[str, str] | None = None,
    ) -> "OperationReceipt":
        if classification not in {
            OperationClassification.SYNTHETIC_MECHANICS_ONLY,
            OperationClassification.CONTROLLED_REBUILD_NON_ALPHA,
        }:
            raise UnauthorizedOperation(
                "candidate and real-history authorization receipts cannot be issued by repository code"
            )
        if type(operation) is not str or not operation:
            raise ContractError("operation must be an exact nonempty string")
        if not isinstance(classification, OperationClassification):
            raise ContractError("operation classification must use the declared enum")
        normalized_scope = _scope_tuple(scope)
        issued_at = datetime.now(timezone.utc)
        not_before = issued_at
        expires_at = issued_at + timedelta(days=1)
        nonce = os.urandom(32).hex()
        core = cls._core(
            operation,
            boundary.repository_id,
            classification,
            normalized_scope,
            False,
            None,
            None,
            issued_at,
            not_before,
            expires_at,
            nonce,
            EXTERNAL_AUTHORITY_REGISTRY_HASH,
            False,
        )
        return cls(
            operation,
            boundary.repository_id,
            classification,
            normalized_scope,
            False,
            None,
            None,
            None,
            issued_at,
            not_before,
            expires_at,
            nonce,
            EXTERNAL_AUTHORITY_REGISTRY_HASH,
            False,
            sha256_json(core),
        )

    @classmethod
    def load_external(cls, path: Path, boundary: RepoBoundary) -> "OperationReceipt":
        boundary.assert_active_path(
            path, purpose="external authorization receipt", subtree="state/authorizations"
        )
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, ValueError) as exc:
            raise UnauthorizedOperation("external authorization receipt is invalid") from exc
        try:
            receipt = cls.from_dict(payload)
        except IntegrityError as exc:
            raise UnauthorizedOperation("external authorization receipt fields are invalid") from exc
        receipt.verify(boundary)
        if receipt.classification not in {
            OperationClassification.EXTERNAL_CANDIDATE_AUTHORIZATION,
            OperationClassification.EXTERNAL_REAL_HISTORY_AUTHORIZATION,
        } or receipt.externally_authorized is not True:
            raise UnauthorizedOperation("receipt does not grant an external gated operation")
        return receipt

    @classmethod
    def from_dict(cls, payload: Mapping[str, object]) -> "OperationReceipt":
        expected = {
            "authority_registry_hash",
            "authority_key_id",
            "classification",
            "externally_authorized",
            "expires_at",
            "issued_at",
            "nonce",
            "not_before",
            "operation",
            "receipt_id",
            "repository_id",
            "scope",
            "signature_algorithm",
            "signature_hex",
            "single_use",
        }
        if not isinstance(payload, Mapping) or set(payload) != expected:
            raise IntegrityError("operation receipt schema is invalid")
        string_fields = {
            "authority_registry_hash",
            "classification",
            "expires_at",
            "issued_at",
            "nonce",
            "not_before",
            "operation",
            "receipt_id",
            "repository_id",
        }
        if (
            any(type(payload[name]) is not str for name in string_fields)
            or type(payload["externally_authorized"]) is not bool
            or type(payload["single_use"]) is not bool
            or not isinstance(payload["scope"], list)
            or any(
                not isinstance(item, list)
                or len(item) != 2
                or type(item[0]) is not str
                or not item[0]
                or type(item[1]) is not str
                or not item[1]
                for item in payload["scope"]
            )
            or any(
                payload[name] is not None and type(payload[name]) is not str
                for name in ("authority_key_id", "signature_algorithm", "signature_hex")
            )
        ):
            raise IntegrityError("operation receipt field types are invalid")
        try:
            scope = tuple((item[0], item[1]) for item in payload["scope"])
            result = cls(
                operation=payload["operation"],
                repository_id=payload["repository_id"],
                classification=OperationClassification(payload["classification"]),
                scope=scope,
                externally_authorized=payload["externally_authorized"],
                authority_key_id=payload["authority_key_id"],
                signature_algorithm=payload["signature_algorithm"],
                signature_hex=payload["signature_hex"],
                issued_at=datetime.fromisoformat(payload["issued_at"]),
                not_before=datetime.fromisoformat(payload["not_before"]),
                expires_at=datetime.fromisoformat(payload["expires_at"]),
                nonce=payload["nonce"],
                authority_registry_hash=payload["authority_registry_hash"],
                single_use=payload["single_use"],
                receipt_id=payload["receipt_id"],
            )
        except (IndexError, TypeError, ValueError) as exc:
            raise IntegrityError("operation receipt fields are invalid") from exc
        if result.as_dict() != payload:
            raise IntegrityError("operation receipt is not canonically encoded")
        return result

    def as_dict(self) -> dict[str, object]:
        return {
            **self._core(
                self.operation,
                self.repository_id,
                self.classification,
                self.scope,
                self.externally_authorized,
                self.authority_key_id,
                self.signature_algorithm,
                self.issued_at,
                self.not_before,
                self.expires_at,
                self.nonce,
                self.authority_registry_hash,
                self.single_use,
            ),
            "signature_hex": self.signature_hex,
            "receipt_id": self.receipt_id,
        }

    def verify(
        self,
        boundary: RepoBoundary,
        *,
        operation: str | None = None,
        classification: OperationClassification | None = None,
        required_scope: Mapping[str, str] | None = None,
    ) -> None:
        if (
            type(self.operation) is not str
            or not self.operation
            or type(self.repository_id) is not str
            or self.repository_id != boundary.repository_id
            or not isinstance(self.classification, OperationClassification)
            or type(self.externally_authorized) is not bool
            or type(self.single_use) is not bool
        ):
            raise UnauthorizedOperation("operation receipt is bound to a different repository")
        issued = _utc(self.issued_at, name="receipt.issued_at")
        not_before = _utc(self.not_before, name="receipt.not_before")
        expires = _utc(self.expires_at, name="receipt.expires_at")
        now = datetime.now(timezone.utc)
        if (
            issued > not_before
            or not_before >= expires
            or now < not_before
            or now >= expires
            or re.fullmatch(r"[0-9a-f]{64}", self.nonce) is None
            or self.authority_registry_hash != EXTERNAL_AUTHORITY_REGISTRY_HASH
        ):
            raise UnauthorizedOperation("operation receipt lifecycle is invalid or inactive")
        if self.scope != tuple(sorted(set(self.scope))) or any(
            type(key) is not str
            or not key
            or type(value) is not str
            or not value
            for key, value in self.scope
        ):
            raise IntegrityError("operation receipt scope is not canonical")
        core = self._core(
            self.operation,
            self.repository_id,
            self.classification,
            self.scope,
            self.externally_authorized,
            self.authority_key_id,
            self.signature_algorithm,
            issued,
            not_before,
            expires,
            self.nonce,
            self.authority_registry_hash,
            self.single_use,
        )
        if sha256_json(core) != self.receipt_id:
            raise IntegrityError("operation receipt hash is invalid")
        if self.classification in {
            OperationClassification.EXTERNAL_CANDIDATE_AUTHORIZATION,
            OperationClassification.EXTERNAL_REAL_HISTORY_AUTHORIZATION,
        }:
            key = EXTERNAL_AUTHORITY_KEYS.get(self.authority_key_id or "")
            if (
                self.externally_authorized is not True
                or self.single_use is not True
                or not self.authority_key_id
                or self.signature_algorithm != EXTERNAL_SIGNATURE_ALGORITHM
                or not self.signature_hex
                or key is None
                or key.revoked
                or now < key.valid_from
                or now >= key.valid_until
                or self.classification not in key.allowed_classifications
                or not _verify_external_signature(
                    key=key,
                    signature_hex=self.signature_hex,
                    message=canonical_bytes(core),
                )
            ):
                raise UnauthorizedOperation(
                    "external gated receipt lacks a valid pinned-authority signature"
                )
        elif (
            self.classification
            not in {
                OperationClassification.SYNTHETIC_MECHANICS_ONLY,
                OperationClassification.CONTROLLED_REBUILD_NON_ALPHA,
            }
            or self.externally_authorized is not False
            or self.authority_key_id is not None
            or self.signature_algorithm is not None
            or self.signature_hex is not None
            or self.single_use is not False
        ):
            raise UnauthorizedOperation("local receipt claims invalid external authority")
        if operation is not None and self.operation != operation:
            raise UnauthorizedOperation("operation receipt grants a different operation")
        if classification is not None and self.classification is not classification:
            raise UnauthorizedOperation("operation receipt classification is not permitted")
        if required_scope is not None and self.scope != _scope_tuple(required_scope):
            raise UnauthorizedOperation("operation receipt scope is not the exact required scope")

    def consume(
        self,
        boundary: RepoBoundary,
        *,
        operation: str,
        classification: OperationClassification,
        required_scope: Mapping[str, str],
    ) -> Path:
        """Atomically consume one external authorization at its canonical path."""

        self.verify(
            boundary,
            operation=operation,
            classification=classification,
            required_scope=required_scope,
        )
        if self.externally_authorized is not True or self.single_use is not True:
            raise UnauthorizedOperation("only one-use external receipts can be consumed")
        root = self._authorization_use_root(boundary)
        root.mkdir(parents=True, exist_ok=True)
        path = root / f"{self.receipt_id}.json"
        payload = self._authorization_use_payload()
        try:
            descriptor = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        except FileExistsError as exc:
            raise UnauthorizedOperation("external authorization receipt was already used") from exc
        try:
            os.write(descriptor, canonical_bytes(payload) + b"\n")
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
        fsync_directory(root)
        return self.assert_consumed(
            boundary,
            operation=operation,
            classification=classification,
            required_scope=required_scope,
        )

    def _authorization_use_root(self, boundary: RepoBoundary) -> Path:
        return boundary.assert_active_path(
            boundary.active_root / "state" / "authorization_uses",
            purpose="authorization-use ledger",
        )

    def _authorization_use_payload(self) -> dict[str, str]:
        return {
            "nonce": self.nonce,
            "operation": self.operation,
            "receipt_id": self.receipt_id,
            "repository_id": self.repository_id,
            "scope_hash": sha256_json([list(item) for item in self.scope]),
        }

    def assert_consumed(
        self,
        boundary: RepoBoundary,
        *,
        operation: str,
        classification: OperationClassification,
        required_scope: Mapping[str, str],
    ) -> Path:
        """Verify the exact canonical one-use record for an external action."""

        self.verify(
            boundary,
            operation=operation,
            classification=classification,
            required_scope=required_scope,
        )
        if self.externally_authorized is not True or self.single_use is not True:
            raise UnauthorizedOperation("only one-use external receipts have use records")
        path = self._authorization_use_root(boundary) / f"{self.receipt_id}.json"
        try:
            assert_plain_file(path)
            raw = path.read_bytes()
            payload = json.loads(raw.decode("utf-8"))
        except (OSError, UnicodeDecodeError, ValueError, ContractError, IntegrityError) as exc:
            raise UnauthorizedOperation(
                "external authorization lacks its canonical use record"
            ) from exc
        expected = self._authorization_use_payload()
        if payload != expected or raw != canonical_bytes(expected) + b"\n":
            raise IntegrityError("external authorization use record is invalid")
        return path
