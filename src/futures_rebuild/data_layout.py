"""Phase-first, manifest-addressed immutable data publication."""

from __future__ import annotations

import json
import os
import re
import shutil
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping, Sequence

from .boundary import OperationReceipt, RepoBoundary
from .canonical import (
    assert_no_linklike_ancestors,
    assert_plain_file,
    canonical_bytes,
    contained_path,
    fsync_directory,
    is_linklike,
    sha256_file,
    sha256_json,
)
from .errors import ContractError, IntegrityError, UnauthorizedOperation
from .locking import FileLease


LAYOUT_VERSION = "2.0.0"
MANIFEST_VERSION = "2.0.0"
PHYSICAL_LAYOUT_REVISION = "2.3.0"
MANIFEST_ROOT = Path("manifests/data_releases")
STAGING_ROOT = Path("state/data_publication_staging")
PUBLICATION_INTENT_FILENAME = "_publication_intent.json"
PUBLICATION_INTENT_VERSION = "1.0.0"
DATA_ROOTS = frozenset(
    {
        "causally_gated_normalized",
        "calendar_eligibility",
        "dbn",
        "evaluations",
        "features",
        "market_state",
        "outcome_sources",
        "outcomes",
        "predictions",
        "raw",
        "reference",
        "status_eligibility",
    }
)
MANIFEST_PHASES = DATA_ROOTS | frozenset(
    {"controls", "evidence", "foundation", "migration", "readiness"}
)
LAYOUT_CONTRACT_PATH = Path("configs/data_layout_contract.json")
_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_PHASE = re.compile(r"^[a-z][a-z0-9_]*$")
_SEGMENT = r"[^/]+"
_MARKET = r"[0-9A-Z]{1,16}"
_YEAR = r"\d{4}"
_LOGICAL_TEMPLATES = {
    "causally_gated_normalized": "data/causally_gated_normalized/{market}/{year}/{interval}/{filename}",
    "calendar_eligibility": "data/calendar_eligibility/{market}/{year}/{interval}/{filename}",
    "dbn": "data/dbn/{family}/{market}/{year}/{filename}",
    "evaluations": "data/evaluations/{classification}/{trial-id}/{fold-id}/{filename}",
    "features": "data/features/{feature-spec-id}/{market}/{year}/{interval}/{filename}",
    "market_state": "data/market_state/{status|statistics}/{market}/{year}/{interval}/{filename}",
    "outcome_sources": "data/outcome_sources/{market}/{year}/{interval}/{filename}",
    "outcomes": "data/outcomes/{label-method-id}/{market}/{year}/{interval}/{filename}",
    "predictions": "data/predictions/{bundle-id}/{market}/{year}/{session-date}/{filename}",
    "raw": "data/raw/{market}/{year}/{interval}/{filename}",
    "reference": "data/reference/{definitions|economics|exchange_calendars}/{filename}",
    "status_eligibility": "data/status_eligibility/{market}/{year}/{interval}/{filename}",
}
_RELEASE_ID_PHYSICAL_TEMPLATES = {
    phase: template.rsplit("/", 1)[0] + "/{release-id}/" + template.rsplit("/", 1)[1]
    for phase, template in _LOGICAL_TEMPLATES.items()
}
FLAT_PHYSICAL_PHASES = frozenset({"dbn"})
RETAINED_RELEASE_ID_COPY_PHASES = frozenset()
_PHYSICAL_TEMPLATES = {
    phase: (
        template
        if phase in FLAT_PHYSICAL_PHASES
        else _RELEASE_ID_PHYSICAL_TEMPLATES[phase]
    )
    for phase, template in _LOGICAL_TEMPLATES.items()
}
_RETAINED_PHYSICAL_TEMPLATES = {
    phase: _RELEASE_ID_PHYSICAL_TEMPLATES[phase]
    for phase in sorted(RETAINED_RELEASE_ID_COPY_PHASES)
}
_LOGICAL_PATTERNS = {
    "dbn": re.compile(
        rf"^data/dbn/(definition|ohlcv_1d|ohlcv_1h|ohlcv_1m|ohlcv_1s|statistics|status|trades)/{_MARKET}/{_YEAR}/{_SEGMENT}$"
    ),
    "raw": re.compile(rf"^data/raw/{_MARKET}/{_YEAR}/{_SEGMENT}/{_SEGMENT}$"),
    "causally_gated_normalized": re.compile(
        rf"^data/causally_gated_normalized/{_MARKET}/{_YEAR}/{_SEGMENT}/{_SEGMENT}$"
    ),
    "calendar_eligibility": re.compile(
        rf"^data/calendar_eligibility/{_MARKET}/{_YEAR}/{_SEGMENT}/{_SEGMENT}$"
    ),
    "reference": re.compile(
        rf"^data/reference/(definitions|economics|exchange_calendars)/{_SEGMENT}$"
    ),
    "market_state": re.compile(
        rf"^data/market_state/(status|statistics)/{_MARKET}/{_YEAR}/{_SEGMENT}/{_SEGMENT}$"
    ),
    "status_eligibility": re.compile(
        rf"^data/status_eligibility/{_MARKET}/{_YEAR}/{_SEGMENT}/{_SEGMENT}$"
    ),
    "features": re.compile(
        rf"^data/features/{_SEGMENT}/{_MARKET}/{_YEAR}/{_SEGMENT}/{_SEGMENT}$"
    ),
    "outcome_sources": re.compile(
        rf"^data/outcome_sources/{_MARKET}/{_YEAR}/{_SEGMENT}/{_SEGMENT}$"
    ),
    "outcomes": re.compile(
        rf"^data/outcomes/{_SEGMENT}/{_MARKET}/{_YEAR}/{_SEGMENT}/{_SEGMENT}$"
    ),
    "predictions": re.compile(
        rf"^data/predictions/{_SEGMENT}/{_MARKET}/{_YEAR}/\d{{4}}-\d{{2}}-\d{{2}}/{_SEGMENT}$"
    ),
    "evaluations": re.compile(
        rf"^data/evaluations/{_SEGMENT}/{_SEGMENT}/{_SEGMENT}/{_SEGMENT}$"
    ),
}


def _plain_relative(value: str, *, name: str) -> Path:
    if type(value) is not str or not value:
        raise ContractError(f"{name} must be a nonempty string")
    path = Path(value)
    if path.is_absolute() or ".." in path.parts or path.as_posix() != value:
        raise ContractError(f"{name} must be a canonical contained path")
    return path


def _logical_data_path(value: str, phase: str) -> Path:
    path = _plain_relative(value, name="logical data path")
    if len(path.parts) < 3 or path.parts[:2] != ("data", phase):
        raise ContractError("logical data path is outside its declared phase")
    pattern = _LOGICAL_PATTERNS.get(phase)
    if pattern is None or pattern.fullmatch(value) is None:
        raise ContractError("logical data path does not match its phase template")
    return path


def verify_layout_contract(path: Path) -> dict[str, object]:
    """Verify that the tracked contract exactly describes this implementation."""

    try:
        raw = path.read_bytes()
        payload = json.loads(raw.decode("utf-8"))
    except (OSError, UnicodeDecodeError, ValueError) as exc:
        raise IntegrityError("data layout contract JSON is invalid") from exc
    expected_keys = {
        "allowed_data_roots",
        "allowed_manifest_phases",
        "layout_version",
        "logical_path_templates",
        "manifest_root",
        "manifest_version",
        "physical_layout_revision",
        "physical_path_templates",
        "retained_release_id_copy_phases",
        "retained_release_id_path_templates",
        "release_id_directory_position",
        "staging_root",
    }
    if (
        not isinstance(payload, dict)
        or set(payload) != expected_keys
        or payload["layout_version"] != LAYOUT_VERSION
        or payload["manifest_version"] != MANIFEST_VERSION
        or payload["physical_layout_revision"] != PHYSICAL_LAYOUT_REVISION
        or payload["manifest_root"] != MANIFEST_ROOT.as_posix()
        or payload["staging_root"] != STAGING_ROOT.as_posix()
        or payload["release_id_directory_position"]
        != "ALL_DATA_PHASES_EXCEPT_DECLARED_FLAT_PHASES"
        or payload["allowed_data_roots"] != sorted(DATA_ROOTS)
        or payload["allowed_manifest_phases"] != sorted(MANIFEST_PHASES)
        or payload["logical_path_templates"] != _LOGICAL_TEMPLATES
        or payload["physical_path_templates"] != _PHYSICAL_TEMPLATES
        or payload["retained_release_id_copy_phases"]
        != sorted(RETAINED_RELEASE_ID_COPY_PHASES)
        or payload["retained_release_id_path_templates"]
        != _RETAINED_PHYSICAL_TEMPLATES
    ):
        raise IntegrityError("data layout contract differs from layout-v2 code")
    return payload


@dataclass(frozen=True, order=True)
class DataFileEntry:
    logical_path: str
    size: int
    sha256: str

    def __post_init__(self) -> None:
        if (
            type(self.logical_path) is not str
            or isinstance(self.size, bool)
            or type(self.size) is not int
            or type(self.sha256) is not str
            or self.size < 0
            or _SHA256.fullmatch(self.sha256) is None
        ):
            raise ContractError("data file entry fields are invalid")
        _plain_relative(self.logical_path, name="logical data path")

    def as_dict(self) -> dict[str, object]:
        return {
            "logical_path": self.logical_path,
            "sha256": self.sha256,
            "size": self.size,
        }

    @property
    def path(self) -> str:
        """Compatibility spelling for consumers while layout-v2 stays logical-path based."""

        return self.logical_path


@dataclass(frozen=True)
class DataReleaseManifest:
    release_id: str
    phase: str
    release_kind: str
    schema_version: str
    source_release_ids: tuple[str, ...]
    files: tuple[DataFileEntry, ...]
    embedded_documents: Mapping[str, object]
    metadata: Mapping[str, object]
    layout_version: str = LAYOUT_VERSION
    manifest_version: str = MANIFEST_VERSION

    def __post_init__(self) -> None:
        if (
            _SHA256.fullmatch(self.release_id) is None
            or self.phase not in MANIFEST_PHASES
            or _PHASE.fullmatch(self.phase) is None
            or type(self.release_kind) is not str
            or not self.release_kind
            or type(self.schema_version) is not str
            or not self.schema_version
            or self.layout_version != LAYOUT_VERSION
            or self.manifest_version != MANIFEST_VERSION
            or self.source_release_ids != tuple(sorted(set(self.source_release_ids)))
            or any(_SHA256.fullmatch(item) is None for item in self.source_release_ids)
            or self.files != tuple(sorted(self.files))
            or len({item.logical_path for item in self.files}) != len(self.files)
            or (not self.files and not self.embedded_documents)
        ):
            raise ContractError("data release manifest fields are invalid")
        if self.phase in DATA_ROOTS:
            for item in self.files:
                _logical_data_path(item.logical_path, self.phase)
        elif self.files:
            raise ContractError("manifest-only phases cannot own data files")
        try:
            canonical_bytes(dict(self.embedded_documents))
            canonical_bytes(dict(self.metadata))
        except (TypeError, ValueError) as exc:
            raise ContractError("manifest documents and metadata must be canonical JSON") from exc
        if self.release_id != sha256_json(self.core_dict()):
            raise ContractError("release ID does not match the manifest core")

    def core_dict(self) -> dict[str, object]:
        return {
            "embedded_documents": dict(self.embedded_documents),
            "files": [entry.as_dict() for entry in self.files],
            "layout_version": self.layout_version,
            "manifest_version": self.manifest_version,
            "metadata": dict(self.metadata),
            "phase": self.phase,
            "release_kind": self.release_kind,
            "schema_version": self.schema_version,
            "source_release_ids": list(self.source_release_ids),
        }

    def as_dict(self) -> dict[str, object]:
        return {**self.core_dict(), "release_id": self.release_id}

    @classmethod
    def build(
        cls,
        stage: Path,
        *,
        phase: str,
        release_kind: str,
        schema_version: str,
        logical_paths: Mapping[str, str] | None = None,
        source_release_ids: Sequence[str] = (),
        embedded_documents: Mapping[str, object] | None = None,
        metadata: Mapping[str, object] | None = None,
    ) -> "DataReleaseManifest":
        assert_no_linklike_ancestors(stage)
        if not stage.is_dir() or is_linklike(stage):
            raise ContractError("data release stage must be a plain directory")
        path_map = dict(logical_paths or {})
        entries: list[DataFileEntry] = []
        observed_stage_files: set[str] = set()
        for path in sorted(stage.rglob("*")):
            if path.name == PUBLICATION_INTENT_FILENAME:
                raise ContractError("stage contains the reserved publication intent")
            if is_linklike(path):
                raise ContractError(f"link-like staged path is forbidden: {path}")
            if path.is_dir():
                continue
            assert_plain_file(path)
            relative = path.relative_to(stage).as_posix()
            observed_stage_files.add(relative)
            logical = path_map.get(relative)
            if logical is None:
                raise ContractError(f"staged file has no logical data path: {relative}")
            if phase in DATA_ROOTS:
                _logical_data_path(logical, phase)
            entries.append(DataFileEntry(logical, path.stat().st_size, sha256_file(path)))
        if set(path_map) != observed_stage_files:
            raise ContractError("logical path map differs from the exact staged file set")
        core = {
            "embedded_documents": dict(embedded_documents or {}),
            "files": [entry.as_dict() for entry in sorted(entries)],
            "layout_version": LAYOUT_VERSION,
            "manifest_version": MANIFEST_VERSION,
            "metadata": dict(metadata or {}),
            "phase": phase,
            "release_kind": release_kind,
            "schema_version": schema_version,
            "source_release_ids": sorted(set(source_release_ids)),
        }
        return cls(
            release_id=sha256_json(core),
            phase=phase,
            release_kind=release_kind,
            schema_version=schema_version,
            source_release_ids=tuple(core["source_release_ids"]),
            files=tuple(sorted(entries)),
            embedded_documents=dict(embedded_documents or {}),
            metadata=dict(metadata or {}),
        )

    def physical_relative_path(self, entry: DataFileEntry) -> Path:
        logical = _logical_data_path(entry.logical_path, self.phase)
        if self.phase in FLAT_PHYSICAL_PHASES:
            return logical
        return logical.parent / self.release_id / logical.name

    def retained_release_id_relative_path(self, entry: DataFileEntry) -> Path:
        """Return the historical pre-cutover DBN release-ID path."""

        if self.phase != "dbn":
            raise ContractError("only DBN has a historical release-ID path")
        logical = _logical_data_path(entry.logical_path, self.phase)
        return logical.parent / self.release_id / logical.name


def manifest_relative_path(phase: str, release_id: str) -> Path:
    if phase not in MANIFEST_PHASES or _SHA256.fullmatch(release_id) is None:
        raise ContractError("manifest phase or release ID is invalid")
    return MANIFEST_ROOT / phase / f"{release_id}.json"


def _parse_manifest(payload: object) -> DataReleaseManifest:
    expected = {
        "embedded_documents",
        "files",
        "layout_version",
        "manifest_version",
        "metadata",
        "phase",
        "release_id",
        "release_kind",
        "schema_version",
        "source_release_ids",
    }
    if not isinstance(payload, dict) or set(payload) != expected:
        raise IntegrityError("data release manifest schema is not exact")
    if (
        not isinstance(payload["files"], list)
        or not isinstance(payload["source_release_ids"], list)
        or not isinstance(payload["embedded_documents"], dict)
        or not isinstance(payload["metadata"], dict)
    ):
        raise IntegrityError("data release manifest collections are invalid")
    try:
        files = tuple(
            DataFileEntry(item["logical_path"], item["size"], item["sha256"])
            for item in payload["files"]
            if isinstance(item, dict)
            and set(item) == {"logical_path", "sha256", "size"}
        )
        if len(files) != len(payload["files"]):
            raise IntegrityError("data release file entry schema is not exact")
        return DataReleaseManifest(
            release_id=payload["release_id"],
            phase=payload["phase"],
            release_kind=payload["release_kind"],
            schema_version=payload["schema_version"],
            source_release_ids=tuple(payload["source_release_ids"]),
            files=files,
            embedded_documents=dict(payload["embedded_documents"]),
            metadata=dict(payload["metadata"]),
            layout_version=payload["layout_version"],
            manifest_version=payload["manifest_version"],
        )
    except (KeyError, TypeError, ContractError) as exc:
        raise IntegrityError("data release manifest fields are invalid") from exc


def verify_data_release_manifest(
    manifest_path: Path, boundary: RepoBoundary, *, verify_files: bool = True
) -> DataReleaseManifest:
    path = boundary.assert_active_path(
        manifest_path, purpose="data release manifest", subtree=MANIFEST_ROOT.as_posix()
    )
    assert_plain_file(path)
    raw = path.read_bytes()
    try:
        payload = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, ValueError) as exc:
        raise IntegrityError("data release manifest JSON is invalid") from exc
    if raw != canonical_bytes(payload) + b"\n":
        raise IntegrityError("data release manifest is not canonical JSON")
    manifest = _parse_manifest(payload)
    expected = boundary.active_root / manifest_relative_path(
        manifest.phase, manifest.release_id
    )
    if path != expected.resolve(strict=True):
        raise IntegrityError("data release manifest path differs from its identity")
    if verify_files:
        for entry in manifest.files:
            physical = boundary.assert_active_path(
                boundary.active_root / manifest.physical_relative_path(entry),
                purpose="manifested data file",
                subtree=f"data/{manifest.phase}",
            )
            assert_plain_file(physical)
            if physical.stat().st_size != entry.size or sha256_file(physical) != entry.sha256:
                raise IntegrityError(f"manifested data file failed verification: {entry.logical_path}")
    return manifest


@dataclass(frozen=True)
class DataReleaseReceipt:
    repository_id: str
    manifest_path: str
    manifest_sha256: str
    layout_version: str
    phase: str
    release_id: str
    release_kind: str
    schema_version: str
    receipt_id: str

    @classmethod
    def from_manifest(
        cls, path: Path, boundary: RepoBoundary, *, verify_files: bool = True
    ) -> "DataReleaseReceipt":
        manifest = verify_data_release_manifest(path, boundary, verify_files=verify_files)
        relative = path.resolve(strict=True).relative_to(boundary.active_root).as_posix()
        core = {
            "layout_version": LAYOUT_VERSION,
            "manifest_path": relative,
            "manifest_sha256": sha256_file(path),
            "phase": manifest.phase,
            "release_id": manifest.release_id,
            "release_kind": manifest.release_kind,
            "repository_id": boundary.repository_id,
            "schema_version": manifest.schema_version,
        }
        return cls(**core, receipt_id=sha256_json(core))

    @classmethod
    def from_dict(cls, payload: Mapping[str, object]) -> "DataReleaseReceipt":
        expected = {
            "layout_version",
            "manifest_path",
            "manifest_sha256",
            "phase",
            "receipt_id",
            "release_id",
            "release_kind",
            "repository_id",
            "schema_version",
        }
        if set(payload) != expected or any(
            type(payload.get(field)) is not str for field in expected
        ):
            raise IntegrityError("data release receipt schema is invalid")
        receipt = cls(
            layout_version=payload["layout_version"],  # type: ignore[arg-type]
            manifest_path=payload["manifest_path"],  # type: ignore[arg-type]
            manifest_sha256=payload["manifest_sha256"],  # type: ignore[arg-type]
            phase=payload["phase"],  # type: ignore[arg-type]
            receipt_id=payload["receipt_id"],  # type: ignore[arg-type]
            release_id=payload["release_id"],  # type: ignore[arg-type]
            release_kind=payload["release_kind"],  # type: ignore[arg-type]
            repository_id=payload["repository_id"],  # type: ignore[arg-type]
            schema_version=payload["schema_version"],  # type: ignore[arg-type]
        )
        receipt._verify_identity()
        return receipt

    def _identity_core(self) -> dict[str, str]:
        return {
            "layout_version": self.layout_version,
            "manifest_path": self.manifest_path,
            "manifest_sha256": self.manifest_sha256,
            "phase": self.phase,
            "release_id": self.release_id,
            "release_kind": self.release_kind,
            "repository_id": self.repository_id,
            "schema_version": self.schema_version,
        }

    def _verify_identity(self) -> None:
        if (
            self.layout_version != LAYOUT_VERSION
            or self.phase not in MANIFEST_PHASES
            or _SHA256.fullmatch(self.release_id) is None
            or _SHA256.fullmatch(self.manifest_sha256) is None
            or _SHA256.fullmatch(self.receipt_id) is None
            or self.manifest_path
            != manifest_relative_path(self.phase, self.release_id).as_posix()
            or sha256_json(self._identity_core()) != self.receipt_id
        ):
            raise IntegrityError("data release receipt identity is invalid")

    def verify(self, boundary: RepoBoundary) -> DataReleaseManifest:
        self._verify_identity()
        if self.repository_id != boundary.repository_id:
            raise IntegrityError("data release receipt belongs to another repository")
        manifest = verify_data_release_manifest(
            boundary.active_root / _plain_relative(self.manifest_path, name="manifest path"),
            boundary,
        )
        if (
            manifest.release_id != self.release_id
            or manifest.phase != self.phase
            or manifest.release_kind != self.release_kind
            or manifest.schema_version != self.schema_version
            or sha256_file(boundary.active_root / self.manifest_path)
            != self.manifest_sha256
        ):
            raise IntegrityError("data release receipt differs from its manifest")
        return manifest

    def resolve_file(self, logical_path: str, boundary: RepoBoundary) -> Path:
        manifest = self.verify(boundary)
        matches = [item for item in manifest.files if item.logical_path == logical_path]
        if len(matches) != 1:
            raise IntegrityError("logical data file is absent or ambiguous")
        return boundary.active_root / manifest.physical_relative_path(matches[0])

    def resolve_unique_filename(self, filename: str, boundary: RepoBoundary) -> Path:
        relative = _plain_relative(filename, name="release filename")
        manifest = self.verify(boundary)
        matches = [
            item
            for item in manifest.files
            if Path(item.logical_path).name == relative.name
            and (
                len(relative.parts) == 1
                or Path(item.logical_path).as_posix().endswith(relative.as_posix())
            )
        ]
        if len(matches) != 1:
            raise IntegrityError("release filename is absent or ambiguous")
        return boundary.active_root / manifest.physical_relative_path(matches[0])

    def embedded_document(self, name: str, boundary: RepoBoundary) -> object:
        relative = _plain_relative(name, name="embedded document name")
        if len(relative.parts) != 1:
            raise ContractError("embedded document name must be one filename")
        manifest = self.verify(boundary)
        if name not in manifest.embedded_documents:
            raise IntegrityError("embedded release document is absent")
        return manifest.embedded_documents[name]

    def as_dict(self) -> dict[str, str]:
        return {
            "layout_version": self.layout_version,
            "manifest_path": self.manifest_path,
            "manifest_sha256": self.manifest_sha256,
            "phase": self.phase,
            "receipt_id": self.receipt_id,
            "release_id": self.release_id,
            "release_kind": self.release_kind,
            "repository_id": self.repository_id,
            "schema_version": self.schema_version,
        }


class PhasePublisher:
    """Publish manifest-addressed files from the one authorized staging tree."""

    def __init__(
        self,
        *,
        boundary: RepoBoundary,
        operation_receipt: OperationReceipt,
        lock_path: Path,
    ) -> None:
        operation_receipt.verify(boundary, operation="PUBLISH_RELEASE")
        self.boundary = boundary
        self.operation_receipt = operation_receipt
        self.staging_root = boundary.assert_active_path(
            boundary.active_root / STAGING_ROOT / "_probe",
            purpose="data publication staging",
            subtree=STAGING_ROOT.as_posix(),
        ).parent
        self.manifest_root = boundary.assert_active_path(
            boundary.active_root / MANIFEST_ROOT / "_probe",
            purpose="data release manifests",
            subtree=MANIFEST_ROOT.as_posix(),
        ).parent
        self.lock_path = boundary.assert_active_path(
            lock_path, purpose="data publication lock", subtree="state/locks"
        )

    def create_stage(self, purpose: str) -> Path:
        if not purpose or not purpose.replace("_", "").isalnum():
            raise ContractError("stage purpose must be an alphanumeric identifier")
        self.staging_root.mkdir(parents=True, exist_ok=True)
        stage = self.staging_root / f"{purpose}-{uuid.uuid4().hex}"
        stage.mkdir()
        return stage

    def _intent_path(self, stage: Path) -> Path:
        return stage / PUBLICATION_INTENT_FILENAME

    def _write_intent(
        self,
        stage: Path,
        manifest: DataReleaseManifest,
        staged_paths: Mapping[str, str],
    ) -> None:
        intent = {
            "intent_version": PUBLICATION_INTENT_VERSION,
            "manifest": manifest.as_dict(),
            "staged_paths": dict(sorted(staged_paths.items())),
            "state": "PREPARED",
        }
        encoded = canonical_bytes(intent) + b"\n"
        path = self._intent_path(stage)
        if path.exists():
            assert_plain_file(path)
            if path.read_bytes() != encoded:
                raise IntegrityError("publication intent differs from the requested release")
            return
        descriptor = os.open(
            path,
            os.O_CREAT | os.O_EXCL | os.O_WRONLY | getattr(os, "O_BINARY", 0),
        )
        try:
            os.write(descriptor, encoded)
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
        fsync_directory(stage)

    def recover_stage(self, stage: Path) -> Path:
        """Complete a commit-last publication from its durable intent."""

        self.operation_receipt.verify(self.boundary, operation="PUBLISH_RELEASE")
        stage = self.boundary.assert_active_path(
            stage, purpose="data publication recovery", subtree=STAGING_ROOT.as_posix()
        )
        intent_path = self._intent_path(stage)
        try:
            assert_plain_file(intent_path)
            raw = intent_path.read_bytes()
            payload = json.loads(raw.decode("utf-8"))
        except (OSError, UnicodeDecodeError, ValueError) as exc:
            raise IntegrityError("publication recovery intent is invalid") from exc
        if (
            not isinstance(payload, dict)
            or set(payload) != {"intent_version", "manifest", "staged_paths", "state"}
            or payload["intent_version"] != PUBLICATION_INTENT_VERSION
            or payload["state"] != "PREPARED"
            or not isinstance(payload["staged_paths"], dict)
            or any(
                type(key) is not str or type(value) is not str
                for key, value in payload["staged_paths"].items()
            )
            or raw != canonical_bytes(payload) + b"\n"
        ):
            raise IntegrityError("publication recovery intent schema is invalid")
        manifest = _parse_manifest(payload["manifest"])
        return self.publish(stage, manifest, staged_paths=payload["staged_paths"])

    def publish(
        self,
        stage: Path,
        manifest: DataReleaseManifest,
        *,
        staged_paths: Mapping[str, str] | None = None,
    ) -> Path:
        self.operation_receipt.verify(self.boundary, operation="PUBLISH_RELEASE")
        stage = self.boundary.assert_active_path(
            stage, purpose="data publication stage", subtree=STAGING_ROOT.as_posix()
        )
        stage.relative_to(self.staging_root)
        logical_to_source: dict[str, Path] = {}
        observed_stage_files: set[str] = set()
        for path in sorted(stage.rglob("*")):
            if path.name == PUBLICATION_INTENT_FILENAME:
                continue
            if is_linklike(path):
                raise IntegrityError("data publication stage contains a link-like path")
            if path.is_file():
                assert_plain_file(path)
                observed_stage_files.add(path.relative_to(stage).as_posix())
        if staged_paths is not None:
            if set(staged_paths) != {entry.logical_path for entry in manifest.files}:
                raise IntegrityError("staged path map differs from the manifest file set")
            expected_stage_files = set(staged_paths.values())
            if (
                len(expected_stage_files) != len(staged_paths)
                or not observed_stage_files.issubset(expected_stage_files)
            ):
                raise IntegrityError("publication stage contains unexpected or ambiguous files")
            for entry in manifest.files:
                relative = _plain_relative(
                    staged_paths[entry.logical_path], name="staged relative path"
                )
                source = contained_path(stage, relative.as_posix())
                target = self.boundary.active_root / manifest.physical_relative_path(entry)
                if source.exists():
                    assert_plain_file(source)
                    if source.stat().st_size != entry.size or sha256_file(source) != entry.sha256:
                        raise IntegrityError("staged path map differs from manifest bytes")
                elif target.exists():
                    assert_plain_file(target)
                    if target.stat().st_size != entry.size or sha256_file(target) != entry.sha256:
                        raise IntegrityError("promoted data differs from publication intent")
                else:
                    raise IntegrityError("publication intent has neither staged nor promoted data")
                logical_to_source[entry.logical_path] = source
        else:
            if self._intent_path(stage).exists():
                raise IntegrityError("publication recovery requires its exact staged path map")
            staged_by_identity: dict[tuple[int, str], list[Path]] = {}
            for path in sorted(
                item
                for item in stage.rglob("*")
                if item.is_file() and item.name != PUBLICATION_INTENT_FILENAME
            ):
                identity = (path.stat().st_size, sha256_file(path))
                staged_by_identity.setdefault(identity, []).append(path)
            for entry in manifest.files:
                candidates = staged_by_identity.get((entry.size, entry.sha256), [])
                if len(candidates) != 1:
                    raise IntegrityError(
                        "staged file cannot be matched uniquely to its manifest"
                    )
                logical_to_source[entry.logical_path] = candidates.pop()
            if any(candidates for candidates in staged_by_identity.values()):
                raise IntegrityError("publication stage contains an unmanifested file")
            staged_paths = {
                logical: source.relative_to(stage).as_posix()
                for logical, source in logical_to_source.items()
            }
        assert staged_paths is not None
        self._write_intent(stage, manifest, staged_paths)
        manifest_path = self.boundary.active_root / manifest_relative_path(
            manifest.phase, manifest.release_id
        )
        with FileLease(self.lock_path):
            if manifest_path.exists():
                observed = verify_data_release_manifest(manifest_path, self.boundary)
                if observed.as_dict() != manifest.as_dict():
                    raise IntegrityError("existing manifest conflicts with requested release")
                shutil.rmtree(stage)
                return manifest_path
            for entry in manifest.files:
                source = logical_to_source[entry.logical_path]
                target = self.boundary.active_root / manifest.physical_relative_path(entry)
                self.boundary.assert_active_path(
                    target,
                    purpose="manifested data publication",
                    subtree=f"data/{manifest.phase}",
                )
                target.parent.mkdir(parents=True, exist_ok=True)
                if target.exists():
                    assert_plain_file(target)
                    if target.stat().st_size != entry.size or sha256_file(target) != entry.sha256:
                        raise IntegrityError("data publication would overwrite different bytes")
                    if source.exists():
                        source.unlink()
                else:
                    assert_plain_file(source)
                    os.replace(source, target)
                fsync_directory(target.parent)
            manifest_path.parent.mkdir(parents=True, exist_ok=True)
            temporary = manifest_path.parent / f".{manifest.release_id}-{uuid.uuid4().hex}.tmp"
            descriptor = os.open(
                temporary,
                os.O_CREAT | os.O_EXCL | os.O_WRONLY | getattr(os, "O_BINARY", 0),
            )
            try:
                os.write(descriptor, canonical_bytes(manifest.as_dict()) + b"\n")
                os.fsync(descriptor)
            finally:
                os.close(descriptor)
            os.replace(temporary, manifest_path)
            fsync_directory(manifest_path.parent)
            shutil.rmtree(stage)
            verify_data_release_manifest(manifest_path, self.boundary)
            return manifest_path


def verify_data_tree_closure(boundary: RepoBoundary) -> dict[str, int]:
    manifested: dict[str, str] = {}
    retained: dict[str, str] = {}
    manifests = 0
    if (boundary.active_root / MANIFEST_ROOT).exists():
        for path in sorted((boundary.active_root / MANIFEST_ROOT).glob("*/*.json")):
            manifest = verify_data_release_manifest(path, boundary)
            manifests += 1
            for entry in manifest.files:
                physical = manifest.physical_relative_path(entry).as_posix()
                previous = manifested.setdefault(physical, manifest.release_id)
                if previous != manifest.release_id:
                    raise IntegrityError("one data file is owned by multiple releases")
                if manifest.phase in RETAINED_RELEASE_ID_COPY_PHASES:
                    retained_path = manifest.retained_release_id_relative_path(entry)
                    retained_file = boundary.active_root / retained_path
                    if retained_file.exists():
                        boundary.assert_active_path(
                            retained_file,
                            purpose="retained release-ID transition copy",
                            subtree=f"data/{manifest.phase}",
                        )
                        assert_plain_file(retained_file)
                        if (
                            retained_file.stat().st_size != entry.size
                            or sha256_file(retained_file) != entry.sha256
                        ):
                            raise IntegrityError(
                                "retained release-ID transition copy failed verification: "
                                f"{entry.logical_path}"
                            )
                        retained_relative = retained_path.as_posix()
                        previous_retained = retained.setdefault(
                            retained_relative, manifest.release_id
                        )
                        if previous_retained != manifest.release_id:
                            raise IntegrityError(
                                "one retained transition copy belongs to multiple releases"
                            )
    observed: set[str] = set()
    for phase in sorted(DATA_ROOTS):
        root = boundary.active_root / "data" / phase
        if not root.exists():
            continue
        assert_no_linklike_ancestors(root)
        for path in root.rglob("*"):
            if is_linklike(path):
                raise IntegrityError("data tree contains a link-like path")
            if path.is_file():
                observed.add(path.relative_to(boundary.active_root).as_posix())
    if observed != set(manifested) | set(retained):
        raise IntegrityError("data tree contains orphaned or missing manifested files")
    return {"data_files": len(observed), "manifests": manifests}
