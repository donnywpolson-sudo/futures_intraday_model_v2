import json
import os
import uuid
from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path

import pytest

from futures_rebuild.boundary import (
    OperationClassification,
    OperationReceipt,
    RepoBoundary,
)
from futures_rebuild.identity import ActualContractIdentity
from futures_rebuild.data_layout import (
    DataReleaseManifest as ReleaseManifest,
    DataReleaseReceipt as VerifiedReleaseReceipt,
    PhasePublisher as AtomicPublisher,
)


UTC = timezone.utc


def pytest_configure(config: pytest.Config) -> None:
    """Keep deep synthetic release trees below the Windows path ceiling.

    A process-specific root avoids collisions when separate Codex tasks test the
    repository concurrently. An explicit command-line ``--basetemp`` remains an
    intentional override.
    """

    if os.name == "nt" and config.option.basetemp is None:
        anchor = Path.cwd().anchor
        if not anchor:
            raise RuntimeError("cannot determine the Windows test-drive anchor")
        token = f"fv2t-{os.getpid()}-{uuid.uuid4().hex[:8]}"
        candidate = Path(anchor) / token
        try:
            candidate.mkdir()
        except OSError:
            candidate = Path.cwd() / ".pytest_tmp" / token
            candidate.mkdir(parents=True)
        config.option.basetemp = str(candidate)


@pytest.fixture
def boundary(tmp_path) -> RepoBoundary:
    active = tmp_path / "active"
    legacy = tmp_path / "legacy"
    stock = tmp_path / "stock"
    active.mkdir()
    legacy.mkdir()
    stock.mkdir()
    (active / "configs").mkdir()
    (active / "bundles").mkdir()
    return RepoBoundary(active.resolve(), (legacy.resolve(),), (stock.resolve(),))


@pytest.fixture
def operation_factory(boundary):
    def issue(
        operation: str,
        *,
        classification: OperationClassification = (
            OperationClassification.SYNTHETIC_MECHANICS_ONLY
        ),
        scope: dict[str, str] | None = None,
    ) -> OperationReceipt:
        return OperationReceipt.issue_local(
            boundary,
            operation=operation,
            classification=classification,
            scope=scope,
        )

    return issue


@pytest.fixture
def release_factory(boundary, operation_factory):
    counter = 0

    def publish(
        *,
        release_kind: str,
        filename: str,
        content: bytes | str | dict | list,
        schema_version: str = "1.0.0",
        metadata: dict | None = None,
        phase: str = "evaluations",
        logical_path: str | None = None,
        source_release_ids: tuple[str, ...] = (),
        embedded_documents: dict | None = None,
    ) -> tuple[object, VerifiedReleaseReceipt]:
        nonlocal counter
        counter += 1
        publisher = AtomicPublisher(
            boundary=boundary,
            operation_receipt=operation_factory("PUBLISH_RELEASE"),
            lock_path=boundary.active_root / "state" / "locks" / "data-publication.lock",
        )
        stage = publisher.create_stage("synthetic")
        documents = dict(embedded_documents or {})
        staged_paths: dict[str, str] = {}
        logical_paths: dict[str, str] = {}
        if phase == "evaluations" and logical_path is None:
            routed = {
                "actual_contract_definitions": (
                    "reference",
                    f"data/reference/definitions/{Path(filename).name}",
                ),
                "actual_contract_economics": (
                    "reference",
                    f"data/reference/economics/{Path(filename).name}",
                ),
                "futures_phase2_causal_interval": (
                    "causally_gated_normalized",
                    f"data/causally_gated_normalized/ES/2026/1m/{Path(filename).name}",
                ),
                "feature_release": (
                    "features",
                    f"data/features/synthetic/ES/2026/1m/{Path(filename).name}",
                ),
            }.get(release_kind)
            if routed is not None:
                phase, logical_path = routed
        if release_kind == "versioned_session_policy" and not documents:
            phase = "controls"
            documents[filename] = content
        elif not documents:
            path = stage / filename
            path.parent.mkdir(parents=True, exist_ok=True)
            if isinstance(content, bytes):
                path.write_bytes(content)
            elif isinstance(content, str):
                path.write_text(content, encoding="utf-8")
            else:
                path.write_text(
                    json.dumps(content, sort_keys=True, separators=(",", ":")),
                    encoding="utf-8",
                )
            logical = logical_path or (
                f"data/evaluations/SYNTHETIC/{counter:08d}/fold-0001/{Path(filename).name}"
            )
            logical_paths[filename] = logical
            staged_paths[logical] = filename
        manifest = ReleaseManifest.build(
            stage,
            phase=phase,
            release_kind=release_kind,
            schema_version=schema_version,
            logical_paths=logical_paths,
            source_release_ids=source_release_ids,
            embedded_documents=documents,
            metadata=metadata,
        )
        manifest_path = publisher.publish(
            stage, manifest, staged_paths=staged_paths or None
        )
        receipt = VerifiedReleaseReceipt.from_manifest(manifest_path, boundary)
        if manifest.files:
            payload_path = receipt.resolve_unique_filename(Path(filename).name, boundary)
            release_root: object = payload_path.parent
        else:
            release_root = manifest_path.parent
        return release_root, receipt

    return publish


@pytest.fixture
def contract() -> ActualContractIdentity:
    return ActualContractIdentity(
        dataset="GLBX.MDP3",
        publisher_id=1,
        instrument_id=12345,
        instrument_id_date_utc=date(2026, 7, 14),
        exchange_session_date=date(2026, 7, 14),
        raw_symbol="ESZ6",
        exchange="XCME",
        definition_release_id="d" * 64,
        definition_manifest_sha256="a" * 64,
        definition_row_id="b" * 64,
        currency="USD",
        multiplier=Decimal("50"),
        min_tick=Decimal("0.25"),
    )


@pytest.fixture
def decision() -> datetime:
    return datetime(2026, 7, 14, 15, 1, tzinfo=UTC)
