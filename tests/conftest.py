import json
import os
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
from futures_rebuild.release import (
    AtomicPublisher,
    ReleaseManifest,
    VerifiedReleaseReceipt,
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
        config.option.basetemp = str(Path(anchor) / f"fv2t-{os.getpid()}")


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
    ) -> tuple[object, VerifiedReleaseReceipt]:
        nonlocal counter
        counter += 1
        publisher = AtomicPublisher(
            boundary.active_root / "data" / "vault" / ".staging" / "releases" / f"release-{counter}",
            boundary.active_root / "data" / "vault" / "releases",
            boundary.active_root / "state" / "locks" / f"release-{counter}.lock",
            boundary=boundary,
            operation_receipt=operation_factory("PUBLISH_RELEASE"),
        )
        stage = publisher.create_stage("synthetic")
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
        manifest = ReleaseManifest.build(
            stage,
            release_kind=release_kind,
            schema_version=schema_version,
            metadata=metadata,
        )
        release = publisher.publish(stage, manifest)
        return release, VerifiedReleaseReceipt.from_release(release, boundary)

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
