import json
from datetime import datetime, timedelta, timezone

import pytest

import futures_rebuild.locking as locking_module
from futures_rebuild.errors import ContractError, IntegrityError, LeaseBusy
from futures_rebuild.locking import FileLease
from futures_rebuild.release import AtomicPublisher, ReleaseManifest, verify_release


UTC = timezone.utc


def test_atomic_release_is_content_addressed_and_tamper_evident(boundary, operation_factory) -> None:
    publisher = AtomicPublisher(
        boundary.active_root / "data" / "vault" / ".staging" / "releases" / "test-one",
        boundary.active_root / "data" / "vault" / "releases",
        boundary.active_root / "state" / "locks" / "publish-one.lock",
        boundary=boundary,
        operation_receipt=operation_factory("PUBLISH_RELEASE"),
    )
    stage = publisher.create_stage("synthetic")
    (stage / "rows.json").write_text('{"x":1}\n', encoding="utf-8")
    manifest = ReleaseManifest.build(stage, release_kind="synthetic", schema_version="1")
    release = publisher.publish(stage, manifest)
    assert release.name == manifest.release_id
    assert verify_release(release).release_id == manifest.release_id
    (release / "rows.json").write_text('{"x":2}\n', encoding="utf-8")
    with pytest.raises(IntegrityError):
        verify_release(release)


def test_idempotent_publication_removes_only_its_duplicate_stage(
    boundary, operation_factory
) -> None:
    publisher = AtomicPublisher(
        boundary.active_root / "data" / "vault" / ".staging" / "releases" / "dedupe",
        boundary.active_root / "data" / "vault" / "releases",
        boundary.active_root / "state" / "locks" / "dedupe.lock",
        boundary=boundary,
        operation_receipt=operation_factory("PUBLISH_RELEASE"),
    )
    first = publisher.create_stage("same")
    (first / "rows.json").write_text('{"x":1}\n', encoding="utf-8")
    manifest = ReleaseManifest.build(first, release_kind="same", schema_version="1")
    target = publisher.publish(first, manifest)
    unrelated = publisher.create_stage("unrelated")
    (unrelated / "keep.txt").write_text("preserve", encoding="utf-8")
    duplicate = publisher.create_stage("same")
    (duplicate / "rows.json").write_text('{"x":1}\n', encoding="utf-8")
    duplicate_manifest = ReleaseManifest.build(
        duplicate, release_kind="same", schema_version="1"
    )
    assert publisher.publish(duplicate, duplicate_manifest) == target
    assert not duplicate.exists()
    assert (unrelated / "keep.txt").read_text(encoding="utf-8") == "preserve"


def test_release_rejects_wrong_directory_identity_and_extra_empty_tree(boundary, operation_factory) -> None:
    publisher = AtomicPublisher(
        boundary.active_root / "data" / "vault" / ".staging" / "releases" / "test-two",
        boundary.active_root / "data" / "vault" / "releases",
        boundary.active_root / "state" / "locks" / "publish-two.lock",
        boundary=boundary,
        operation_receipt=operation_factory("PUBLISH_RELEASE"),
    )
    stage = publisher.create_stage("synthetic")
    (stage / "rows.json").write_text('{"x":1}\n', encoding="utf-8")
    manifest = ReleaseManifest.build(stage, release_kind="synthetic", schema_version="1")
    release = publisher.publish(stage, manifest)
    wrong = boundary.active_root / "wrong-name"
    wrong.mkdir()
    for source in release.iterdir():
        (wrong / source.name).write_bytes(source.read_bytes())
    with pytest.raises(IntegrityError, match="directory name"):
        verify_release(wrong)
    (release / "unexpected-empty").mkdir()
    with pytest.raises(IntegrityError, match="directories"):
        verify_release(release)


@pytest.mark.parametrize("location", ["top", "entry"])
def test_release_manifest_rejects_semantically_ignored_extra_fields(
    boundary, operation_factory, location
) -> None:
    publisher = AtomicPublisher(
        boundary.active_root / "data" / "vault" / ".staging" / "releases" / "test-three",
        boundary.active_root / "data" / "vault" / "releases",
        boundary.active_root / "state" / "locks" / "publish-three.lock",
        boundary=boundary,
        operation_receipt=operation_factory("PUBLISH_RELEASE"),
    )
    stage = publisher.create_stage("schema")
    (stage / "rows.json").write_text('{"x":1}\n', encoding="utf-8")
    manifest = ReleaseManifest.build(stage, release_kind="synthetic", schema_version="1")
    release = publisher.publish(stage, manifest)
    path = release / "release_manifest.json"
    payload = json.loads(path.read_text(encoding="utf-8"))
    if location == "top":
        payload["ignored"] = "must fail closed"
    else:
        payload["files"][0]["ignored"] = "must fail closed"
    path.write_text(json.dumps(payload), encoding="utf-8")
    with pytest.raises(IntegrityError, match="manifest"):
        verify_release(release)


def test_second_writer_fails_and_only_proven_dead_stale_lock_is_quarantined(
    tmp_path, monkeypatch
) -> None:
    lock = tmp_path / "writer.lock"
    acquired = datetime(2026, 1, 1, tzinfo=UTC)
    monkeypatch.setattr(locking_module, "_utc_now", lambda: acquired)
    first = FileLease(lock).acquire()
    with pytest.raises(LeaseBusy):
        FileLease(lock).acquire()
    monkeypatch.setattr(
        locking_module, "_utc_now", lambda: acquired + timedelta(hours=2)
    )
    with pytest.raises(LeaseBusy, match="alive or its death cannot be proved"):
        FileLease.quarantine_stale(
            lock,
            tmp_path / "recovery",
            older_than=timedelta(hours=1),
            expected_token=first.record.token,
        )
    monkeypatch.setattr(locking_module, "_local_process_alive", lambda pid: False)
    recovery = FileLease.quarantine_stale(
        lock,
        tmp_path / "recovery",
        older_than=timedelta(hours=1),
        expected_token=first.record.token,
    )
    assert recovery.exists() and not lock.exists()


def test_stale_recovery_rejects_caller_time_and_nonpositive_or_tiny_thresholds(
    tmp_path, monkeypatch
) -> None:
    lock = tmp_path / "writer.lock"
    acquired = datetime(2026, 1, 1, tzinfo=UTC)
    monkeypatch.setattr(locking_module, "_utc_now", lambda: acquired)
    lease = FileLease(lock).acquire()
    with pytest.raises(TypeError):
        FileLease.quarantine_stale(  # type: ignore[call-arg]
            lock,
            tmp_path / "recovery",
            older_than=timedelta(hours=1),
            now=acquired + timedelta(days=1),
            expected_token=lease.record.token,
        )
    for threshold in (timedelta(0), timedelta(seconds=-1), timedelta(minutes=1)):
        with pytest.raises(ContractError, match="at least"):
            FileLease.quarantine_stale(
                lock,
                tmp_path / "recovery",
                older_than=threshold,
                expected_token=lease.record.token,
            )


def test_lease_inspection_rejects_noncanonical_rewrites(tmp_path) -> None:
    lock = tmp_path / "writer.lock"
    FileLease(lock).acquire()
    payload = json.loads(lock.read_text(encoding="utf-8"))
    lock.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    with pytest.raises(IntegrityError, match="invalid lease record"):
        FileLease.inspect(lock)
    lock.unlink()


@pytest.mark.parametrize("bad_size", [True, "1", 1.0, None])
def test_release_manifest_rejects_coercible_file_sizes(
    boundary, operation_factory, bad_size
) -> None:
    publisher = AtomicPublisher(
        boundary.active_root / "data" / "vault" / ".staging" / "releases" / "typed",
        boundary.active_root / "data" / "vault" / "releases",
        boundary.active_root / "state" / "locks" / "typed.lock",
        boundary=boundary,
        operation_receipt=operation_factory("PUBLISH_RELEASE"),
    )
    stage = publisher.create_stage("typed")
    (stage / "rows.json").write_text('{"x":1}\n', encoding="utf-8")
    manifest = ReleaseManifest.build(stage, release_kind="synthetic", schema_version="1")
    release = publisher.publish(stage, manifest)
    path = release / "release_manifest.json"
    payload = json.loads(path.read_text(encoding="utf-8"))
    payload["files"][0]["size"] = bad_size
    path.write_text(json.dumps(payload), encoding="utf-8")
    with pytest.raises(IntegrityError, match="manifest"):
        verify_release(release)
