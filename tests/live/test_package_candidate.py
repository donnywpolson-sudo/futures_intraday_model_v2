from __future__ import annotations

import json
from pathlib import Path

import pytest

from futures_rebuild.canonical import canonical_bytes, sha256_file, sha256_json
from futures_rebuild.live_cockpit import package_candidate
from futures_rebuild.live_cockpit.approval import (
    PREDECESSOR_ATTEMPT,
    RESULT_OUTPUT_RELATIVE,
)


def _synthetic_plan_root(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    *,
    patch_canary: bool = True,
) -> Path:
    root = tmp_path / "repo"
    root.mkdir()
    executable = root / "FuturesLiveCockpit" / "FuturesLiveCockpit.exe"
    executable.parent.mkdir()
    executable.write_bytes(b"synthetic executable")

    def fake_git(_root: Path, *args: str) -> str:
        if args == ("rev-parse", "--show-toplevel"):
            return str(root.resolve())
        if args == ("branch", "--show-current"):
            return "codex/synthetic-package"
        if args == ("rev-parse", "HEAD"):
            return "1" * 40
        raise AssertionError(f"unexpected git query: {args}")

    monkeypatch.setattr(package_candidate, "_git", fake_git)
    monkeypatch.setattr(
        package_candidate,
        "_input_hashes",
        lambda _root: [
            {"path": "synthetic-input.txt", "bytes": 9, "sha256": "2" * 64}
        ],
    )
    if patch_canary:
        monkeypatch.setattr(
            package_candidate, "_validate_canary", lambda _root: "3" * 64
        )
    monkeypatch.setattr(
        package_candidate, "_validate_dependency_lock", lambda _root: "4" * 64
    )
    return root


def test_package_candidate_plan_binds_reviewed_bytes_and_is_create_only(
    local_evidence_root: Path,
) -> None:
    root = local_evidence_root
    plan = package_candidate.build_plan(root)
    body = dict(plan)
    plan_id = body.pop("plan_id")
    assert plan_id == sha256_json(body)
    inputs = {item["path"]: item["sha256"] for item in plan["inputs"]}
    assert inputs["src/futures_rebuild/live_cockpit/engine.py"] == (
        "24fa44158b3f4a49a851dfca85362ebb1b28b7aa111f3abb5e838dcd96f4561b"
    )
    assert inputs["src/futures_rebuild/live_cockpit/protocol.py"] == sha256_file(
        root / "src/futures_rebuild/live_cockpit/protocol.py"
    )
    assert inputs[package_candidate.SUCCESSFUL_CANARY] == (
        "95c1c9b73f9c2c155aaadb02805f67f9ca32ce528e38ad9ec46c94d364130a45"
    )
    assert plan["source_isolation"]["base"] == "EXACT_GIT_HEAD_ARCHIVE"
    assert plan["source_isolation"]["working_tree_other_paths"] == "EXCLUDED"
    assert plan["reviewed_successor"]["smoke_plan_predecessor_attempt"] == (
        PREDECESSOR_ATTEMPT
    )
    assert plan["reviewed_successor"]["smoke_result_output_relative"] == (
        RESULT_OUTPUT_RELATIVE
    )
    assert plan["preservation"]["current_installation"] == "NO_ACCESS_NO_MUTATION"
    assert plan["preservation"]["production_cache"] == "NO_ACCESS_NO_MUTATION"
    assert plan["limits"]["maximum_provider_requests"] == 0
    assert plan["limits"]["maximum_installations"] == 0
    assert plan["success_condition"] == "CANDIDATE_VERIFIED"


def test_package_candidate_paths_preserve_windows_headroom() -> None:
    root = Path(__file__).parents[2]
    plan_id = "a" * 64
    artifact_root = package_candidate._scoped_path(
        root, package_candidate.ARTIFACT_TEMPLATE, plan_id
    )
    report_root = package_candidate._scoped_path(
        root, package_candidate.REPORT_TEMPLATE, plan_id
    )
    scratch_root = package_candidate._scoped_path(
        root, package_candidate.SCRATCH_TEMPLATE, plan_id
    )
    projected = package_candidate._validate_path_budget(
        scratch_root=scratch_root,
        artifact_root=artifact_root,
    )
    assert artifact_root.name == plan_id[: package_candidate.PLAN_PREFIX_LENGTH]
    assert scratch_root.name == plan_id[: package_candidate.PLAN_PREFIX_LENGTH]
    assert report_root.name == plan_id
    assert max(projected.values()) <= package_candidate.MAX_WINDOWS_PACKAGE_PATH


def test_package_candidate_validation_rejects_input_drift(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = _synthetic_plan_root(tmp_path, monkeypatch)
    plan = package_candidate.build_plan(root)
    plan["limits"]["maximum_provider_requests"] = 1
    with pytest.raises(
        package_candidate.PackageCandidateError,
        match="identity mismatch",
    ):
        package_candidate.validate_plan(root, plan)


def test_package_confirmation_is_plain_language_and_create_only(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = _synthetic_plan_root(tmp_path, monkeypatch)
    plan_root = tmp_path / "plans"
    plan_path, confirmation = package_candidate.prepare_confirmation(
            root,
            plan_root=plan_root,
    )
    assert confirmation["status"] == "CONFIRMATION_REQUIRED"
    assert confirmation["operation"] == package_candidate.OPERATION
    assert "approval_to_paste" not in confirmation
    assert confirmation["limits"]["maximum_provider_requests"] == 0
    assert confirmation["preservation"]["current_installation"] == "NO_ACCESS_NO_MUTATION"
    with pytest.raises(
        package_candidate.PackageCandidateError,
        match="create-only output exists",
    ):
        package_candidate.prepare_confirmation(
            root,
            plan_root=plan_root,
        )


def test_package_candidate_missing_canary_fails_closed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = _synthetic_plan_root(tmp_path, monkeypatch, patch_canary=False)
    with pytest.raises(
        package_candidate.PackageCandidateError,
        match="successful canary terminal is unavailable",
    ):
        package_candidate.build_plan(root)


def test_package_cli_has_no_approval_line_flag() -> None:
    assert not hasattr(
        package_candidate._parser().parse_args(["run", "--plan", "plan.json"]),
        "approval_line",
    )


def test_candidate_topology_rejects_credential_locator(tmp_path: Path) -> None:
    candidate = tmp_path / "FuturesLiveCockpit"
    internal = candidate / "_internal"
    internal.mkdir(parents=True)
    (candidate / "FuturesLiveCockpit.exe").write_bytes(b"exe")
    (internal / "FuturesLiveCockpit.spec").write_text("spec", encoding="utf-8")
    (internal / "futures_live_cockpit.py").write_text("entry", encoding="utf-8")
    (internal / "credential-source.json").write_text("{}", encoding="utf-8")
    with pytest.raises(
        package_candidate.PackageCandidateError,
        match="forbidden secret, binding, or evidence path",
    ):
        package_candidate._validate_candidate(candidate)


@pytest.mark.parametrize(
    "relative",
    [
        "_internal/state/live_cockpit/execution_binding.json",
        "_internal/state/authorization_uses/receipt.json",
        "_internal/state/unpublished_evidence/audit.json",
    ],
)
def test_candidate_topology_rejects_binding_and_protected_evidence(
    tmp_path: Path, relative: str
) -> None:
    candidate = tmp_path / "FuturesLiveCockpit"
    internal = candidate / "_internal"
    internal.mkdir(parents=True)
    (candidate / "FuturesLiveCockpit.exe").write_bytes(b"exe")
    (internal / "FuturesLiveCockpit.spec").write_text("spec", encoding="utf-8")
    (internal / "futures_live_cockpit.py").write_text("entry", encoding="utf-8")
    forbidden = candidate / relative
    forbidden.parent.mkdir(parents=True, exist_ok=True)
    forbidden.write_text("{}", encoding="utf-8")

    with pytest.raises(
        package_candidate.PackageCandidateError,
        match="forbidden secret, binding, or evidence path",
    ):
        package_candidate._validate_candidate(candidate)


def test_candidate_topology_rejects_plaintext_private_key(tmp_path: Path) -> None:
    candidate = tmp_path / "FuturesLiveCockpit"
    internal = candidate / "_internal"
    internal.mkdir(parents=True)
    (candidate / "FuturesLiveCockpit.exe").write_bytes(b"exe")
    (internal / "FuturesLiveCockpit.spec").write_text("spec", encoding="utf-8")
    (internal / "futures_live_cockpit.py").write_text("entry", encoding="utf-8")
    (internal / "unexpected.bin").write_bytes(
        b"prefix-----BEGIN PRIVATE KEY-----suffix"
    )

    with pytest.raises(
        package_candidate.PackageCandidateError,
        match="plaintext private-key material",
    ):
        package_candidate._validate_candidate(candidate)


def test_candidate_finalizes_exact_package_bound_smoke_plan(
    tmp_path: Path,
) -> None:
    root = Path(__file__).parents[2]
    candidate = tmp_path / "FuturesLiveCockpit"
    config_root = candidate / "_internal" / "configs"
    config_root.mkdir(parents=True)
    executable = candidate / "FuturesLiveCockpit.exe"
    executable.write_bytes(b"synthetic executable")
    plan_path = config_root / "live_cockpit_smoke_plan.json"
    plan_path.write_bytes(
        (root / package_candidate.SMOKE_PLAN_PLACEHOLDER).read_bytes()
    )

    plan, finalized_path = package_candidate._finalize_smoke_plan(candidate)

    assert finalized_path == plan_path
    assert plan["scope"]["prepared_executable_sha256"] == sha256_file(executable)
    assert plan["scope"]["result_output_relative"] == RESULT_OUTPUT_RELATIVE
    assert plan["predecessor_attempt"] == PREDECESSOR_ATTEMPT
    assert plan_path.read_bytes() == canonical_bytes(plan) + b"\n"
