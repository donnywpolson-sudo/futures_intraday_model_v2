from __future__ import annotations

import base64
import importlib.metadata
import inspect
import json
from pathlib import Path
import secrets
import shutil

import pytest

from futures_rebuild.canonical import canonical_bytes, sha256_file, sha256_json
from futures_rebuild.live_cockpit import package_candidate
from futures_rebuild.live_cockpit.approval import (
    PREDECESSOR_ATTEMPT,
    RESULT_OUTPUT_RELATIVE,
)


ROOT = Path(__file__).parents[2]


def _candidate(tmp_path: Path) -> Path:
    candidate = tmp_path / "FuturesLiveCockpit"
    internal = candidate / "_internal"
    internal.mkdir(parents=True)
    (candidate / "FuturesLiveCockpit.exe").write_bytes(b"synthetic executable")
    (internal / "FuturesLiveCockpit.spec").write_text("spec", encoding="utf-8")
    (internal / "futures_live_cockpit.py").write_text("entry", encoding="utf-8")
    return candidate


def _pem(label: str, payload: bytes | None = None, *, newline: str = "\n") -> bytes:
    key_shaped = payload if payload is not None else b"\x30" + secrets.token_bytes(95)
    encoded = base64.b64encode(key_shaped).decode("ascii")
    lines = [encoded[index : index + 64] for index in range(0, len(encoded), 64)]
    return (
        f"-----BEGIN {label}-----{newline}"
        + newline.join(lines)
        + f"{newline}-----END {label}-----{newline}"
    ).encode("ascii")


def _rejected(candidate: Path, *, root: Path = ROOT) -> dict[str, object]:
    with pytest.raises(package_candidate.PrivateKeyScanError) as caught:
        package_candidate._scan_candidate_private_keys(candidate, root=root)
    return caught.value.result


def _source_arrow() -> Path:
    distribution = importlib.metadata.distribution("pyarrow")
    matches = [
        item
        for item in (distribution.files or [])
        if str(item).replace("\\", "/").lower() == "pyarrow/arrow.dll"
    ]
    assert len(matches) == 1
    return Path(distribution.locate_file(matches[0])).resolve()


def _copy_arrow(candidate: Path, relative: str = "_internal/pyarrow/arrow.dll") -> Path:
    destination = candidate / relative
    destination.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(_source_arrow(), destination)
    return destination


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


def test_isolated_archive_contains_only_observation_runtime_configs() -> None:
    required = {
        "configs/alpha_tiered.yaml",
        "configs/live_cockpit_smoke_plan.json",
    }
    forbidden = {
        "configs/prop_firm_execution_connections.json",
        "configs/prop_firm_profiles.json",
        "configs/prop_firm_execution_costs.json",
        "configs/prop_firm_execution_instruments.json",
        "configs/prop_firm_strategy_risk_policies.json",
        "configs/prop_firm_payout_policies.json",
    }

    assert required <= set(package_candidate.ARCHIVE_PATHS)
    assert forbidden.isdisjoint(package_candidate.ARCHIVE_PATHS)
    assert forbidden.isdisjoint(package_candidate.PACKAGE_INPUTS)
    assert all("/execution/" not in path for path in package_candidate.RUNTIME_OVERLAYS)


def test_packaged_archive_rejects_execution_modules() -> None:
    safe = "futures_rebuild.live_cockpit.engine\npyarrow.compute\n"
    rejected = (
        safe
        + "futures_rebuild.live_cockpit.execution\n"
        + "futures_rebuild.live_cockpit.execution.tradovate_adapter\n"
    )

    assert package_candidate._forbidden_archive_members(safe) == []
    assert package_candidate._forbidden_archive_members(rejected) == [
        "futures_rebuild.live_cockpit.execution",
        "futures_rebuild.live_cockpit.execution.tradovate_adapter",
    ]


def test_packaging_spec_excludes_execution_runtime() -> None:
    spec = (ROOT / "FuturesLiveCockpit/_internal/FuturesLiveCockpit.spec").read_text(
        encoding="utf-8"
    )

    assert "futures_rebuild.live_cockpit.execution'" in spec
    assert "execution.tradovate_adapter" not in spec
    assert "execution.manual_assistant" not in spec
    assert "prop_firm_execution" not in spec
    assert "mff_execution" not in spec


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
    candidate = _candidate(tmp_path)
    (candidate / "_internal/unexpected.bin").write_bytes(
        b"prefix-----BEGIN PRIVATE KEY-----suffix"
    )

    with pytest.raises(
        package_candidate.PackageCandidateError,
        match="rejected private-key material",
    ):
        package_candidate._validate_candidate(candidate)


@pytest.mark.parametrize(
    ("label", "payload"),
    [
        ("PRIVATE KEY", None),
        ("RSA PRIVATE KEY", None),
        ("EC PRIVATE KEY", None),
        ("ENCRYPTED PRIVATE KEY", None),
        ("OPENSSH PRIVATE KEY", b"openssh-key-v1\x00" + b"x" * 80),
    ],
)
def test_complete_private_key_pem_types_fail_closed(
    tmp_path: Path, label: str, payload: bytes | None
) -> None:
    candidate = _candidate(tmp_path)
    (candidate / "_internal/key.txt").write_bytes(_pem(label, payload))

    result = _rejected(candidate)

    assert result["classification_counts"]["ACTUAL_PRIVATE_KEY_MATERIAL"] == 1
    assert result["findings"][0]["label"] == label


def test_malformed_complete_private_key_block_fails_closed(tmp_path: Path) -> None:
    candidate = _candidate(tmp_path)
    (candidate / "_internal/malformed.bin").write_bytes(
        b"binary\x00-----BEGIN PRIVATE KEY-----\nnot base64!?\n"
        b"-----END PRIVATE KEY-----\x00"
    )

    result = _rejected(candidate)

    assert result["classification_counts"] == {
        "SUSPICIOUS_COMPLETE_PRIVATE_KEY_BLOCK": 1
    }


def test_complete_private_key_inside_arbitrary_binary_fails_closed(
    tmp_path: Path,
) -> None:
    candidate = _candidate(tmp_path)
    payload = _pem("PRIVATE KEY")
    (candidate / "_internal/arbitrary.bin").write_bytes(
        secrets.token_bytes(31) + payload + secrets.token_bytes(29)
    )

    result = _rejected(candidate)

    assert result["classification_counts"]["ACTUAL_PRIVATE_KEY_MATERIAL"] == 1


@pytest.mark.parametrize("encoding", ["utf-16le", "utf-16be"])
def test_utf16_private_key_block_fails_closed(
    tmp_path: Path, encoding: str
) -> None:
    candidate = _candidate(tmp_path)
    pem = _pem("PRIVATE KEY").decode("ascii").encode(encoding)
    (candidate / "_internal/utf16.bin").write_bytes(b"\x00\xff" + pem)

    result = _rejected(candidate)

    encodings = {
        item["encoding"]
        for item in result["findings"]
        if item["classification"] == "ACTUAL_PRIVATE_KEY_MATERIAL"
    }
    assert encoding.upper() in encodings


def test_escaped_private_key_marker_in_json_fails_closed(tmp_path: Path) -> None:
    candidate = _candidate(tmp_path)
    escaped = "".join(
        "\\u002d" if character == "-" else character
        for character in "-----BEGIN PRIVATE KEY-----"
    )
    (candidate / "_internal/settings.json").write_text(
        '{"value":"' + escaped + '"}', encoding="utf-8"
    )

    result = _rejected(candidate)

    assert result["classification_counts"]["TEXT_PRIVATE_KEY_MARKER"] == 1
    assert result["findings"][0]["encoding"] == "ESCAPED_UTF-8"


@pytest.mark.parametrize(
    ("encoding", "bom", "expected"),
    [
        ("utf-16le", b"\xff\xfe", "ESCAPED_UTF-16LE"),
        ("utf-16be", b"\xfe\xff", "ESCAPED_UTF-16BE"),
    ],
)
def test_escaped_utf16_private_key_marker_fails_closed(
    tmp_path: Path, encoding: str, bom: bytes, expected: str
) -> None:
    candidate = _candidate(tmp_path)
    escaped = "".join(
        "\\u002d" if character == "-" else character
        for character in "-----BEGIN PRIVATE KEY-----"
    )
    document = '{"value":"' + escaped + '"}'
    (candidate / "_internal/settings.json").write_bytes(
        bom + document.encode(encoding)
    )

    result = _rejected(candidate)

    assert result["classification_counts"]["TEXT_PRIVATE_KEY_MARKER"] >= 1
    assert expected in {item["encoding"] for item in result["findings"]}


def test_unknown_marker_only_binary_fails_closed(tmp_path: Path) -> None:
    candidate = _candidate(tmp_path)
    (candidate / "_internal/unknown.dll").write_bytes(
        b"MZ\x00\x00-----BEGIN PRIVATE KEY-----\x00"
    )

    result = _rejected(candidate)

    assert result["classification_counts"] == {
        "UNVERIFIED_BINARY_PRIVATE_KEY_MARKER": 1
    }


def test_filename_alone_cannot_admit_synthetic_arrow_dll(tmp_path: Path) -> None:
    candidate = _candidate(tmp_path)
    path = candidate / "_internal/pyarrow/arrow.dll"
    path.parent.mkdir(parents=True)
    path.write_bytes(b"MZ\x00\x00-----BEGIN PRIVATE KEY-----\x00")

    result = _rejected(candidate)

    assert result["classification_counts"] == {
        "UNVERIFIED_BINARY_PRIVATE_KEY_MARKER": 1
    }


def test_renamed_arrow_copy_cannot_inherit_provenance(tmp_path: Path) -> None:
    candidate = _candidate(tmp_path)
    _copy_arrow(candidate, "_internal/renamed/arrow.dll")

    result = _rejected(candidate)

    assert result["classification_counts"] == {
        "UNVERIFIED_BINARY_PRIVATE_KEY_MARKER": 4
    }


def test_one_byte_modified_arrow_fails_provenance(tmp_path: Path) -> None:
    candidate = _candidate(tmp_path)
    path = _copy_arrow(candidate)
    value = bytearray(path.read_bytes())
    value[-1] ^= 1
    path.write_bytes(value)

    result = _rejected(candidate)

    assert result["classification_counts"] == {
        "UNVERIFIED_BINARY_PRIVATE_KEY_MARKER": 4
    }


def test_complete_private_key_appended_to_verified_arrow_fails_closed(
    tmp_path: Path,
) -> None:
    candidate = _candidate(tmp_path)
    path = _copy_arrow(candidate)
    with path.open("ab") as stream:
        stream.write(_pem("PRIVATE KEY"))

    result = _rejected(candidate)

    assert "ACTUAL_PRIVATE_KEY_MATERIAL" in result["classification_counts"]
    assert "UNVERIFIED_BINARY_PRIVATE_KEY_MARKER" in result["classification_counts"]


def test_mismatched_distribution_version_fails_provenance(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    candidate = _candidate(tmp_path)
    _copy_arrow(candidate)
    receipt = package_candidate._validated_dependency_receipt(ROOT)
    receipt["runtime"]["packages"]["pyarrow"] = "0.0.0"
    monkeypatch.setattr(
        package_candidate,
        "_validated_dependency_receipt",
        lambda _root: receipt,
    )

    result = _rejected(candidate)

    assert result["classification_counts"] == {
        "UNVERIFIED_BINARY_PRIVATE_KEY_MARKER": 4
    }


def test_dependency_absent_from_lock_fails_provenance(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    candidate = _candidate(tmp_path)
    _copy_arrow(candidate)
    receipt = package_candidate._validated_dependency_receipt(ROOT)
    del receipt["runtime"]["packages"]["pyarrow"]
    monkeypatch.setattr(
        package_candidate,
        "_validated_dependency_receipt",
        lambda _root: receipt,
    )

    result = _rejected(candidate)

    assert result["classification_counts"] == {
        "UNVERIFIED_BINARY_PRIVATE_KEY_MARKER": 4
    }


def test_exact_pyarrow_arrow_dll_parser_literals_are_provenance_bound(
    tmp_path: Path,
) -> None:
    candidate = _candidate(tmp_path)
    packaged = _copy_arrow(candidate)

    files, _, result = package_candidate._validate_candidate(
        candidate, root=ROOT
    )

    assert result["result"] == "VERIFIED_DEPENDENCY_PARSER_LITERALS_ONLY"
    assert result["classification_counts"] == {
        "VERIFIED_DEPENDENCY_PARSER_LITERAL": 4
    }
    assert {item["offset"] for item in result["findings"]} == {
        16_223_016,
        16_223_048,
        16_223_080,
        16_281_600,
    }
    assert {item["label"] for item in result["findings"]} == {
        "PRIVATE KEY",
        "RSA PRIVATE KEY",
        "EC PRIVATE KEY",
    }
    for finding in result["findings"]:
        dependency = finding["dependency"]
        assert dependency["distribution"] == "pyarrow"
        assert dependency["version"] == "24.0.0"
        assert dependency["record_path"] == "pyarrow/arrow.dll"
        assert dependency["source_sha256"] == sha256_file(_source_arrow())
        assert dependency["packaged_sha256"] == sha256_file(packaged)
        assert dependency["source_sha256"] == dependency["packaged_sha256"]
        assert dependency["candidate_relative_path"] == "_internal/pyarrow/arrow.dll"
        assert dependency["locked_wheel_filename"] == (
            "pyarrow-24.0.0-cp311-cp311-win_amd64.whl"
        )
        assert dependency["locked_wheel_sha256"] == (
            "35405aecb474e683fb36af650618fd5340ee5471fc65a21b36076a18bbc6c981"
        )
        assert dependency["native_format"] == "PE"
    assert any(item["path"] == "_internal/pyarrow/arrow.dll" for item in files)


def test_scanner_output_never_contains_private_key_payload(tmp_path: Path) -> None:
    candidate = _candidate(tmp_path)
    payload = b"\x30" + secrets.token_bytes(95)
    encoded = base64.b64encode(payload).decode("ascii")
    (candidate / "_internal/key.txt").write_bytes(_pem("PRIVATE KEY", payload))

    with pytest.raises(package_candidate.PrivateKeyScanError) as caught:
        package_candidate._scan_candidate_private_keys(candidate, root=ROOT)

    rendered = json.dumps(caught.value.result, sort_keys=True)
    assert encoded not in rendered
    assert encoded not in str(caught.value)


def test_ordinary_safe_package_has_no_private_key_material(tmp_path: Path) -> None:
    candidate = _candidate(tmp_path)

    _, _, result = package_candidate._validate_candidate(candidate, root=ROOT)

    assert result == {
        "scanner_version": package_candidate.PRIVATE_KEY_SCANNER_VERSION,
        "result": "NO_PRIVATE_KEY_MATERIAL",
        "classification_counts": {},
        "findings": [],
    }


def test_candidate_receipt_records_structured_private_key_scan() -> None:
    source = inspect.getsource(package_candidate.run_candidate)

    assert package_candidate.CANDIDATE_SCHEMA == (
        "live_cockpit_package_candidate_receipt/1.2.0"
    )
    assert '"private_key_scan": private_key_scan' in source
    assert '"execution_surface_scan": execution_surface_scan' in source
    assert 'category = "PRIVATE_KEY_SCAN_REJECTED"' in source


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

    package_plan = {
        "basis": {"head": "b" * 40},
        "inputs": [
            {"path": "src/example.py", "bytes": 7, "sha256": "c" * 64},
        ],
    }
    plan, finalized_path = package_candidate._finalize_smoke_plan(
        candidate,
        package_plan,
    )

    assert finalized_path == plan_path
    assert plan["scope"]["prepared_executable_sha256"] == sha256_file(executable)
    assert plan["scope"]["result_output_relative"] == RESULT_OUTPUT_RELATIVE
    assert plan["predecessor_attempt"] == PREDECESSOR_ATTEMPT
    assert plan["successor_binding"]["source_revision"] == "b" * 40
    assert plan["successor_binding"]["package_inputs"] == package_plan["inputs"]
    assert plan["successor_binding"]["candidate_executable_sha256"] == (
        sha256_file(executable)
    )
    assert plan_path.read_bytes() == canonical_bytes(plan) + b"\n"
