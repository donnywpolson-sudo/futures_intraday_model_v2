"""Receipt-bound quotation and acquisition for immutable OHLCV completion batches."""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any, Callable, Mapping, Sequence

from .boundary import OperationClassification, OperationReceipt, RepoBoundary
from .canonical import canonical_bytes, sha256_file, sha256_json
from .errors import ContractError, IntegrityError
from .ohlcv_historical_backfill import (
    NO_DATA_EVIDENCE_SCHEMA,
    DatabentoBatchProvider,
    execute_manifest,
    load_manifest,
    request_fingerprint,
)
from .ohlcv_historical_backfill_v3 import (
    BATCH_SCHEMA,
    build_staged_execution_manifest,
    derive_completion_batch,
    load_bound_completion_plan,
    quote_completion_plan,
)


QUOTE_OPERATION = "QUOTE_OHLCV_58_COMPLETION_BATCH"
ACQUISITION_OPERATION = "ACQUIRE_OHLCV_58_COMPLETION_BATCH"
REPORT_ROOT = Path("reports/ohlcv_58_completion")
STATE_ROOT = Path("state/ohlcv_58_completion")


def _execution_root(path: Path) -> Path:
    """Use Windows extended-length syntax for deeply isolated acquisition paths."""

    if os.name != "nt":
        return path
    text = str(path)
    if text.startswith("\\\\?\\"):
        return path
    return Path("\\\\?\\" + text)


def _load_canonical(path: Path, label: str) -> dict[str, Any]:
    try:
        raw = path.read_bytes()
        value = json.loads(raw)
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise IntegrityError(f"unable to read {label}: {path}") from exc
    if not isinstance(value, dict) or canonical_bytes(value) + b"\n" != raw:
        raise IntegrityError(f"{label} is not a canonical JSON object")
    return value


def _contained(root: Path, relative: Path, *, subtree: Path | None = None) -> Path:
    if relative.is_absolute() or ".." in relative.parts:
        raise ContractError("campaign paths must be project-relative")
    candidate = (root / relative).resolve(strict=False)
    permitted = (root / subtree).resolve(strict=False) if subtree is not None else root
    try:
        candidate.relative_to(permitted)
    except ValueError as exc:
        raise ContractError("campaign path escapes its permitted subtree") from exc
    return candidate


def _create_json(path: Path, value: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        with path.open("xb") as stream:
            stream.write(canonical_bytes(dict(value)) + b"\n")
            stream.flush()
            os.fsync(stream.fileno())
    except FileExistsError as exc:
        raise IntegrityError(f"refusing to overwrite campaign artifact: {path}") from exc


def load_batch(root: Path, batch_path: Path, *, expected_sha256: str) -> dict[str, Any]:
    root = root.resolve(strict=True)
    batch_path = batch_path.resolve(strict=True)
    permitted = (root / REPORT_ROOT).resolve(strict=True)
    try:
        batch_path.relative_to(permitted)
    except ValueError as exc:
        raise ContractError("completion batch is outside its report root") from exc
    if sha256_file(batch_path) != expected_sha256:
        raise IntegrityError("completion batch hash differs")
    batch = _load_canonical(batch_path, "completion batch")
    core = dict(batch)
    plan_id = str(core.pop("plan_id", ""))
    if batch.get("schema_version") != BATCH_SCHEMA or plan_id != sha256_json(core):
        raise IntegrityError("completion batch identity is invalid")
    authority = batch.get("authority")
    if not isinstance(authority, Mapping) or any(
        authority.get(key) is not False
        for key in ("active_data_mutation", "credential_access", "provider_network_access", "publication")
    ):
        raise IntegrityError("completion batch claims authority")
    bindings = batch.get("bindings")
    if not isinstance(bindings, Mapping):
        raise IntegrityError("completion batch lacks source bindings")
    for relative, expected in bindings.items():
        if type(relative) is not str or type(expected) is not str:
            raise IntegrityError("completion batch binding is invalid")
        if sha256_file(_contained(root, Path(relative))) != expected:
            raise IntegrityError(f"completion batch source binding drifted: {relative}")
    return batch


def certify_wave_a_preparation(
    root: Path,
    *,
    plan_path: Path,
    plan_id: str,
    plan_sha256: str,
    batch_path: Path,
    batch_sha256: str,
    output_path: Path,
) -> dict[str, Any]:
    """Independently rederive and certify the provider-free MSF-hourly preparation."""

    root = root.resolve(strict=True)
    plan = load_bound_completion_plan(
        root,
        plan_path,
        expected_plan_id=plan_id,
        expected_sha256=plan_sha256,
    )
    batch = load_batch(root, batch_path, expected_sha256=batch_sha256)
    expected_batch = derive_completion_batch(
        plan,
        selection={"MSF": ["ohlcv-1h"]},
        canary_markets=("MSF",),
        automatic_continuation_after_canaries=False,
    )
    if batch != expected_batch:
        raise IntegrityError("Wave A batch differs from independent derivation")
    if (
        len(plan["requests"]) != 49
        or plan["execution_limits"]["target_dbn_file_count_maximum"] != 489
        or plan["coverage"]["ohlcv-1d"]["present_root_count"] != 34
        or plan["coverage"]["ohlcv-1h"]["present_root_count"] != 33
        or len(batch["requests"]) != 1
        or batch["execution_limits"]["target_dbn_file_count_maximum"] != 9
        or batch["selection"] != {"MSF": ["ohlcv-1h"]}
    ):
        raise IntegrityError("Wave A completion counts differ")
    source_paths = (
        Path("src/futures_rebuild/ohlcv_historical_backfill.py"),
        Path("src/futures_rebuild/ohlcv_historical_backfill_v3.py"),
        Path("src/futures_rebuild/ohlcv_completion_campaign.py"),
        Path("src/futures_rebuild/ohlcv_completion_publication.py"),
    )
    certificate = {
        "authority": {
            "credential_access": False,
            "provider_network_access": False,
            "publication": False,
            "submission": False,
        },
        "batch": {
            "path": batch_path.resolve(strict=True).relative_to(root).as_posix(),
            "plan_id": batch["plan_id"],
            "sha256": batch_sha256,
        },
        "current_counts": {"ohlcv-1d": 34, "ohlcv-1h": 33},
        "implementation_bindings": {
            path.as_posix(): sha256_file(root / path) for path in source_paths
        },
        "next_action": "REQUIRES_SEPARATE_METADATA_ONLY_LIVE_QUOTE_APPROVAL",
        "plan": {
            "path": plan_path.resolve(strict=True).relative_to(root).as_posix(),
            "plan_id": plan_id,
            "sha256": plan_sha256,
        },
        "provider_calls": 0,
        "schema_version": "ohlcv_58_wave_a_preparation_certificate/1.0.0",
        "status": "PASS_PROVIDER_FREE_WAVE_A_READY_FOR_QUOTE_APPROVAL",
        "wave_a": {
            "market": "MSF",
            "schema": "ohlcv-1h",
            "target_dbn_count": 9,
        },
    }
    output = _contained(root, output_path, subtree=REPORT_ROOT)
    if output.exists():
        existing = _load_canonical(output, "Wave A preparation certificate")
        if existing != certificate:
            raise IntegrityError("existing Wave A certificate differs")
        return {"status": "NO_ACTION_CERTIFICATE_ALREADY_EXISTS", "certificate": existing}
    _create_json(output, certificate)
    return certificate


def _approval_scope(
    *, operation: str, plan_id: str, plan_sha256: str, values: Mapping[str, str]
) -> dict[str, str]:
    return {
        **dict(values),
        "approval_command": operation,
        "approval_plan_id": plan_id,
        "approval_plan_sha256": plan_sha256,
    }


def required_quote_scope(batch: Mapping[str, Any], batch_sha256: str) -> dict[str, str]:
    limits = batch["execution_limits"]
    return _approval_scope(
        operation=QUOTE_OPERATION,
        plan_id=str(batch["plan_id"]),
        plan_sha256=batch_sha256,
        values={
            "batch_sha256": batch_sha256,
            "credential_source": "PROJECT_ROOT_API_ENV_ONLY",
            "download": "false",
            "maximum_authorized_cost_usd": "0",
            "provider_request_count": str(limits["provider_request_count"]),
            "provider_row_read": "false",
            "submission": "false",
        },
    )


def quote_authorized(
    root: Path,
    *,
    batch_path: Path,
    batch_sha256: str,
    output_path: Path,
    authorization: OperationReceipt,
    provider_factory: Callable[[Path], DatabentoBatchProvider] = DatabentoBatchProvider,
) -> dict[str, Any]:
    root = root.resolve(strict=True)
    batch = load_batch(root, batch_path, expected_sha256=batch_sha256)
    scope = required_quote_scope(batch, batch_sha256)
    boundary = RepoBoundary(root)
    output = _contained(root, output_path, subtree=REPORT_ROOT)
    if output.exists():
        authorization.assert_consumed(
            boundary,
            operation=QUOTE_OPERATION,
            classification=OperationClassification.EXTERNAL_CANDIDATE_AUTHORIZATION,
            required_scope=scope,
        )
        quote = _load_canonical(output, "completion quote")
        if quote.get("plan_id") != batch["plan_id"] or quote.get("plan_sha256") != batch_sha256:
            raise IntegrityError("existing completion quote binds another batch")
        return {"status": "NO_ACTION_QUOTE_ALREADY_EXISTS", "quote": quote}
    use_path = authorization.consume(
        boundary,
        operation=QUOTE_OPERATION,
        classification=OperationClassification.EXTERNAL_CANDIDATE_AUTHORIZATION,
        required_scope=scope,
    )
    provider = provider_factory(root)
    quote = quote_completion_plan(
        batch,
        plan_sha256=batch_sha256,
        get_cost=lambda **kwargs: provider.get_cost(kwargs),
        get_billable_size=lambda **kwargs: provider.get_billable_size(kwargs),
        get_record_count=lambda **kwargs: provider.get_record_count(kwargs),
    )
    quote["authorization_receipt_id"] = authorization.receipt_id
    quote["authorization_use_path"] = use_path.relative_to(root).as_posix()
    _create_json(output, quote)
    return quote


def prepare_execution_manifest(
    root: Path,
    *,
    batch_path: Path,
    batch_sha256: str,
    quote_path: Path,
    output_path: Path,
) -> dict[str, Any]:
    root = root.resolve(strict=True)
    batch = load_batch(root, batch_path, expected_sha256=batch_sha256)
    quote_path = quote_path.resolve(strict=True)
    quote = _load_canonical(quote_path, "completion quote")
    quote_sha = sha256_file(quote_path)
    if (
        quote.get("status") != "PASS_WITHIN_APPROVED_ZERO_COST_CAP"
        or quote.get("plan_id") != batch["plan_id"]
        or quote.get("plan_sha256") != batch_sha256
        or quote.get("estimated_data_cost_usd") not in {"0", "0.0"}
    ):
        raise IntegrityError("completion quote does not authorize a zero-cost manifest")
    quotes = quote.get("quotes")
    if not isinstance(quotes, list) or len(quotes) != len(batch["requests"]):
        raise IntegrityError("completion quote request coverage differs")
    if any(
        not isinstance(item, Mapping)
        or type(item.get("api_billable_uncompressed_bytes")) is not int
        or type(item.get("provider_record_count")) is not int
        for item in quotes
    ):
        raise IntegrityError("completion quote lacks storage or record-count evidence")
    rows = build_staged_execution_manifest(
        batch,
        markets=list(batch["selection"]),
        provider_metadata_sha256=quote_sha,
    )
    output = _contained(root, output_path, subtree=REPORT_ROOT)
    output.parent.mkdir(parents=True, exist_ok=True)
    try:
        with output.open("xb") as stream:
            for row in rows:
                stream.write(canonical_bytes(row) + b"\n")
            stream.flush()
            os.fsync(stream.fileno())
    except FileExistsError as exc:
        raise IntegrityError("refusing to overwrite completion execution manifest") from exc
    return {
        "manifest_path": output.relative_to(root).as_posix(),
        "manifest_sha256": sha256_file(output),
        "quote_sha256": quote_sha,
        "selected_targets": len(rows),
    }


def build_no_data_manifest_successor(
    root: Path,
    *,
    predecessor_manifest_path: Path,
    predecessor_manifest_sha256: str,
    output_path: Path,
    target_id: str,
    evidence_path: Path,
    evidence_sha256: str,
) -> dict[str, Any]:
    root = root.resolve(strict=True)
    predecessor = predecessor_manifest_path.resolve(strict=True)
    if sha256_file(predecessor) != predecessor_manifest_sha256:
        raise IntegrityError("no-data successor predecessor hash differs")
    evidence_file = evidence_path.resolve(strict=True)
    try:
        evidence_relative = evidence_file.relative_to(root).as_posix()
        evidence_file.relative_to((root / REPORT_ROOT).resolve(strict=True))
    except ValueError as exc:
        raise ContractError("no-data successor evidence is outside the report root") from exc
    if sha256_file(evidence_file) != evidence_sha256:
        raise IntegrityError("no-data successor evidence hash differs")
    evidence = _load_canonical(evidence_file, "provider no-data evidence")
    if evidence.get("schema_version") != NO_DATA_EVIDENCE_SCHEMA:
        raise IntegrityError("provider no-data evidence schema differs")
    evidence_target = evidence.get("target")
    job = evidence.get("job")
    probe = evidence.get("metadata_probe")
    provider_manifest = evidence.get("provider_file_manifest")
    request = evidence.get("request")
    if not all(isinstance(value, Mapping) for value in (evidence_target, job, probe, provider_manifest, request)):
        raise IntegrityError("provider no-data evidence structure differs")
    if evidence_target.get("target_id") != target_id:  # type: ignore[union-attr]
        raise IntegrityError("provider no-data evidence target differs")
    if (
        job.get("state") != "done"  # type: ignore[union-attr]
        or job.get("progress") != 100  # type: ignore[union-attr]
        or str(job.get("cost_usd")) not in {"0", "0.0"}  # type: ignore[union-attr]
        or probe.get("http_status") != 422  # type: ignore[union-attr]
        or probe.get("error_code") != "symbology_invalid_request"  # type: ignore[union-attr]
        or probe.get("error_message") != "None of the symbols could be resolved"  # type: ignore[union-attr]
    ):
        raise IntegrityError("provider no-data evidence outcome differs")
    provider_manifest_hash = provider_manifest.get("provider_manifest_hash")  # type: ignore[union-attr]
    if not isinstance(provider_manifest_hash, str) or len(provider_manifest_hash) != 64:
        raise IntegrityError("provider no-data manifest hash is invalid")
    request_fields = {key: value for key, value in request.items() if key != "market"}  # type: ignore[union-attr]
    job_request_fingerprint = request_fingerprint(request_fields)
    rows = load_manifest(predecessor)
    matches = [row for row in rows if row.get("target_id") == target_id]
    if len(matches) != 1:
        raise IntegrityError("no-data successor target is missing or duplicated")
    target = matches[0]
    if (
        target.get("current_state") != "MISSING"
        or target.get("execution_action") != "DOWNLOAD_VALIDATE_INSTALL_ABSENT_TARGET_ONLY"
        or target.get("market") != request.get("market")  # type: ignore[union-attr]
        or target.get("schema") != request.get("schema")  # type: ignore[union-attr]
        or target.get("intended_start_inclusive") != probe.get("start_inclusive")  # type: ignore[union-attr]
        or target.get("intended_end_exclusive") != probe.get("end_exclusive")  # type: ignore[union-attr]
    ):
        raise IntegrityError("no-data successor target does not match provider evidence")
    successor_rows: list[dict[str, object]] = []
    for row in rows:
        successor = {**row, "manifest_predecessor_sha256": predecessor_manifest_sha256}
        if row.get("target_id") == target_id:
            successor.update(
                {
                    "activation_status": "NO_DATA_EVIDENCE_ONLY",
                    "current_state": "NO_DATA_CONFIRMED",
                    "execution_action": "NO_FILE_CREATE",
                    "expected_incremental_bytes": 0,
                    "no_data_evidence": {
                        "evidence_path": evidence_relative,
                        "evidence_sha256": evidence_sha256,
                        "job_id": job["job_id"],  # type: ignore[index]
                        "provider_error_code": probe["error_code"],  # type: ignore[index]
                        "provider_error_message": probe["error_message"],  # type: ignore[index]
                        "provider_error_status": probe["http_status"],  # type: ignore[index]
                        "provider_manifest_hash": provider_manifest_hash,
                        "request_fingerprint": job_request_fingerprint,
                        "schema_version": NO_DATA_EVIDENCE_SCHEMA,
                    },
                    "provider_record_count": 0,
                    "validation_requirements": [
                        "EXACT_PROVIDER_JOB_REQUEST",
                        "PROVIDER_JOB_DONE_NONEXPIRED",
                        "PROVIDER_MANIFEST_OMITS_EXACT_TARGET",
                        "PROVIDER_SYMBOLOGY_UNRESOLVED_EXACT_INTERVAL",
                        "NO_FILE_CREATE",
                    ],
                }
            )
        successor_rows.append(successor)
    output = _contained(root, output_path, subtree=REPORT_ROOT)
    output.parent.mkdir(parents=True, exist_ok=True)
    try:
        with output.open("xb") as stream:
            for row in successor_rows:
                stream.write(canonical_bytes(row) + b"\n")
            stream.flush()
            os.fsync(stream.fileno())
    except FileExistsError as exc:
        raise IntegrityError("refusing to overwrite no-data manifest successor") from exc
    return {
        "evidence_sha256": evidence_sha256,
        "manifest_path": output.relative_to(root).as_posix(),
        "manifest_sha256": sha256_file(output),
        "predecessor_manifest_sha256": predecessor_manifest_sha256,
        "selected_targets": len(successor_rows),
        "target_id": target_id,
    }


def required_acquisition_scope(
    root: Path,
    batch: Mapping[str, Any],
    batch_sha256: str,
    quote_sha256: str,
    manifest_sha256: str,
    state_root: Path,
    required_reuse_job_id: str | None = None,
    predecessor_manifest_sha256: str | None = None,
    no_data_evidence_sha256: str | None = None,
) -> dict[str, str]:
    return _approval_scope(
        operation=ACQUISITION_OPERATION,
        plan_id=str(batch["plan_id"]),
        plan_sha256=batch_sha256,
        values={
            "batch_sha256": batch_sha256,
            "credential_source": "PROJECT_ROOT_API_ENV_ONLY",
            "executor_sha256": sha256_file(root / "src/futures_rebuild/ohlcv_completion_campaign.py"),
            "manifest_sha256": manifest_sha256,
            "maximum_authorized_cost_usd": "0",
            "provider_implementation_sha256": sha256_file(
                root / "src/futures_rebuild/ohlcv_historical_backfill.py"
            ),
            "provider_request_count": str(batch["execution_limits"]["provider_request_count"]),
            "publication": "false",
            "quote_sha256": quote_sha256,
            "required_reuse_job_id": required_reuse_job_id or "NONE_INITIAL_SUBMISSION_ALLOWED",
            "predecessor_manifest_sha256": predecessor_manifest_sha256 or "NONE",
            "no_data_evidence_sha256": no_data_evidence_sha256 or "NONE",
            "state_root": state_root.as_posix(),
        },
    )


def execute_authorized_batch(
    root: Path,
    *,
    batch_path: Path,
    batch_sha256: str,
    quote_path: Path,
    manifest_path: Path,
    manifest_sha256: str,
    state_root: Path,
    authorization: OperationReceipt,
    required_reuse_job_id: str | None = None,
    predecessor_manifest_sha256: str | None = None,
    no_data_evidence_path: Path | None = None,
    no_data_evidence_sha256: str | None = None,
    provider_factory: Callable[[Path], DatabentoBatchProvider] = DatabentoBatchProvider,
) -> dict[str, Any]:
    root = root.resolve(strict=True)
    batch = load_batch(root, batch_path, expected_sha256=batch_sha256)
    quote = _load_canonical(quote_path.resolve(strict=True), "completion quote")
    quote_sha = sha256_file(quote_path)
    if (
        quote.get("status") != "PASS_WITHIN_APPROVED_ZERO_COST_CAP"
        or quote.get("plan_id") != batch["plan_id"]
        or quote.get("plan_sha256") != batch_sha256
        or quote.get("estimated_data_cost_usd") not in {"0", "0.0"}
    ):
        raise IntegrityError("completion acquisition quote is not passing")
    if sha256_file(manifest_path.resolve(strict=True)) != manifest_sha256:
        raise IntegrityError("completion acquisition manifest hash differs")
    successor_values = (
        predecessor_manifest_sha256,
        no_data_evidence_path,
        no_data_evidence_sha256,
    )
    if any(value is not None for value in successor_values):
        if any(value is None for value in successor_values):
            raise IntegrityError("completion no-data successor bindings are incomplete")
        evidence_path = no_data_evidence_path.resolve(strict=True)  # type: ignore[union-attr]
        try:
            evidence_relative = evidence_path.relative_to(root).as_posix()
            evidence_path.relative_to((root / REPORT_ROOT).resolve(strict=True))
        except ValueError as exc:
            raise ContractError("completion no-data evidence is outside the report root") from exc
        if sha256_file(evidence_path) != no_data_evidence_sha256:
            raise IntegrityError("completion no-data evidence hash differs")
        successor_rows = load_manifest(manifest_path)
        if {row.get("manifest_predecessor_sha256") for row in successor_rows} != {
            predecessor_manifest_sha256
        }:
            raise IntegrityError("completion successor predecessor binding differs")
        evidence_rows = [row for row in successor_rows if "no_data_evidence" in row]
        if not evidence_rows:
            raise IntegrityError("completion successor lacks a no-data target")
        for evidence_row in evidence_rows:
            row_evidence = evidence_row["no_data_evidence"]
            if (
                not isinstance(row_evidence, Mapping)
                or row_evidence.get("evidence_path") != evidence_relative
                or row_evidence.get("evidence_sha256") != no_data_evidence_sha256
            ):
                raise IntegrityError("completion successor no-data evidence binding differs")
    if state_root.is_absolute() or ".." in state_root.parts:
        raise ContractError("completion acquisition state root must be project-relative")
    if not state_root.as_posix().startswith(STATE_ROOT.as_posix() + "/"):
        raise ContractError("completion acquisition state root is outside the campaign state root")
    isolated_root = (root / state_root).resolve(strict=False)
    execution_root = _execution_root(isolated_root)
    scope = required_acquisition_scope(
        root,
        batch,
        batch_sha256,
        quote_sha,
        manifest_sha256,
        state_root,
        required_reuse_job_id,
        predecessor_manifest_sha256,
        no_data_evidence_sha256,
    )
    boundary = RepoBoundary(root)
    use_path = root / "state/authorization_uses" / f"{authorization.receipt_id}.json"

    def forbidden_provider(_: Path) -> DatabentoBatchProvider:
        raise AssertionError("provider touched during completed-batch no-op")

    def project_provider_factory(_: Path) -> DatabentoBatchProvider:
        return provider_factory(root)

    if use_path.exists():
        authorization.assert_consumed(
            boundary,
            operation=ACQUISITION_OPERATION,
            classification=OperationClassification.EXTERNAL_CANDIDATE_AUTHORIZATION,
            required_scope=scope,
        )
        noop = execute_manifest(
            root=execution_root,
            manifest_path=manifest_path,
            execute=True,
            manifest_sha256=manifest_sha256,
            maximum_authorized_cost_usd="0",
            markets=tuple(batch["selection"]),
            resume=True,
            reuse_job_id=required_reuse_job_id,
            resume_from_manifest_sha256=predecessor_manifest_sha256,
            provider_factory=forbidden_provider,
        )
        if noop.get("actions") != 0:
            raise IntegrityError("consumed completion batch is not an idempotent no-op")
        return {"status": "NO_ACTION_BATCH_ALREADY_COMPLETE", "resume": noop}

    authorization.consume(
        boundary,
        operation=ACQUISITION_OPERATION,
        classification=OperationClassification.EXTERNAL_CANDIDATE_AUTHORIZATION,
        required_scope=scope,
    )
    isolated_root.mkdir(parents=True, exist_ok=True)
    policy = batch.get("execution_policy")
    if not isinstance(policy, Mapping):
        raise IntegrityError("completion batch lacks an execution policy")
    canaries = tuple(str(value) for value in policy.get("canary_markets", []))
    automatic = policy.get("automatic_continuation_after_canaries") is True
    all_markets = tuple(str(value) for value in batch["selection"])
    phase_results: list[dict[str, Any]] = []
    if automatic and canaries:
        canary_result = execute_manifest(
            root=execution_root,
            manifest_path=manifest_path,
            execute=True,
            manifest_sha256=manifest_sha256,
            maximum_authorized_cost_usd="0",
            markets=canaries,
            resume=True,
            reuse_job_id=required_reuse_job_id,
            resume_from_manifest_sha256=predecessor_manifest_sha256,
            provider_factory=project_provider_factory,
        )
        canary_noop = execute_manifest(
            root=execution_root,
            manifest_path=manifest_path,
            execute=True,
            manifest_sha256=manifest_sha256,
            maximum_authorized_cost_usd="0",
            markets=canaries,
            resume=True,
            reuse_job_id=required_reuse_job_id,
            resume_from_manifest_sha256=predecessor_manifest_sha256,
            provider_factory=forbidden_provider,
        )
        if canary_noop.get("actions") != 0:
            raise IntegrityError("completion canary resume is not a provider-free no-op")
        phase_results.extend(
            [
                {"phase": "CANARIES", "result": canary_result},
                {"phase": "CANARY_NOOP", "result": canary_noop},
            ]
        )
        remaining = tuple(value for value in all_markets if value not in set(canaries))
        if remaining:
            phase_results.append(
                {
                    "phase": "AUTHORIZED_AUTOMATIC_CONTINUATION",
                    "result": execute_manifest(
                        root=execution_root,
                        manifest_path=manifest_path,
                        execute=True,
                        manifest_sha256=manifest_sha256,
                        maximum_authorized_cost_usd="0",
                        markets=remaining,
                        resume=True,
                        reuse_job_id=required_reuse_job_id,
                        resume_from_manifest_sha256=predecessor_manifest_sha256,
                        provider_factory=project_provider_factory,
                    ),
                }
            )
    else:
        phase_results.append(
            {
                "phase": "EXACT_BATCH",
                "result": execute_manifest(
                    root=execution_root,
                    manifest_path=manifest_path,
                    execute=True,
                    manifest_sha256=manifest_sha256,
                    maximum_authorized_cost_usd="0",
                    markets=all_markets,
                    resume=True,
                    reuse_job_id=required_reuse_job_id,
                    resume_from_manifest_sha256=predecessor_manifest_sha256,
                    provider_factory=project_provider_factory,
                ),
            }
        )
    return {
        "authorization_receipt_id": authorization.receipt_id,
        "authorization_use_path": use_path.relative_to(root).as_posix(),
        "phases": phase_results,
        "status": "ACQUISITION_EXECUTED",
    }
