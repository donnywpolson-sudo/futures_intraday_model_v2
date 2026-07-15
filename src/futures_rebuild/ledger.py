"""Trusted-clock prediction ledger with records, anchors, and persistent external head."""

from __future__ import annotations

import json
import hashlib
import hmac
import os
import re
import uuid
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from decimal import Decimal
from pathlib import Path

from .boundary import OperationClassification, OperationReceipt, RepoBoundary
from .canonical import (
    assert_no_linklike_ancestors,
    assert_plain_file,
    canonical_bytes,
    fsync_directory,
    is_linklike,
    sha256_json,
)
from .clock import ProductionClock, SyntheticClock, TrustedClock, require_trusted_clock
from .errors import ContractError, IntegrityError
from .identity import ActualContractIdentity
from .locking import FileLease
from .schemas import PredictionRow, prediction_id_for
from .time_contracts import require_utc


RECORD_NAME = re.compile(r"^(?P<sequence>\d{20})_(?P<id>[0-9a-f]{64})\.json$")
ANCHOR_NAME = re.compile(r"^(?P<sequence>\d{20})_(?P<hash>[0-9a-f]{64})\.json$")
HEAD_VERSION = "1.0.0"
INTENT_VERSION = "1.0.0"
_PREDICTION_ROOT = Path("state/predictions")
_PREDICTION_LOCK_PATH = Path("state/locks/ledger.lock")
_PREDICTION_ANCHOR_ROOT = Path("state/anchors")
_PREDICTION_HEAD_PATH = Path("state/ledger_heads/head.json")


def _prediction_payload(row: PredictionRow) -> dict[str, object]:
    return {
        "abstained": row.abstained,
        "abstention_reasons": list(row.abstention_reasons),
        "actual_contract": row.actual.as_dict(),
        "bundle_id": row.bundle_id,
        "bundle_classification": row.bundle_classification,
        "candidate_provenance_id": row.candidate_provenance_id,
        "decision_at": row.decision_at.isoformat(),
        "economics_record_id": row.economics_record_id,
        "expected_return": row.expected_return,
        "feature_row_id": row.feature_row_id,
        "label_unlock_at": row.label_unlock_at.isoformat(),
        "planned_entry_at": row.planned_entry_at.isoformat(),
        "prediction_id": row.prediction_id,
        "probability_down": row.probability_down,
        "probability_neutral": row.probability_neutral,
        "probability_up": row.probability_up,
        "production_eligible": row.production_eligible,
        "recorded_at": row.recorded_at.isoformat(),
        "source_release_id": row.source_release_id,
        "source_release_receipt_id": row.source_release_receipt_id,
        "uncertainty": row.uncertainty,
    }


def _prediction_from_payload(payload: object) -> PredictionRow:
    if not isinstance(payload, dict):
        raise IntegrityError("prediction ledger payload is not an object")
    expected_keys = {
        "abstained",
        "abstention_reasons",
        "actual_contract",
        "bundle_id",
        "bundle_classification",
        "candidate_provenance_id",
        "decision_at",
        "economics_record_id",
        "expected_return",
        "feature_row_id",
        "label_unlock_at",
        "planned_entry_at",
        "prediction_id",
        "probability_down",
        "probability_neutral",
        "probability_up",
        "production_eligible",
        "recorded_at",
        "source_release_id",
        "source_release_receipt_id",
        "uncertainty",
    }
    if set(payload) != expected_keys or not isinstance(payload["actual_contract"], dict):
        raise IntegrityError("prediction ledger payload fields are invalid")
    actual_payload = payload["actual_contract"]
    assert isinstance(actual_payload, dict)
    if set(actual_payload) != {
        "currency",
        "dataset",
        "definition_manifest_sha256",
        "definition_release_id",
        "definition_row_id",
        "exchange",
        "exchange_session_date",
        "instrument_id",
        "instrument_id_date_utc",
        "min_tick",
        "multiplier",
        "publisher_id",
        "raw_symbol",
    }:
        raise IntegrityError("prediction actual-contract schema is invalid")
    actual_string_fields = {
        "currency",
        "dataset",
        "definition_manifest_sha256",
        "definition_release_id",
        "definition_row_id",
        "exchange",
        "exchange_session_date",
        "instrument_id_date_utc",
        "min_tick",
        "multiplier",
        "raw_symbol",
    }
    prediction_string_fields = {
        "bundle_classification",
        "bundle_id",
        "decision_at",
        "economics_record_id",
        "feature_row_id",
        "label_unlock_at",
        "planned_entry_at",
        "prediction_id",
        "recorded_at",
        "source_release_id",
        "source_release_receipt_id",
    }
    forecast_fields = {
        "expected_return",
        "probability_down",
        "probability_neutral",
        "probability_up",
        "uncertainty",
    }
    if (
        any(type(actual_payload[name]) is not str for name in actual_string_fields)
        or type(actual_payload["publisher_id"]) is not int
        or type(actual_payload["instrument_id"]) is not int
        or any(type(payload[name]) is not str for name in prediction_string_fields)
        or type(payload["abstained"]) is not bool
        or type(payload["production_eligible"]) is not bool
        or not isinstance(payload["abstention_reasons"], list)
        or any(type(item) is not str for item in payload["abstention_reasons"])
        or (
            payload["candidate_provenance_id"] is not None
            and type(payload["candidate_provenance_id"]) is not str
        )
        or any(
            payload[name] is not None
            and type(payload[name]) not in {int, float}
            for name in forecast_fields
        )
    ):
        raise IntegrityError("prediction ledger payload field types are invalid")
    try:
        actual = ActualContractIdentity(
            dataset=actual_payload["dataset"],
            publisher_id=actual_payload["publisher_id"],
            instrument_id=actual_payload["instrument_id"],
            instrument_id_date_utc=date.fromisoformat(
                actual_payload["instrument_id_date_utc"]
            ),
            exchange_session_date=date.fromisoformat(
                actual_payload["exchange_session_date"]
            ),
            raw_symbol=actual_payload["raw_symbol"],
            exchange=actual_payload["exchange"],
            definition_release_id=actual_payload["definition_release_id"],
            definition_manifest_sha256=actual_payload["definition_manifest_sha256"],
            definition_row_id=actual_payload["definition_row_id"],
            currency=actual_payload["currency"],
            multiplier=Decimal(actual_payload["multiplier"]),
            min_tick=Decimal(actual_payload["min_tick"]),
        )
        row = PredictionRow(
            prediction_id=payload["prediction_id"],
            bundle_id=payload["bundle_id"],
            actual=actual,
            decision_at=datetime.fromisoformat(payload["decision_at"]),
            recorded_at=datetime.fromisoformat(payload["recorded_at"]),
            source_release_id=payload["source_release_id"],
            source_release_receipt_id=payload["source_release_receipt_id"],
            economics_record_id=payload["economics_record_id"],
            feature_row_id=payload["feature_row_id"],
            planned_entry_at=datetime.fromisoformat(payload["planned_entry_at"]),
            label_unlock_at=datetime.fromisoformat(payload["label_unlock_at"]),
            abstained=payload["abstained"],  # type: ignore[arg-type]
            abstention_reasons=tuple(payload["abstention_reasons"]),  # type: ignore[arg-type]
            expected_return=payload["expected_return"],  # type: ignore[arg-type]
            probability_up=payload["probability_up"],  # type: ignore[arg-type]
            probability_down=payload["probability_down"],  # type: ignore[arg-type]
            probability_neutral=payload["probability_neutral"],  # type: ignore[arg-type]
            uncertainty=payload["uncertainty"],  # type: ignore[arg-type]
            bundle_classification=payload["bundle_classification"],  # type: ignore[arg-type]
            candidate_provenance_id=payload["candidate_provenance_id"],  # type: ignore[arg-type]
            production_eligible=payload["production_eligible"],  # type: ignore[arg-type]
        )
    except (KeyError, TypeError, ValueError, ArithmeticError, ContractError) as exc:
        raise IntegrityError("prediction ledger payload violates its schema") from exc
    if actual.as_dict() != actual_payload:
        raise IntegrityError("actual-contract identity is not canonically encoded")
    expected_id = prediction_id_for(
        bundle_id=row.bundle_id,
        actual=row.actual,
        decision_at=row.decision_at,
        recorded_at=row.recorded_at,
        source_release_id=row.source_release_id,
        source_release_receipt_id=row.source_release_receipt_id,
        economics_record_id=row.economics_record_id,
        feature_row_id=row.feature_row_id,
        planned_entry_at=row.planned_entry_at,
        label_unlock_at=row.label_unlock_at,
        bundle_classification=row.bundle_classification,
        candidate_provenance_id=row.candidate_provenance_id,
        production_eligible=row.production_eligible,
    )
    if row.prediction_id != expected_id:
        raise IntegrityError("prediction ID does not match its immutable input identity")
    return row


@dataclass(frozen=True)
class LedgerHeadContract:
    sequence: int
    record_hash: str
    anchor_hash: str

    def __post_init__(self) -> None:
        if type(self.sequence) is not int or self.sequence < 0:
            raise ContractError("ledger head sequence must be an exact nonnegative integer")
        if self.sequence == 0:
            if self.record_hash != "GENESIS":
                raise ContractError("genesis ledger head record hash is invalid")
        elif re.fullmatch(r"[0-9a-f]{64}", self.record_hash) is None:
            raise ContractError("ledger head record hash is invalid")
        if re.fullmatch(r"[0-9a-f]{64}", self.anchor_hash) is None:
            raise ContractError("ledger head anchor hash is invalid")

    @classmethod
    def genesis(cls) -> "LedgerHeadContract":
        core = {"record_hash": "GENESIS", "sequence": 0}
        return cls(0, "GENESIS", sha256_json(core))


@dataclass(frozen=True)
class LedgerAppendResult:
    path: Path
    head: LedgerHeadContract
    idempotent_retry: bool


@dataclass(frozen=True)
class PredictionCensusReceipt:
    ledger_id: str
    repository_id: str
    head: LedgerHeadContract
    prediction_ids: tuple[str, ...]
    receipt_id: str
    issuer_mac: str

    def verify(self, ledger: "PredictionLedger") -> None:
        ledger.verify_census(self)


class PredictionLedger:
    def __init__(
        self,
        root: Path,
        lock_path: Path,
        anchor_root: Path,
        persistent_head_path: Path,
        *,
        max_append_delay: timedelta,
        clock: TrustedClock,
        boundary: RepoBoundary,
        operation_receipt: OperationReceipt,
    ) -> None:
        if type(max_append_delay) is not timedelta or max_append_delay <= timedelta(0):
            raise ContractError("maximum prediction append delay must be positive")
        operation_receipt.verify(boundary, operation="APPEND_PREDICTION")
        self.boundary = boundary
        self.operation_receipt = operation_receipt
        self.clock = require_trusted_clock(
            clock,
            boundary=boundary,
            operation_receipt=operation_receipt,
            allow_synthetic=(
                operation_receipt.classification
                is OperationClassification.SYNTHETIC_MECHANICS_ONLY
            ),
        )
        expected_clock_type = (
            SyntheticClock
            if operation_receipt.classification
            is OperationClassification.SYNTHETIC_MECHANICS_ONLY
            else ProductionClock
        )
        if type(self.clock) is not expected_clock_type:
            raise ContractError("ledger clock type differs from its operation capability")
        canonical_paths = (
            (root, _PREDICTION_ROOT, "prediction ledger"),
            (lock_path, _PREDICTION_LOCK_PATH, "prediction lock"),
            (anchor_root, _PREDICTION_ANCHOR_ROOT, "prediction anchors"),
            (persistent_head_path, _PREDICTION_HEAD_PATH, "persistent ledger head"),
        )
        resolved_paths: list[Path] = []
        for supplied, relative, purpose in canonical_paths:
            candidate = boundary.assert_active_path(supplied, purpose=purpose)
            if candidate != (boundary.active_root / relative).resolve(strict=False):
                raise ContractError(f"{purpose} must use its canonical repository path")
            resolved_paths.append(candidate)
        (
            self.root,
            self.lock_path,
            self.anchor_root,
            self.persistent_head_path,
        ) = resolved_paths
        self.intent_path = boundary.assert_active_path(
            persistent_head_path.with_name(f"{persistent_head_path.name}.intent"),
            purpose="persistent ledger append intent",
        )
        resolved = [
            item.resolve(strict=False)
            for item in (self.root, self.anchor_root, self.persistent_head_path.parent)
        ]
        for index, left in enumerate(resolved):
            for right in resolved[index + 1 :]:
                try:
                    left.relative_to(right)
                except ValueError:
                    pass
                else:
                    raise ContractError("ledger, anchor, and persistent-head trees must be separate")
                try:
                    right.relative_to(left)
                except ValueError:
                    pass
                else:
                    raise ContractError("ledger, anchor, and persistent-head trees must be separate")
        self.max_append_delay = max_append_delay
        self.ledger_id = sha256_json(
            {
                "anchor_root": self.anchor_root.relative_to(boundary.active_root).as_posix(),
                "head_path": self.persistent_head_path.relative_to(
                    boundary.active_root
                ).as_posix(),
                "ledger_root": self.root.relative_to(boundary.active_root).as_posix(),
                "lock_path": self.lock_path.relative_to(boundary.active_root).as_posix(),
                "repository_id": boundary.repository_id,
            }
        )
        self._census_secret = os.urandom(32)
        self._candidate_session_binding: tuple[tuple[str, str], ...] | None = None

    @staticmethod
    def _exact_json_files(root: Path, pattern: re.Pattern[str]) -> list[Path]:
        if not root.exists():
            return []
        assert_no_linklike_ancestors(root)
        if not root.is_dir() or is_linklike(root):
            raise IntegrityError(f"ledger root is not a plain directory: {root}")
        files: list[Path] = []
        for path in root.iterdir():
            if is_linklike(path) or not path.is_file() or pattern.fullmatch(path.name) is None:
                raise IntegrityError(f"ledger contains an unexpected path: {path}")
            assert_plain_file(path)
            files.append(path)
        return sorted(files)

    def _load_records(self) -> list[dict[str, object]]:
        records: list[dict[str, object]] = []
        previous = "GENESIS"
        previous_time: datetime | None = None
        for expected_sequence, path in enumerate(
            self._exact_json_files(self.root, RECORD_NAME), start=1
        ):
            try:
                record = json.loads(path.read_text(encoding="utf-8"))
            except (OSError, ValueError) as exc:
                raise IntegrityError(f"invalid ledger record: {path}") from exc
            if not isinstance(record, dict) or set(record) != {
                "appended_at",
                "prediction",
                "previous_hash",
                "record_hash",
                "sequence",
            }:
                raise IntegrityError("prediction ledger record fields are invalid")
            body = {key: value for key, value in record.items() if key != "record_hash"}
            if record["sequence"] != expected_sequence or record["previous_hash"] != previous:
                raise IntegrityError("prediction ledger sequence or chain is broken")
            if sha256_json(body) != record["record_hash"]:
                raise IntegrityError("prediction ledger record hash is invalid")
            row = _prediction_from_payload(record["prediction"])
            expected_classification = (
                OperationClassification.EXTERNAL_CANDIDATE_AUTHORIZATION
                if row.production_eligible
                else OperationClassification.SYNTHETIC_MECHANICS_ONLY
            )
            if self.operation_receipt.classification is not expected_classification:
                raise IntegrityError(
                    "prediction ledger capability differs from row production provenance"
                )
            appended = require_utc(
                datetime.fromisoformat(str(record["appended_at"])), "appended_at"
            )
            if (
                appended < row.recorded_at
                or appended >= row.planned_entry_at
                or appended >= row.label_unlock_at
                or appended - row.decision_at > self.max_append_delay
                or (previous_time is not None and appended < previous_time)
            ):
                raise IntegrityError("prediction ledger append time violates prospective timing")
            expected_name = f"{expected_sequence:020d}_{row.prediction_id}.json"
            if path.name != expected_name:
                raise IntegrityError("prediction filename disagrees with sequence or identity")
            previous = str(record["record_hash"])
            previous_time = appended
            records.append(record)
        return records

    def _load_anchors(self, records: list[dict[str, object]]) -> list[dict[str, object]]:
        anchors: list[dict[str, object]] = []
        previous_anchor = LedgerHeadContract.genesis().anchor_hash
        for expected_sequence, path in enumerate(
            self._exact_json_files(self.anchor_root, ANCHOR_NAME), start=1
        ):
            try:
                anchor = json.loads(path.read_text(encoding="utf-8"))
            except (OSError, ValueError) as exc:
                raise IntegrityError(f"invalid ledger anchor: {path}") from exc
            if not isinstance(anchor, dict) or set(anchor) != {
                "anchor_hash",
                "prediction_id",
                "previous_anchor_hash",
                "record_hash",
                "sequence",
            }:
                raise IntegrityError("ledger anchor fields are invalid")
            body = {key: value for key, value in anchor.items() if key != "anchor_hash"}
            if (
                anchor["sequence"] != expected_sequence
                or anchor["previous_anchor_hash"] != previous_anchor
                or sha256_json(body) != anchor["anchor_hash"]
                or path.name != f"{expected_sequence:020d}_{anchor['anchor_hash']}.json"
                or expected_sequence > len(records)
            ):
                raise IntegrityError("ledger anchor sequence, chain, or identity is invalid")
            record = records[expected_sequence - 1]
            prediction = record["prediction"]
            assert isinstance(prediction, dict)
            if (
                anchor["record_hash"] != record["record_hash"]
                or anchor["prediction_id"] != prediction["prediction_id"]
            ):
                raise IntegrityError("ledger anchor disagrees with prediction record")
            previous_anchor = str(anchor["anchor_hash"])
            anchors.append(anchor)
        return anchors

    @staticmethod
    def _head(
        records: list[dict[str, object]], anchors: list[dict[str, object]]
    ) -> LedgerHeadContract:
        if not records and not anchors:
            return LedgerHeadContract.genesis()
        if len(records) != len(anchors):
            raise IntegrityError("ledger has an unanchored prediction tail")
        return LedgerHeadContract(
            len(records),
            str(records[-1]["record_hash"]),
            str(anchors[-1]["anchor_hash"]),
        )

    def _head_payload(self, head: LedgerHeadContract) -> dict[str, object]:
        core = {
            "anchor_hash": head.anchor_hash,
            "head_version": HEAD_VERSION,
            "ledger_id": self.ledger_id,
            "record_hash": head.record_hash,
            "repository_id": self.boundary.repository_id,
            "sequence": head.sequence,
        }
        return {**core, "head_id": sha256_json(core)}

    def _intent_payload(
        self,
        prior: LedgerHeadContract,
        prediction_payload: dict[str, object],
        appended_at: datetime,
    ) -> dict[str, object]:
        core: dict[str, object] = {
            "appended_at": require_utc(appended_at, "appended_at").isoformat(),
            "intent_version": INTENT_VERSION,
            "ledger_id": self.ledger_id,
            "prediction_payload_sha256": sha256_json(prediction_payload),
            "prior_head": {
                "anchor_hash": prior.anchor_hash,
                "record_hash": prior.record_hash,
                "sequence": prior.sequence,
            },
            "repository_id": self.boundary.repository_id,
        }
        return {**core, "intent_id": sha256_json(core)}

    def _load_intent(self) -> dict[str, object] | None:
        if not self.intent_path.exists():
            return None
        assert_plain_file(self.intent_path)
        try:
            payload = json.loads(self.intent_path.read_text(encoding="utf-8"))
        except (OSError, ValueError) as exc:
            raise IntegrityError("persistent ledger append intent is invalid") from exc
        expected = {
            "appended_at",
            "intent_id",
            "intent_version",
            "ledger_id",
            "prediction_payload_sha256",
            "prior_head",
            "repository_id",
        }
        if not isinstance(payload, dict) or set(payload) != expected:
            raise IntegrityError("persistent ledger append intent schema is invalid")
        prior = payload["prior_head"]
        if not isinstance(prior, dict) or set(prior) != {
            "anchor_hash", "record_hash", "sequence"
        }:
            raise IntegrityError("persistent ledger append intent prior head is invalid")
        core = {key: payload[key] for key in expected if key != "intent_id"}
        try:
            require_utc(datetime.fromisoformat(str(payload["appended_at"])), "appended_at")
            parsed_prior = LedgerHeadContract(
                int(prior["sequence"]), str(prior["record_hash"]), str(prior["anchor_hash"])
            )
        except (TypeError, ValueError, ContractError) as exc:
            raise IntegrityError("persistent ledger append intent values are invalid") from exc
        if (
            payload["intent_version"] != INTENT_VERSION
            or payload["ledger_id"] != self.ledger_id
            or payload["repository_id"] != self.boundary.repository_id
            or re.fullmatch(r"[0-9a-f]{64}", str(payload["prediction_payload_sha256"]))
            is None
            or sha256_json(core) != payload["intent_id"]
            or parsed_prior.sequence < 0
        ):
            raise IntegrityError("persistent ledger append intent identity is invalid")
        return payload

    @staticmethod
    def _intent_prior(intent: dict[str, object]) -> LedgerHeadContract:
        prior = intent["prior_head"]
        assert isinstance(prior, dict)
        return LedgerHeadContract(
            int(prior["sequence"]), str(prior["record_hash"]), str(prior["anchor_hash"])
        )

    def _write_intent(
        self,
        prior: LedgerHeadContract,
        prediction_payload: dict[str, object],
        appended_at: datetime,
    ) -> None:
        self._write_new_json(
            self.intent_path,
            self._intent_payload(prior, prediction_payload, appended_at),
        )

    def _clear_intent(self) -> None:
        if not self.intent_path.exists():
            raise IntegrityError("ledger recovery intent disappeared before completion")
        assert_plain_file(self.intent_path)
        self.intent_path.unlink()
        fsync_directory(self.intent_path.parent)

    def _load_persistent_head(
        self, recovery_prior: LedgerHeadContract | None = None
    ) -> LedgerHeadContract:
        if not self.persistent_head_path.exists():
            if self.root.exists() or self.anchor_root.exists():
                if self._exact_json_files(self.root, RECORD_NAME) or self._exact_json_files(
                    self.anchor_root, ANCHOR_NAME
                ):
                    if recovery_prior is not None:
                        return recovery_prior
                    raise IntegrityError("persistent external ledger head is missing")
            return LedgerHeadContract.genesis()
        assert_plain_file(self.persistent_head_path)
        try:
            payload = json.loads(self.persistent_head_path.read_text(encoding="utf-8"))
        except (OSError, ValueError) as exc:
            raise IntegrityError("persistent external ledger head is invalid") from exc
        expected = {
            "anchor_hash",
            "head_id",
            "head_version",
            "ledger_id",
            "record_hash",
            "repository_id",
            "sequence",
        }
        if not isinstance(payload, dict) or set(payload) != expected:
            raise IntegrityError("persistent external ledger head schema is invalid")
        core = {key: payload[key] for key in expected if key != "head_id"}
        if (
            payload["head_version"] != HEAD_VERSION
            or payload["ledger_id"] != self.ledger_id
            or payload["repository_id"] != self.boundary.repository_id
            or sha256_json(core) != payload["head_id"]
        ):
            raise IntegrityError("persistent external ledger head identity is invalid")
        return LedgerHeadContract(
            int(payload["sequence"]),
            str(payload["record_hash"]),
            str(payload["anchor_hash"]),
        )

    def _write_persistent_head(self, head: LedgerHeadContract) -> None:
        payload = self._head_payload(head)
        self.persistent_head_path.parent.mkdir(parents=True, exist_ok=True)
        self.boundary.assert_active_path(
            self.persistent_head_path, purpose="persistent ledger head"
        )
        temporary = self.persistent_head_path.parent / f".head-{uuid.uuid4().hex}.tmp"
        descriptor = os.open(
            temporary,
            os.O_CREAT | os.O_EXCL | os.O_WRONLY | getattr(os, "O_BINARY", 0),
        )
        try:
            os.write(descriptor, canonical_bytes(payload) + b"\n")
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
        os.replace(temporary, self.persistent_head_path)
        fsync_directory(self.persistent_head_path.parent)

    def verify(self) -> list[dict[str, object]]:
        if self._load_intent() is not None:
            raise IntegrityError("ledger has an incomplete append intent requiring recovery")
        records = self._load_records()
        anchors = self._load_anchors(records)
        actual = self._head(records, anchors)
        if actual != self._load_persistent_head():
            raise IntegrityError("ledger differs from its persistent external head")
        return records

    def issue_census(self) -> PredictionCensusReceipt:
        """Bind every prediction, including abstentions, to the verified ledger head."""

        records = self.verify()
        if not records:
            raise ContractError("prediction census cannot be issued for an empty ledger")
        anchors = self._load_anchors(records)
        head = self._head(records, anchors)
        prediction_ids: list[str] = []
        for record in records:
            prediction = record["prediction"]
            assert isinstance(prediction, dict)
            prediction_id = prediction["prediction_id"]
            if type(prediction_id) is not str:
                raise IntegrityError("prediction census encountered an invalid identity")
            prediction_ids.append(prediction_id)
        if len(set(prediction_ids)) != len(prediction_ids):
            raise IntegrityError("prediction ledger contains duplicate prediction identities")
        core = {
            "head": {
                "anchor_hash": head.anchor_hash,
                "record_hash": head.record_hash,
                "sequence": head.sequence,
            },
            "ledger_id": self.ledger_id,
            "prediction_ids": prediction_ids,
            "repository_id": self.boundary.repository_id,
        }
        receipt_id = sha256_json(core)
        issuer_mac = hmac.new(
            self._census_secret,
            canonical_bytes({**core, "receipt_id": receipt_id}),
            hashlib.sha256,
        ).hexdigest()
        return PredictionCensusReceipt(
            self.ledger_id,
            self.boundary.repository_id,
            head,
            tuple(prediction_ids),
            receipt_id,
            issuer_mac,
        )

    def prediction_rows(self) -> tuple[PredictionRow, ...]:
        """Return the exact schema-verified rows in append order."""

        rows: list[PredictionRow] = []
        for record in self.verify():
            payload = record.get("prediction")
            rows.append(_prediction_from_payload(payload))
        return tuple(rows)

    def verify_census(self, receipt: PredictionCensusReceipt) -> None:
        if type(receipt) is not PredictionCensusReceipt:
            raise IntegrityError("prediction census was not issued by this ledger")
        expected = self.issue_census()
        if (
            not hmac.compare_digest(expected.issuer_mac, receipt.issuer_mac)
            or receipt != expected
        ):
            raise IntegrityError(
                "prediction census is forged, stale, truncated, or from another ledger"
            )

    @staticmethod
    def _write_new_json(path: Path, payload: dict[str, object]) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        assert_no_linklike_ancestors(path.parent)
        temporary = path.parent / f".tmp-{uuid.uuid4().hex}"
        descriptor = os.open(
            temporary,
            os.O_CREAT | os.O_EXCL | os.O_WRONLY | getattr(os, "O_BINARY", 0),
        )
        try:
            os.write(descriptor, canonical_bytes(payload) + b"\n")
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
        if path.exists():
            raise IntegrityError(f"append-only ledger path already exists: {path}")
        os.rename(temporary, path)
        fsync_directory(path.parent)

    def _write_anchor(
        self, record: dict[str, object], previous: LedgerHeadContract
    ) -> LedgerHeadContract:
        prediction = record["prediction"]
        assert isinstance(prediction, dict)
        body = {
            "prediction_id": prediction["prediction_id"],
            "previous_anchor_hash": previous.anchor_hash,
            "record_hash": record["record_hash"],
            "sequence": record["sequence"],
        }
        anchor = {**body, "anchor_hash": sha256_json(body)}
        path = self.anchor_root / f"{int(record['sequence']):020d}_{anchor['anchor_hash']}.json"
        self._write_new_json(path, anchor)
        return LedgerHeadContract(
            int(record["sequence"]), str(record["record_hash"]), str(anchor["anchor_hash"])
        )

    def append(
        self,
        prediction: PredictionRow,
        *,
        expected_head: LedgerHeadContract,
    ) -> LedgerAppendResult:
        """Append at trusted time. Exact recovery/retry may occur after entry."""

        self.operation_receipt.verify(self.boundary, operation="APPEND_PREDICTION")
        expected_classification = (
            OperationClassification.EXTERNAL_CANDIDATE_AUTHORIZATION
            if prediction.production_eligible
            else OperationClassification.SYNTHETIC_MECHANICS_ONLY
        )
        authorization_scope = (
            {
                "bundle_id": prediction.bundle_id,
                "candidate_provenance_id": prediction.candidate_provenance_id or "",
            }
            if prediction.production_eligible
            else {}
        )
        self.operation_receipt.verify(
            self.boundary,
            operation="APPEND_PREDICTION",
            classification=expected_classification,
            required_scope=authorization_scope,
        )
        binding = tuple(sorted(authorization_scope.items()))
        if (
            prediction.production_eligible
            and self._candidate_session_binding is not None
            and self._candidate_session_binding != binding
        ):
            raise IntegrityError(
                "candidate ledger session cannot cross bundle/provenance boundaries"
            )
        if prediction.production_eligible and self._candidate_session_binding is not None:
            self.operation_receipt.assert_consumed(
                self.boundary,
                operation="APPEND_PREDICTION",
                classification=OperationClassification.EXTERNAL_CANDIDATE_AUTHORIZATION,
                required_scope=authorization_scope,
            )
        observed_timestamp = self.clock.now()
        expected_prediction_id = prediction_id_for(
            bundle_id=prediction.bundle_id,
            actual=prediction.actual,
            decision_at=prediction.decision_at,
            recorded_at=prediction.recorded_at,
            source_release_id=prediction.source_release_id,
            source_release_receipt_id=prediction.source_release_receipt_id,
            economics_record_id=prediction.economics_record_id,
            feature_row_id=prediction.feature_row_id,
            planned_entry_at=prediction.planned_entry_at,
            label_unlock_at=prediction.label_unlock_at,
            bundle_classification=prediction.bundle_classification,
            candidate_provenance_id=prediction.candidate_provenance_id,
            production_eligible=prediction.production_eligible,
        )
        if prediction.prediction_id != expected_prediction_id:
            raise IntegrityError("prediction ID does not match its immutable input identity")
        payload = _prediction_payload(prediction)
        with FileLease(self.lock_path):
            self.boundary.assert_active_path(self.root, purpose="prediction ledger")
            self.boundary.assert_active_path(self.anchor_root, purpose="prediction anchors")
            intent = self._load_intent()
            recovery_prior = self._intent_prior(intent) if intent is not None else None
            if intent is not None:
                if (
                    intent["prediction_payload_sha256"] != sha256_json(payload)
                    or recovery_prior != expected_head
                ):
                    raise IntegrityError(
                        "ledger recovery intent does not match exact prior head and retry"
                    )
                timestamp = require_utc(
                    datetime.fromisoformat(str(intent["appended_at"])), "appended_at"
                )
            else:
                timestamp = observed_timestamp
            stored_head = self._load_persistent_head(recovery_prior)
            self.root.mkdir(parents=True, exist_ok=True)
            self.anchor_root.mkdir(parents=True, exist_ok=True)
            records = self._load_records()
            anchors = self._load_anchors(records)
            if len(records) > len(anchors) + 1 or len(anchors) > len(records):
                raise IntegrityError("ledger crash tail exceeds one recoverable event")
            if len(records) == len(anchors) + 1:
                if intent is None:
                    raise IntegrityError("unanchored ledger tail has no durable recovery intent")
                prior = self._head(records[:-1], anchors)
                tail = records[-1]
                if stored_head != prior or expected_head != prior or tail["prediction"] != payload:
                    raise IntegrityError("unanchored tail does not match exact prior head and retry")
                head = self._write_anchor(tail, prior)
                self._write_persistent_head(head)
                self._clear_intent()
                return LedgerAppendResult(
                    self.root / f"{head.sequence:020d}_{prediction.prediction_id}.json",
                    head,
                    True,
                )
            actual_head = self._head(records, anchors)
            if stored_head != actual_head:
                # Crash after anchor but before persistent-head replacement.
                if (
                    intent is None
                    or
                    len(records) != stored_head.sequence + 1
                    or not records
                    or records[-1]["prediction"] != payload
                    or expected_head != stored_head
                    or self._head(records[:-1], anchors[:-1]) != stored_head
                ):
                    raise IntegrityError("ledger is ahead of persistent head without exact retry")
                self._write_persistent_head(actual_head)
                self._clear_intent()
                return LedgerAppendResult(
                    self.root / f"{actual_head.sequence:020d}_{prediction.prediction_id}.json",
                    actual_head,
                    True,
                )
            for index, record in enumerate(records):
                existing = record["prediction"]
                assert isinstance(existing, dict)
                if existing["prediction_id"] == prediction.prediction_id:
                    prior = self._head(records[:index], anchors[:index])
                    if (
                        index != len(records) - 1
                        or existing != payload
                        or expected_head != prior
                        or record["previous_hash"] != prior.record_hash
                    ):
                        raise IntegrityError("prediction retry conflicts with ledger history")
                    if intent is not None:
                        self._clear_intent()
                    return LedgerAppendResult(
                        self.root / f"{len(records):020d}_{prediction.prediction_id}.json",
                        actual_head,
                        True,
                    )
            if actual_head != expected_head:
                raise IntegrityError("append expected head is stale or incorrect")
            if (
                timestamp < prediction.recorded_at
                or timestamp >= prediction.planned_entry_at
                or timestamp >= prediction.label_unlock_at
                or timestamp - prediction.decision_at > self.max_append_delay
            ):
                raise IntegrityError("prediction append missed its prospective timing window")
            if records and timestamp < require_utc(
                datetime.fromisoformat(str(records[-1]["appended_at"])), "appended_at"
            ):
                raise IntegrityError("prediction ledger clock moved backwards")
            if (
                prediction.production_eligible
                and self._candidate_session_binding is None
            ):
                self.operation_receipt.consume(
                    self.boundary,
                    operation="APPEND_PREDICTION",
                    classification=(
                        OperationClassification.EXTERNAL_CANDIDATE_AUTHORIZATION
                    ),
                    required_scope=authorization_scope,
                )
                self._candidate_session_binding = binding
            if intent is None:
                self._write_intent(actual_head, payload, timestamp)
            sequence = len(records) + 1
            body = {
                "appended_at": timestamp.isoformat(),
                "prediction": payload,
                "previous_hash": actual_head.record_hash,
                "sequence": sequence,
            }
            record = {**body, "record_hash": sha256_json(body)}
            target = self.root / f"{sequence:020d}_{prediction.prediction_id}.json"
            self._write_new_json(target, record)
            head = self._write_anchor(record, actual_head)
            self._write_persistent_head(head)
            self._clear_intent()
            self.verify()
            return LedgerAppendResult(target, head, False)

    def quarantine_orphan_temps(self, recovery_root: Path) -> tuple[Path, ...]:
        recovery_root = self.boundary.assert_active_path(
            recovery_root, purpose="ledger recovery"
        )
        recovery_root.mkdir(parents=True, exist_ok=True)
        moved: list[Path] = []
        with FileLease(self.lock_path):
            for root in (self.root, self.anchor_root):
                if not root.exists():
                    continue
                for source in sorted(root.glob(".tmp-*")):
                    if is_linklike(source) or not source.is_file():
                        raise IntegrityError("orphan ledger temp is not a plain file")
                    target = recovery_root / f"{source.name}.{uuid.uuid4().hex}.orphan"
                    os.rename(source, target)
                    moved.append(target)
                fsync_directory(root)
            fsync_directory(recovery_root)
        return tuple(moved)
