"""Compact deterministic Parquet encoding for causal-observation evidence.

The encoding changes storage only.  Readers reconstruct the exact logical
Python dictionaries consumed by the independent causal-observation verifier.
"""

from __future__ import annotations

import os
import re
from pathlib import Path
from typing import Iterable, Mapping, Sequence

import pyarrow as pa
import pyarrow.parquet as pq

from .canonical import io_path, sha256_json
from .errors import ContractError, IntegrityError


FORMAT_VERSION = "causal_observation_parquet/1.1.0"
FILENAMES = {
    "observations": "observations.parquet",
    "missingness": "missingness.parquet",
    "roll": "roll.parquet",
    "quality": "quality.parquet",
    "cadence": "cadence.parquet",
}
_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_HASH = pa.binary(32)


def _schema(name: str, fields: Sequence[pa.Field]) -> pa.Schema:
    return pa.schema(
        fields,
        metadata={
            b"format_version": FORMAT_VERSION.encode("ascii"),
            b"logical_table": name.encode("ascii"),
        },
    )


SCHEMAS: dict[str, pa.Schema] = {
    "observations": _schema(
        "observations",
        [
            pa.field("market", pa.string(), nullable=False),
            pa.field("source_contract_id", _HASH, nullable=False),
            pa.field("source_release_id", _HASH, nullable=False),
            pa.field("source_file_path", pa.string(), nullable=False),
            pa.field("source_file_sha256", _HASH, nullable=False),
            pa.field("source_row_sha256", _HASH, nullable=False),
            pa.field("source_cadence", pa.string(), nullable=False),
            pa.field("bar_start_ns", pa.int64(), nullable=False),
            pa.field("bar_end_ns", pa.int64(), nullable=False),
            pa.field("source_timestamp_ns", pa.int64(), nullable=False),
            pa.field("available_at_ns", pa.int64(), nullable=False),
            pa.field("decision_eligible_at_ns", pa.int64(), nullable=False),
            pa.field("publisher_id", pa.uint16(), nullable=False),
            pa.field("instrument_id", pa.uint32(), nullable=False),
            pa.field("raw_symbol", pa.string(), nullable=False),
            pa.field("actual_contract", pa.string(), nullable=False),
            pa.field("definition_source_file_path", pa.string(), nullable=False),
            pa.field("definition_source_file_sha256", _HASH, nullable=False),
            pa.field("definition_row_sha256", _HASH, nullable=False),
            pa.field("definition_event_at_ns", pa.int64(), nullable=False),
            pa.field("definition_received_at_ns", pa.int64(), nullable=False),
            pa.field("listing_activation_ns", pa.uint64(), nullable=False),
            pa.field("expiration_ns", pa.uint64(), nullable=False),
            pa.field("open_nano", pa.int64(), nullable=False),
            pa.field("high_nano", pa.int64(), nullable=False),
            pa.field("low_nano", pa.int64(), nullable=False),
            pa.field("close_nano", pa.int64(), nullable=False),
            pa.field("volume", pa.uint64(), nullable=False),
            pa.field("currency", pa.string(), nullable=False),
            pa.field("min_price_increment_nano", pa.int64(), nullable=False),
            pa.field("multiplier_nano", pa.int64(), nullable=False),
            pa.field("project_session_id", pa.string(), nullable=False),
            pa.field("project_trade_date", pa.string(), nullable=False),
            pa.field("project_grouping_start_ns", pa.int64(), nullable=False),
            pa.field("project_grouping_end_ns", pa.int64(), nullable=False),
            pa.field("project_timezone", pa.string(), nullable=False),
            pa.field("official_schedule_state", pa.string(), nullable=False),
        ],
    ),
    "missingness": _schema(
        "missingness",
        [
            pa.field("observation_ordinal", pa.uint32()),
            pa.field("market", pa.string(), nullable=False),
            pa.field("interval_start_ns", pa.int64(), nullable=False),
            pa.field("interval_end_ns", pa.int64(), nullable=False),
            pa.field("state", pa.string(), nullable=False),
            pa.field("authority", pa.string(), nullable=False),
            pa.field("evidence_sha256", _HASH),
        ],
    ),
    "roll": _schema(
        "roll",
        [
            pa.field("observation_ordinal", pa.uint32(), nullable=False),
            pa.field("actual_contract_before", pa.string(), nullable=False),
            pa.field("actual_contract_after", pa.string(), nullable=False),
            pa.field("effective_time_ns", pa.int64()),
            pa.field("roll_flag", pa.bool_(), nullable=False),
            pa.field("price_discontinuity_flag", pa.bool_(), nullable=False),
            pa.field("crossing_status", pa.string(), nullable=False),
        ],
    ),
    "quality": _schema(
        "quality",
        [
            pa.field("observation_ordinal", pa.uint32(), nullable=False),
            pa.field("ohlc_valid", pa.bool_(), nullable=False),
            pa.field("volume_valid", pa.bool_(), nullable=False),
            pa.field("timestamp_order_valid", pa.bool_(), nullable=False),
            pa.field("duplicate_state", pa.string(), nullable=False),
            pa.field(
                "quality_flags",
                pa.list_(pa.field("element", pa.string())),
                nullable=False,
            ),
        ],
    ),
    "cadence": _schema(
        "cadence",
        [
            pa.field("observation_ordinal", pa.uint32(), nullable=False),
            pa.field("source_cadence", pa.string(), nullable=False),
            pa.field("comparison_cadence", pa.string(), nullable=False),
            pa.field("interval_boundary_compatible", pa.bool_(), nullable=False),
            pa.field("result", pa.string(), nullable=False),
            pa.field("exception_state", pa.string(), nullable=False),
        ],
    ),
}

_HASH_FIELDS = {
    name: frozenset(
        field.name for field in schema if field.type.equals(_HASH)
    )
    for name, schema in SCHEMAS.items()
}


def _hash_bytes(value: object, field: str, *, nullable: bool) -> bytes | None:
    if value is None and nullable:
        return None
    if type(value) is not str or _SHA256.fullmatch(value) is None:
        raise ContractError(f"{field} must be a SHA-256 identity")
    return bytes.fromhex(value)


def _encode_physical(name: str, row: Mapping[str, object]) -> dict[str, object]:
    schema = SCHEMAS[name]
    if set(row) != set(schema.names):
        raise ContractError(f"{name} Parquet row schema is not exact")
    encoded = dict(row)
    for field in schema:
        if field.name in _HASH_FIELDS[name]:
            encoded[field.name] = _hash_bytes(
                row[field.name], f"{name}.{field.name}", nullable=field.nullable
            )
    return encoded


def _decode_physical(name: str, row: Mapping[str, object]) -> dict[str, object]:
    decoded = dict(row)
    for field in SCHEMAS[name]:
        if field.name not in _HASH_FIELDS[name]:
            continue
        value = row[field.name]
        if value is None and field.nullable:
            continue
        if type(value) is not bytes or len(value) != 32:
            raise IntegrityError(f"{name}.{field.name} binary identity is invalid")
        decoded[field.name] = value.hex()
    return decoded


def _parquet_io_path(path: Path) -> Path | str:
    """Give Arrow an extended-length absolute path on Windows."""

    return str(io_path(path)) if os.name == "nt" else path


def _writer(path: Path, schema: pa.Schema) -> pq.ParquetWriter:
    io_path = Path(_parquet_io_path(path))
    if io_path.exists():
        raise IntegrityError("immutable causal-observation Parquet output already exists")
    io_path.parent.mkdir(parents=True, exist_ok=True)
    return pq.ParquetWriter(
        _parquet_io_path(path),
        schema,
        compression="zstd",
        compression_level=9,
        use_dictionary=True,
        write_statistics=True,
        version="2.6",
        data_page_version="2.0",
        use_deprecated_int96_timestamps=False,
    )


def write_table(
    path: Path,
    *,
    name: str,
    row_groups: Iterable[Sequence[Mapping[str, object]]],
) -> int:
    """Write exact caller-defined row groups and return the logical row count."""

    if name not in SCHEMAS:
        raise ContractError("unknown causal-observation Parquet table")
    io_path = Path(_parquet_io_path(path))
    writer = _writer(path, SCHEMAS[name])
    count = 0
    try:
        for rows in row_groups:
            if not rows:
                continue
            physical_rows = (
                [_observation_core(row) for row in rows]
                if name == "observations" and "row_id" in rows[0]
                else list(rows)
            )
            encoded = [_encode_physical(name, row) for row in physical_rows]
            table = pa.Table.from_pylist(encoded, schema=SCHEMAS[name])
            writer.write_table(table, row_group_size=len(encoded))
            count += len(encoded)
    except Exception:
        writer.close()
        if io_path.is_file():
            io_path.unlink()
        raise
    else:
        writer.close()
    if count <= 0 and name != "cadence":
        if io_path.is_file():
            io_path.unlink()
        raise IntegrityError("causal-observation Parquet table cannot be empty")
    with io_path.open("r+b") as handle:
        os.fsync(handle.fileno())
    return count


def write_bundle(
    directory: Path,
    *,
    tables: Mapping[str, Sequence[Mapping[str, object]]],
) -> dict[str, int]:
    """Write the exact five logical tables with one caller partition row group."""

    if set(tables) != set(FILENAMES):
        raise ContractError("causal-observation Parquet bundle is not exact")
    return write_partitioned_bundle(
        directory,
        table_row_groups={name: (tables[name],) for name in FILENAMES},
    )


def _observation_core(row: Mapping[str, object]) -> dict[str, object]:
    core = {key: value for key, value in row.items() if key != "row_id"}
    if set(core) != set(SCHEMAS["observations"].names):
        raise ContractError("observations Parquet row schema is not exact")
    if row.get("row_id") != sha256_json(core):
        raise IntegrityError("observation row identity is not reconstructible")
    return core


def _encode_group(
    name: str,
    rows: Sequence[Mapping[str, object]],
    observations: Sequence[Mapping[str, object]],
) -> list[dict[str, object]]:
    if name == "observations":
        return [_observation_core(row) for row in rows]
    by_id = {str(row["row_id"]): index for index, row in enumerate(observations)}
    if len(by_id) != len(observations):
        raise IntegrityError("observation identities are duplicate")
    encoded: list[dict[str, object]] = []
    for row in rows:
        value = dict(row)
        if name == "missingness":
            row_id = value.pop("observation_row_id")
            value.pop("evidence_id")
            ordinal = None if row_id is None else by_id.get(str(row_id))
            if row_id is not None and ordinal is None:
                raise IntegrityError("missingness references another row group")
            core = {"observation_row_id": row_id, **value}
            if row.get("evidence_id") != sha256_json(core):
                raise IntegrityError("missingness identity is not reconstructible")
            if ordinal is not None:
                observation = observations[ordinal]
                expected_evidence = sha256_json(
                    {
                        "market": value["market"],
                        "source_row_sha256": observation["source_row_sha256"],
                        "interval_start_ns": value["interval_start_ns"],
                        "interval_end_ns": value["interval_end_ns"],
                        "authority": value["authority"],
                    }
                )
                if value["evidence_sha256"] != expected_evidence:
                    raise IntegrityError(
                        "observed missingness evidence is not reconstructible"
                    )
                value["evidence_sha256"] = None
            value = {"observation_ordinal": ordinal, **value}
        elif name == "roll":
            row_id = str(value.pop("row_id"))
            causal = value.pop("causal_selection_evidence_sha256")
            ordinal = by_id.get(row_id)
            if ordinal is None:
                raise IntegrityError("roll references another row group")
            observation = observations[ordinal]
            expected = sha256_json(
                {
                    "definition_row_sha256": observation["definition_row_sha256"],
                    "definition_received_at_ns": observation["definition_received_at_ns"],
                    "prior_contract": value["actual_contract_before"],
                }
            )
            if causal != expected:
                raise IntegrityError("roll causal identity is not reconstructible")
            value = {"observation_ordinal": ordinal, **value}
        elif name == "quality":
            row_id = str(value.pop("row_id"))
            ordinal = by_id.get(row_id)
            if ordinal is None:
                raise IntegrityError("quality references another row group")
            observation = observations[ordinal]
            expected = {
                "row_identity_sha256": row_id,
                "source_contract_id": observation["source_contract_id"],
                "source_release_id": observation["source_release_id"],
                "source_file_sha256": observation["source_file_sha256"],
            }
            if any(value.get(field) != expected_value for field, expected_value in expected.items()):
                raise IntegrityError("quality source identity is not reconstructible")
            for field in expected:
                value.pop(field)
            value = {"observation_ordinal": ordinal, **value}
        elif name == "cadence":
            row_id = str(value.pop("row_id"))
            ordinal = by_id.get(row_id)
            if ordinal is None:
                raise IntegrityError("cadence references another row group")
            comparison_id = value.pop("comparison_id")
            core = {"row_id": row_id, **value}
            if comparison_id != sha256_json(core):
                raise IntegrityError("cadence identity is not reconstructible")
            value = {"observation_ordinal": ordinal, **value}
        else:  # pragma: no cover - guarded by caller
            raise ContractError("unknown causal-observation Parquet table")
        encoded.append(value)
    return encoded


def write_partitioned_bundle(
    directory: Path,
    *,
    table_row_groups: Mapping[
        str, Sequence[Sequence[Mapping[str, object]]]
    ],
) -> dict[str, int]:
    """Write market-year files with aligned caller-defined monthly row groups."""

    if set(table_row_groups) != set(FILENAMES):
        raise ContractError("causal-observation Parquet bundle is not exact")
    observations = tuple(table_row_groups["observations"])
    if not observations or any(
        len(table_row_groups[name]) != len(observations) for name in FILENAMES
    ):
        raise ContractError("causal-observation Parquet row groups are not aligned")
    counts: dict[str, int] = {}
    for name, filename in FILENAMES.items():
        groups = [
            _encode_group(name, tuple(rows), tuple(observations[index]))
            for index, rows in enumerate(table_row_groups[name])
        ]
        counts[name] = write_table(
            directory / filename, name=name, row_groups=groups
        )
    return counts


def _parquet(path: Path, name: str) -> pq.ParquetFile:
    try:
        parquet = pq.ParquetFile(_parquet_io_path(path))
    except (OSError, pa.ArrowException) as exc:
        raise IntegrityError("causal-observation Parquet file is unreadable") from exc
    if not parquet.schema_arrow.equals(SCHEMAS[name], check_metadata=True):
        raise IntegrityError("causal-observation Parquet schema differs")
    if name != "cadence" and (
        parquet.metadata.num_rows <= 0 or parquet.metadata.num_row_groups <= 0
    ):
        raise IntegrityError("causal-observation Parquet file is empty")
    if name == "cadence" and parquet.metadata.num_rows < 0:
        raise IntegrityError("causal-observation cadence row count is invalid")
    return parquet


def _read_physical_groups(path: Path, name: str) -> list[list[dict[str, object]]]:
    parquet = _parquet(path, name)
    groups: list[list[dict[str, object]]] = []
    try:
        for index in range(parquet.metadata.num_row_groups):
            groups.append(
                [
                    _decode_physical(name, row)
                    for row in parquet.read_row_group(index).to_pylist()
                ]
            )
    except (OSError, pa.ArrowException, ValueError) as exc:
        raise IntegrityError("causal-observation Parquet rows are invalid") from exc
    return groups


def _decode_group(
    name: str,
    rows: Sequence[Mapping[str, object]],
    observations: Sequence[Mapping[str, object]],
) -> list[dict[str, object]]:
    if name == "observations":
        return [{"row_id": sha256_json(dict(row)), **dict(row)} for row in rows]
    result: list[dict[str, object]] = []
    for physical in rows:
        value = dict(physical)
        ordinal = value.pop("observation_ordinal")
        if ordinal is None:
            observation = None
        elif type(ordinal) is not int or not 0 <= ordinal < len(observations):
            raise IntegrityError("evidence observation ordinal is invalid")
        else:
            observation = observations[ordinal]
        if name == "missingness":
            row_id = None if observation is None else observation["row_id"]
            if observation is not None:
                if value["evidence_sha256"] is not None:
                    raise IntegrityError(
                        "observed missingness stores a redundant evidence identity"
                    )
                value["evidence_sha256"] = sha256_json(
                    {
                        "market": value["market"],
                        "source_row_sha256": observation["source_row_sha256"],
                        "interval_start_ns": value["interval_start_ns"],
                        "interval_end_ns": value["interval_end_ns"],
                        "authority": value["authority"],
                    }
                )
            elif value["evidence_sha256"] is None:
                raise IntegrityError("gap missingness evidence identity is absent")
            core = {"observation_row_id": row_id, **value}
            value = {"evidence_id": sha256_json(core), **core}
        elif name == "roll":
            if observation is None:
                raise IntegrityError("roll observation ordinal is null")
            causal = sha256_json(
                {
                    "definition_row_sha256": observation["definition_row_sha256"],
                    "definition_received_at_ns": observation["definition_received_at_ns"],
                    "prior_contract": value["actual_contract_before"],
                }
            )
            value = {
                "row_id": observation["row_id"],
                "actual_contract_before": value["actual_contract_before"],
                "actual_contract_after": value["actual_contract_after"],
                "effective_time_ns": value["effective_time_ns"],
                "causal_selection_evidence_sha256": causal,
                "roll_flag": value["roll_flag"],
                "price_discontinuity_flag": value["price_discontinuity_flag"],
                "crossing_status": value["crossing_status"],
            }
        elif name == "quality":
            if observation is None:
                raise IntegrityError("quality observation ordinal is null")
            value = {
                "row_id": observation["row_id"],
                "row_identity_sha256": observation["row_id"],
                **value,
                "source_contract_id": observation["source_contract_id"],
                "source_release_id": observation["source_release_id"],
                "source_file_sha256": observation["source_file_sha256"],
            }
        elif name == "cadence":
            if observation is None:
                raise IntegrityError("cadence observation ordinal is null")
            core = {"row_id": observation["row_id"], **value}
            value = {"comparison_id": sha256_json(core), **core}
        else:  # pragma: no cover - guarded by caller
            raise ContractError("unknown causal-observation Parquet table")
        result.append(value)
    return result


def read_bundle(directory: Path) -> dict[str, list[dict[str, object]]]:
    physical = {
        name: _read_physical_groups(directory / filename, name)
        for name, filename in FILENAMES.items()
    }
    count = len(physical["observations"])
    required = ("missingness", "roll", "quality")
    if any(len(physical[name]) != count for name in required):
        raise IntegrityError("causal-observation Parquet row groups are misaligned")
    cadence_count = len(physical["cadence"])
    if cadence_count not in {0, count}:
        raise IntegrityError("causal-observation Parquet row groups are misaligned")
    if cadence_count == 0:
        # Cadence comparisons are optional.  Keep the physical empty-table
        # representation while supplying one empty logical group per
        # observation group for the aligned decoder below.
        physical["cadence"] = [[] for _ in range(count)]
    logical: dict[str, list[dict[str, object]]] = {name: [] for name in FILENAMES}
    for index in range(count):
        observations = _decode_group(
            "observations", physical["observations"][index], ()
        )
        logical["observations"].extend(observations)
        for name in FILENAMES:
            if name != "observations":
                logical[name].extend(
                    _decode_group(name, physical[name][index], observations)
                )
    return logical


def read_table(path: Path, *, name: str) -> list[dict[str, object]]:
    if name not in SCHEMAS:
        raise ContractError("unknown causal-observation Parquet table")
    if name != "observations":
        raise ContractError("dependent evidence requires read_bundle")
    return [
        row
        for group in _read_physical_groups(path, name)
        for row in _decode_group(name, group, ())
    ]


def row_count(path: Path, *, name: str) -> int:
    return _parquet(path, name).metadata.num_rows
