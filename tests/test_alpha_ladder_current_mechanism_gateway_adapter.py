from __future__ import annotations

from futures_rebuild.alpha_ladder_frozen_mechanism import (
    build_frozen_mechanism,
    validate_frozen_mechanism,
)
from futures_rebuild.canonical import sha256_json


def test_current_counted_successor_schema_retains_frozen_mechanism_gate() -> None:
    mechanism = build_frozen_mechanism(
        contract_id="a" * 64,
        profile_id="b" * 64,
        source_protocol_id="c" * 64,
        source_protocol_sha256="d" * 64,
        all_markets=tuple(f"M{index:02d}" for index in range(41)),
    )
    core = {key: value for key, value in mechanism.items() if key != "mechanism_id"}
    core["schema_version"] = (
        "alpha_ladder_full_regular_source_observable_successor/1.0.0"
    )
    successor = {**core, "mechanism_id": sha256_json(core)}

    assert validate_frozen_mechanism(successor) == successor
