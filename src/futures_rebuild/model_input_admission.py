"""Active minute-input admission over the unchanged preparation implementation.

Source permissions are a ceiling, not permission to run a research operation.
Closed studies and legacy replay continue to use their frozen implementations.
"""
from pathlib import Path

from . import active_data_view
from .canonical import sha256_file
from .causal_timeframe_storage import contained, read_record
from .errors import ContractError, IntegrityError, UnauthorizedOperation
from .foundation.statistics_v2 import AsOfStatisticsV2
from .model_input_preparation import WindowRequest, prepare_inputs

POINTER = "configs/active_model_input_admission.json"
SCHEMA = "active_model_input_admission/1"
STATE = "ACTIVE_OHLCV_STATISTICS_DIAGNOSTIC"
BINDINGS = (
    "configs/active_annual_foundation_independence.json",
    "configs/active_dbn_congruence_release_v1.json",
    "configs/source_contract.json",
    "configs/statistics_foundation_roles.json",
    "data/causally_gated_normalized/_control/catalog.json",
    "src/futures_rebuild/foundation/decoder.py",
    "src/futures_rebuild/foundation/market_state.py",
    "src/futures_rebuild/foundation/statistics_v2.py",
    "src/futures_rebuild/model_input_preparation.py",
    "src/futures_rebuild/model_input_admission.py",
)


def active_policy(root):
    """Metadata-only check; missing, inactive or changed policy fails closed."""
    policy = read_record(contained(Path(root), POINTER))
    if (policy.get("schema") != SCHEMA or policy.get("status") != STATE
            or policy.get("research_authority") is not False
            or policy.get("statistics_feature_eligible") is not False
            or set(policy.get("bindings", {})) != set(BINDINGS)):
        raise UnauthorizedOperation("Strict minute input policy is absent or not admitted")
    for relative, digest in policy["bindings"].items():
        if sha256_file(contained(Path(root), relative)) != digest:
            raise IntegrityError("Strict minute input binding changed: " + relative)
    return policy


def read_windows(repository_root, *, market, year, requests, purpose="LINEAGE_AUDIT",
                 require_status=False, statistics=None, max_age_ns=None, settlement_flags=None):
    """Return every requested window, retaining refused/indeterminate outcomes.

    Use for newly declared minute-input studies. Callers supply the complete feature
    lookback or target path; only ELIGIBLE features / COMPLETE targets may be used.
    OHLCV is unchanged. Optional statistics remain diagnostic-only even with age and
    settlement declarations. Research still needs its own declaration and approval.
    """
    root = Path(repository_root).resolve(strict=True)
    policy = active_policy(root)
    if (type(purpose) is not str or not purpose or type(require_status) is not bool
            or type(year) is not int):
        raise ContractError("Declare a source purpose and boolean status requirement")
    if statistics is not None and type(statistics) is not AsOfStatisticsV2:
        raise ContractError("Optional statistics require the verified V2 interpretation")
    if require_status and year >= 2025:
        raise UnauthorizedOperation("Status-dependent windows are not admitted by this OHLCV path")
    # Apply the caller's cohort permission, not merely preparation's audit permission.
    active_data_view.resolve(repository_root=root, market=market, year=year,
                             purpose=purpose, require_status=require_status)
    results = prepare_inputs(root, market=market, year=year, requests=requests,
                             statistics=statistics, max_age_ns=max_age_ns,
                             settlement_flags=settlement_flags)
    if active_policy(root) != policy:
        raise IntegrityError("Strict minute input policy changed during read")
    for result in results:
        result.update(input_admission_id=policy["id"], input_admission_status=STATE,
                      requested_purpose=purpose)
    return results
