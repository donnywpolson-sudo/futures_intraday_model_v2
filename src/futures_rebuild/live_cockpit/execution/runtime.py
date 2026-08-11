"""Cockpit execution capability state with no provider client on startup."""

from __future__ import annotations

import importlib.util
import json
from pathlib import Path
from typing import Any, Mapping

from ..protocol import PROTOCOL_VERSION, validate_command
from .adapter import DisabledExecutionAdapter, ExecutionAdapter
from .arm_state import ArmState
from .config import active_connection, load_account_binding, load_execution_config
from .credential_store import WindowsCredentialStore
from .domain import ExecutionMode


class ExecutionRuntime:
    """Owns fail-closed capability state; ordinary construction is network-free."""

    def __init__(self, *, root: Path, adapter: ExecutionAdapter | None = None) -> None:
        self.root = root
        self.configuration_valid = True
        self.configuration_error: str | None = None
        try:
            self.config = load_execution_config(root=root)
            self.connection_id, self.connection, self.connection_hash = active_connection(self.config)
        except (OSError, ValueError, json.JSONDecodeError) as exc:
            self.configuration_valid = False
            self.configuration_error = str(exc)[:160]
            self.config = {}
            self.connection_id = "UNSET"
            self.connection_hash = "0" * 64
            self.connection = {
                "entitlement_status": "UNCONFIRMED",
                "credential_reference": {
                    "mechanism": "WINDOWS_CREDENTIAL_MANAGER",
                    "target_name": "futures_intraday_model_v2/tradovate",
                    "configured": False,
                },
                "blockers": ["EXECUTION_CONFIG_MISSING_OR_INVALID"],
            }
        self.mode = (
            ExecutionMode.LOCAL_EXECUTION_SIMULATOR
            if adapter is not None and adapter.provider_id == "LOCAL_EXECUTION_SIMULATOR"
            else ExecutionMode.OBSERVATION_ONLY
        )
        self.arm_state = ArmState(mode=self.mode)
        self.adapter = DisabledExecutionAdapter() if adapter is None else adapter
        self.binding_error: str | None = None
        try:
            self.binding = load_account_binding(root=root)
        except (OSError, ValueError) as exc:
            self.binding = None
            self.binding_error = str(exc)[:160]
        try:
            profiles = json.loads((root / "configs/prop_firm_profiles.json").read_text(encoding="utf-8"))
            self.profile_id = str(profiles["active_profile_id"])
            self.profile = profiles["profiles"][self.profile_id]
            self.account_stage = str(self.profile["active_account_stage"])
        except (OSError, KeyError, TypeError, json.JSONDecodeError):
            self.profile_id = "UNSET"
            self.profile = {}
            self.account_stage = "UNSET"
        try:
            costs = json.loads((root / "configs/prop_firm_execution_costs.json").read_text(encoding="utf-8"))
            self.cost_profile_id = str(costs["active_cost_profile_id"])
            self.cost_profile = costs["cost_profiles"][self.cost_profile_id]
        except (OSError, KeyError, TypeError, json.JSONDecodeError):
            self.cost_profile_id = "UNSET"
            self.cost_profile = {}

    @property
    def blockers(self) -> tuple[str, ...]:
        values = list(self.connection["blockers"])
        if not self.configuration_valid:
            values.append("EXECUTION_CONFIG_MISSING_OR_INVALID")
        if self.profile_id == "UNSET":
            values.append("ACTIVE_MFF_PROFILE_UNAVAILABLE")
        if self.cost_profile_id == "UNSET":
            values.append("ACTIVE_COST_PROFILE_UNAVAILABLE")
        if self.binding_error:
            values.append("ACCOUNT_BINDING_INVALID")
        elif self.binding is None:
            values.append("EXACT_ACCOUNT_BINDING_UNSET")
        if self.adapter.connected and self.mode is not ExecutionMode.LOCAL_EXECUTION_SIMULATOR:
            values.append("UNEXPECTED_EXECUTION_ADAPTER_CONNECTED")
        return tuple(dict.fromkeys(str(value) for value in values))

    def capability_payload(self) -> dict[str, Any]:
        arm = self.arm_state.snapshot()
        local_simulator = self.mode is ExecutionMode.LOCAL_EXECUTION_SIMULATOR
        return {
            "mode": self.mode.value,
            "origin": "LOCAL_SIMULATOR" if local_simulator else "LOCAL_CONFIGURATION",
            "simulated": local_simulator,
            "provider_id": "LOCAL_EXECUTION_SIMULATOR" if local_simulator else "my_funded_futures",
            "platform_id": "LOCAL_FAKE_BROKER" if local_simulator else "tradovate",
            "profile_id": self.profile_id,
            "account_stage": "SIMULATED" if local_simulator else self.account_stage,
            "connection_id": "LOCAL_EXECUTION_SIMULATOR" if local_simulator else self.connection_id,
            "connection_hash": self.connection_hash,
            "entitlement_status": str(self.connection["entitlement_status"]),
            "account_binding_present": self.binding is not None,
            "account_binding_id": self.binding.binding_id if self.binding else None,
            "cost_profile_id": self.cost_profile_id,
            "exact_costs_verified": self.cost_profile.get("exact_provider_account_costs_verified") is True,
            "production_readiness": False,
            "execution_authorized": False,
            "order_paths_reachable": False,
            "provider_connection_opened": False,
            "armed": arm.armed,
            "arm_expires_at": arm.expires_at.isoformat() if arm.expires_at else None,
            "blockers": list(self.blockers),
            "verified_micro_mappings": ["MES", "MCL", "M6E"],
            "disabled_signal_roots": ["ZN"],
        }

    def self_check_payload(self) -> dict[str, Any]:
        credential_store = WindowsCredentialStore()
        return {
            "tradovate_adapter_import": importlib.util.find_spec("futures_rebuild.live_cockpit.execution.tradovate_adapter") is not None,
            "configuration_valid": self.configuration_valid,
            "active_provider": "my_funded_futures",
            "active_profile": self.profile_id,
            "account_stage": self.account_stage,
            "entitlement_status": self.connection["entitlement_status"],
            "credential_reference_available": credential_store.available(),
            "credential_check_mode": "NOT_ACCESSED",
            "credential_configured": None,
            "account_binding_present": self.binding is not None,
            "active_costs_verified": self.cost_profile.get("exact_provider_account_costs_verified") is True,
            "news_feed_bound": False,
            "session_calendar_bound": False,
            "price_limit_feed_bound": False,
            "production_readiness": False,
            "execution_authorized": False,
            "current_mode": self.mode.value,
            "order_paths_reachable": False,
            "provider_connection_opened": False,
            "blockers": list(self.blockers),
        }

    def preview_order(self, value: Mapping[str, object]) -> dict[str, Any]:
        try:
            validate_command(
                {
                    "v": PROTOCOL_VERSION,
                    "type": "PREVIEW_ORDER_INTENT",
                    "payload": dict(value),
                }
            )
        except ValueError:
            return {
                "ok": False,
                "authoritative_maximum": 0,
                "blockers": ["PREVIEW_PAYLOAD_INVALID"],
            }
        requested = value.get("quantity")
        symbol = value.get("execution_symbol")
        assert isinstance(requested, int) and not isinstance(requested, bool)
        assert symbol in {"MES", "MCL", "M6E"}
        return {"ok": False, "authoritative_maximum": 0, "blockers": list(self.blockers)}

    def shutdown(self) -> None:
        self.arm_state.disarm("APPLICATION_SHUTDOWN")
        self.adapter.close()
