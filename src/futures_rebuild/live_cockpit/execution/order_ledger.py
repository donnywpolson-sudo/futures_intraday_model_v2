"""Crash-safe, append-only execution intent and lifecycle journal."""

from __future__ import annotations

from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import threading
from typing import Any, Mapping

from .errors import UnknownBrokerState


SCHEMA = "live_cockpit_execution_event/1.0.0"
ZERO_HASH = "0" * 64
FORBIDDEN_KEY_SUFFIXES = frozenset({"token", "password", "secret", "authorization", "apikey"})


def _canonical(value: object) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True)


def _secret_key(value: object) -> bool:
    normalized = "".join(character for character in str(value).lower() if character.isalnum())
    return any(normalized.endswith(marker) for marker in FORBIDDEN_KEY_SUFFIXES)


def _contains_secret_key(value: object) -> bool:
    if isinstance(value, Mapping):
        return any(_secret_key(key) or _contains_secret_key(item) for key, item in value.items())
    if isinstance(value, (list, tuple)):
        return any(_contains_secret_key(item) for item in value)
    return False


class OrderLedger:
    def __init__(self, path: Path) -> None:
        self.path = path
        self._lock = threading.RLock()

    def read(self) -> list[dict[str, Any]]:
        try:
            lines = self.path.read_text(encoding="utf-8").splitlines()
        except FileNotFoundError:
            return []
        previous = ZERO_HASH
        events: list[dict[str, Any]] = []
        for line in lines:
            try:
                event = json.loads(line)
            except json.JSONDecodeError as exc:
                raise UnknownBrokerState("execution journal contains an incomplete event") from exc
            if not isinstance(event, dict) or event.get("schema_version") != SCHEMA:
                raise UnknownBrokerState("execution journal schema is invalid")
            observed_hash = event.get("event_hash")
            core = {key: value for key, value in event.items() if key != "event_hash"}
            expected = hashlib.sha256((_canonical(core) + "\n").encode("utf-8")).hexdigest()
            if core.get("previous_hash") != previous or observed_hash != expected:
                raise UnknownBrokerState("execution journal hash chain is invalid")
            previous = str(observed_hash)
            events.append(event)
        return events

    def append(self, *, event_type: str, payload: Mapping[str, object], observed_at: datetime | None = None) -> dict[str, Any]:
        if not event_type or len(event_type) > 80:
            raise ValueError("execution event type must be bounded")
        if _contains_secret_key(payload):
            raise ValueError("execution journal payload contains a forbidden secret key")
        instant = datetime.now(timezone.utc) if observed_at is None else observed_at
        if instant.tzinfo is None or instant.utcoffset() is None:
            raise ValueError("execution event timestamp must be timezone-aware")
        with self._lock:
            events = self.read()
            previous = events[-1]["event_hash"] if events else ZERO_HASH
            core: dict[str, Any] = {
                "schema_version": SCHEMA,
                "sequence": len(events) + 1,
                "observed_at": instant.isoformat(),
                "event_type": event_type,
                "previous_hash": previous,
                "payload": dict(payload),
            }
            event = {**core, "event_hash": hashlib.sha256((_canonical(core) + "\n").encode("utf-8")).hexdigest()}
            self.path.parent.mkdir(parents=True, exist_ok=True)
            with self.path.open("a", encoding="utf-8", newline="\n") as stream:
                stream.write(_canonical(event) + "\n")
                stream.flush()
                os.fsync(stream.fileno())
            return event

    def intent_provider_order(self, intent_id: str) -> int | None:
        matches = [event for event in self.read() if event["event_type"] == "INTENT_PROVIDER_ORDER_BOUND" and event["payload"].get("intent_id") == intent_id]
        if len(matches) > 1:
            raise UnknownBrokerState("one intent maps to multiple provider orders")
        return int(matches[0]["payload"]["provider_order_id"]) if matches else None

    def bind_intent(self, *, intent_id: str, provider_order_id: int) -> dict[str, Any]:
        if self.intent_provider_order(intent_id) is not None:
            raise UnknownBrokerState("intent already has a provider order")
        return self.append(event_type="INTENT_PROVIDER_ORDER_BOUND", payload={"intent_id": intent_id, "provider_order_id": provider_order_id})

    def uncertain_intents(self) -> set[str]:
        uncertain: set[str] = set()
        resolved: set[str] = set()
        for event in self.read():
            intent_id = event["payload"].get("intent_id")
            if not isinstance(intent_id, str):
                continue
            if event["event_type"] == "ORDER_OUTCOME_UNKNOWN":
                uncertain.add(intent_id)
            elif event["event_type"] in {"INTENT_PROVIDER_ORDER_BOUND", "ORDER_REJECTED"}:
                resolved.add(intent_id)
        return uncertain - resolved
