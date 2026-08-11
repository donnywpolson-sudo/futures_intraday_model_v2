# Historical public source snapshot record

This document records the historical sanitized public source export identified
below. It does not describe the current operational checkout. Files omitted
from this historical snapshot may legitimately exist in the operational
repository, and this record is not evidence that any named file is absent from
the current checkout.

## Snapshot identity

- Snapshot commit: `e9363688873d90af41c998054d4b219f5e950f0e`
- Snapshot date: `2026-07-25`
- Snapshot type: `sanitized public source export`

## Historical scope

The snapshot identified above was a sanitized public source export of the
operational `futures_intraday_model_v2` project. It began with a new,
single-commit Git history so private operational history was not published.

The snapshot omitted:

- `CODEX_HANDOFF.md` and other mutable continuation state;
- approval and authorization receipts, live-smoke plans, and attempt results;
- generated manifests, reports, runtime state, market data, and build output;
- credentials and ignored local environment files, including `api.env`; and
- personal absolute filesystem paths.

The contracts, source code, tests, and documentation included in that public
export described research mechanics and a fail-closed safety model. Hash-bound
operational evidence remained outside that historical public export. These
historical omission statements do not classify or describe every artifact in
the current operational checkout.

## Historical validation boundary

The following command was the public-safe mechanics and documentation check
for the named historical snapshot. It is retained as historical context and is
not the complete current operational test command.

```powershell
python -m pytest tests\test_data_layout.py tests\live\test_live_cockpit.py tests\live\test_live_cockpit_build_script.py tests\test_operational_documents.py tests\test_retirement.py tests\test_dependency_lock.py tests\test_exchange_calendar.py tests\test_foundation_orchestrator.py tests\test_foundation_successor.py
```

Complete operational tests could fail closed when private, local, or
hash-bound operational evidence was absent from the sanitized snapshot.
Passing the historical public-safe checks was not a Master Audit, Meta Audit,
model-trust result, provider authorization, or trading-readiness claim.

## Current navigation

For the current operational checkout:

- use `CURRENT_WORKFLOW.md` for normal work;
- use `AGENTS.md` for durable safety policy; and
- use `SOURCE_OF_TRUTH.md` for repository navigation.

This historical record is not a current workflow authority.

## Non-authority boundary

This historical record does not authorize provider access, market-data reads,
real-history evaluation, prediction materialization, candidate sealing,
holdout access, publication, installation, activation, live smoke, trading,
order placement, deletion, movement or renaming, staging, commit, or push.
