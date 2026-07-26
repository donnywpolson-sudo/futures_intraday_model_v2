# Public source snapshot

This repository is a sanitized source snapshot of the operational
`futures_intraday_model_v2` project. It starts with a new, single-commit Git
history so private operational history is not published.

The public snapshot deliberately omits:

- `CODEX_HANDOFF.md` and other mutable continuation state;
- approval and authorization receipts, live-smoke plans, and attempt results;
- generated manifests, reports, runtime state, market data, and build output;
- credentials and ignored local environment files, including `api.env`; and
- personal absolute filesystem paths.

The remaining contracts, source code, tests, and documentation describe the
research mechanics and fail-closed safety model. They do not grant authority
for provider calls, downloads, real-history evaluation, prediction writes,
holdout access, live smoke, trading, or order placement.

Hash-bound operational evidence remains outside this public repository.

## Validation boundary

The public-safe mechanics and documentation checks are:

```powershell
python -m pytest tests\test_data_layout.py tests\live\test_live_cockpit.py tests\live\test_live_cockpit_build_script.py tests\test_operational_documents.py tests\test_retirement.py tests\test_dependency_lock.py tests\test_exchange_calendar.py tests\test_foundation_orchestrator.py tests\test_foundation_successor.py
```

The complete operational suite intentionally fails closed in this snapshot
because its private, hash-bound approvals, manifests, reports, and
machine-specific evidence are absent. A passing public-safe suite is not a
Master Audit, Meta Audit, model-trust result, provider authorization, or
trading-readiness claim.
