# futures_intraday_model_v2

Standalone 41-market systematic-futures research infrastructure with immutable
GLBX.MDP3 data releases, point-in-time causality, explicit research gates, and
an observation-only Windows live cockpit.

The project makes no alpha or trading claim. Provider calls, real-history
evaluation, WFA/OOS, prediction materialization, candidate sealing, holdout
access, live smoke, trading, remote push, and destructive operations require
separate exact approvals.

## Setup

Use 64-bit Python 3.11.9 from the repository root.

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --require-hashes -r requirements.sha256.lock
python -m pip install --no-deps -e .
python -m pytest -q
```

`requirements.sha256.lock` is the complete exact runtime, test, provider,
cockpit, and packaging environment. `requirements.lock` is its readable
name/version companion.

## Credentials

Create local `api.env` with:

```text
DATABENTO_API_KEY=your-key
```

The file is ignored by Git. Never print, stage, commit, package, archive, or
copy it into an installation. The installer writes only a locator pointing back
to this v2-local file. Environment variable `DATABENTO_API_KEY` may be used
instead.

## Pipeline

The validated profile view is `configs/alpha_tiered.yaml`; the canonical
admission authority is `configs/research_universe_contract.json`.

```powershell
futures-pipeline list
futures-pipeline validate-profiles
futures-pipeline smoke
```

The default Phase 1A-11 interface is synthetic mechanics only and has no
provider, real-history, prediction, candidate, holdout, or order authority.
See `PROJECT_OUTLINE.md` for every phase command, output class, gate, and stop
condition.

## Master audits

Root `MASTER_AUDIT.md` is the canonical non-authorizing state audit. Run it with
a frozen invocation:

```powershell
futures-master-audit --invocation configs/master_audit_v3/invocation.example.json
.\.venv\Scripts\python.exe -m pytest -q --junitxml=.pytest_tmp/full-suite.xml
futures-meta-audit --junitxml .pytest_tmp/full-suite.xml --suite-evidence-output .pytest_tmp/full-suite-evidence.json
futures-retirement-audit
```

The example intentionally fails closed while the checked-in universe approval
is pending or required evidence is absent. `META_MASTER_AUDIT.md` defines the
independent blind-first review of the Master Audit itself. `futures-meta-audit`
checks its frozen threat coverage against the Master, stage matrix, executable
test nodes, and an exact full-suite receipt. `futures-retirement-audit` proves
standalone closure without opening an external repository.

## Futures Live Cockpit

```powershell
futures-live-cockpit --self-check
futures-live-cockpit --demo
futures-live-cockpit --live-smoke --approval <approved-receipt.json>
powershell -NoProfile -File scripts/build_live_cockpit.ps1
powershell -NoProfile -File scripts/install_live_cockpit.ps1 -Upgrade -WhatIf
```

The cockpit provides search/grouping, live charts, history controls, bounded
cache, persisted preferences, explicit provider errors, prediction abstention,
and all 41 approved markets. It is observation-only: it contains no broker,
order, or trading-control interface and creates no auto-start entry.

Do not run a provider-backed smoke or replace shortcuts without its separate
exact approval. The installer preserves the existing version, verifies the
packaged self-check, and restores prior shortcut metadata if cutover fails.

## Project controls

- `AGENTS.md` — durable policy and authorization boundaries.
- `PROJECT_OUTLINE.md` — authoritative Phase 1A-11 runbook.
- `CODEX_HANDOFF.md` — current continuation state.
- `MASTER_AUDIT.md` / `META_MASTER_AUDIT.md` — state and audit-quality controls.
- `configs/source_contract.json` — accepted source boundary.
- `manifests/**` — immutable release and approval evidence.

All authoritative runtime imports, configuration, data discovery, credentials,
and installed cockpit components are v2-owned. Other repositories are not
runtime dependencies.
