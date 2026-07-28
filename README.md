# Futures Intraday Model

This repository is the standalone 41-market systematic-futures research
project. It combines immutable GLBX.MDP3 data releases, point-in-time
causality, explicit research gates, and an observation-only Windows live
cockpit.

The project makes no alpha or trading claim. Provider calls, real-history
evaluation, WFA/OOS, prediction materialization, candidate sealing, holdout
access, live smoke, trading, remote push, and destructive operations require
separate exact approvals.

## If you use Codex or ChatGPT

Start with:

```text
Read AGENTS.md, PROJECT_OUTLINE.md, and CODEX_HANDOFF.md. Reconcile the handoff
against the current repository and Git state. Tell me the safest next bounded
step. Do not run provider calls, broad data builds, real-history research,
prediction writes, holdout access, live smoke, trading, commits, or pushes
unless the exact action is already authorized.
```

`PROJECT_OUTLINE.md` is the authoritative steady-state workflow. This README is
setup and operator orientation; `CODEX_HANDOFF.md` is only current continuation
state.

## Repository map

- `src/futures_rebuild/`: v2-native pipeline, immutable-release, audit, and
  cockpit implementation.
- `configs/`: universe, profile, source, session, economics, audit, and
  packaging contracts.
- `manifests/`: durable release, approval, migration, and provenance evidence.
- `data/`: ignored immutable source/releases and generated data, admitted only
  through verified manifests.
- `state/`: ignored run checkpoints, leases, ledgers, and trial declarations.
- `tests/`: synthetic mechanics, contract, failure-path, and packaging checks.
- `scripts/`: bounded build and installation wrappers.

## Setup

Use 64-bit Python 3.11.9 from the repository root.

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --require-hashes -r requirements.sha256.lock
$env:SETUPTOOLS_USE_DISTUTILS = 'stdlib'
python -m pip install --no-deps --no-build-isolation -e .
Remove-Item Env:SETUPTOOLS_USE_DISTUTILS
python -m pytest -q
```

`requirements.sha256.lock` is the complete exact runtime, test, provider,
cockpit, and packaging environment. `requirements.lock` is its readable
name/version companion. The temporary setuptools mode matches the packaged build
script's Windows bootstrap and prevents the Python 3.11 stdlib-distutils import
order from breaking the local editable install.

On a new computer, clone the repository, enter its root, perform the setup
above, restore approved immutable data releases through their documented
copy/verification process, and create a new local credential file. Do not copy
an environment directory, installed cockpit, cache, or secret through Git.

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

## Daily use

Activate the pinned environment and inspect current state before work:

```powershell
.\.venv\Scripts\Activate.ps1
git status --short --untracked-files=all
futures-pipeline validate-profiles
```

Before reviewing a proposed commit, run the affected targeted tests, then:

```powershell
python -m pytest
git diff --check
```

Do not stage `api.env`, immutable data payloads, runtime checkpoints, build
output, or installed cockpit files.

## Historical observability and current calendar

Historical research is based on rows actually decoded from the accepted,
immutable Databento DBN release. Missing time stays missing; the pipeline does
not fill gaps, synthesize opens or closes, or interpret no rows as a closed
exchange. Pre-2025 intervals are classified
`CAUSAL_PRICE_ONLY_EMPIRICAL_OBSERVABILITY`; status-era intervals may be
`CAUSAL_PRICE_PLUS_STATUS_GATED_EMPIRICAL_OBSERVABILITY`.

This is deliberately not an official historical CME session calendar. The
activated CME calendar remains authoritative for current/forward cockpit
scheduling. Publishing the schema-7 historical-observability foundation still
requires its own exact hash-bound approval.

## Master audits

Root `MASTER_AUDIT.md` is the canonical non-authorizing state audit. Run it with
a frozen invocation:

```powershell
futures-master-audit --invocation configs/master_audit_v3/invocation.example.json
.\.venv\Scripts\python.exe -m pytest -q --junitxml=.pytest_tmp/full-suite.xml
futures-meta-audit --junitxml .pytest_tmp/full-suite.xml --suite-evidence-output .pytest_tmp/full-suite-evidence.json
futures-retirement-audit
```

The example intentionally fails closed because it omits the exact universe
approval-receipt evidence and required subcheck results.
`META_MASTER_AUDIT.md` defines the independent blind-first review of the Master
Audit itself. `futures-meta-audit` checks its frozen threat coverage against the
Master, stage matrix, executable test nodes, and an exact full-suite receipt.
`futures-retirement-audit` proves standalone closure without opening an external
repository.

## Futures Live Cockpit

```powershell
futures-live-cockpit --self-check
futures-live-cockpit --demo
futures-live-cockpit --live-smoke --approval <approved-receipt.json> --result-output reports/live_cockpit/bounded_live_smoke_result_attempt_2.json
powershell -NoProfile -File scripts/build_live_cockpit.ps1
powershell -NoProfile -File scripts/install_live_cockpit.ps1 -Upgrade -WhatIf
powershell -NoProfile -File scripts/activate_live_cockpit.ps1 -PreparedInstallPath <prepared-version> -LiveSmokeResult reports/live_cockpit/bounded_live_smoke_result_attempt_2.json -WhatIf
```

The packaged application is published directly to `FuturesLiveCockpit/`. Its
only top-level items are `FuturesLiveCockpit.exe` and `_internal/`.
This directory is installation input, not the authenticated launch target, and
intentionally contains no credential locator. After an approved preparation
and activation, start normal authenticated use through the Desktop or Start
Menu shortcut; the installed version contains only a non-secret locator back
to the ignored v2-local `api.env`.

The cockpit provides search/grouping, live charts, history controls, bounded
cache, persisted preferences, explicit provider errors, prediction abstention,
and all 41 approved markets. It is observation-only: it contains no broker,
order, or trading-control interface and creates no auto-start entry.

Do not run a provider-backed smoke or replace shortcuts without its separate
exact approval. The installer prepares a new isolated version, verifies its
packaged self-check, records rollback metadata, and leaves both existing
shortcuts unchanged. The activation script accepts only a create-only passing
smoke result bound to that exact frozen executable; it restores and verifies
the prior shortcuts if cutover fails.

## Project controls

- `AGENTS.md`: durable policy and authorization boundaries.
- `PROJECT_OUTLINE.md`: authoritative Phase 1A-11 runbook.
- `CODEX_HANDOFF.md`: current continuation state.
- `MASTER_AUDIT.md` / `META_MASTER_AUDIT.md`: state and audit-quality controls.
- `configs/source_contract.json`: accepted source boundary.
- `manifests/**`: immutable release and approval evidence.

All authoritative runtime imports, configuration, data discovery, credentials,
and installed cockpit components are v2-owned. Other repositories are not
runtime dependencies.

Local data payloads, reports, runtime state, logs, packages, caches, virtual
environments, credentials, and installations remain outside Git unless a
specific small durable manifest or report is explicitly approved for tracking.
