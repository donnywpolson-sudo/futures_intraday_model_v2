# Futures Intraday Model

This repository is the standalone 41-market systematic-futures research
project. It combines immutable GLBX.MDP3 data releases, point-in-time
causality, explicit research gates, and an observation-only Windows live
cockpit.

The project makes no alpha or trading claim. Provider calls, real-history
evaluation, WFA/OOS, prediction materialization, candidate sealing, holdout
access, live smoke, trading, remote push, and destructive operations require
one plain-language Codex confirmation. `api.env` remains ignored and is never
copied into packages, installations, reports, or logs.

## If you use Codex or ChatGPT

Start with:

```text
Read CURRENT_WORKFLOW.md and AGENTS.md, then inspect current repository and Git
state. Complete ordinary local work directly. Pause only for the one
plain-language confirmation required before high-risk work.
```

`CURRENT_WORKFLOW.md` is the workflow authority. This README is setup and
operator orientation; `PROJECT_OUTLINE.md` describes the research pipeline.

## Certified final-evaluation boundary

The declared **Final Sealed 252-Session Holdout** identity `0ff48f99d8b6d3a262ddf0a060bea8e733fc95aa7c4b4d43f19a0f78b107d4d1` is preserved as historical custody evidence, but its exact active manifest and certification bytes are not locally or Git recoverable. Its current authority status is `UNRESOLVED_AUTHORITY_HOLDOUT_ACCESS_FORBIDDEN`; no holdout or forward value access is permitted. The Alpha ladder is rebound only to the last complete tracked authority, and that rollback neither reinterprets prior results nor creates a replacement holdout.

For future counted mechanisms, the prepared ladder successor separates Stage 0
synthetic engineering from the mandatory ES Tier 0 pilot. Tier 1 uses one frozen
pack: NQ, CL, GC, and one pre-result-selected ZN or 6E; no macro is selected yet.
ES/NQ together count as one equity family, while CL, GC, and the selected macro
provide energy, metals, and macro evidence. Tier 2 preserves the balanced 16
markets and Tier 3 preserves the 41-market scientific universe. Every market
result remains visible. Scientific Alpha eligibility is separate from
live-execution eligibility, and both are required before deployment
consideration. Final historical evaluation remains blocked until Final-252 is
separately reconstructed, certified, and activated. No new mechanism has
started.

The canonical source and clean-restart baseline are complete. The
development-only causal-observation contract is frozen, and its observation-only
safety implementation is built, validated, committed, and remotely recoverable.
The seven-market development canary ran once, passed independent verification,
and remains unpublished and inactive. The new 41-market causal observation
release is not built. Later full-build attempts remained inactive and failed
closed and remain evidence only. The non-active V10 successor performs compact
Parquet/Zstandard normalization and two-pass independent certification one
market at a time, with immutable market and sealed-year checkpoints. It is
synthetic-test proven. Its ES-2025 canary and complete ES 2010-2025 checkpoint
and two-pass maximum-robustness certification passed under consumed authority
and remain inactive. The complete GC 2010-2025 checkpoint also passed and
remains inactive; GC maximum-robustness certification has not started. The
other 39 markets have not started. After all 41 markets and the release-wide
certificate pass, a separately approved publication phase will create one annual active Parquet at
`data/active/causally_gated_normalized/{market}/{year}/{year}.parquet` plus its
manifest while preserving the certified V10 monthly evidence. The Final Sealed
252-Session Holdout remains pristine and unread. See
`PROJECT_OUTLINE.md` for the current pipeline and status.

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

Use 64-bit Python 3.11.9 from the repository root. The Windows launcher is used
only to create `.venv`; after that, commands use explicit local executable paths
and do not depend on activation or `PATH`.

```powershell
py -3.11 -m venv .venv
.\.venv\Scripts\python.exe -m pip install --require-hashes -r requirements.sha256.lock
$env:SETUPTOOLS_USE_DISTUTILS = 'stdlib'
.\.venv\Scripts\python.exe -m pip install --no-deps --no-build-isolation -e .
Remove-Item Env:SETUPTOOLS_USE_DISTUTILS
.\.venv\Scripts\python.exe -m pytest -q
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
.\.venv\Scripts\futures-pipeline.exe list
.\.venv\Scripts\futures-pipeline.exe validate-profiles
.\.venv\Scripts\futures-pipeline.exe smoke
```

The default Phase 1A-11 interface is synthetic mechanics only and has no
provider, real-history, prediction, candidate, holdout, or order authority.
See `PROJECT_OUTLINE.md` for the current stages, outputs, gates, and status.

## Phase 8 economics

Phase 8 uses the protected 41-market economics rulebook and a passing immutable
Databento actual-contract audit. The audit verifies point-in-time definitions,
tick math, and continuous-contract roll boundaries; it does not calculate
returns across a roll. CME documents are retained as historic/reference
evidence and are needed only to resolve an audit conflict or unresolved
economics signature.

Future research preparation uses provider-neutral profiles in
[`configs/prop_firm_profiles.json`](configs/prop_firm_profiles.json). The
active immutable profile is `mff_rapid_eod_50k_2026_08_10` with explicit
`sim_funded` stage, zero-based ledger, micro-only execution, and a portfolio-wide
30-micro-equivalent cap. Historic Phase 8 configs and prior-provider policy
artifacts remain unchanged where immutable evidence binds their hashes. See
[`configs/prop_firm_phase8_evaluation.json`](configs/prop_firm_phase8_evaluation.json)
and `src/futures_rebuild/prop_firm_phase8.py` for current provider-neutral
Phase 8 preparation. See
[`docs/PROP_FIRM_RISK.md`](docs/PROP_FIRM_RISK.md) and
[`docs/NAMING_AND_LINEAGE.md`](docs/NAMING_AND_LINEAGE.md).
Runtime sizing resolves micro metadata, provisional-or-verified economics, open
and working stop risk, and every cap from the selected hash-bound bindings.
Session IDs are derived from verified provider-calendar records, and funded
drawdown/payout state has deterministic restart serialization. The current MFF
platform and official fees remain unresolved, so production/live readiness is
false and the named research stress-cost profile cannot be treated as verified.

The public prepare-only interfaces are:

```powershell
.\.venv\Scripts\python.exe -m futures_rebuild.pipeline prop-firm-risk-policy
.\.venv\Scripts\python.exe -m futures_rebuild.pipeline prop-firm-phase8
```

## Daily use

Use the pinned executables and inspect current state before work:

```powershell
git status --short --untracked-files=all
.\.venv\Scripts\futures-pipeline.exe validate-profiles
```

Before reviewing a proposed commit, run the affected targeted tests, then:

```powershell
.\.venv\Scripts\python.exe -m pytest
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
scheduling. Publishing the schema-7 historical-observability foundation is a
high-risk action and needs one plain-language Codex confirmation.

## Retirement audit

```powershell
.\.venv\Scripts\futures-retirement-audit.exe
```

`futures-retirement-audit` proves standalone closure without opening an external
repository.

## Futures Live Cockpit

```powershell
.\.venv\Scripts\futures-live-cockpit.exe --self-check
.\.venv\Scripts\futures-live-cockpit.exe --demo
.\.venv\Scripts\futures-high-risk-prepare.exe --operation cockpit-live-smoke --scope duration_seconds=120 --output reports/live_cockpit/bounded_live_smoke_result.json
```

The packaged application is published directly to `FuturesLiveCockpit/`. Its
only top-level items are `FuturesLiveCockpit.exe` and `_internal/`, so the
package remains credential-free. When launched from this canonical checkout,
the executable resolves the exact parent repository `api.env` without copying
it. Prepared Local AppData versions continue to use only a non-secret locator
back to the same ignored v2-local `api.env`.

The cockpit provides search/grouping, live charts, history controls, bounded
cache, persisted preferences, explicit provider errors, prediction abstention,
and all 41 approved markets. It now also displays a gated MFF/Tradovate
execution-capability panel and a manual order-preparation assistant. MFF
Evaluation and Rapid EOD Sim Funded are `MANUAL_ONLY`; no Tradovate client,
credential lookup, or provider account binding is used. The operator reconciles
a local snapshot, prepares and copies instructions, manually enters the order,
then reports submission, fills, protection, rejection/cancellation, exit, and
reconciliation. Every restart makes prior manual state stale. Provider API
readiness, automatic authorization, production readiness, and account binding
remain false/unset. The deterministic local simulator is synthetic-only.

The 41-market overview remains live, while disk history is intentionally
limited to the five quick-chart markets `ES`, `CL`, `ZN`, `6E`, and `NQ`.
Those markets have one-click switching on every chart interval and support
`1W`, `2W`, `1M`, and `3M` ranges. Current-contract rows retain eight days;
continuous quick-chart rows retain at most 95 days, and the shared SQLite cache
is capped at 1,000,000 bars. Extended-range downloads always stop at the
existing provider estimate for manual review; the automatic-small-repair policy
applies only to the one-week quick-market cache.

**FuturesLiveCockpit does not transmit orders for MFF simulated accounts.**

See [`docs/MFF_TRADOVATE_EXECUTION.md`](docs/MFF_TRADOVATE_EXECUTION.md) for
execution modes, official-source distinctions, credential storage, account
binding, reconciliation, emergency behavior, blockers, self-check, packaging,
and the separate authorization required for future provider smokes.

Do not run a provider-backed smoke or replace shortcuts without one
plain-language confirmation. High-risk CLIs prepare a scope summary only;
Codex performs the approved task while preserving the existing installation and
shortcuts for rollback.

## Project controls

- `CURRENT_WORKFLOW.md`: the single day-to-day workflow guide.
- `AGENTS.md`: durable safety and research-integrity rules.
- `PROJECT_OUTLINE.md`: Phase 1A-11 research runbook.
- `configs/source_contract.json`: accepted source boundary.
- `manifests/**`: immutable release and approval evidence.
- `docs/LEGACY_WORKFLOWS.md`: retained historic plans, approval artifacts, and
  successor code that must not be used as new workflow instructions.

All authoritative runtime imports, configuration, data discovery, credentials,
and installed cockpit components are v2-owned. Other repositories are not
runtime dependencies.

Local data payloads, reports, runtime state, logs, packages, caches, virtual
environments, credentials, and installations remain outside Git unless a
specific small durable manifest or report is explicitly approved for tracking.

For normal work and high-risk boundaries, follow
[`CURRENT_WORKFLOW.md`](CURRENT_WORKFLOW.md). This README does not define a
second workflow. Historic workflow material remains evidence only.
