# Futures intraday research project

## Objective

Operate a reproducible, point-in-time, bias-resistant research system over the
approved 41-market GLBX.MDP3 universe, with immutable data releases, explicit
trial accounting, chronological validation, net economics, locked
holdout/forward cohorts, and an observation-only live cockpit. Automatic order
execution is outside this project's scope.

## Source-of-truth roles

- `AGENTS.md`: durable project policy and approval boundaries.
- `PROJECT_OUTLINE.md`: authoritative workflow, commands, gates, outputs, and
  stop conditions.
- `CODEX_HANDOFF.md`: current multi-step continuation state.
- `README.md`: installation and operator orientation.
- `MASTER_AUDIT.md` and `META_MASTER_AUDIT.md`: canonical project-state and
  audit-quality specifications.
- `configs/research_universe_contract.json`: canonical markets, cohorts,
  admission, and approval receipt.
- `configs/alpha_tiered.yaml`: operational profile view.
- `configs/source_contract.json`: accepted immutable source-family boundary.
- `configs/*.json` and `configs/*.yaml`: sessions, identity, costs, coverage,
  pipeline, audit, and packaging contracts.
- `manifests/**`: immutable release, approval, selection, and provenance
  metadata.
- `state/trial_registry/**`: pre-outcome trial declarations and attempt
  genealogy.

## Research discipline

- Establish source integrity, provenance, timestamp availability, actual
  contract identity, sessions, rolls, and economics before interpreting a
  model result.
- Never change targets, features, thresholds, markets, costs, or evaluation
  rules because a locked or observed result looks favorable.
- Preserve negative results, warnings, exclusions, failed attempts, and stopped
  branches in the trial genealogy.
- Compare complex candidates against simple causal baselines under the same
  split, cost, position, and risk rules.
- Synthetic runs verify mechanics only. They do not establish alpha, historical
  readiness, executable economics, or permission to access protected data.

## Data manifest and rule index

This section is the first lookup point; it does not duplicate contract
authority.

- `configs/source_contract.json` declares the only source families and roots
  that may be discovered.
- `configs/research_universe_contract.json` controls point-in-time market
  admission, cohorts, and eligibility.
- `configs/alpha_tiered.yaml` is a validated profile view of that universe.
- Foundation coverage, status-scope, session, identity, and economics contracts
  define acceptance for the causal foundation.
- `configs/historical_observability_policy.json` classifies historical
  foundation rows as immutable DBN observability. It does not establish
  official historical CME open, close, halt, pause, or holiday states.
- `configs/exchange_calendar_policy.json` reserves the activated official CME
  calendar for current/forward cockpit scheduling.
- `manifests/data_releases/**` contains content-addressed release descriptors;
  accepted payloads live in v2-owned immutable release roots.
- `state/trial_registry/**` declares every real-data attempt before outcomes are
  read. Run checkpoints and leases are recovery state, not readiness proof.
- `MASTER_AUDIT.md` and `configs/master_audit_v3/**` define evidence
  classification; they grant no execution authority.

## Active layout

```text
configs/                         durable contracts and operational profiles
data/vault/                      immutable v2-owned source snapshots/releases
data/dbn/                        accepted DBN source-family view
data/raw/                        Phase 1B immutable raw releases
data/causally_gated_normalized/  Phase 2 causal/session-normalized releases
data/outcome_sources/            outcome-capability inputs kept from features
data/outcomes/                   Phase 3 outcome releases
data/features/                   Phase 4 feature releases
data/predictions/                separately authorized Phase 6 OOS releases
data/evaluations/                separately authorized Phase 7-9 evidence
manifests/                       release, approval, and provenance metadata
src/futures_rebuild/             v2-native implementation
state/                           checkpoints, leases, ledgers, trial registry
tests/                           contract, synthetic, failure, and package tests
```

Staging, repair, cache, report, and recovery roots are evidence or working
state, never active inputs unless a content-addressed release and its governing
contract explicitly admit them. Other repositories and absolute external paths
are not active roots.

## Profile ladder

Profiles are defined by `configs/alpha_tiered.yaml` and checked against the
canonical universe.

- `tier_0`: ES engineering smoke only; never alpha evidence.
- `tier_1_research`: core discovery/replication profile. Approved cohort rules
  determine selection eligibility.
- `tier_1_holdout` and `tier_1_forward`: locked core validation.
- `tier_2_research`: broader balanced-market replication.
- `tier_2_holdout` and `tier_2_forward`: locked balanced validation.
- `tier_3_research`: all 41 markets. Report the 38 traditional markets
  separately from BTC, ETH, and PA; satellite/frontier results cannot rescue
  traditional-universe failure.
- `tier_3_holdout` and `tier_3_forward`: locked full-universe validation.
- `all_raw`: source inventory only, never research evidence.

Profiles can narrow but cannot silently expand the universe, change admission
or selection eligibility, or unlock holdout/forward data.

## Phase 1A-11 workflow

| Phase | Purpose | V2 interface | Main output |
| --- | --- | --- | --- |
| 1A | Preflight exact provider requests; ingest and verify immutable DBN/sidecar pairs | `futures-pipeline phase1a` | DBN release manifests and acquisition evidence |
| 1B | Convert accepted DBNs and independently reconcile rows, schemas, definitions, hashes, and sidecars | `futures-pipeline phase1b` | immutable raw releases and ingest reports |
| 2 | Build point-in-time causal, trade-date-grouped, actual-contract data from observed DBN rows | `futures-pipeline phase2` | empirical-observability causal foundation releases |
| 3 | Build outcomes with explicit entry lag, horizon, maturity, and unresolved states | `futures-pipeline phase3` | separate labeled/outcome-source releases |
| 4 | Build leakage-audited causal feature matrices without outcome access | `futures-pipeline phase4` | immutable feature releases |
| 5 | Freeze nested chronological split plans with purge and embargo | `futures-pipeline phase5` | split-plan manifests |
| 6 | Run separately approved WFA builders and materialize OOS predictions | `futures-pipeline phase6` | sealed prediction releases |
| 7 | Audit saved prediction identity, coverage, abstention, and signal quality | `futures-pipeline phase7` | prediction-audit reports |
| 8 | Evaluate net economics, baselines, portfolio/risk, and promotion eligibility | `futures-pipeline phase8` | model-selection and risk reports |
| 9 | Run bounded registered robustness, negative-control, and statistical-validity tests | `futures-pipeline phase9` | research-audit reports |
| 10 | Seal an explicitly approved candidate and its complete serving bundle | `futures-pipeline phase10` | immutable candidate bundle/receipt |
| 11 | Guard one authorized locked-holdout or forward evaluation using only the sealed bundle | `futures-pipeline phase11` | guarded evaluation evidence |

The public CLI defaults to generated synthetic mechanics. Synthetic mode
executes the complete dependency order while retaining zero provider, alpha,
prediction, sealing, holdout, and order authority. Any production adapter must
check the corresponding exact receipt before reading protected data or writing
an authoritative artifact.

## Non-negotiable data rules

- Raw DBNs and accepted release bytes are immutable. Corrections publish a new
  release; they never overwrite an accepted one.
- Every file is bound by canonical path, byte count, SHA-256, schema,
  provenance, query semantics, and release identity before use.
- Actual instrument identity and point-in-time definition lineage are
  authoritative. Continuous symbols are selection references, never executable
  contract identity.
- Phase 1B preserves source event semantics. Phase 2 is the first phase allowed
  to apply causal session normalization, explicit missing/degraded states, and
  trainability gates.
- Historical Phase 2 admits actual decoded DBN rows only. It performs no gap
  filling, interpolation, synthetic open/close generation, or inference that
  unobserved time was closed. Its trade-date rollover is grouping logic, not
  official historical exchange-hours authority.
- The activated official CME calendar governs current/forward cockpit
  scheduling only and is not retrofitted onto historical research releases.
- Missing, sparse, degraded, unresolved, and partially decoded states remain in
  coverage denominators. They cannot become trainable through a global waiver.
- Feature builders may not discover or read outcome, label, prediction, or
  evaluation releases. Generated data from another repository is never an
  authoritative input.
- Every research artifact must trace to exact source, universe, config, trial,
  implementation, and upstream release identities.

## Label, feature, and split rules

- Labels declare decision time, entry lag, horizon, maturity, unresolved-state
  behavior, and the exact outcome-source capability they consume.
- Features use only information available by their decision timestamp; as-of
  joins, update frequency, lookback, warmup, missingness, and drift policy are
  explicit.
- Chronological splits are nested and never shuffled. Purge and embargo cover
  overlapping label horizons; all fitted transforms use training rows only.
- Holdout and forward cohorts cannot drive feature, target, market, cost,
  threshold, model, or policy selection.

## Runnable commands

From the repository root, use explicit executables from the pinned Python
3.11.9 environment without depending on activation or `PATH`:

```powershell
.\.venv\Scripts\futures-pipeline.exe list
.\.venv\Scripts\futures-pipeline.exe validate-profiles
.\.venv\Scripts\futures-pipeline.exe --output reports/pipeline_audit/synthetic-phase1a-11.json smoke
.\.venv\Scripts\futures-pipeline.exe phase1a
.\.venv\Scripts\futures-pipeline.exe phase1b
.\.venv\Scripts\futures-pipeline.exe phase2
.\.venv\Scripts\futures-pipeline.exe phase3
.\.venv\Scripts\futures-pipeline.exe phase4
.\.venv\Scripts\futures-pipeline.exe phase5
.\.venv\Scripts\futures-pipeline.exe phase6
.\.venv\Scripts\futures-pipeline.exe phase7
.\.venv\Scripts\futures-pipeline.exe phase8
.\.venv\Scripts\futures-pipeline.exe phase9
.\.venv\Scripts\futures-pipeline.exe phase10
.\.venv\Scripts\futures-pipeline.exe phase11
```

The global options precede the subcommand when using the module directly:

```powershell
.\.venv\Scripts\python.exe -m futures_rebuild.pipeline --output reports/pipeline_audit/smoke.json smoke
```

Outputs are create-only. Choose a new path for each run.

## Audit commands

```powershell
.\.venv\Scripts\futures-master-audit.exe --invocation <frozen-invocation.json>
.\.venv\Scripts\python.exe -m pytest -q --junitxml=.pytest_tmp/full-suite.xml
.\.venv\Scripts\futures-meta-audit.exe --junitxml .pytest_tmp/full-suite.xml --suite-evidence-output .pytest_tmp/full-suite-evidence.json
.\.venv\Scripts\futures-retirement-audit.exe
```

The Master Audit classifies one exact target without granting authority. The
Meta Audit checks its independently derived threat registry, Master coverage,
stage mappings, executable test nodes, and full-suite receipt. The retirement
audit verifies standalone closure without resolving or opening an external
repository.

## Cockpit workflow

```powershell
.\.venv\Scripts\futures-live-cockpit.exe --self-check
.\.venv\Scripts\futures-live-cockpit.exe --demo
.\.venv\Scripts\futures-live-cockpit.exe --live-smoke --approval <approved-receipt.json> --result-output reports/live_cockpit/bounded_live_smoke_result_attempt_2.json
powershell -NoProfile -File scripts/build_live_cockpit.ps1
powershell -NoProfile -File scripts/install_live_cockpit.ps1 -Upgrade -WhatIf
powershell -NoProfile -File scripts/activate_live_cockpit.ps1 -PreparedInstallPath <prepared-version> -LiveSmokeResult reports/live_cockpit/bounded_live_smoke_result_attempt_2.json -WhatIf
```

Packaging publishes `FuturesLiveCockpit/` with exactly
`FuturesLiveCockpit.exe` and `_internal/` at its top level.

The normal UI is observation-only and may read live GLBX.MDP3 data through the
v2-local credential locator. A provider-backed smoke requires its exact durable
approval. Preparation installs and self-checks an isolated version without
changing shortcuts. Shortcut activation follows only after dependency, package,
self-check, demo, all-market, and exact package-bound approved live-smoke
evidence pass.

## Approval gates

Separate approvals are required for:

1. a provider request or download, bound to provider, dataset, symbols, dates,
   schema, request count, cost ceiling, and destinations;
2. copy migration, bound to source/destination mapping hashes, bytes, parent
   release, exclusions, and rollback;
3. each real-history trial or WFA/OOS program, after an immutable trial
   declaration;
4. prediction materialization;
5. candidate sealing;
6. holdout or forward access;
7. bounded provider-backed cockpit smoke;
8. paper, shadow, or live trading and every order path;
9. remote push; and
10. destructive deletion or cutover.

Approval for one class never authorizes another.

## Evaluation and model-trust standard

Before a model-trust, promotion, or sealing claim, require:

- exact raw/foundation coverage, causal identity, sessions, rolls, labels,
  features, and split evidence;
- an immutable pre-outcome trial declaration covering targets, features,
  models, seeds, hyperparameter budget, thresholds, costs, sizing, metrics,
  multiplicity, stopped branches, and finite stop rules;
- flat/no-trade, cost-only, simple trend, simple mean-reversion, and relevant
  causal regime/liquidity baselines under identical evaluation rules;
- gross-to-net PnL conservation, fees, spread/slippage/delay, carry/roll,
  capacity, margin/liquidation, concentration, shared liquidity, and portfolio
  interaction;
- dependence-aware uncertainty, effective independent breadth, temporal and
  parameter stability, negative controls, multiple-testing adjustment, and
  traditional-versus-satellite reporting;
- Phase 7 prediction integrity, Phase 8 economics/portfolio/risk review, and
  Phase 9 statistical/adversarial evidence with all blockers preserved.

Passing a metric, a broad row count, or a favorable satellite result cannot
substitute for these gates.

## Bounded execution policy

Any provider request, broad build, data mutation, real-history evaluation,
WFA/model operation, prediction write, candidate/holdout action, package
installation, live smoke, shortcut change, or destructive operation requires a
plan that binds the exact command family, immutable inputs, approval receipt,
scope ceilings, duration, outputs/logs, forbidden actions, rollback, and stop
condition. If any binding is missing or stale, do not start.

## Acceptance standards

- Every accepted market-year is exact-schema, hash, provenance, session,
  identity, and source-availability verified.
- Unknown/missing states remain in coverage denominators and are ineligible.
- Features, outcomes, predictions, and evaluation are separate immutable
  capabilities and releases.
- Every real-data attempt has a pre-outcome registry record and finite stop rule.
- Costs, dependence, market/family concentration, traditional/satellite
  separation, baselines, negative controls, and portfolio risk are explicit.
- Holdout and forward cohorts remain physically and procedurally locked.
- The cockpit exposes exactly the approved 41 markets, has no order path, keeps
  secrets outside Git/packages/installations, handles failures visibly, bounds
  cache/state, creates no autostart, and has verified shortcut rollback.
- The project works with external repositories unavailable.
- `FOUNDATION_READY`, `HISTORICAL_RESEARCH_READY`, and
  `OBSERVATION_COCKPIT_READY` each require a `SUPPORTABLE` Master Audit result.
- Meta Audit closure requires no unresolved Critical/High or P0/P1 deficiency.

## Reporting standard

Label material claims as `Verified`, `Inferred`, `Assumed`, or
`Not established`. Each report names its exact scope, evidence paths and hashes,
config/trial identity, command, result, limitations, stale-risk, and next gate.
Never present gross-only, synthetic, warning, failed, inferred, or incomplete
evidence as alpha, promotion, holdout, cockpit, paper, or live readiness.

## Stop conditions

Stop before the boundary when an approval is missing, a hash or schema is stale,
an input is incomplete or ambiguous, an immutable destination exists, a profile
drifts, a secret may be exposed, a real trial is unregistered, a holdout could
be disclosed, an order path is reachable, or rollback cannot be proven. Report
the exact rejected item and smallest missing approval or input.
