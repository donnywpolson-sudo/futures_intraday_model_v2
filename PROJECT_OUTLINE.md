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
| 2 | Build point-in-time causal, session-normalized, actual-contract data | `futures-pipeline phase2` | causal foundation releases |
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

## Runnable commands

From the repository root in the pinned Python 3.11.9 environment:

```powershell
futures-pipeline list
futures-pipeline validate-profiles
futures-pipeline --output reports/pipeline_audit/synthetic-phase1a-11.json smoke
futures-pipeline phase1a
futures-pipeline phase1b
futures-pipeline phase2
futures-pipeline phase3
futures-pipeline phase4
futures-pipeline phase5
futures-pipeline phase6
futures-pipeline phase7
futures-pipeline phase8
futures-pipeline phase9
futures-pipeline phase10
futures-pipeline phase11
```

The global options precede the subcommand when using the module directly:

```powershell
python -m futures_rebuild.pipeline --output reports/pipeline_audit/smoke.json smoke
```

Outputs are create-only. Choose a new path for each run.

## Audit commands

```powershell
futures-master-audit --invocation <frozen-invocation.json>
.\.venv\Scripts\python.exe -m pytest -q --junitxml=.pytest_tmp/full-suite.xml
futures-meta-audit --junitxml .pytest_tmp/full-suite.xml --suite-evidence-output .pytest_tmp/full-suite-evidence.json
futures-retirement-audit
```

The Master Audit classifies one exact target without granting authority. The
Meta Audit checks its independently derived threat registry, Master coverage,
stage mappings, executable test nodes, and full-suite receipt. The retirement
audit verifies standalone closure without resolving or opening an external
repository.

## Cockpit workflow

```powershell
futures-live-cockpit --self-check
futures-live-cockpit --demo
futures-live-cockpit --live-smoke --approval <approved-receipt.json>
powershell -NoProfile -File scripts/build_live_cockpit.ps1
powershell -NoProfile -File scripts/install_live_cockpit.ps1 -Upgrade -WhatIf
```

The normal UI is observation-only and may read live GLBX.MDP3 data through the
v2-local credential locator. A provider-backed smoke requires its exact durable
approval. Installation/shortcut cutover follows only after dependency, package,
self-check, demo, all-market, and approved bounded live-smoke evidence pass.

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

## Stop conditions

Stop before the boundary when an approval is missing, a hash or schema is stale,
an input is incomplete or ambiguous, an immutable destination exists, a profile
drifts, a secret may be exposed, a real trial is unregistered, a holdout could
be disclosed, an order path is reachable, or rollback cannot be proven. Report
the exact rejected item and smallest missing approval or input.
