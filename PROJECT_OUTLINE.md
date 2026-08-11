# Futures intraday research runbook

## Purpose and scope

This runbook describes the current research architecture, phases, outputs, gates,
and stop conditions for a reproducible, point-in-time, bias-resistant systematic
futures research system. It covers two strictly separated Alpha lanes: the
standard/full-contract lane and the integer-micro source lane. It does not make
an Alpha, model-trust, production-readiness, payout-readiness, or trading claim.

This document is not the normal-work workflow authority. It grants no provider
access, market-row read, real-history evaluation, holdout or forward access,
publication, installation, activation, broker or account access, order
placement, trading, staging, commit, push, deletion, cleanup, move, or rename
authority.

## Authority and navigation

The repository uses a two-tier workflow, but procedure and durable policy remain
separate from this research runbook.

- CURRENT_WORKFLOW.md controls normal work and the high-risk confirmation
  procedure.
- `AGENTS.md` contains durable safety and research-integrity policy.
- `configs/repository_surface.json` is the machine-readable path-role
  registry.
- `SOURCE_OF_TRUTH.md` is the generated concise repository-navigation view.
- `README.md` provides setup and operator orientation.
- `PROJECT_OUTLINE.md` describes current research architecture, phases,
  outputs, gates, and stop conditions.
- `PIPELINE_FOLDER_MAP.md` is a topology and reference guide, not workflow
  authority.
- `docs/LEGACY_WORKFLOWS.md` classifies retired workflow surfaces.
- `MASTER_AUDIT.md` and `META_MASTER_AUDIT.md` are audit specifications;
  neither grants operation authority.
- `CODEX_HANDOFF.md` may hold continuation context. Continuation context does
  not grant authority.

The complete pre-reconciliation outline and its historical chronology are
preserved in
`docs/history/PROJECT_OUTLINE_SNAPSHOT_2026-08-11.md`, with provenance in
`docs/history/PROJECT_OUTLINE_SNAPSHOT_2026-08-11.json`. Those records are
historical, exact-byte evidence and do not control current work.

## Current source-of-truth inputs

Currentness comes from declared roles and explicit pointers, not from filenames,
version suffixes, timestamps, or directory presence.

- `configs/research_universe_contract.json` is the canonical market-admission,
  cohort, and eligibility contract.
- `configs/alpha_tiered.yaml` is the operational profile view; it cannot
  silently expand the admission contract.
- `configs/source_contract.json` defines the discoverable source families and
  source-root boundary.
- `configs/active_alpha_research_ladder.json` selects and hash-binds the active
  standard Alpha ladder.
- `data/active/catalog.json` selects the standard-lane research data view.
- `configs/active_micro_alpha_research_ladder.json` selects the accepted
  machine-local micro source lineage when present.
- `data/active/catalogs/apex_micro.json` is that lineage's machine-local source
  catalog when present.
- `pyproject.toml` defines the package and public command surface.

The standard pointer/catalog and the micro pointer/catalog have separate roles
and cannot replace one another. Local-only pointers or catalogs may be absent
from a clean provider-free export; absence there is not silently converted into
authority, evidence, or a passing research gate.

The micro pointer and catalog establish source selection only. They do not by
themselves establish a frozen mechanism, registered trial, historical-row
authority, research passage, holdout authority, production readiness, execution
readiness, or trading authority. Directory presence, including ignored or
untracked material, grants neither research nor execution authority.

## Research invariants

Every current research lane follows these invariants:

- Accepted source releases and provenance are immutable. Corrections create
  immutable successors rather than overwriting accepted bytes.
- Every admitted file binds its canonical path, byte count, hash, schema,
  provenance, query semantics, release identity, and point-in-time
  availability.
- Actual tradable contract identity and time-valid definition lineage govern
  research. Continuous symbols may guide selection but are not executable
  contract identity.
- Missing is missing. Historical processing does not fill gaps, interpolate
  rows, synthesize opens or closes, or infer that unobserved time was closed.
  Missing, sparse, degraded, unresolved, and partially decoded states remain in
  coverage denominators.
- Features, outcomes, predictions, and evaluations are separate immutable
  capabilities. Feature construction cannot discover or read later-stage
  outcome, prediction, or evaluation releases.
- Labels declare decision time, entry lag, horizon, maturity, unresolved-state
  handling, and their exact outcome-source capability. Features use only
  information available at their decision timestamp.
- Validation is chronological and never shuffled. Fitted transforms use
  training rows only; purge and embargo cover overlapping label horizons where
  required.
- A real-data attempt requires immutable preregistration before outcomes are
  read. The declaration fixes targets, features, models, budgets, costs,
  thresholds, metrics, multiplicity handling, and finite stop rules. Consumed
  single-use attempts are not reusable.
- Complex candidates face explicit flat/no-trade, cost-only, trend,
  mean-reversion, and relevant causal baselines under the same split, cost,
  position, and risk rules. Abstention is explicit.
- Economics are net: fees, spread, slippage, delay, roll/carry, capacity,
  margin, liquidation, concentration, shared liquidity, and portfolio
  interaction are reconstructed. A bare `PASS` label never substitutes for
  the underlying gates.
- Dependence-aware uncertainty, effective independent breadth, temporal and
  parameter stability, negative controls, multiple-testing adjustment, and
  traditional-versus-satellite reporting are required before model trust.
- One controlled project-level holdout boundary is shared across contract
  scales. A different lane or smaller contract does not create a second
  holdout claim. Holdout and forward cohorts cannot drive research choices.
- Missing, stale, ambiguous, incomplete, contradictory, or future-known input
  fails closed. Negative, failed, excluded, and stopped results remain evidence.

`MASTER_AUDIT.md` defines the detailed audit evidence and supportability
specification. This runbook does not duplicate it.

## Current research lanes

### Standard/full-contract Alpha lane

The standard lane is selected only through
`configs/active_alpha_research_ladder.json` and its bound registry, with
`data/active/catalog.json` selecting the admitted data view. Catalog or
archive globs cannot replace that selection.

Its ladder progresses from Tier 0 engineering and qualification through
increasingly broad Tier 1, Tier 2, and Tier 3 research, locked validation, one
project-level holdout, and forward monitoring. Tier 3 reports the traditional
market group separately from satellite/frontier markets; favorable satellite
results cannot rescue traditional-universe failure. The mechanism identity
cannot change between gates.

### Micro-source lane

The micro lane uses integer contracts and a separately selected source lineage.
When present, `configs/active_micro_alpha_research_ladder.json` and
`data/active/catalogs/apex_micro.json` identify its accepted machine-local
source selection. Exact local bindings belong to the local-evidence boundary,
not the provider-free current lane.

Micro Tier 0 begins with MES mechanics and qualification. Tier 1 covers the
declared equity, energy, metals, and FX core; later tiers may broaden only under
the governing admission and eligibility contracts. Traditional and crypto
satellite results remain separate. Product-effective dates, provider
availability, contract economics, calendars, and actual instrument identity
must each be established; one cannot stand in for another.

Micro source selection does not grant research or trading authority. No micro
source pointer, catalog, market family, or contract scale grants provider
access, historical-row access, holdout access, execution readiness, or trading
authority.

### Synthetic mechanics lane

`futures-pipeline` is synthetic-only. It exercises phase mechanics and
dependency ordering without establishing Alpha, source readiness, economic
passage, registration, prediction authority, sealing authority, holdout access,
or order authority.

### Certified real-history boundary

`CertifiedResearchGateway` is the only current real-history registration and
economic-execution boundary. It requires an active hash-bound Alpha ladder,
immutable trial identity, exact source/catalog bindings, and passing
source-bound readiness evidence. Preparatory source and readiness censuses are
separate operations: they do not fit, predict, evaluate, register, publish, or
promote a trial.

Retired versioned lifecycle, bracket, direct-foundation, Phase 5-8,
Standard-only, Final, and Authoritative surfaces remain historical or
synthetic-testable only. Their detailed lineage belongs in
`docs/LEGACY_WORKFLOWS.md` and the historical outline snapshot.

## Current phase map

The established Phase 1A-11 taxonomy remains the conceptual research sequence.
The `futures-pipeline` subcommands below are synthetic mechanics entry points;
real-history work must pass through the certified gateway and its separate
authority boundary.

| Phase | Purpose and current entry point | Principal input | Principal output | Gate or stop condition | Authority boundary |
| --- | --- | --- | --- | --- | --- |
| 1A | Preflight and acquisition mechanics through `futures-pipeline phase1a` | Exact source request and admission contracts | Source release manifests and acquisition evidence | Source identity, sidecar, hash, or provenance mismatch | Synthetic execution grants no provider or row-read authority |
| 1B | Decode and reconcile accepted source mechanics through `futures-pipeline phase1b` | Admitted immutable source release | Immutable raw release and reconciliation report | Schema, definition, row, sidecar, or byte mismatch | Real decoding requires separately authorized source access |
| 2 | Construct causal observations through `futures-pipeline phase2` | Actual decoded rows and causal policies | Immutable causal foundation release | Identity, availability, session, missingness, or trainability failure | No synthetic gap filling or retrospective calendar authority |
| 3 | Construct outcomes through `futures-pipeline phase3` | Declared outcome-source capability | Separate immutable outcome release | Entry, horizon, maturity, or unresolved-state failure | Outcomes remain inaccessible to feature construction |
| 4 | Build causal features through `futures-pipeline phase4` | Causal foundation only | Immutable feature release | Availability, lookback, warmup, join, or leakage failure | No outcome, prediction, or evaluation reads |
| 5 | Freeze validation through `futures-pipeline phase5` | Registered trial and eligible feature/outcome identities | Immutable chronological split plan | Temporal order, purge, embargo, or training-only-fit failure | No holdout-driven selection |
| 6 | Materialize OOS predictions through `futures-pipeline phase6` | Frozen mechanism, split plan, approved training inputs | Sealed OOS prediction release | Identity, budget, coverage, or preregistration drift | Real fitting and prediction need gateway authority |
| 7 | Audit predictions through `futures-pipeline phase7` | Saved immutable predictions | Prediction-integrity and signal-quality evidence | Identity, coverage, abstention, or conservation failure | Audit does not grant promotion |
| 8 | Evaluate net economics through `futures-pipeline phase8` | Audited OOS predictions and explicit economics | Model-selection, portfolio, risk, and prop-firm preparation evidence | Baseline, net-cost, breadth, drawdown, concentration, or rule failure | Evaluation does not grant production or trading readiness |
| 9 | Test robustness through `futures-pipeline phase9` | Registered candidate evidence | Research-audit and adversarial evidence | Negative control, multiplicity, stability, or dependence failure | Bounded tests cannot widen the registered trial |
| 10 | Seal a candidate through `futures-pipeline phase10` | Fully passing frozen candidate and evidence graph | Immutable candidate bundle and receipt | Missing gate, identity drift, or destination conflict | Publication and activation remain separate |
| 11 | Guard holdout or forward evaluation through `futures-pipeline phase11` | Exact sealed bundle and one authorized boundary | Guarded evaluation evidence | Missing authority, prior use, drift, or cohort leakage | One controlled access does not authorize production or orders |

Synthetic success at any phase proves mechanics only. It cannot substitute for
certified source, registered real-history evaluation, or a later authority
gate.

## Real-history boundary

Before current real-history registration or economic execution:

1. The active lane, ladder, catalog, calendar, and contract scale must be
   explicit and mutually consistent.
2. The immutable mechanism and preregistration must match the requested trial.
3. Required source-bound readiness certificates and their row, session,
   identity, and provenance evidence must pass.
4. `CertifiedResearchGateway` must derive and enforce the exact trial,
   mechanism, protocol, source, evidence, and single-use receipt identities.
5. Stage-specific economic, baseline, trade-count, breadth, drawdown, subgroup,
   and formal-test gates must be reconstructed from evidence.
6. Holdout and forward boundaries remain sealed until their declared
   prerequisite tier and explicit authority are satisfied.

A directory, report label, prepared census, or favorable synthetic result does
not satisfy these conditions.

## Economics and prop-firm preparation

Provider-neutral profiles live in `configs/prop_firm_profiles.json`.
`configs/prop_firm_phase8_evaluation.json` defines the preparation contract,
and `src/futures_rebuild/prop_firm_phase8.py` owns the current deterministic
Phase 8 preparation logic. The supported prepare-only surfaces are
`futures-pipeline prop-firm-risk-policy` and
`futures-pipeline prop-firm-phase8`.

Preparation is non-authorizing. Active profile and stage are explicit and
hash-bound where required. Research economics, platform or execution mapping,
platform costs, account-stage rules, payout policy, and operational readiness
are separate concerns. Any unresolved required value fails closed. Model or
risk preparation does not establish production, funded-account, payout,
installation, activation, execution, or trading readiness.

## Cockpit and execution boundary

Public command definitions come only from `pyproject.toml`. Source files,
ignored files, generated packages, or untracked execution-looking modules do
not become current or authorized merely because they exist.

The committed cockpit authority is observation-only. Missing, stale, ambiguous,
or invalid inputs produce a visible error or abstention, never a trade.
Credentials stay outside Git, packages, reports, and logs. This runbook does not
authorize broker access, account access, order placement, packaging,
installation, activation, live smoke, or trading.

Any future execution route requires its own current policy, public surface,
credential boundary, immutable readiness evidence, installation and activation
process, rollback boundary, and explicit approval. It cannot be inferred from
directory presence or continuation context.

## Evidence, outputs, and folder roles

Use `SOURCE_OF_TRUTH.md` and `configs/repository_surface.json` for complete
path classification. Major roles are:

- `configs/`: contracts, policies, profiles, and machine-readable pointers.
- `data/`: protected source, immutable releases, and selected active views.
- `manifests/`: provenance, release, and binding metadata.
- `reports/`: generated or evidence-bearing findings under governing
  contracts.
- `state/`: runtime state, trial records, receipts, and unpublished evidence.
- `src/`: current and retained source code as classified by the registry.
- `scripts/`: preparation, audit, packaging, and historical utilities.
- `tests/`: current, local-evidence, high-risk, live, and retired lanes.
- `docs/`: current supporting guidance and preserved historical records.
- `FuturesLiveCockpit/`: mixed packaging input and generated output.

Tracked does not imply current. Ignored does not imply disposable. Historical
files may remain at exact original paths because path and byte identities are
lineage-bound. Generated-looking output is not automatically safe to delete.

Reports label material claims as Verified, Inferred, Assumed, or Not
established, and name their scope, evidence, identities, limitations, and next
gate. Negative and failed results remain part of the evidence graph.

## Stop conditions

Stop at the current gate when:

- a required input, pointer, catalog, release, certificate, or manifest is
  missing, stale, incomplete, ambiguous, contradictory, or future-known;
- market, contract, calendar, source, mechanism, trial, profile, stage, or
  economics identity is unresolved or has drifted;
- readiness evidence is absent or a scientific, economic, audit, or
  supportability gate fails;
- a single-use attempt or authorization has already been consumed;
- provider access, historical-row access, or a write would exceed authority;
- a holdout or forward boundary could be disclosed, reused, or influence
  selection;
- production, publication, installation, activation, payout, execution, or
  trading readiness has not been independently established;
- evidence conflicts, a destination exists, a secret could be exposed, or
  rollback and preservation cannot be proven.

Do not convert a stop into a pass by changing the mechanism, excluding missing
rows, relabeling an outcome, substituting satellite success, or invoking a
retired surface. Preserve negative and failed results as evidence and return to
the governing workflow for the next authorized decision.

## Historical record and retired surfaces

The entire former root outline, including versioned attempts, consumed
authorizations, failure narratives, supersession records, and detailed lineage,
is preserved byte-for-byte in
`docs/history/PROJECT_OUTLINE_SNAPSHOT_2026-08-11.md`; its exact provenance is
bound by `docs/history/PROJECT_OUTLINE_SNAPSHOT_2026-08-11.json`.

Use `docs/LEGACY_WORKFLOWS.md` for retired module and workflow classification,
and `PIPELINE_FOLDER_MAP.md` for the existing detailed topology reference.
Historical descriptions remain evidence, not instructions or authority for new
work.
