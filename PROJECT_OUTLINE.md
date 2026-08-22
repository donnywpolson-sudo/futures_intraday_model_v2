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

## Certified final-evaluation boundary

The project uses one research-selection-pristine **Final Sealed 252-Session Holdout**: trade dates 2025-07-14 through 2026-07-13, manifest `0ff48f99d8b6d3a262ddf0a060bea8e733fc95aa7c4b4d43f19a0f78b107d4d1`. Development ends exclusively at 22:00 UTC on 13 July 2025 and forward monitoring begins at 00:00 UTC on 14 July 2026. The manifest is purpose-limited, grants no row or evaluation access, creates no market-specific or micro holdout, and is not a general exchange calendar. Complete 2018-cutoff project-session continuity remains unresolved for portions of 2023-2024 and is not claimed.

The user-facing pipeline is: Canonical Source Foundation; Research Design and Mechanism Freeze; Tier 0 Engineering and ES Qualification; Tier 1 Four-Market Confirmation; Tier 2 Balanced 16-Market Replication; Tier 3 Full 41-Market Replication; Final Project-Level 252-Session Evaluation; Post-Cutoff Forward Monitoring. Existing Phase 1A-11 labels remain internal synthetic/capability terminology only. The previous counted mechanism remains closed after Tier-0 ES failure, and the next mechanism is not started.

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
- `configs/micro_contract_universe_v1.json` is the definitive cumulative micro
  policy universe; membership grants no activation or research authority.
- `configs/core_databento_standard_l0_dependency_policy_v1.json` fixes the
  core historical dependency at Databento `GLBX.MDP3`, CME Standard access,
  and certified L0-only design.
- `configs/data_surface_registry_v1.json` is the data-specific role and
  fail-closed selection registry subordinate to
  `configs/repository_surface.json`.
- `configs/data_capability_baseline_v1.json` binds the completed capability
  assessment and bounded micro delta without creating new observability.
- `configs/data_phase_closed_v1.json` is the versioned logical data-phase
  closure record.
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

### Data Phase Closed v1 boundary

The standard/full-size Tier-1 historical foundation is certified with its
recorded caveats, and the historical capability assessment is complete. The
17-market micro policy universe has complete, structurally verified opaque
source custody across five L0 schemas. The active micro catalog remains the
legacy four (`MES`, `MCL`, `MGC`, `M6E`); the other 13 markets remain inactive,
not research-certified, and unauthorized for research. Tier membership is a
planning policy only, not an authority grant.

Logical data-phase closure does not require full conversion of the 13
additional micro markets. Unknown provider normalization generations and
preserved degraded provider dates remain fail-closed pending bounded
intended-use certification. Legacy-four provider-package receipt completeness
is only partially established, same-volume hardlinks are not backups, and no
independent off-machine disaster-recovery copy is claimed.

Future years, markets, schemas, and reference or external extensions are added
through new immutable custody, canonical receipts, source hash and epoch
classification, bounded intended-use certification, an immutable successor,
and explicit activation. Accepted source bytes are never replaced in place.
Classified stale physical material may remain when it is proved unselectable;
file presence, newest-directory selection, modification time, and broad globs
never establish currentness or activation.

The next permitted boundary is **Design and certify the research
feature/label/split/transform successor**. Alpha research remains disabled, and
that successor phase is not started by data closure.

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

The governing conceptual pipeline is below. Existing `futures-pipeline`
subcommands remain synthetic mechanics entry points and do not independently
grant the real-history authority represented by these phases.

| Phase | Purpose and current entry point | Principal input | Principal output | Gate or stop condition | Authority boundary |
| --- | --- | --- | --- | --- | --- |
| 0 | Immutable provider custody; completed evidence is the current entry point | Declared source request and custody contract | Immutable bytes, receipts, hashes, and epoch evidence | Identity, hash, condition, or pending-job mismatch | New provider use remains separately controlled |
| 1 | Certified historical data foundation; certified standard foundation is current | Accepted immutable source releases | Intended-use foundation certification | Source, identity, timing, missingness, or certification failure | Certification does not grant Alpha research |
| 2 | Historical data capability and alpha investigability; completed assessment is current | Certified observability and declared constraints | Bounded capability and investigability assessment | Unsupported field, schema, source, or Alpha claim | Assessment grants no trial or row-read authority |
| 3 | Hypothesis, feature, label, split, and transform contracts; this is the next unstarted boundary | Closed data architecture and capability limits | Immutable preregistration and separated causal contracts | Leakage, timing, dependency, split, transform, or authority failure | Requires separate real-history research authority |
| 4 | ES discovery sandbox | Registered Phase 3 contracts and approved ES inputs | Bounded discovery evidence | Budget, preregistration, leakage, or holdout failure | Discovery cannot promote or open holdout |
| 5 | Full-size Tier-1 falsification | Frozen discovery candidate and full-size certified inputs | Cross-market falsification evidence | Baseline, breadth, stability, or frozen-identity failure | No micro or production activation follows automatically |
| 6 | Micro transfer and execution validation | Separately certified and explicitly activated micro inputs | Transfer and execution evidence | Certification, transfer, fill, cost, or identity failure | Current inactive custody grants no Phase 6 authority |
| 7 | Expanded robustness | Passing frozen cross-scale candidate | Dependence-aware robustness evidence | Negative control, multiplicity, breadth, or stability failure | Robustness cannot widen the registered mechanism |
| 8 | Economic and execution validation | Audited predictions and explicit execution economics | Net economic, capacity, portfolio, and risk evidence | Cost, drawdown, concentration, capacity, or rule failure | Evaluation does not grant production or trading readiness |
| 9 | Sealed holdout | Fully passing frozen candidate and one explicit access authority | Single-use guarded holdout evidence | Missing authority, prior use, drift, or cohort leakage | One holdout use grants no production or order authority |
| 10 | Paper/live readiness | Passing sealed candidate and separate operational evidence | Paper, publication, and live-readiness receipts | Operational, safety, publication, or readiness failure | Trading and order paths remain separately controlled |

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

<!-- rlac_20260814T0642492268888Z_0b571482:CURRENT_DIRECT_AUTHORITY -->
## Current governing data architecture — rlac_20260814T0642492268888Z_0b571482

This section supersedes earlier data-architecture descriptions in this current document. Those retained earlier bytes are HISTORICAL / SUPERSEDED / NON-AUTHORITATIVE context and do not select data.

### PHASE 0 — IMMUTABLE PROVIDER CUSTODY

Databento GLBX.MDP3 DBNs and provider metadata/support files are the canonical immutable source.

Core source schemas:

- `ohlcv-1s`
- `ohlcv-1m`
- `definition`
- `statistics`
- `status`

Trades remain separately sealed and are not a core foundation input. No provider source file is modified or overwritten.

### PHASE 1 — CERTIFIED CAUSAL FOUNDATION

Permanent current transformation:

```text
Databento DBN
    -> certified causal 1s
    -> certified causal 1m
    -> certified causal reference metadata
```

The materialized legacy layer formerly installed at `data/raw` is QUARANTINED, HISTORICAL, and NON-AUTHORITATIVE after this cutover. Temporary decoded chunks may exist only beneath task-owned staging and must not become permanent authority.

### PHASE 1A — CROSS-RESOLUTION CERTIFICATION

```text
Databento ohlcv-1s
    -> temporary one-minute reconstruction
    -> exact comparison with Databento ohlcv-1m
    -> compact certification receipt
```

Temporary reconstructed bars are not retained as a permanent data layer.

### PHASE 1B — ACTIVE AUTHORITY

`data/active` contains lightweight exact catalogs and pointers only. Authority is determined through exact DBN source hashes, exact certified causal release IDs, exact manifests, exact policies, and exact certification receipts.

Newest-folder authority, broad-glob authority, file-existence activation, implicit fallback selection, and unregistered-path research access are prohibited.

### PHASE 1C — LEGACY RETIREMENT

The legacy materialized layer is quarantined only after direct DBN-to-causal authority is proved, every current dependency is zero, post-quarantine validation passes, and rollback is prepared. The historical path string `data/raw` appears here only as QUARANTINED / HISTORICAL / NON-AUTHORITATIVE lineage. Permanent deletion requires a later separately authorized gate.

The minimum active runtime set is not the minimum permanent custody set. Original source, future-tier custody, sealed data, certification evidence, and rollback assets remain required even when not selected by current runtime authority.
