# Systematic Futures Research Master Audit

Version: 3.3.0
Classification: `NON_AUTHORIZING_EVIDENCE_CLASSIFICATION`
Mode: `EVIDENCE_ONLY`

## Purpose

This is the canonical audit specification for this repository. It decides
whether declared immutable evidence can support one requested project state.
It does not authorize provider access, real-history research, prediction
materialization, candidate sealing, holdout access, trading, or publication of
a readiness receipt.

The machine classifier is `futures-master-audit`. Every invocation must bind
this file, the stage matrix, the research-universe contract, and every evidence
item by exact size and SHA-256.

## Invocation contract and frozen hash domains

One invocation requests exactly one target state and freezes:

- the clean committed Git identity and declared repository boundary;
- this specification and
  `configs/master_audit_v3/stage_requirement_matrix.json`;
- the canonical source, universe, profile, session, identity, economics, and
  dependency contracts relevant to the target;
- every release manifest, receipt, trial declaration, package inventory,
  shortcut record, test receipt, and other admitted evidence item;
- auditor implementation/configuration identities, runtime class, command
  budget, and output destination.

Hash domains are explicit and non-overlapping. A file cannot silently serve as
both evidence and authority, an operational profile cannot replace the
universe contract, and a producer receipt cannot replace independent
replication. Any mutation after freezing invalidates the affected claim.

## Active target states

- `FOUNDATION_READY`: the approved universe, immutable source releases,
  causal/identity foundation, feature/outcome isolation, recovery, and
  reproducibility evidence are supportable.
- `HISTORICAL_RESEARCH_READY`: the non-alpha prerequisites for a separately
  approved real-history program are supportable. This state does not authorize
  an evaluation or make an alpha claim.
- `OBSERVATION_COCKPIT_READY`: the installed 41-market cockpit is dependency
  complete, secret-safe, observation-only, failure-safe, recoverable, and
  rollback verified.

An active state may be reported as ready only when the classifier returns
`SUPPORTABLE` for that exact target and a separate state transition process
binds the result to the clean committed Git state. Historical and prospective
target names in the stage matrix remain available for later separately
authorized work.

## Independent audit workflow

1. Freeze the requested target, Git identity, source/universe identities,
   evidence scope, command budget, and auditor version.
2. Perform blind repository-first threat discovery before using this document
   as a checklist. Look for ways the system can falsely pass, including stale
   paths, duplicate authority, mutable data, secret leakage, profile drift,
   hidden outcome access, approval reuse, and incomplete recovery.
3. Register all safe-to-read evidence with exact path, byte count, SHA-256,
   provenance, and limitations.
4. Run an independent implementation or calculation where the claim needs
   replication. Do not accept a producer's own success flag as sufficient.
5. Classify every required subcheck. Preserve contradictions, uncertainty,
   failures, missing evidence, and negative results.
6. Reconcile the blind findings against this specification and the frozen stage
   matrix. New blind findings expand the evidence work; this document never
   narrows a discovered risk.
7. Emit canonical machine JSON and a concise human report. The result is
   evidence only.

## Invocation and safety boundary

An invocation uses schema `systematic_futures_audit/3.0.0`, requests exactly one
target, and declares all readable paths. Undeclared paths, absolute or escaping
paths, links, stale hashes, unsafe evidence, or a runtime class other than
static/hash reads are precheck errors.

The classifier does not discover project data, run pipeline code, access
holdout rows, call a provider, publish readiness, seal a candidate, or place an
order. Domain results must be supplied as evidence-referenced subcheck records.
A claimed `PASS` or `FAIL` without evidence is invalid.

## Evidence, registry, and applicability

Use one canonical evidence index per invocation. Each evidence record has one
identity, relative path, byte count, SHA-256, provenance, safety disposition,
limitations, and every claim reference that consumes it. Duplicate aliases,
dangling references, undeclared bytes, unsafe paths, or contradictory
identities are precheck failures.

Evidence strength descends from independently reproduced primary bytes and
calculations, to verified immutable manifests/receipts, to producer reports,
then to prose assertions. Weaker evidence cannot overrule contradictory
stronger evidence. Test success establishes only the behavior actually tested.

Evidence state and check status are separate:

- evidence may be `VERIFIED`, `CONTRADICTED`, `UNSAFE`, `STALE`, `MISSING`, or
  `UNREVIEWED`;
- applicability must be positively established from the frozen target matrix;
  absence of evidence never makes a required check not applicable;
- reused evidence is allowed only when its exact identity, semantics,
  limitations, and authoritative owner satisfy every consuming subcheck.

## Status and decision semantics

Subcheck statuses are `PASS`, `FAIL`, `ERROR`, `MISSING_EVIDENCE`, `UNKNOWN`,
`NOT_RUN`, and `NOT_APPLICABLE`. A caller cannot supply `NOT_RUN`; the
classifier assigns it only when a subcheck is outside the selected target.

Precedence is `FAIL`, `ERROR`, `MISSING_EVIDENCE`, `UNKNOWN`, `NOT_RUN`, `PASS`,
then `NOT_APPLICABLE`.

- Any required `FAIL` produces `BLOCKED`.
- Every required check being `PASS` or positively `NOT_APPLICABLE` produces
  `SUPPORTABLE`.
- Every other valid classification produces `INSUFFICIENT_EVIDENCE`.

Logical exit codes are 0 for supportable, 10 for blocked, 11 for insufficient
evidence, 12 for a precheck error, and 13 for a fatal auditor failure.

## Six audit stages

The stage order is cumulative; a later stage never erases an earlier blocker.

1. Freeze the invocation, immutable snapshot, capability matrix, and blind
   threat discovery.
2. Evaluate G1-G3 for integrity, point-in-time causality, identity, and
   feature/label/split isolation.
3. Evaluate G4-G5 for complete selection accounting and dependence-aware net
   OOS evidence.
4. Evaluate G6 for sealed holdout escrow and disclosure control.
5. Evaluate G7 for execution economics, portfolio interaction, capacity, and
   pathwise survival.
6. Evaluate G8, cumulative target requirements, reporting, recovery, and the
   final non-authorizing decision.

Only stages and subchecks required by the frozen target may be marked
applicable. Earlier-stage evidence remains required when a later target depends
on it.

## Eight gates

### G1 — Evidence integrity and reproducibility

Verify independent replication, claim/evidence closure, exact schemas,
dependency identity, canonical serialization, deterministic timestamps and
hashes, immutable release closure, and fail-closed applicability.

### G2 — Point-in-time causality and futures identity

Verify event, availability, receive, and decision-time ordering; market-year
admission as of the decision; actual tradable contract identity; definition
lineage; sessions, holidays, halts, limits, expiry, rolls, and missing states.
Session rollover alone is not a trading-hours authority. Historical foundation
rows must bind the empirical-observability policy and an immutable accepted DBN
source: actual decoded rows only, no gap filling or synthetic opens/closes, and
unobserved time classified as missing rather than closed. That evidence makes
no official historical CME open, close, halt, pause, or holiday claim. The
activated, mapped, freshness-checked CME calendar is a separate authority for
current/forward cockpit scheduling. Continuous symbols are selection
references, never executable identity.

### G3 — Split, feature, label, and pipeline isolation

Verify physical and capability separation among features, outcomes,
predictions, and evaluation; explicit entry lag and label horizon; overlap
purge and embargo; chronological nesting; train-only transforms; and
standalone reproduction from verified v2 releases.

### G4 — Complete trial accounting and selection control

Verify immutable trial registration before every real-data attempt, candidate
genealogy, AI-assisted choices, all tried variants, multiplicity penalties,
optional-stopping limits, finite stop rules, simple baselines, and negative
controls. A new directory, renamed model, or expanded universe does not reset
trial history.

### G5 — Dependence-aware net out-of-sample evidence

Verify effective independent breadth, event/market/family concentration,
traditional-versus-satellite reporting, carry and roll attribution, PnL
conservation, specification stability, dependence-aware uncertainty, temporal
stability, and portfolio compatibility. Satellite results may not rescue
failure in the 38-market traditional universe.

### G6 — Holdout escrow and disclosure control

Verify immutable holdout identity, access log, frozen candidate and code,
one-time or budgeted access, indirect-query controls, and disclosure limits.
Unknown or unauthorized access is a failure, not an implied pass.

### G7 — Net execution economics and robustness

Verify order-intent equivalence, costs, delay, slippage, carry, margin,
liquidation, capacity, concentration, shared liquidity, cross-strategy netting,
signal decay, break-even margins, and availability-adjusted attainable PnL.
Historical L0 data alone cannot prove executable historical spreads.

### G8 — Readiness, cockpit, monitoring, and recovery

Verify state capability, serving parity, change detection, abstention, recovery,
independent validation, and audit trail. Current/forward calendar evidence must
meet its coverage and freshness horizon. Historical readiness instead requires
a schema-7 foundation bound to the exact empirical-observability policy,
predecessor release, source DBN release, and per-interval observed-row evidence.
Legacy schema-4/5/6 foundations remain reproducible but are
`HISTORICAL_OBSERVABILITY_CONTRACT_NOT_BOUND` for current readiness. For
`OBSERVATION_COCKPIT_READY`, also verify all of the following:

- exact locked dependencies, assets, licenses, and packaged byte closure;
- all 41 approved markets and intended tier/family grouping;
- v2-local credential discovery with no secret in Git, logs, reports, cache,
  installation, package, or shortcut;
- an explicit observation-only architecture with no order-placement import,
  interface, route, or authority;
- bounded provider error handling and prediction-panel abstention;
- state/cache bounds, corrupt-state recovery, clean shutdown, and no autostart;
- packaged self-check, demo smoke, and one separately approved bounded live
  smoke with a create-only result bound to the exact frozen executable; and
- exact Desktop/Start Menu shortcut targets plus a tested rollback to the prior
  installation.

## Universe, profiles, and source ownership

`configs/research_universe_contract.json` is the canonical universe. An
approved contract is valid only when its `approval_receipt_id` equals the
content hash of evidence present in the audit invocation. Pending, stale, or
mismatched approval makes every universe-owned subcheck
`MISSING_EVIDENCE`.

`configs/alpha_tiered.yaml` is only a validated operational view. It may narrow
the canonical universe, but cannot admit a market, change selection eligibility,
unlock holdout/forward cohorts, or override the contract. `all_raw` is inventory
only.

Only verified immutable releases declared by `configs/source_contract.json`
may feed the project. A correction creates a successor; accepted bytes are
never overwritten. Generated raw, causal, feature, label, model, prediction, or
evaluation output from any other repository cannot become an authoritative
input.

## Required false-pass tests

Every audit implementation and invocation must fail closed for at least:

- missing or reused approvals, stale receipt hashes, and mismatched Git state;
- incomplete releases, mutated files, links, unexpected files, and manifest
  publication before payload closure;
- universe/profile drift, silent market expansion, and satellite rescue;
- future-known definitions, session/roll leakage, feature access to outcomes,
  and evaluation without a predeclared trial;
- a historical observability release that fills missing time, treats no rows as
  closed, asserts official CME schedule authority, loses quarantines, or binds
  the wrong predecessor/source release;
- stale or incomplete current/forward CME captures, ambiguous product
  mappings, unknown schedule states, and broken cockpit schedule freshness;
- missing, unknown, or partially decoded source states;
- secret bytes or credential filenames in tracked or packaged artifacts;
- cockpit order paths, provider reconnect loops, cache mutation during a bounded
  smoke, missing assets/licenses, and broken rollback;
- holdout discovery or access without a separate exact approval; and
- operation with a required external repository or mutable legacy path.

## Runtime and stopping rules

- Read only frozen, declared, safe paths. Do not follow links, discover sibling
  repositories, execute project code, import provider clients, or materialize a
  protected artifact.
- Stop the affected claim on a stale hash, schema mismatch, duplicate authority,
  unsafe path, missing approval, contradictory primary evidence, undeclared
  capability, or exhausted command budget.
- Preserve partial evidence and errors, but never convert an interrupted or
  incomplete check into a pass.
- Auditor failure and precheck failure are distinct from a domain failure.
  Reports must retain the logical exit category and the smallest missing or
  contradicted requirement.

## Outputs and authority

The canonical output contains the target, decision, gate and subcheck statuses,
verified evidence identities, universe approval state, limitations, and logical
exit code. It must state that it grants no provider, research, holdout,
candidate, prediction, trading, or publication authority.

The machine result uses schema `systematic_futures_audit/3.0.0` and includes:
`audit_id`, `target_state`, `target_state_decision`, `logical_exit_code`,
`universe_contract_approved`, ordered `gate_statuses`, normalized `subchecks`,
the verified evidence index, and an explicit all-false authority envelope.
Every subcheck record carries its gate/subcheck identity, status, reason,
evidence references, and limitations. The human report must reconcile exactly
to the machine result rather than restating a more favorable conclusion.

A green test suite or copied files alone cannot satisfy this audit. The evidence
must support the requested state, and the Meta Audit must find no unresolved
Critical/High or P0/P1 weakness in this specification before final closure.
