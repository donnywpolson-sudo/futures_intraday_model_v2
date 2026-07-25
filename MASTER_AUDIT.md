# Systematic Futures Research Master Audit

Version: 3.1.0
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

## Eight gates

### G1 — Evidence integrity and reproducibility

Verify independent replication, claim/evidence closure, exact schemas,
dependency identity, canonical serialization, deterministic timestamps and
hashes, immutable release closure, and fail-closed applicability.

### G2 — Point-in-time causality and futures identity

Verify event, availability, receive, and decision-time ordering; market-year
admission as of the decision; actual tradable contract identity; definition
lineage; sessions, holidays, halts, limits, expiry, rolls, and missing states.
Continuous symbols are selection references, never executable identity.

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
independent validation, and audit trail. For `OBSERVATION_COCKPIT_READY`, also
verify all of the following:

- exact locked dependencies, assets, licenses, and packaged byte closure;
- all 41 approved markets and intended tier/family grouping;
- v2-local credential discovery with no secret in Git, logs, reports, cache,
  installation, package, or shortcut;
- an explicit observation-only architecture with no order-placement import,
  interface, route, or authority;
- bounded provider error handling and prediction-panel abstention;
- state/cache bounds, corrupt-state recovery, clean shutdown, and no autostart;
- packaged self-check, demo smoke, and one separately approved bounded live
  smoke; and
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
- missing, unknown, or partially decoded source states;
- secret bytes or credential filenames in tracked or packaged artifacts;
- cockpit order paths, provider reconnect loops, cache mutation during a bounded
  smoke, missing assets/licenses, and broken rollback;
- holdout discovery or access without a separate exact approval; and
- operation with a required external repository or mutable legacy path.

## Outputs and authority

The canonical output contains the target, decision, gate and subcheck statuses,
verified evidence identities, universe approval state, limitations, and logical
exit code. It must state that it grants no provider, research, holdout,
candidate, prediction, trading, or publication authority.

A green test suite or copied files alone cannot satisfy this audit. The evidence
must support the requested state, and the Meta Audit must find no unresolved
Critical/High or P0/P1 weakness in this specification before final closure.
