# Systematic Futures Research Pipeline Audit v3.0.0

## Purpose

This is the v2 rebuild's master audit specification. It classifies whether
declared, immutable evidence supports one requested v2 state. It is strictly
`EVIDENCE_ONLY`: an audit result cannot publish readiness, authorize real-history
research, unlock a holdout, seal a candidate, call a provider, or trade.

The legacy v2.2.7 audit is migration evidence only. This specification and its
hash-bound v2 registries are authoritative for the rebuild.

## Invocation and safety boundary

An invocation must use schema `systematic_futures_audit/3.0.0`, name exactly one
v2 target state, declare every readable path, bind this specification, the stage
matrix, and the GLBX universe contract by SHA-256, and provide a single canonical
evidence index. Undeclared paths, links, legacy paths, stale hashes, unsafe reads,
or runtime permissions beyond static/hash reads are precheck errors.

The auditor does not discover project data or execute project code. Domain checks
are supplied as explicit, evidence-referenced subcheck results. A `PASS` or `FAIL`
without evidence is invalid. Missing required results remain
`MISSING_EVIDENCE`; they never become zero, pass, or not applicable.

## Audit workflow

1. Freeze and verify invocation, specification, stage matrix, universe contract,
   runtime limits, and evidence identities.
2. Perform blind repo-first threat discovery outside this classifier and register
   the resulting immutable evidence without using this checklist as the search
   boundary.
3. Apply the eight gates to the requested v2 state using the frozen matrix.
4. Reconcile findings against earlier assessments and retain contradictions,
   failures, unknowns, and limitations.
5. Emit a canonical machine classification and a concise human rendering. The
   classification is evidence, not a state transition.

## Status and decision semantics

Subcheck statuses are `PASS`, `FAIL`, `ERROR`, `MISSING_EVIDENCE`, `UNKNOWN`,
`NOT_RUN`, and `NOT_APPLICABLE`. Caller-supplied `NOT_RUN` is forbidden; the
classifier assigns it only to checks outside the requested target state.

Worst-applicable precedence is `FAIL`, `ERROR`, `MISSING_EVIDENCE`, `UNKNOWN`,
`NOT_RUN`, `PASS`, then `NOT_APPLICABLE`. A failure produces `BLOCKED`. All
required checks passing or positively not applicable produces `SUPPORTABLE`.
Every other complete classification is `INSUFFICIENT_EVIDENCE`.

Logical exit codes are: `0` supportable, `10` blocked, `11` insufficient evidence,
`12` precheck/contract error, and `13` fatal auditor failure.

## Six stages and eight gates

### Stage 1 — Contract, immutable scope, blind discovery, and capabilities

Verify one immutable evidence universe, exact scope and hashes, independent
replication capability, frozen registries, and fail-closed applicability.

### Stage 2 — Integrity, causality, and split isolation

- G1 `EVIDENCE_INTEGRITY_AND_REPRODUCIBILITY`: independent replication,
  claim/evidence closure, and registry integrity.
- G2 `POINT_IN_TIME_CAUSALITY_AND_FUTURES_IDENTITY`: bitemporal availability,
  point-in-time universe admission, actual-contract identity, sessions, and rolls.
- G3 `SPLIT_FEATURE_LABEL_AND_PIPELINE_ISOLATION`: feature/outcome separation,
  purge/embargo, nested selection, and train-only transforms.

### Stage 3 — Selection-aware inference and net OOS evidence

- G4 `COMPLETE_TRIAL_ACCOUNTING_AND_SELECTION_CONTROL`: genealogy, AI provenance,
  multiplicity, optional stopping, complexity, and simple baselines.
- G5 `DEPENDENCE_AWARE_NET_OOS_EVIDENCE`: effective breadth, event and family
  concentration, carry/roll attribution, uncertainty, stability, and portfolio
  compatibility.

### Stage 4 — Sealed final holdout

- G6 `HOLDOUT_ESCROW_AND_DISCLOSURE_CONTROL`: immutable holdout identity,
  one-time access, indirect-query accounting, and disclosure limits.

### Stage 5 — Execution economics and pathwise survival

- G7 `NET_EXECUTION_ECONOMICS_AND_ROBUSTNESS`: order-intent parity, margin,
  liquidation, costs, delay, capacity, netting, shared liquidity, and attainable
  PnL. Full L0 history alone cannot prove historical executable spreads.

### Stage 6 — Target-state readiness and recovery

- G8 `TARGET_STATE_READINESS_GOVERNANCE_AND_RECOVERY`: capability and serving
  parity, monitoring, abstention, order-state invariants, recovery, change control,
  and independent validation.

## GLBX.MDP3 universe ownership

The universe is a separate canonical contract, never an embedded mutable list.
G1 owns manifest closure, G2.S2 owns point-in-time market-year admission, G5.S1
and G5.S5 own breadth/family concentration, and G6 owns holdout/forward isolation.
Other gates reference those decisions without re-adjudicating them.

The current universe contract is provisional. Until it is explicitly marked
`APPROVED` and bound to a valid approval receipt identity, every required
universe-owned check is `MISSING_EVIDENCE`, even if an invocation claims it passed.
Universe expansion cannot reopen a closed hypothesis or reset trial accounting.

## Outputs

The canonical JSON output contains the target state and decision, gate and
subcheck statuses, verified evidence identities, universe approval state, logical
exit code, and an explicit statement that the audit grants no operational or
research authority. Output is inline by default; a caller may explicitly request
a contained v2-local report path.
