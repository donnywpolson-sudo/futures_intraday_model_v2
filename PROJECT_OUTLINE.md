# Futures Intraday Research Pipeline

## 1. Purpose and authority

This is the practical, human-readable reference for the current research
pipeline. `CURRENT_WORKFLOW.md` controls procedure and approvals, `AGENTS.md`
controls durable safety and research integrity, and active pointers and their
bound contracts control exact identity. If prose differs from those live
authorities, the live authorities win.

This file grants no provider access, market-row read, holdout or forward
access, publication, Git staging/commit/push, activation, trading, or order
authority.

## 2. Current status

| Area | Current state |
| --- | --- |
| Canonical DBN source | **COMPLETE / FROZEN / VALIDATED** |
| Source closure and old-derived-data retirement | **COMPLETE / ACTIVE** |
| Clean restart | **COMPLETE / CERTIFIED** |
| Causal-observation contract | **FROZEN** |
| Observation-only safety implementation | **BUILT / VALIDATED / REMOTELY RECOVERABLE** |
| Causal observation release | **NOT BUILT** |
| Development-only canary | **EXECUTED ONCE / PASSED / INACTIVE** |
| Full development builder | **IMPLEMENTED / NOT ROW-AUTHORIZED / NOT EXECUTED** |
| Outcomes | **NOT STARTED** |
| Features | **NOT STARTED** |
| WFA | **NOT STARTED** |
| New mechanism | **NOT STARTED** |
| Final Sealed 252-Session Holdout | **SEALED / PRISTINE / UNREAD** |
| Forward monitoring | **NOT STARTED** |

Direct DBN use by features, models, WFA, or backtests is forbidden. Raw DBN
may feed only the frozen development-only causal-observation contract through
the committed observation-only safety implementation. The representative
seven-market development canary ran once under consumed authority and passed
independent verification; its candidates remain unpublished and inactive. The
full development build has not run.

The canonical DBN source is frozen and validated, and active source closure
blocks old derived inputs and retired execution paths. Prior foundation
releases may remain as historical evidence. They do not mean that the new
clean-restart 41-market development-only causal observation foundation has
been built or activated.

## 3. Full pipeline

1. Canonical Source Custody and Closure
2. Causal Observation Foundation
3. Outcome/Target Foundation
4. Feature Foundation
5. Chronological Validation and WFA
6. Mechanism Definition and Freeze
7. Tier 0 — Engineering and ES Qualification
8. Tier 1 — Four-Market Confirmation
9. Tier 2 — Balanced 16-Market Replication
10. Tier 3 — Full 41-Market Replication
11. Final Sealed 252-Session Holdout
12. Post-Cutoff Forward Monitoring

Internal phase names may remain in code, but they are not the primary
user-facing pipeline.

## 4. Stage table

| Stage | Status | Main input | Main output | Main gate |
| --- | --- | --- | --- | --- |
| 1. Canonical Source Custody and Closure | Complete | Canonical DBN custody | Frozen validated source plus active no-read closure | Current source contract and closure proof pass |
| 2. Causal Observation Foundation | Contract frozen; canary passed; full release not built | Approved development-only canonical DBN scope | Certified causal observation release | Full build requires a separate exact row-read packet and independent certification |
| 3. Outcome/Target Foundation | Not started for the new mechanism | Certified causal observations | Separate immutable outcomes | Point-in-time maturity and separation contract |
| 4. Feature Foundation | Not started for the new mechanism | Certified causal observations | Separate immutable features | Availability, leakage, and transform checks |
| 5. Chronological Validation and WFA | Not started for the new mechanism | Frozen outcomes and features | Chronological folds and WFA evidence | Training-only transforms, purge, and embargo where required |
| 6. Mechanism Definition and Freeze | Not started for the new mechanism | Preregistered development evidence | One frozen mechanism identity | Finite budget and preregistration pass |
| 7. Tier 0 — Engineering and ES Qualification | Not started for the new mechanism | Frozen mechanism and approved Tier 0 inputs | Engineering certificate and ES decision | Synthetic engineering, then row-certified ES qualification |
| 8. Tier 1 — Four-Market Confirmation | Not started for the new mechanism | Passing Tier 0 mechanism | Four-market confirmation evidence | Frozen identity and four-market gates pass |
| 9. Tier 2 — Balanced 16-Market Replication | Not started for the new mechanism | Passing Tier 1 mechanism | Balanced replication evidence | Frozen identity and Tier 2 gates pass |
| 10. Tier 3 — Full 41-Market Replication | Not started for the new mechanism | Passing Tier 2 mechanism | Traditional and satellite replication evidence | Traditional 38 pass independently; satellites cannot rescue failure |
| 11. Final Sealed 252-Session Holdout | Construction complete; evaluation locked / not started | Passing frozen Tier 3 mechanism and separate access authority | Single project-level final evaluation | Pristine manifest, prerequisites, and explicit single-use authority |
| 12. Post-Cutoff Forward Monitoring | Not started | Final frozen candidate after the holdout decision | Forward-only monitoring evidence | No research selection or rescue from forward results |

Exact approvals, receipts, identities, hashes, and detailed evidence
requirements belong in the current contracts, not in this outline.

## 5. Next-stage requirements

The frozen causal-observation contract and committed observation-only safety
implementation define:

- the development-only boundary;
- bar start, end, and availability semantics;
- point-in-time actual contract identity;
- roll handling;
- project-session versus official-schedule semantics;
- missingness and quality states;
- cadence authority and reconciliation;
- backward/as-of joins and staleness;
- tradability;
- storage and materialization;
- independent certification; and
- exclusion of micros, holdout, forward, outcomes, features, and evaluation.

The canary produced 48,635 valid observations with no invalid quality rows or
duplicates. Its 940 observed gaps and all incomplete official schedule states
remained `UNKNOWN_FAIL_CLOSED`; one ES roll discontinuity was recorded, and
cadence comparisons never overwrote source evidence. No real negative-price
row appeared, so negative-price support remains synthetic-test proven.

The non-public full development builder processes one market-year at a time
and creates independently verified, inactive monthly candidates. Only after
that runner is committed, pushed, and remotely verified may an immutable full
development row-read packet be prepared for separate approval. This document
grants no row-read, publication, or activation authority.

## 6. Universe, tiers, and time boundaries

The active standard ladder fixes 41 standard roots and 17 deferred micros.
Exact standard tier membership is:

- **Tier 0 — Engineering and ES Qualification:** ES.
- **Tier 1 — Four-Market Confirmation:** ES, CL, ZN, 6E.
- **Tier 2 — Balanced 16-Market Replication:** ES, NQ, CL, NG, RB, GC, HG,
  SR3, ZN, ZB, 6E, 6J, ZC, ZS, LE, HE.
- **Tier 3 — Full 41-Market Replication:** ES, NQ, RTY, YM, CL, NG, RB, HO,
  GC, SI, HG, PL, SR3, SR1, ZQ, TN, ZT, ZF, ZN, ZB, UB, 6A, 6B, 6C, 6E,
  6J, 6M, 6N, 6S, ZC, ZS, ZL, ZM, ZW, KE, LE, HE, GF, BTC, ETH, PA.

Tier 3 contains 38 traditional markets and the BTC, ETH, and PA satellites.
The traditional group must pass independently; satellite results cannot rescue
traditional failure.

The deferred micros are MES, MCL, MGC, M6E, MNQ, MYM, M2K, M6A, SIL, MBT,
MET, M6B, MJY, MCD, MSF, MNG, and MHG. They are excluded from the next
41-market foundation, cannot rescue standard-market failure, and cannot create
a micro-specific or second holdout.

The previous mechanism is closed after failure at Tier 0 ES qualification.
It cannot advance or be retried. A new mechanism has not started and must
restart at Tier 0 synthetic engineering after the required foundations and
mechanism freeze exist.

- Development end (exclusive): `2025-07-13T22:00:00Z`.
- Final Sealed 252-Session Holdout: 2025-07-14 through 2026-07-13.
- Forward timestamp boundary: `2026-07-14T00:00:00Z`.
- General 2023–2024 official-session continuity remains unresolved and is not
  established by the purpose-limited final-252 manifest.

## 7. Core anti-bias rules

- Accepted sources are immutable; corrections create immutable successors.
- Actual contract identity and every value's availability are point-in-time.
- Missing stays missing; future-known mapping is forbidden.
- Same-bar lookahead is forbidden, and roll handling is explicit.
- Observations, outcomes, features, predictions, and evaluation remain
  separate.
- Splits are chronological; fitted transforms use training data only.
- Purge and embargo apply where overlapping horizons require them.
- Real-data work is preregistered with finite budgets and stop rules.
- Holdout results cannot drive selection, and satellites or micros cannot
  rescue a failed required cohort.
- Failed and stopped results remain evidence.

## 8. Evidence and history

Use the active pointer and its bound ladder contract/profile for exact tier and
mechanism state. Use `configs/source_contract.json` and its source-contract
registry for the canonical source and retirement boundary; source and release
registries preserve exact immutable identities. Use the final-252 manifest for
the sealed session boundary.

Current contracts hold detailed evidence requirements. Git history and
`docs/history/` preserve prior prose and foundation history;
`docs/LEGACY_WORKFLOWS.md` identifies retired workflow surfaces. Prior
foundation releases remain evidence only and do not override active pointers,
contracts, or closure.

NEXT GATE AFTER RUNNER COMMIT AND PUSH: FULL DEVELOPMENT BUILD PACKET PREPARATION
