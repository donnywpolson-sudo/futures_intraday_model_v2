# Current workflow

This is the day-to-day guide for this repository. When another operational
document differs, this guide controls normal-work procedure; research evidence
requirements still apply to real research.

## Normal local work

Ask Codex for the outcome. It may inspect the repository, edit code or docs,
run tests, and create non-research local artifacts. Staging and local commits
are separate actions; each requires explicit user authorization. You do not
need to copy commands, hashes, plan IDs, or approval lines for ordinary local
work. A local commit never includes a push.

## High-risk work

Before provider/network use, a real-data read or evaluation, publication or
active-data mutation, installation/activation, deletion/cutover,
holdout/forward access, trading/order access, or push, Codex will ask once in
plain language. The request states the scope, impact or cost, outputs, and
preservation/rollback boundary. Replying `Approve` is sufficient.

## Certified final-evaluation boundary

The project uses one research-selection-pristine **Final Sealed 252-Session Holdout**: trade dates 2025-07-14 through 2026-07-13, manifest `0ff48f99d8b6d3a262ddf0a060bea8e733fc95aa7c4b4d43f19a0f78b107d4d1`. Development ends exclusively at 2025-07-13T22:00:00Z and forward monitoring begins at 2026-07-14T00:00:00Z. The manifest is purpose-limited, grants no row or evaluation access, creates no market-specific or micro holdout, and is not a general exchange calendar. Complete 2018-cutoff project-session continuity remains unresolved for portions of 2023-2024 and is not claimed.

The user-facing pipeline is: Canonical Source Foundation; Research Design and Mechanism Freeze; Tier 0 Engineering and ES Qualification; Tier 1 Four-Market Confirmation; Tier 2 Balanced 16-Market Replication; Tier 3 Full 41-Market Replication; Final Project-Level 252-Session Evaluation; Post-Cutoff Forward Monitoring. Existing Phase 1A-11 labels remain internal synthetic/capability terminology only. The previous counted mechanism remains closed after Tier-0 ES failure, and the next mechanism is not started.

Real research still needs its durable trial declaration and immutable evidence.
Before registration, every row-dependent sample, fold, feature, execution,
baseline, cost, risk, metric, and promotion requirement needs a source-bound
readiness certificate produced under separate row-access authority. Synthetic
tests and aggregate coverage prove mechanics, not real-source readiness. The
certificate must name every exact preregistered fold and independently account
for each baseline, including a true zero-trade baseline and scenario-specific
risk abstentions. Its semantics and actual bound files must be revalidated
before a single-use historical execution claim is consumed.
Future trial publication must use the certified registration writer: the
registration identity binds one canonical readiness-evidence path, file hash,
certificate identity, trial family, and protocol. Future historical execution
must use the certified claim wrapper, which reloads that exact immutable
registration and certificate before it invokes the one-use authorization
claim. A caller-supplied or cross-trial certificate is not sufficient.
The only current code surface for those two actions is
`CertifiedResearchGateway`. It derives the exact trial, family, protocol,
registration hash, certificate identity, and evidence hash used by the
single-use receipt. The shared receipt boundary rejects every unknown or
retired real-history trial operation before a claim is written. Preparatory
source and readiness censuses remain separate operations and cannot fit,
predict, evaluate, register, or promote a trial.
The gateway also requires one active, hash-bound Alpha research ladder. With no
active ladder pointer, all new real-history registration and execution fails
closed. A ladder-bound mechanism progresses in order through Tier 0, Tier 1,
Tier 2, Tier 3, the Final Sealed 252-Session Holdout, and then forward monitoring.
Tier 0 is one visible level with two mandatory gates in order: synthetic ES
engineering, followed by one row-certified ES pilot with 504 training sessions
and 63 evaluation sessions. Both gates must pass for Tier 0 to pass. The pilot
is a go/no-go qualification, not multi-market alpha confirmation.
The two Tier 0 gates retain separate evidence and authority boundaries.
Synthetic success cannot authorize or substitute for the real-history pilot.
The Tier 0 pilot registration must bind the immutable frozen mechanism, its
passing synthetic certificate, the exact ES 504/63 row certificate and session
manifest, and a separate passing four-market Tier 1 row certificate and
session manifest that already exclude the pilot sessions. This prevents the
single pilot attempt from being spent on a mechanism that cannot advance to
Tier 1. Immutable operational artifacts continue to use `tier_0` for the
synthetic gate and `pilot` for the ES gate; `pilot` is an internal gate
identifier, not a separate ladder level.
The mechanism hash cannot change between gates or levels; the pilot's
evaluation sessions are excluded from every later market; and Tier 3 requires
independent traditional-market passage with satellite results unable to rescue
failure.
Economic PASS decisions are recomputed against the immutable mechanism's
stage-specific stress, baseline, trade-count, breadth, drawdown, subgroup, and
formal-test gates; a bare PASS label is never sufficient.
The active ladder pointer, not `configs/alpha_tiered.yaml` by itself, is the
current operational truth. `futures-pipeline` is synthetic-only. Current
real-history actions consist only of a separately authorized immutable
readiness census followed, after a passing certificate, by registration and
execution through `CertifiedResearchGateway`.
Any row-certified preparatory census also consumes its own authorization even
when it times out or produces no report. A successor must use a new immutable
plan and approval; it may not silently reuse the consumed claim.
The cockpit starts observation-only and credentials remain outside Git. MFF
Evaluation and Rapid EOD Sim Funded are manual-only: the cockpit may prepare a
risk-gated ticket and persist explicitly operator-reported state, but it does
not authenticate, bind an API account, or transmit an order. MFF Live API
capability remains unconfirmed. See `docs/MFF_TRADOVATE_EXECUTION.md`.

When present, the machine-local accepted micro-contract source catalog is the
immutable legacy publication at `data/active/catalogs/apex_micro.json`. Its
pointer and catalog are exact local evidence, not tracked checkout inputs and
not dependencies of the canonical `current` test lane. Exact-byte checks run
only in the fail-closed `local_evidence` lane with an explicit hash manifest.
New code and prospective artifacts use the `micro_futures_*` namespace. The prepare-only generic cutover
plan is `configs/micro_futures_catalog_migration_plan_v1.json`; creating its
proposed catalog or pointer is a separate active-data mutation and is not
authorized by ordinary local work. See `docs/NAMING_AND_LINEAGE.md`.

## Prop-firm risk profiles

Future research preparation uses the provider-neutral profiles in
`configs/prop_firm_profiles.json`. The active immutable profile is
`mff_rapid_eod_50k_2026_08_10` at explicit stage `sim_funded`. Its modeled
ledger starts at $0, uses an EOD-only $2,000 trailing floor with a permanent
+$100 lock, and permits micro-only strategy intents under one portfolio-wide
30-micro-equivalent cap. Evaluation and inactive Live rules are separate
stages and cannot leak into funded research.

Strategy risk, execution mapping, platform costs, and payout policy live in
separate `prop_firm_*` configurations and their selected IDs/hashes are part of
every current run/cache identity. The platform and official MFF fees are
currently `UNSET`; unknown current news/session/price-limit data also fails
closed. Production/live readiness therefore remains false. The hash-bound
`configs/prop_firm_risk_profile.json`, previous provider profile, and former
Phase 8 chain are historical evidence. Current Phase 8 model-evaluation
preparation uses
`configs/prop_firm_phase8_evaluation.json` and
`src/futures_rebuild/prop_firm_phase8.py`. See `docs/PROP_FIRM_RISK.md` and
`docs/NAMING_AND_LINEAGE.md` before changing providers.

Read-only preparation commands:

```powershell
.\.venv\Scripts\python.exe -m futures_rebuild.pipeline prop-firm-risk-policy
.\.venv\Scripts\python.exe -m futures_rebuild.pipeline prop-firm-phase8
```

These commands print deterministic, non-authorizing preparation records. They
do not access provider services or rows and do not grant model or prop-firm
evaluation, publication, active-data, payout, deployment, or trading authority.

## Historic workflow material

Old hash-bound plans, approvals, receipts, closure runs, and successor modules
are evidence only. Do not use them as instructions for new work. See
`docs/LEGACY_WORKFLOWS.md` when a historic artifact must be interpreted.
Versioned V4-V12, Standard-only, Final, and Authoritative registration and
historical-execution helpers are retired. Importing an old helper does not make
it a current execution route; their historical operation names fail closed.

## Working state

`CODEX_HANDOFF.md` is optional context for an interrupted or high-risk task. It
does not grant authority. Preserve unrelated work; never use broad staging, and
report only the decision or blocker that needs attention.

<!-- rlac_20260814T0642492268888Z_0b571482:CURRENT_DIRECT_AUTHORITY -->
## Current data gate after rlac_20260814T0642492268888Z_0b571482

This section supersedes earlier data-authority and alpha-gateway descriptions in this current document. Those retained earlier bytes are HISTORICAL / SUPERSEDED / NON-AUTHORITATIVE context; they grant no current data or research access.

- Direct DBN-to-causal authority cutover: COMPLETE after independent validation.
- Former materialized raw layer: all 3,792 Wave E payload targets are permanently absent, with 15,000 total completed-deletion absences. Preserved empty parent directories are evidence only; rollback to the former raw payload is permanently unavailable.
- Provider source and certified causal foundation: hash-verified and unchanged.
- Tier 0/Tier 1 dual-resolution foundation: certified under `4d68f69d910a584df2fc0bc8ac10b82214ac451dd7eb262cfb8ee388710be9b6` with its existing caveats.
- Research and alpha authority: NOT AUTHORIZED. Sealed 2025/2026 rows and trades remain inaccessible.
- exact next permitted phase: design, implement, and independently certify the shared feature/label/split/transform research successor for full-size and micro Tier 0/Tier 1. That work still requires its own authority and may not begin as part of this cutover.

The minimum active runtime set is not the minimum permanent custody set. Source custody, future-tier DBNs, sealed data, certification evidence, and rollback assets remain protected.
