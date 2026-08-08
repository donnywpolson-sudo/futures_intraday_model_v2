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
closed. A ladder-bound mechanism progresses in order from synthetic Tier 0 to
one ES pilot, Tier 1, Tier 2, Tier 3, one project-level 2025 holdout, and then
forward monitoring. The mechanism hash cannot change between stages; the
pilot's evaluation sessions are excluded from every later market; and Tier 3
requires independent traditional-market passage with satellite results unable
to rescue failure.
The pilot boundary is deliberately stricter than an ES-only source check. Its
registration must bind the immutable frozen mechanism, its passing Tier 0
synthetic certificate, the exact ES 504/63 row certificate and session
manifest, and a separate passing four-market Tier 1 row certificate and
session manifest that already exclude the pilot sessions. This prevents a
pilot attempt from being spent on a mechanism that cannot advance to Tier 1.
Economic PASS decisions are recomputed against the immutable mechanism's
stage-specific stress, baseline, trade-count, breadth, drawdown, subgroup, and
formal-test gates; a bare PASS label is never sufficient.
Any row-certified preparatory census also consumes its own authorization even
when it times out or produces no report. A successor must use a new immutable
plan and approval; it may not silently reuse the consumed claim.
The cockpit remains observation-only and credentials remain outside Git.

## Phase 8 risk default

Phase 8 uses the Apex Trader Funding $50K EOD Performance Account as its current
risk profile. See `docs/PROP_FIRM_RISK.md`; the active, switchable parameters
are in `configs/prop_firm_risk_profile.json`.

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
