# Current workflow

This is the day-to-day guide for this repository. When another operational
document differs, this guide controls normal-work procedure; research evidence
requirements still apply to real research.

## Normal local work

Ask Codex to change, fix, build, implement, or complete an outcome. That request
authorizes inspection, editing, testing, non-research local artifacts, exact-path
staging, and a scoped local commit when needed to deliver the outcome. Preserve
unrelated work and never use broad staging. If an intended task path overlaps a
pre-existing user change, Codex asks once before staging or committing it. You
do not need to copy commands, hashes, plan IDs, or approval lines for ordinary
local work. A local commit never includes a push.

## High-risk work

Before provider/network use, a real-data read or evaluation, publication or
active-data mutation, installation/activation, deletion/cutover,
holdout/forward access, trading/order access, or push, Codex will ask once in
plain language. The request states the scope, impact or cost, outputs, and
preservation/rollback boundary. Replying `Approve` is sufficient.

## Certified final-evaluation boundary

The declared **Final Sealed 252-Session Holdout** identity `0ff48f99d8b6d3a262ddf0a060bea8e733fc95aa7c4b4d43f19a0f78b107d4d1` is preserved as historical custody evidence, but its exact active manifest and certification bytes are not locally or Git recoverable. Its current authority status is `UNRESOLVED_AUTHORITY_HOLDOUT_ACCESS_FORBIDDEN`; no holdout or forward value access is permitted. The Alpha ladder is rebound only to the last complete tracked authority, and that rollback neither reinterprets prior results nor creates a replacement holdout.

`PROJECT_OUTLINE.md` is the practical current pipeline reference. The previous
counted mechanism remains closed after Tier 0 ES failure, and the new mechanism
is not started. The family-aware Alpha ladder is active and remotely
recoverable; ZN or 6E remains unselected at the
`PENDING_PRE_RESULT_EXECUTION_GATE`. The next boundary is to freeze the
pre-result execution-design contract. That prepared contract grants no row-read,
selection, mechanism, evaluation, staging, or publication authority. The
causal-observation contract is frozen and its
observation-only safety implementation is built, validated, committed, and
remotely recoverable. The causal observation release is not built. The
seven-market development-only canary ran once under consumed authority,
passed independent verification, and remains unpublished and inactive. The
historical full-build attempts and V9 evidence remain immutable and grant no
fresh authority. V10 is the current non-active implementation: it processes
and maximum-robustness-certifies one complete market at a time in a frozen
41-market order, stores inactive output below
`data/causally_gated_normalized/v10/`, and cannot advance until the preceding
market certificate remains valid. Each completed year is sealed with exact
source, state, partition, and file identities; a fresh attempt may reuse only
an unchanged contiguous sealed-year prefix. Two independent certification
replays run in separate processes. The controller and provider-free production
rehearsal are synthetic-test proven. A dedicated ES-2025 canary successor is
implemented and passed under consumed authority: it bound seven exact
registered DBNs plus sidecars, performed one producer decode and one
fresh-process independent replay, wrote only below the V10 `_canary` lane, and
did not seed the complete ES checkpoint. The complete ES 2010-2025 checkpoint
and its two-pass maximum-robustness certification passed and remain inactive.
The complete GC 2010-2025 checkpoint also passed with all 16 years sealed and
remains inactive; GC maximum-robustness certification is the next gate. The
other 39 markets have not started. No V10 publication or activation has
occurred.

After all 41 individual market certificates and the inactive release-wide
certificate pass, prepare a separate annual active-view publication. The
research-facing layout is exactly
`data/active/causally_gated_normalized/{market}/{year}/{year}.parquet` with one
adjacent `{year}.parquet.manifest.json` per market-year. The annual materializer
must reconstruct only from the certified V10 artifacts, preserve exact logical
rows, causal and source identities, counts, ordering, and development
boundaries, and independently read back every annual file before activation.
Publication and activation remain separately approved, atomic, rollback-safe,
and catalog/pointer-last. The certified month-partitioned V10 checkpoint tree
remains immutable audit and recovery evidence; the active annual view does not
replace or authorize deletion of that evidence.

Real research still needs its durable trial declaration and immutable evidence.
Before registration, every row-dependent sample, fold, feature, execution,
baseline, cost, risk, metric, and promotion requirement needs a source-bound
readiness certificate produced under separate row-access authority. Synthetic
tests and aggregate coverage prove mechanics, not real-source readiness. The
certificate must name every exact preregistered fold and independently account
for each baseline, including a true zero-trade baseline and scenario-specific
risk abstentions. Its semantics and actual bound files must be revalidated
before a single-use historical execution claim is consumed.

The controlled next sequence is: freeze the execution-design contract; prepare
a one-use ZN/6E execution-proxy row-read authorization; execute that selector
only after approval; freeze the selected macro; separately define and freeze the
mechanism; run Stage 0 synthetic engineering; qualify Tier 0 ES; qualify Tier 1
NQ, CL, GC, and the selected macro; replicate through the exact Tier 2 set of 16
and Tier 3 set of 41; then separately recover and certify Final-252 before any
sealed-holdout access.
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
closed. The ladder successor governs future counted mechanisms only; it does
not reinterpret the previous mechanism, which remains closed and nonretryable.

Stage 0 is synthetic engineering. It can prove mechanics, causality,
accounting, and failure behavior, but makes no Alpha claim and reads no real
history. The complete material mechanism identity must be frozen before Tier 0.
Tier 0 is one row-certified ES pilot with 504 training and 63 evaluation
sessions. ES must pass; failure closes the mechanism, and NQ cannot rescue it.

Tier 1 evaluates one frozen incremental pack: NQ, CL, GC, and exactly one macro
market selected from ZN or 6E by a separately approved pre-result execution
gate. No macro is selected yet, so Tier-1 execution remains fail closed. ES and
NQ together provide at most one independent equity-family credit; NQ is a
correlated extension whose result remains individually visible. CL supplies
energy evidence, GC supplies metals evidence, and the selected ZN or 6E supplies
macro evidence. ES, CL, GC, and the selected macro must pass, every required
result must exist, the mechanism identity must match, and no between-market
tuning may occur. NQ failure makes NQ individually Alpha-ineligible but does
not erase otherwise successful independent-family evidence. No pooled score
can conceal a required market or family failure.

Tier 2 preserves the exact balanced 16-market pack and predecessor promotion
thresholds. Tier 3 preserves the exact 41-standard-market universe: 38
traditional markets plus BTC, ETH, and PA satellites. Traditional markets must
pass independently; satellites cannot rescue failure. The 17 deferred micros
remain disjoint, cannot rescue failure, and cannot create another holdout.
Every tier uses the same frozen mechanism, and material changes restart a new
counted mechanism at Stage 0.

Final historical evaluation is a future gate only. It is currently
`BLOCKED_FINAL_252_AUTHORITY_UNRESOLVED`; no manifest or active pointer exists,
and holdout or forward access is forbidden. Forward deployment consideration
requires a market to be independently Alpha-eligible and live-execution
eligible. OHLCV execution proxies do not certify bid/ask spread, depth, queue
position, market impact, or live fills. The 41 markets are a scientific
universe, not an automatic deployment list.
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

Preserve unrelated work; never use broad staging, and report only the decision
or blocker that needs attention.

## Current data boundary

Canonical source closure is complete and active. The canonical DBN source is
frozen and validated. The development-only causal-observation contract is
frozen, and its observation-only safety implementation is built, validated,
committed, and remotely recoverable, but the new clean-restart 41-market causal
observation release is not built. Raw DBN cannot feed features, models, WFA,
or backtests directly; it may feed only that exact approved causal builder.

Old derived releases and retired foundation runners remain no-read. Historical
canary and full-build authorizations are consumed; their inactive outputs and
V9 artifacts remain evidence only. The V10 market-by-market controller,
checkpoint writer, sealed-year recovery, independent market certifier, and
long-path-safe provider-free rehearsal are implemented. The ES-2025 canary and
complete ES 2010-2025 checkpoint and maximum-robustness certification passed
under consumed authority and remain inactive. The complete GC 2010-2025
checkpoint also passed under consumed authority and remains inactive; GC
maximum-robustness certification has not started. The other 39 markets and the
complete 41-market release remain unbuilt. Each later market row read,
certification, scheduler action, publication, and activation still requires the
applicable approval. The Final Sealed 252-Session Holdout and forward values
remain inaccessible.

When a milestone changes the active stage, status, or next goal, check
`PROJECT_OUTLINE.md`, `CURRENT_WORKFLOW.md`, and `README.md`; update only
affected files or record `NO_CHANGE` in the task report. Change `AGENTS.md`
only when durable policy changes.
