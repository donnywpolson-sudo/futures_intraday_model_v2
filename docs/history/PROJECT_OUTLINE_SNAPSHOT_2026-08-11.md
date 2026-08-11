# Historical PROJECT_OUTLINE snapshot — 2026-08-11

This document preserves the complete `PROJECT_OUTLINE.md` source body from
commit `f4a0444e92f80124c3340fd6ad81fc242953d2bc` before Phase 3 root-document reconciliation.

It is historical evidence and reference material only. It is non-authoritative:
it does not control normal work, replace `CURRENT_WORKFLOW.md`, grant research
authority, or authorize provider access, historical-row access, publication,
active-state mutation, installation, activation, live smoke, trading, order
placement, deletion, movement or renaming, staging, commit, or push.

The embedded source body records statements as they existed at the source
commit. This snapshot does not claim to be the current research runbook and
does not claim that those embedded historical statements remain current. Use
`CURRENT_WORKFLOW.md` for normal work.

Source path: `PROJECT_OUTLINE.md`
Source commit: `f4a0444e92f80124c3340fd6ad81fc242953d2bc`
Record date: `2026-08-11`

The exact preserved source body begins below.

<!-- BEGIN EXACT PROJECT_OUTLINE SOURCE BODY -->
# Futures intraday research project

## Objective

Operate a reproducible, point-in-time, bias-resistant research system with two
strictly separated Alpha lanes: the approved 41-market standard/full-contract
universe and a provider-profile-selected integer-micro universe. Both use immutable data
releases, explicit trial accounting, chronological validation, net economics,
one sealed 2025 holdout, post-freeze forward monitoring, and an observation-only
live cockpit. Automatic order execution is outside this project's scope.

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
- `state/unpublished_evidence/alpha_research_architecture_v2/<id>/architecture.json`:
  corrected dual-lane successor design; it is unpublished and grants no authority.
- `configs/apex_micro_tier01_databento_preflight_plan.json`: preserved obsolete
  MES/MGC/M6E/M6A preflight; it is superseded and cannot execute as current.
- `configs/apex_micro_tier01_databento_metadata_preflight_v2.json`: preserved
  executed metadata-only plan for MES, MCL, MGC, and M6E. Its one authorized
  attempt failed closed on a `list_schemas` read timeout after two calls; its
  report and consumed-authorization record are immutable evidence, not authority.
- `configs/apex_micro_tier01_databento_metadata_preflight_v3.json`: preserved
  unexecuted local preparation superseded before staging when its executor
  self-hash drifted; it cannot execute and contacted no provider.
- `configs/apex_micro_tier01_databento_metadata_preflight_v4.json`: immutable
  timeout successor executed once under its separate approval. It failed closed
  after three metadata calls because the valid provider dataset range contained
  nested schema ranges; its $0 report and consumed authorization are preserved.
- `configs/apex_micro_tier01_databento_metadata_preflight_v5.json`: immutable
  annual market-year successor executed once under its separate approval. It
  failed closed after four calls when the first `MES.FUT` parent symbology
  request used `2000-01-01` and received a provider client error; its $0 report
  and consumed authorization are preserved.
- `configs/apex_micro_tier01_databento_metadata_preflight_v6.json`: immutable
  executed provider-range-safe successor. Its one approved attempt failed
  closed at call four after the first successful resolve returned list-shaped
  `partial` and the local validator incorrectly used set membership. Its $0
  report and consumed authorization are preserved; it cannot run again.
- `configs/apex_micro_tier01_databento_metadata_preflight_v7.json`: immutable
  executed list-shape-safe successor. Its approved attempt passed the three
  initial metadata calls, then failed closed locally at the first resolve in a
  combined exact response-echo check that ran before the corrected empty-list
  checks. The sealed report contains no provider value; local SDK-contract
  evidence identifies the empty-message expectation as the bounded v8
  remediation candidate. Its $0 report and consumed authorization are
  preserved; it cannot run again.
- `configs/apex_micro_tier01_databento_metadata_preflight_v8.json`: immutable
  executed success-echo-safe successor. Its approved attempt reached the first
  broad MES parent resolve and failed closed on a nonempty symbology status,
  consistent with but not assumed to be a prelaunch gap. Its sanitized
  classifier mislabeled the affected field as `symbols`; its $0 report and
  consumed authorization are preserved and it cannot run again.
- `configs/apex_micro_tier01_databento_metadata_preflight_v9.json`: immutable
  executed two-stage prelaunch-discovery successor. Its approved attempt made
  four calls and proved the affected field was `partial`, then failed closed
  because its local validator required the provider's opaque nonempty list to
  equal the requested parent symbol exactly. Its $0 report and consumed authorization are
  preserved; it cannot run again.
- `configs/apex_micro_tier01_databento_metadata_preflight_v10.json`: immutable
  executed opaque-single-partial-safe successor. Its approved attempt made four
  calls and proved the affected `partial` list did not satisfy the local
  exact-one-entry ceiling. Its sealed report records neither contents nor exact
  cardinality; its $0 evidence and consumed authorization are preserved and it
  cannot run again.
- `configs/apex_micro_tier01_databento_metadata_preflight_v11.json`: immutable
  executed bounded opaque-partial-flag-safe successor. Its approved attempt
  made four calls and reached the first MES response's `status` field, then
  failed closed because the local validator incorrectly required exact integer
  zero. Its sealed $0 report records only the field name; the consumed
  authorization cannot run again.
- `configs/apex_micro_tier01_databento_metadata_preflight_v12.json`: immutable
  executed SDK-contract-safe status successor. Its approved attempt made four
  calls and reached the first MES response's `message` field, then failed closed
  because the local validator still guessed a two-value message allowlist. Its
  sealed $0 report records only the field name; the consumed authorization
  cannot run again.
- `configs/apex_micro_tier01_databento_metadata_preflight_v13.json`: immutable
  executed SDK-opaque-message-safe successor. Its approved attempt made four
  calls and reached the first MES response's `result` field, then failed closed
  because the local validator incorrectly required the result map to contain
  exactly the requested symbol as its sole group key. Its sealed $0 report
  records only the field name; the consumed authorization cannot run again.
- `configs/apex_micro_tier01_databento_metadata_preflight_v14.json`: immutable
  executed provider-result-group-safe successor. Its approved attempt made five
  calls, passed MES discovery and result-group validation, then failed closed
  because the post-effective parent response retained a nonempty bounded
  `partial` status. Its sealed $0 report records only the field name; the
  consumed authorization cannot run again.
- `configs/apex_micro_tier01_databento_metadata_preflight_v15.json`: immutable
  executed v15 gap-proof bounded-partial-safe predecessor. Its approved attempt
  made six metadata calls, passed MES discovery and post-effective parent
  validation, then failed closed at the strict interval-bound gate for
  `MES.v.0`. Its sealed report records only the field and price-free call
  context; it incurred $0 with zero retries, downloads, row reads, or DBNs.
- `configs/apex_micro_tier01_databento_metadata_preflight_v16.json`: immutable
  executed v16 interval-overlap-safe predecessor. Its approved attempt made
  seven metadata calls and passed MES discovery, parent, and continuous gates,
  then failed closed because MCL parent expansion returned a bounded group key
  outside the locally assumed market-root prefix. Its sealed report records only
  the field and price-free call context; it incurred $0 with zero retries,
  downloads, row reads, or DBNs.
- `configs/apex_micro_tier01_databento_metadata_preflight_v17.json`: immutable
  executed v17 bounded opaque-group-key predecessor. Its approved attempt made
  eight metadata calls, passed MCL parent discovery, and then failed closed
  because parent-family mapping intervals were incorrectly required to form one
  calendar-gap-free roll chain. Its sealed price-free report records only the
  bounded failure classification and call context; it incurred $0 with zero
  retries, downloads, row reads, or DBNs.
- `configs/apex_micro_tier01_databento_metadata_preflight_v18.json`: immutable
  executed v18 parent-family-aware predecessor. Its approved attempt made 13
  metadata calls, passed MES, MCL, and MGC symbology gates, then failed closed
  at M6E discovery because opaque `partial` presence was still interpreted as
  a prelaunch signal. Its sealed price-free report records only the bounded
  failure classification and call context; it incurred $0 with zero retries,
  downloads, row reads, or DBNs.
- `configs/apex_micro_tier01_databento_metadata_preflight_v19.json`: immutable
  executed v19 opaque-partial-semantic-safe predecessor. Its one authorized
  attempt made 15 metadata calls and verified all four parent/continuous
  mapping surfaces, then failed closed because M6E was already active at the
  provider dataset boundary and its earlier product-effective date could not be
  derived. The sealed report incurred $0 with zero retries, downloads, rows, or
  DBNs and cannot execute again.
- `state/unpublished_evidence/apex_micro_m6e_product_effective_date_source_v1/`,
  `state/unpublished_evidence/apex_micro_remaining_product_effective_dates_source_v1/`,
  and `src/futures_rebuild/micro_alpha_product_effective_dates.py`: sealed
  official CME primary-source evidence establishes listing/effective and first
  trade dates for all four markets: M6E 2009-03-22/23, MGC 2010-10-03/04,
  MES 2019-05-05/06, and MCL 2021-07-11/12. The fail-closed loader separates
  exchange launch evidence from Databento availability/continuity mappings.
- `configs/apex_micro_tier01_databento_metadata_preflight_v20.json` and
  `src/futures_rebuild/micro_alpha_databento_preflight_v20.py`: immutable
  executed cumulative predecessor. Its one authorized run reused the sealed
  v19 entitlement/range/symbology evidence and made 68 metadata calls before
  the `MES` `ohlcv-1s` 2020 annual billable-size request reached the 30-second
  per-call timeout. The create-only report records `PROVIDER_TIMEOUT`, $0,
  zero retries, downloads, rows, or DBNs. V20 cannot execute again.
- `configs/apex_micro_tier01_databento_metadata_preflight_v21.json`,
  `src/futures_rebuild/micro_alpha_databento_preflight_v21.py`, and
  `state/unpublished_evidence/apex_micro_metadata_preflight_v21/report.json`:
  immutable executed timeout-safe successor. Its one authorized metadata-only
  run completed all 20 full-range zero-cost proofs and 160 exact annual byte
  estimates in 180 calls, returned `PASS_METADATA_ONLY`, incurred $0, and made
  zero retries, downloads, row reads, or DBNs. The sealed report fixes the
  provider-complete end-exclusive date at 2026-08-09 and requires exact annual
  cost requotes immediately before any download. Its authorization is consumed.
- `src/futures_rebuild/micro_alpha_acquisition_v21.py`, its exact plan/audit,
  authorization use, and
  `state/unpublished_evidence/apex_micro_phase1a_acquisition_v21_failure/`:
  immutable consumed acquisition attempt. Its fresh cost census completed at
  $0, then the 7,200-second global runtime ceiling stopped download scheduling.
  Thirty-six complete DBN/sidecar staging pairs were hash-verified and sealed
  read-only as failed-attempt evidence; none was accepted or finalized, and no
  destination under `data/dbn` exists. V21 cannot retry or supply successor
  bytes.
- `src/futures_rebuild/micro_alpha_acquisition_v22.py`, its exact unexecuted
  plan/audit, v7 cleanup census, and supersession report: preserved preparation
  that exposed a self-referential cleanup snapshot. The census was created
  before its own three output paths appeared, so post-generation deterministic
  reconstruction failed closed. No authorization, provider call, download, or
  cleanup mutation occurred; v22 is superseded and cannot execute as current.
- `src/futures_rebuild/micro_alpha_acquisition_v23.py`, its exact unexecuted
  plan/audit, v8 cleanup census, and supersession report: preserved preparation
  whose plan and census reconstruct exactly, but whose audit self-hashed a
  volatile exact free-disk byte reading. The audit changed after unrelated
  filesystem writes. No authorization, provider call, download, or cleanup
  mutation occurred; v23 is superseded and removed from the current operation
  allowlist.
- `src/futures_rebuild/micro_alpha_acquisition_v24.py`, its exact plan/audit,
  consumed authorization, terminal, and verification-failure report: immutable
  executed acquisition attempt. All 160 annual requests completed at $0 with
  zero retries, yielding 160 DBNs, 160 adjacent sidecars, and 1,849,575,228 DBN
  bytes. The post-run verifier failed closed because v24 marked each hard-linked
  final read-only before removing its Windows staging alias; all 320 removals
  failed and each final remains a two-link file. No row was decoded and no
  catalog or pointer changed. V24 cannot execute again.
- `src/futures_rebuild/micro_alpha_custody_repair_v1.py`, its immutable plan,
  and the v1 supersession report: preserved unexecuted preparation. Audit found
  that v1 did not recheck every sealed implementation/evidence hash at
  execution, froze no sidecar manifest identity per alias, and verified DBN
  hashes only after alias removal. It is classified
  `SUPERSEDED_PREPARATION_INCOMPLETE_EXECUTION_BINDINGS`, has no authorization
  or terminal, and is removed from the current operation allowlist.
- `src/futures_rebuild/micro_alpha_custody_repair_v2.py`, exact plan/audit,
  consumed authorization, and terminal: the binding-complete no-network
  successor executed once on committed HEAD `e8598075...`. It removed only the
  exact 320 staging aliases, verified all 1,849,575,228 DBN bytes before and
  after without decoding, verified all sidecar identities, and left 160 DBNs
  plus 160 sidecars at single-link read-only final paths. Terminal state is
  `SUCCESS_INACTIVE_IMMUTABLE_CUSTODY_REPAIRED`; Phase 1A inactive custody is
  complete and the repair cannot execute again.
- `src/futures_rebuild/micro_alpha_phase1b2_preparation.py` and
  `configs/apex_micro_phase1b2_prepare_only_contract_v1.json`: exact source-safe
  Phase 1B/2 decoder, identity, roll, causal-availability, disposition,
  inactive-catalog, gateway, and Apex micro risk-gate contracts. They bind the
  completed custody terminal but grant no row-read, decoding, catalog,
  registration, evaluation, publication, or trading authority.
- `configs/apex_micro_product_reference_requirements.json`: explicit parent,
  schedule-family, identity, continuity, economics, prelaunch, and unavailable-
  source requirements for the current acquisition scope.
- `configs/source_contract.json`: accepted immutable source-family boundary.
- `configs/*.json` and `configs/*.yaml`: sessions, identity, costs, coverage,
  pipeline, audit, and packaging contracts.
- `manifests/**`: immutable release, approval, selection, and provenance
  metadata.
- `state/trial_registry/**`: pre-outcome trial declarations and attempt
  genealogy.

## Research discipline

- Establish source integrity, provenance, timestamp availability, actual
  contract identity, sessions, rolls, and economics before interpreting a
  model result.
- Never change targets, features, thresholds, markets, costs, or evaluation
  rules because a locked or observed result looks favorable.
- Preserve negative results, warnings, exclusions, failed attempts, and stopped
  branches in the trial genealogy.
- Compare complex candidates against simple causal baselines under the same
  split, cost, position, and risk rules.
- Synthetic runs verify mechanics only. They do not establish alpha, historical
  readiness, executable economics, or permission to access protected data.

## Data manifest and rule index

This section is the first lookup point; it does not duplicate contract
authority.

- `configs/source_contract.json` declares the only source families and roots
  that may be discovered.
- `configs/research_universe_contract.json` controls point-in-time market
  admission, cohorts, and eligibility.
- `configs/alpha_tiered.yaml` is a validated profile view of that universe.
- Foundation coverage, status-scope, session, identity, and economics contracts
  define acceptance for the causal foundation.
- `configs/historical_observability_policy.json` classifies historical
  foundation rows as immutable DBN observability. It does not establish
  official historical CME open, close, halt, pause, or holiday states.
- `configs/exchange_calendar_policy.json` reserves the activated official CME
  calendar for current/forward cockpit scheduling.
- `manifests/data_releases/**` contains content-addressed release descriptors;
  accepted payloads live in v2-owned immutable release roots.
- `state/trial_registry/**` declares every real-data attempt before outcomes are
  read. Run checkpoints and leases are recovery state, not readiness proof.
- `MASTER_AUDIT.md` and `configs/master_audit_v3/**` define evidence
  classification; they grant no execution authority.

## Active layout

```text
configs/                         durable contracts and operational profiles
data/vault/                      immutable v2-owned source snapshots/releases
data/dbn/                        accepted DBN source-family view
data/raw/                        Phase 1B immutable raw releases
data/causally_gated_normalized/  Phase 2 content-addressed immutable release history
data/active/causally_gated_normalized/ catalog-selected standard research view
data/outcome_sources/            outcome-capability inputs kept from features
data/outcomes/                   Phase 3 outcome releases
data/features/                   Phase 4 feature releases
data/predictions/                separately authorized Phase 6 OOS releases
data/evaluations/                separately authorized Phase 7-9 evidence
manifests/                       release, approval, and provenance metadata
src/futures_rebuild/             v2-native implementation
state/                           checkpoints, leases, ledgers, trial registry
tests/                           contract, synthetic, failure, and package tests
```

Staging, repair, cache, report, and recovery roots are evidence or working
state, never active inputs unless a content-addressed release and its governing
contract explicitly admit them. Other repositories and absolute external paths
are not active roots.

The two causally-gated folders have deliberately different roles; they are not
competing active sources. `data/causally_gated_normalized/` retains immutable
content-addressed Phase 2 generations and their receipts. Only
`data/active/causally_gated_normalized/` is the flattened standard-lane view,
and it is usable only through `data/active/catalog.json`; direct archive globs
are forbidden. The source-safe audit at
`state/unpublished_evidence/standard_data_topology_source_safe_audit/report.json`
checks the catalog, sidecars, validation receipts, and Phase 1A/1B/2 release
bindings without opening DBN or parquet payloads. It confirms provenance
metadata, not a new row-level recertification.
The successor audit at
`state/unpublished_evidence/data_topology_source_safe_audit_v2/report.json`
also inventories every top-level `data/` root from filesystem metadata only,
binds the completed micro custody terminal, confirms that the micro pointer and
catalog remain absent, and classifies zero data roots as cleanup candidates.

Cleanup is a separate governed boundary. Cleanup preparation v4 remains
preserved but retired because its dynamic prepared-HEAD record became stale
after the approved implementation commit. The current prepare-only policy at
`state/unpublished_evidence/safe_cleanup_preparation_v5/plan.json` classifies
active/catalog paths and immutable release history as preserve-only, keeps
vault staging and snapshot evidence under manual review, and freezes no cleanup
candidate or execution HEAD. It performs no move or delete. Immediately before
any future cleanup, after all prior writes finish, build the exact literal
candidate and execution-HEAD census,
prove that no catalog, manifest, receipt, plan, or worktree item binds a target,
obtain a separate exact cleanup approval, and rerun catalog/provenance/tests and
micro disk/destination gates afterward. Cleanup that could affect acquisition
paths or free-disk state must finish before the final micro acquisition plan is
frozen. After the passing v21 metadata run, both the source-safe standard
topology report and cleanup v5 policy reconstructed exactly with no row reads,
candidate freeze, move, delete, or relabel. Cleanup therefore remains a later
separately approved boundary and does not create a second active data source.
`scripts/prepare_safe_cleanup_candidate_census_v6.py` froze 13 exact untracked,
Git-ignored cache-directory candidates after the acquisition successor commit.
The create-only census is bound into the download-plan audit and records no row
read or cleanup mutation. It cannot delete, move, relabel, or authorize any
cleanup target; execution still requires a separate exact cleanup approval and
fresh revalidation.
The v21 failed-attempt staging tree is excluded from cleanup candidates and is
preserved read-only. A v7 census successor is prepared locally to bind the
sealed failure report while retaining the same no-mutation boundary; its
v7 output is preserved with the unexecuted v22 preparation. V8 and the exact
v23 outputs are preserved with the volatile-capacity supersession evidence. V9
provides the last exact cache-candidate snapshot. It is preserved as historical
prepare-only evidence, not current cleanup authority. After Phase 1A repair,
the v2 topology audit confirms that no `data/` root should be merged, moved,
deleted, or relabeled. Any cache cleanup remains last, requires a fresh exact
candidate manifest after all project writes finish, and needs separate approval.

## Alpha research lanes

The standard/full-contract lane and Apex integer-micro lane never share
catalogs, registrations, or promotion evidence. They do share one project-level
sealed 2025 holdout claim: changing contract scale never grants a second 2025
access. A registration must bind one exact lane, ladder, calendar, catalog, and
contract scale. A source from the other lane fails closed.

### Standard/full-contract 41-market lane

The current ladder is loaded only through
`configs/active_alpha_research_ladder.json`. That pointer hash-binds the
authoritative successor contract and profile. `configs/alpha_tiered.yaml` and
`configs/research_universe_contract.json` are retained predecessor views; they
remain useful for synthetic compatibility checks but do not authorize current
research.

- `tier_0`: synthetic ES engineering plus one locked 504/63 ES qualification
  fold. Both gates must pass; this is not multi-market confirmation.
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

The standard lane uses 2018-2022 for primary research and reports 2023-2024 as
temporal replication rather than tuning material. Its one 2025 access remains
sealed until the frozen candidate reaches its declared terminal tier. Forward
monitoring begins only after the exact mechanism is frozen; the calendar year
alone cannot make a period forward evidence.

### Micro-futures integer lane (legacy Apex source lineage)

The micro successor has complete inactive Phase 1A raw custody and one preserved
failed-closed Phase 1B/2 attempt. That attempt created all 120 expected inactive
Phase 1B Parquets for 2018-2024. A later separately approved full successor
certified all 24 five-schema intervals and created 24 hash-bound causal
one-minute Phase 2 Parquets, an unpublished source certificate, and an inactive
catalog candidate under create-only inactive custody.
A later one-source diagnostic created one separate inactive M6E 2018 causal
Parquet and proved only the bounded materializer. A later five-schema diagnostic
opened the five M6E 2018 Phase 1B Parquets and passed its transition mechanics,
but correctly refused certification because the legacy definition key counted
308 consecutive repeats. The separately approved definition-only diagnostic
then proved that all 308 repeats have identical retained semantics and that
zero are distinct same-key updates; it preserved every row and reported counts
only. The full successor reconstructed that classification for every interval,
preserved all retained-semantics repeats, and deduplicated zero Phase 1B rows.
The certified 2018-2024 source bytes were subsequently published through 144
immutable manifests and admitted to the dedicated legacy source catalog. They
are active as source data only: no mechanism is frozen and no registration,
evaluation, holdout/forward access, or trading authority follows.
The machine-local accepted legacy pointer is
`configs/active_micro_alpha_research_ladder.json`. Future namespace migration is
prepared under `micro_futures_*` and requires a separate active-data cutover.
Neither this ignored pointer nor its ignored catalog is a tracked-checkout or
canonical-current-test dependency; exact checks belong to `local_evidence`.

- `tier_0`: MES synthetic engineering and one locked MES qualification fold.
- `tier_1`: MES, MCL, MGC, and M6E. These four represent equity, energy,
  metals, and FX.
- `tier_2`: report the four-market core separately from the five additions
  MNQ, MYM, M2K, M6A, and SIL.
- `tier_3`: add MBT and MET for 11 markets. The nine traditional markets must
  be reported separately and pass independently; the two crypto satellites
  cannot rescue their failure.
- `holdout`: raw 2025 bytes may enter inactive custody, but the one shared
  project-level 2025 claim remains sealed against decoding, features, execution
  analysis, and catalog activation.
- `forward`: raw 2026 bytes may enter inactive custody. Decoding is blocked for
  rows before the exact immutable mechanism-freeze timestamp; calendar year
  alone never establishes forward evidence.

Micro products use integer contracts only. Product-effective dates require
official CME listing/effective-date evidence; Databento symbology separately
proves availability and roll continuity and cannot substitute its first mapping
date for an exchange launch date. Prelaunch coverage remains explicit
`PRODUCT_NOT_YET_EFFECTIVE_NO_EMPTY_DBN` evidence. No market inherits its parent
contract's calendar or economics implicitly. The current acquisition scope is
Tier 0/1 only: MES, MCL, MGC, and M6E. No micro equivalent of ZN is invented;
any future micro rates candidate requires official Apex eligibility, provider
availability, and economics verification before outcomes.

Profiles can narrow but cannot silently expand the universe, change admission
or selection eligibility, or unlock holdout/forward data.

## Synthetic Phase 1A-11 mechanics

| Phase | Purpose | V2 interface | Main output |
| --- | --- | --- | --- |
| 1A | Preflight exact provider requests; ingest and verify immutable DBN/sidecar pairs | `futures-pipeline phase1a` | DBN release manifests and acquisition evidence |
| 1B | Convert accepted DBNs and independently reconcile rows, schemas, definitions, hashes, and sidecars | `futures-pipeline phase1b` | immutable raw releases and ingest reports |
| 2 | Build point-in-time causal, trade-date-grouped, actual-contract data from observed DBN rows | `futures-pipeline phase2` | empirical-observability causal foundation releases |
| 3 | Build outcomes with explicit entry lag, horizon, maturity, and unresolved states | `futures-pipeline phase3` | separate labeled/outcome-source releases |
| 4 | Build leakage-audited causal feature matrices without outcome access | `futures-pipeline phase4` | immutable feature releases |
| 5 | Freeze nested chronological split plans with purge and embargo | `futures-pipeline phase5` | split-plan manifests |
| 6 | Run separately approved WFA builders and materialize OOS predictions | `futures-pipeline phase6` | sealed prediction releases |
| 7 | Audit saved prediction identity, coverage, abstention, and signal quality | `futures-pipeline phase7` | prediction-audit reports |
| 8 | Evaluate net economics, baselines, portfolio/risk, and promotion eligibility | `futures-pipeline phase8` | model-selection and risk reports |
| 9 | Run bounded registered robustness, negative-control, and statistical-validity tests | `futures-pipeline phase9` | research-audit reports |
| 10 | Seal an explicitly approved candidate and its complete serving bundle | `futures-pipeline phase10` | immutable candidate bundle/receipt |
| 11 | Guard one authorized locked-holdout or forward evaluation using only the sealed bundle | `futures-pipeline phase11` | guarded evaluation evidence |

The public CLI is generated synthetic mechanics only. Synthetic mode
executes the complete dependency order while retaining zero provider, alpha,
prediction, sealing, holdout, and order authority. Any production adapter must
check the corresponding exact receipt before reading protected data or writing
an authoritative artifact.

## Preserved legacy micro Phase 1A/1B/2 route

This route reuses the standard folder grammar without mixing catalogs:

```text
v2 metadata-only Databento preflight -> FAIL_CLOSED_METADATA_ONLY (2 calls; $0; no rows)
  -> v3 local preparation superseded before staging/execution
  -> v4 preflight -> FAIL_CLOSED_METADATA_ONLY (3 calls; valid nested range rejected)
  -> v5 preflight -> FAIL_CLOSED_METADATA_ONLY (4 calls; first broad-range symbology request rejected)
  -> v6 preflight -> FAIL_CLOSED_METADATA_ONLY (4 calls; local list-shape validator defect)
  -> v7 preflight -> FAIL_CLOSED_METADATA_ONLY (4 calls; over-strict success echo)
  -> v8 preflight -> FAIL_CLOSED_METADATA_ONLY (4 calls; broad prelaunch status rejected)
  -> v9 preflight -> FAIL_CLOSED_METADATA_ONLY (4 calls; opaque partial content compared locally)
  -> v10 preflight -> FAIL_CLOSED_METADATA_ONLY (4 calls; exact-one partial ceiling rejected)
  -> v11 preflight -> FAIL_CLOSED_METADATA_ONLY (4 calls; guessed exact-zero status semantic rejected)
  -> v12 preflight -> FAIL_CLOSED_METADATA_ONLY (4 calls; guessed message allowlist rejected)
  -> v13 preflight -> FAIL_CLOSED_METADATA_ONLY (4 calls; exact-single-result-key assumption rejected)
  -> v14 preflight -> FAIL_CLOSED_METADATA_ONLY (5 calls; post-effective parent partial rejected)
  -> v15 preflight -> FAIL_CLOSED_METADATA_ONLY (6 calls; strict continuous interval bound rejected)
  -> v16 preflight -> FAIL_CLOSED_METADATA_ONLY (7 calls; MCL expanded group-root assumption rejected)
  -> v17 preflight -> FAIL_CLOSED_METADATA_ONLY (8 calls; parent family misclassified as one roll chain)
  -> v18 preflight -> FAIL_CLOSED_METADATA_ONLY (13 calls; opaque discovery partial presence interpreted)
  -> immutable v19 opaque-partial-semantic-safe successor -> FAIL_CLOSED_METADATA_ONLY (15 calls; exchange launch date unresolved)
  -> sealed official CME launch dates for MES/MCL/MGC/M6E
  -> v20 cumulative successor -> FAIL_CLOSED_METADATA_ONLY (68 calls; annual size timeout)
  -> v21 timeout-safe successor -> PASS_METADATA_ONLY (20 cost ranges + 160 annual size estimates; 180 calls; $0)
  -> exact v21-bound annual acquisition plan -> FAIL_CLOSED (runtime ceiling; 36 staged pairs; 0 accepted)
  -> non-resuming v22 successor -> SUPERSEDED_PREPARATION (self-referential cleanup census; no execution)
  -> reconstruction-stable v23 successor -> SUPERSEDED_PREPARATION (volatile free-disk audit snapshot; no execution)
  -> volatile-capacity-safe v24 successor (160 downloads complete; verifier rejected hard-linked finals)
  -> exact no-network staging-alias custody repair -> SUCCESS (320 aliases removed; finals single-link)
  -> Phase 1B/2 v3 -> FAIL_CLOSED (120 inactive Phase 1B Parquets; 0 Phase 2)
  -> first-interval causal diagnostic -> PASS (one separate inactive M6E 2018 Parquet)
  -> first five-schema group diagnostic -> PASS_MECHANICS / DUPLICATE_DISPOSITION
  -> definition repeat-semantics diagnostic -> PASS (308 exact; 0 distinct; no deduplication)
  -> exact-duplicate-safe full Phase 2 successor -> SUCCESS_CERTIFIED_INACTIVE_PHASE2
  -> data/dbn/<schema-folder>/<market>/<year>/<start>_<end>.dbn.zst [Phase 1A]
  -> adjacent <same-name>.manifest.json                              [Phase 1A]
  -> data/raw/<market>/<year>/<interval>/<release>/                  [Phase 1B definition + 1m]
  -> data/market_state/{status|statistics}/<market>/...              [Phase 1B diagnostics]
  -> data/outcome_sources/<market>/...                               [Phase 1B execution]
  -> data/causally_gated_normalized/...                             [Phase 2 causal 1m foundation]
  -> separately certified legacy micro source catalog              [machine-local evidence; active source only]
```

The required Databento Standard historical schemas are `definition`, `status`,
`statistics`, `ohlcv-1m`, and `ohlcv-1s`. They must not be collectively called
raw L0 data. Provider schemas `ohlcv-1m` and `ohlcv-1s` use the on-disk folder
names `ohlcv_1m` and `ohlcv_1s`. Definitions use `<root>.FUT`; the other four
schemas use `<root>.v.0`;
`stype_out` is `instrument_id`. One-minute bars feed the causal feature
foundation. One-second reported bars remain a separate, non-feature execution
source. Status and statistics are diagnostics, not alpha features. Decoding
2025 is blocked; 2026 decoding requires row timestamps at or after a prior
immutable mechanism freeze.

Phase 1A uses the existing standard/full-contract DBN tree, not a parallel
micro data root. Every market x schema x calendar year receives one distinct
DBN and one adjacent immutable sidecar. A product launch year starts on its
official CME-confirmed effective date, the latest year ends on the frozen provider-complete
end-exclusive date, and full intervening years use January 1 boundaries.
Prelaunch intervals produce disposition records and no fabricated empty DBN.
Multi-year DBNs, wrong-year folders, hyphenated schema folders, duplicate
destinations, and alternate micro-root layouts fail closed.

The v21-bound Phase 1A downloader was implemented, tested, plan-audited, and
executed once under its separate approval. Its plan ID is
`a21652882790dfe2a9d56ebce9edab7b223e5d29d49af7edcae2774e3517899b`
and its audit ID is `33c4a63c5b4a6dc371ed5816be2c170b8c64a6eb4efe36c2b8a7d1ff2d846707`.
It used one exact bounded interval per market/schema/year, wrote first to
inactive staging, requoted every request at exactly $0 before the first
download, and used at most two isolated Databento download clients. The run
completed its 160 zero-cost checks and 36 downloads before the global
7,200-second ceiling failed closed. Exactly 36 complete staging pairs
(512,142,314 DBN bytes) plus terminal evidence are preserved read-only; zero
pairs were accepted or finalized and all intended final paths remain absent.
No DBN row or 2025/2026 payload was opened. The consumed authorization cannot
be retried, and its staging bytes cannot be resumed or promoted.

The separately approved v24 successor then redownloaded all 160 exact annual
requests under the longer bounded runtime. It completed 160 fresh $0 cost calls
and 160 downloads using three provider clients and at most two workers, with
zero retries. Its terminal reports 160 DBNs, 160 sidecars, and 1,849,575,228 DBN
bytes, and confirms zero decoding, payload row access, activation, publication,
registration, evaluation, or trading. Final verification nevertheless failed
closed: the create-only finalization used hard links and marked the final names
read-only before deleting their staging aliases. Windows rejected all 320 alias
deletions, leaving exact two-link final/staging pairs. Those bytes remain
inactive and preserved, so v24 itself remained fail-closed. The separately
approved v2 no-network custody repair subsequently removed all 320 exact
staging aliases, verified DBN hashes and sidecar identities before and after,
and proved every final is read-only with link count one. Phase 1A inactive
custody is therefore verified complete without any row decoding.

V22 preserved those non-resuming protections, but its unexecuted post-commit
plan preparation exposed a local determinism defect: v7 recorded the worktree
before the plan, audit, and census outputs themselves appeared. The create-only
v22 artifacts are preserved and superseded without provider access. V23 fixed
that snapshot defect, but its unexecuted audit included an exact live free-disk
byte reading inside its self-hash; the plan and census reconstructed while the
audit changed after unrelated filesystem writes. The exact v23 artifacts are
also preserved and superseded without provider access. V24 keeps the 900-second
per-download bound, $0 cost, 320-call ceiling, one attempt, zero retries,
11,350,292,377-byte ceiling, at most two isolated download clients, and the
43,200-second global ceiling. V9 excludes only the three exact declared
create-only successor outputs from worktree comparison and binds them through
their hashes instead. The audit records the durable required-free-space value
and pass decision but not the volatile observation; live space is rechecked at
creation and immediately before execution. Warning messages remain discarded,
warnings cannot certify or activate source data, and partial final links remain
rollback-safe.

The executed v2 preflight fixed no 2026 end date: it failed closed at the
second metadata call when the deliberately narrowed 10-second SDK timeout was
reached. It incurred $0, performed zero retries and zero timeseries downloads,
read no rows, and created no DBNs. Its create-only report and consumed
authorization remain unpublished evidence. An unexecuted v3 local preparation
was preserved and superseded when its executor self-hash drifted before
staging. The separately approved v4 attempt also incurred $0, used zero retries,
performed no download or row read, and failed closed after three metadata calls
because its flat-range parser rejected the provider's nested schema ranges.
The separately approved v5 attempt validated the nested range, then failed
closed at deterministic call four: the first `MES.FUT` parent symbology request
used `2000-01-01` and the provider returned `BentoClientError`. The sealed
price-free report intentionally records no provider message. It incurred $0,
used zero retries, performed no download or row read, and created no DBN. V6
uses the provider-confirmed dataset start for all parent and continuous
symbology requests, records only a price-free HTTP status and bounded call
context on failure, and fails explicitly if that provider start truncates an
exact product effective date. It retains the same 20 definitions, at most 180
annual estimates, fixed 371-call ceiling, 300-second runtime, 30-second
per-call bound, $0 cost, and zero retries. The one approved v6 attempt reached
the first resolve at call four and then failed locally because Databento's
list-shaped `partial` field was tested against a set, raising `TypeError`. It
made no download, read no rows, created no DBN, and its sealed report and
authorization are preserved. V7 corrects only that local response contract:
`partial` and `not_found` must each be exact empty string lists; malformed or
nonempty values fail closed without recording their contents. Its one approved
run made four calls and failed locally after the first resolve in the combined
pre-list exact response-echo tuple. The report intentionally does not contain
the provider value; the installed SDK contract and deterministic synthetic
reproduction isolate the empty-message expectation as the bounded correction,
not as a recorded provider fact. It incurred $0, made no download, read no
rows, and created no DBN. V8 preserves v7's list
checks, requires integer status zero, permits only the exact empty-or-`OK`
success-message allowlist, and emits sanitized field-specific failures without
recording provider values. Its one approved run made four calls and failed
closed when the broad MES parent query returned a nonempty symbology status;
the sanitized field classifier reported `symbols`, so the report does not
claim whether the provider field was `partial` or `not_found`. V9 corrects the
diagnostic order and requires two-stage proof: a discovery query may accept
only the exact requested parent as a single `partial`, then parent and
continuous queries beginning at the discovered date must both have empty
status lists. A discovery beginning at the provider dataset start remains an
unresolved exact-product-date failure. The maximum call ceiling is 375, with
the same 300-second runtime, 30-second call timeout, $0 cost, and zero retries.
Its one approved attempt made four calls and failed closed at the first MES
discovery resolve because the response contained a nonempty `partial` string
list that did not equal the exact requested-symbol singleton. The
report records only the field name, never its value; it incurred $0, made no
download, read no rows, and created no DBN. V10 corrects only that local
assumption. Under an exact one-symbol request, exact response `symbols` echo,
and exact single result key, discovery permits one opaque string entry by
cardinality and never records its content. Two or more entries, any non-string,
any `not_found`, echo drift, result-key drift, or post-effective nonempty status
still fails closed. V10 retains the exact v9 markets, schemas, 375-call ceiling,
300-second runtime, 30-second call timeout, $0 cost, zero retries, and
metadata-only surface. Its one approved attempt made four calls and failed
closed at the first MES discovery resolve because the opaque exact-string
`partial` list did not satisfy the one-entry ceiling. The sealed report records
only the field name, not contents or exact cardinality; the run incurred $0,
made no download, read no rows, and created no DBN. V11 treats a bounded
nonempty discovery list only as a boolean prelaunch status. It independently
requires the exact one-symbol echo and result key, a first mapping date later
than the discovery start, empty `not_found`, and then empty parent and
continuous status lists from the derived date. Malformed lists, more than
10,000 opaque entries, echo or result drift, inconsistent dates, or any
post-effective gap still fail closed. Neither contents nor exact cardinality
are reported. V11 retained the exact v10 markets, schemas, 375-call ceiling,
300-second runtime, 30-second call timeout, $0 cost, zero retries, and
metadata-only surface. Its approved attempt made four calls and failed closed
at the first MES discovery response because the local validator incorrectly
treated the SDK-opaque application `status` field as an exact-zero success
echo. The sealed report records only the field name, incurred $0, made no
download, read no rows, and created no DBN. V12 corrects only that local
assumption: it requires an exact bounded integer or nonempty-string scalar
shape, records only the shape class, and relies on the installed SDK's HTTP
error rejection plus the existing exact echo, result, interval, message,
prelaunch, and post-effective gates for success. Malformed, boolean, floating,
empty, or unbounded status values fail closed. V12 retained the exact v11
markets, schemas, 375-call ceiling, 300-second runtime, 30-second call timeout,
$0 cost, zero retries, and metadata-only surface. Its approved attempt made four
calls and failed closed at the first MES discovery response because the local
validator still treated the SDK-opaque `message` field as an exact `""` or
`"OK"` allowlist. The sealed report records only the field name, incurred $0,
made no download, read no rows, and created no DBN. V13 corrects only that local
assumption: it requires an exact bounded string shape, records only whether the
string is empty or nonempty, and never records its content. The installed SDK's
HTTP error rejection plus exact echo, result, interval, status, prelaunch, and
post-effective gates remain the success basis. Non-string or over-1,024-character
messages fail closed. V13 retained the exact v12 markets, schemas, 375-call
ceiling, 300-second runtime, 30-second call timeout, $0 cost, zero retries, and
metadata-only surface. Its approved attempt made four calls and failed closed
at the first MES discovery response because the local validator required the
`result` map to contain exactly `MES.FUT` as its sole key. The sealed report
records only the `result` field name, incurred $0, made no download, read no
rows, and created no DBN. V14 corrected only that local assumption: it retained
the exact request echo as the request binding, permits only bounded
market-root-consistent result groups, validates every positive instrument ID
and bounded mapping interval, and records neither result-group keys nor
identity values. Unrelated roots, malformed or duplicate intervals, invalid
identities, excessive groups, and excessive intervals fail closed. Its approved
attempt made five calls, passed the discovery result-group gate, and failed
closed at post-effective MES parent verification because the response retained
a bounded opaque `partial` status. The sealed report records only the field
name, incurred $0, made no download, read no rows, and created no DBN. V15
corrected the empty-only assumption without treating `partial` as success: its
contents and exact count remained unrecorded, while the validated mapping
interval union had to cover the exact post-effective query continuously through
the end-exclusive bound. Its approved attempt made six calls, passed MES
discovery and post-effective parent validation, then failed closed at the
strict interval-bound gate for the `MES.v.0` continuous response. The sealed
report records only the affected field and price-free call context. V16 did not
infer that an interval spanning a query boundary was missing coverage. It
required exact request echoes, exact `d0`/`d1`/`s` entry fields, positive
identities, positive ISO ranges, query overlap, and a gap-free clipped union.
Its approved attempt made seven calls and passed all three MES symbology gates,
then failed closed because MCL parent expansion returned a bounded group key
outside the locally assumed market-root prefix. V17 removed that unsupported
group-key assumption while retaining exact echoes, fields, identities, bounds,
and continuous coverage. Its approved attempt made eight calls, passed MCL
parent discovery, and failed closed because the expanded parent family was
incorrectly treated as a single calendar-gap-free roll chain. V18 separates the
two semantics without weakening roll continuity: parent expansion must prove
valid identities and coverage at both query boundaries, while only the
continuous `<root>.v.0` mapping must prove a gap-free clipped interval union.
Neither group keys nor raw interval values are recorded. V18’s approved attempt
made 13 calls, passed MES, MCL, and MGC symbology gates, then failed closed at
M6E discovery because opaque `partial` presence was still interpreted as a
prelaunch signal. V19 removed that semantic assumption and verified all four
parent-family and continuous-roll mappings, but its approved attempt then failed
closed after 15 calls because M6E was active at Databento's 2010-06-06 dataset
boundary and the earlier exact launch date remained unresolved. A bounded
official CME lookup subsequently established M6E's listing/effective date as
2009-03-22 with trade date 2009-03-23. A second bounded CME-only lookup sealed
the remaining dates: MGC 2010-10-03/04, MES 2019-05-05/06, and MCL
2021-07-11/12. Those comparisons prove first provider
mapping intervals are availability/continuity evidence, not exchange launch-date
evidence. V20 bound both source reports and the complete v19 evidence, did not
repeat the 15 passed metadata/symbology calls, and exposed only annual zero-cost
and billable-size queries. Its authorized run failed closed after 68 metadata
calls when the MES `ohlcv-1s` 2020 billable-size request exceeded the 30-second
per-call bound. The executed v21 timeout-safe successor preserved that immutable failure and retained 160
annual market-schema intervals: MES 8 years x 5 schemas, MCL 6 x 5, MGC 9 x
5, and M6E 9 x 5. It replaces duplicate annual cost checks with 20 zero-cost
full-range proofs whose annual subsets are still requoted exactly immediately
before download. Six isolated clients completed exactly 180 calls under the
same 300-second total ceiling and a 90-second per-call bound. The create-only
report is `PASS_METADATA_ONLY`, fixed the end-exclusive date at 2026-08-09,
estimated 10,318,447,616 bytes, froze an 11,350,292,377-byte ceiling and a
12,424,034,201-byte free-disk requirement, and found zero destination
conflicts. Prelaunch intervals are explicit no-DBN dispositions. Any malformed source,
unapproved host, identity drift, mapping-date substitution, malformed provider
response, nonzero cost, disk shortfall, destination collision, or other existing
gate failure remains fail closed. The deterministic v21 plan was bound to exact
HEAD `a89fa8f3f31423a5422f008846cdac35a34b3355`; its separately approved
authorization is consumed. The run made 160 cost calls and 36 download calls at
$0 before the global runtime ceiling stopped the next request. It made zero
 retries, accepted and finalized zero pairs, decoded zero rows, and changed no
 catalog or active pointer. The unexecuted v22 and v23 plans are superseded. V24
 then executed once under its own exact approval and its authority is consumed.
 All downloads completed, but the canonical verifier rejected the remaining
 staging/final hard links. A later separately approved, exactly bound v2 repair
 removed only those 320 aliases at $0 with zero network calls, retries, or row
 decoding. Its terminal verifies single-link inactive custody, so Phase 1A is
complete while its raw DBNs remain outside research use and the later Phase 2
foundation remains outside active research use.

The lane-scoped Phase 1B/2 executor is now implemented as an offline-only,
inactive-staging route for exactly the 120 annual 2018-2024 DBNs and adjacent
sidecars (1,232,883,585 compressed source bytes). Its source-safe planner
reconciles 140 market-schema-year cells: 120 row sources plus 20 explicit
prelaunch dispositions. Before the first DBN decode it revalidates every
sidecar, annual query and path, custody identity, byte count, product date, and
all 120 source hashes. It rejects 2025/2026 by year before payload access.

The first immutable create-only plan and source-safe audit bound committed HEAD
`3a2bfb60414491be2d6fb39ffab0af28a09b7828`. Its separately approved execution
failed closed during central receipt verification because the exact foundation
operation was missing from the preparatory-operation allowlist. The receipt
was not consumed; no output root, source hash, DBN row, or authorization-use
record was created. That plan and audit are preserved as superseded evidence.
The bounded correction adds only this exact operation to the central allowlist;
an alias remains rejected. The v2 immutable plan and audit then bound committed
HEAD `a3dcd8671fc3a69e5b38515ddc176588e36f1a53`. Its separately approved
single attempt consumed authorization and verified all 120 eligible source
hashes, but failed before completing any decode because the first staged
`.partial` path was 299 characters. It created zero Parquet files and bytes,
opened no 2025/2026 payload, and wrote terminal failure evidence last. V2 is
preserved and cannot be retried.

The prepared v3 remediation retains each full 256-bit scope and release identity
in the plan, receipts, certificates, and catalog candidate while using its
collision-checked first 24 hexadecimal characters only as the filesystem path
alias. All 120 Phase 1B aliases and 24 Phase 2 aliases must be unique, and the
longest staged `.partial` path must be at most 240 characters. The committed v3
plan passed that gate at 239 characters. Its separately approved attempt then
verified all source hashes and completed all 120 Phase 1B Parquet outputs
(6,627,486,838 created bytes), proving the path remediation. It failed closed
after Phase 1B and before the first completed Phase 2 output. Zero retries,
Phase 2 files, certification reports, catalog candidates, sealed-year reads,
provider calls, publication, or activation occurred. The exact Phase 1B files,
authorization use, terminal, plan, and audit are preserved; v3 cannot rerun.

A separately approved one-source diagnostic bound the exact inactive M6E 2018
one-minute Phase 1B Parquet and materialized one separate inactive causal
Parquet under a 1 GiB output, five-minute, one-attempt, zero-retry ceiling. It
passed with 284,373 rows and no source nulls, opened no DBN or second Parquet,
and reported no source values. That result proves the causal materializer and
bounded path only; it does not certify the five-schema identity, economics,
roll, receipt, catalog, or research gates.

The separately approved v2 group diagnostic opened only the five preserved M6E
2018 Phase 1B Parquets (definition, status, statistics, one-minute bars, and
one-second bars), totaling 86,344,286 bytes. It passed the bounded transition
mechanics, serialized all five interval receipts, verified all five hashes
before and after, and created no Phase 2 Parquet. Certification remained closed:
the legacy definition duplicate key `(ts_recv_ns, instrument_id, raw_symbol)`
counted 308 consecutive repeats while every other schema counted zero.

The separately approved v3 definition diagnostic bound only the 68,274-byte M6E
2018 definition Parquet and the sealed group result. It classified all 308
legacy-key repeats as exact retained-semantics duplicates, found zero distinct
same-key updates, preserved all 1,481 Phase 1B rows without deduplication, and
reported no raw values or keys. It opened no DBN, second Parquet, or 2025/2026
payload and created no Parquet.

The v4 full Phase 2 successor bound all 120 preserved Phase 1B Parquets /
6,627,486,838 bytes, 140 coverage cells, 24 five-schema intervals, and 24 causal
one-minute outputs. Its separately approved single attempt completed all 120
certification scans and all 24 materialization passes. Every interval classified
its definition repeats as exact retained-semantics duplicates, all 24 groups
passed identity/economics certification, and per-interval plus cross-year
rank-zero roll continuity passed. The executor preserved the repeats, performed
zero Phase 1B deduplication, and wrote 24 read-only inactive Phase 2 Parquets /
454,578,644 bytes. All output sizes and SHA-256 identities reconcile to the
unpublished source certificate and terminal evidence.

The successful run used at most two workers, 100,000-row batches, one attempt,
zero retries, zero provider calls, and $0 external cost. It opened zero DBNs and
zero 2025/2026 payloads, used status/statistics only as diagnostics, retained
one-second semantics as reported-trade bars only, and created no features,
outcomes, predictions, returns, or evaluation. Terminal evidence was written
last. The source certificate and catalog candidate were consumed by the
successful legacy publication. When present on an evidence-bearing machine,
`data/active/catalogs/apex_micro.json` and
`configs/active_micro_alpha_research_ladder.json` are immutable active
source-catalog lineage. They are not required by a clean tracked checkout or
the canonical current lane. The proposed generic catalog and pointer do not exist.

The next naming boundary is the prepared create-only migration to the generic
micro-futures catalog and pointer; it is a separately controlled active-data
cutover. Mechanism Tier 0, registration, and economic evaluation remain
separately controlled. Official micro
commission verification remains a mechanism-freeze blocker. The shared 2025
holdout and pre-freeze 2026 rows remain sealed, and any filesystem cleanup still
requires a fresh exact candidate manifest and separate approval.

The bounded legacy micro publisher completed its accepted one-use publication:
144 exact byte copies through layout-v2 manifests, the dedicated source catalog,
and its pointer written last. Its implementation and Apex-named paths are now
historical lineage, not a future command surface. The generic prepare-only
migration is `src/futures_rebuild/micro_futures_catalog_migration.py` with
`scripts/prepare_micro_futures_catalog_migration_v1.py`; it has no active-write
function and cannot open DBNs or 2025/2026 payloads, mutate either catalog,
freeze a mechanism, register or evaluate a trial, or authorize trading.

The one-second source proves reported-trade-bar evidence only. It cannot prove
BBO availability, queue priority, guaranteed market-order execution, or precise
within-second tick ordering. Later Phase 2 contracts require entry after causal
availability, conservative same-bar ambiguity, explicit unfilled/no-trigger
states, independently scheduled baselines, locked stress costs, and explicit
missing or sparse checkpoints.

The prepare-only Phase 1B/2 contract and executor freeze the five schema-specific
decoder roles, actual-instrument and continuous-roll requirements, causal
availability, explicit missing/sparse/duplicate/prelaunch/ambiguous
dispositions, inactive micro-catalog certification, and lane-aware gateway
bindings. Status/statistics stay diagnostic-only, and one-second bars stay
reported-trade evidence without BBO, queue, fill, or within-second ordering
claims. The historical Apex risk policy explicitly covers full contracts only,
so it cannot be silently reused for micros. Future risk work uses
provider-neutral `prop_firm_*` profiles. MES, MCL, MGC, and M6E commission
verification remains fail-closed and must be resolved from an official
selected-provider source before mechanism freeze, not from observed outcomes.
The executor and
tests do not imply group-diagnostic or successor row authority; the group
diagnostic implementation must first be committed, then an immutable plan/audit
and separate exact confirmation are mandatory.

## Non-negotiable data rules

- Raw DBNs and accepted release bytes are immutable. Corrections publish a new
  release; they never overwrite an accepted one.
- Every file is bound by canonical path, byte count, SHA-256, schema,
  provenance, query semantics, and release identity before use.
- Actual instrument identity and point-in-time definition lineage are
  authoritative. Continuous symbols are selection references, never executable
  contract identity.
- Phase 1B preserves source event semantics. Phase 2 is the first phase allowed
  to apply causal session normalization, explicit missing/degraded states, and
  trainability gates.
- Historical Phase 2 admits actual decoded DBN rows only. It performs no gap
  filling, interpolation, synthetic open/close generation, or inference that
  unobserved time was closed. Its trade-date rollover is grouping logic, not
  official historical exchange-hours authority.
- The activated official CME calendar governs current/forward cockpit
  scheduling only and is not retrofitted onto historical research releases.
- Missing, sparse, degraded, unresolved, and partially decoded states remain in
  coverage denominators. They cannot become trainable through a global waiver.
- Feature builders may not discover or read outcome, label, prediction, or
  evaluation releases. Generated data from another repository is never an
  authoritative input.
- Every research artifact must trace to exact source, universe, config, trial,
  implementation, and upstream release identities.

## Label, feature, and split rules

- Labels declare decision time, entry lag, horizon, maturity, unresolved-state
  behavior, and the exact outcome-source capability they consume.
- Features use only information available by their decision timestamp; as-of
  joins, update frequency, lookback, warmup, missingness, and drift policy are
  explicit.
- Chronological splits are nested and never shuffled. Purge and embargo cover
  overlapping label horizons; all fitted transforms use training rows only.
- Holdout and forward cohorts cannot drive feature, target, market, cost,
  threshold, model, or policy selection.

## Runnable commands

From the repository root, use explicit executables from the pinned Python
3.11.9 environment without depending on activation or `PATH`:

```powershell
.\.venv\Scripts\futures-pipeline.exe list
.\.venv\Scripts\futures-pipeline.exe validate-profiles
.\.venv\Scripts\futures-pipeline.exe --output reports/pipeline_audit/synthetic-phase1a-11.json smoke
.\.venv\Scripts\futures-pipeline.exe phase1a
.\.venv\Scripts\futures-pipeline.exe phase1b
.\.venv\Scripts\futures-pipeline.exe phase2
.\.venv\Scripts\futures-pipeline.exe phase3
.\.venv\Scripts\futures-pipeline.exe phase4
.\.venv\Scripts\futures-pipeline.exe phase5
.\.venv\Scripts\futures-pipeline.exe phase6
.\.venv\Scripts\futures-pipeline.exe phase7
.\.venv\Scripts\futures-pipeline.exe phase8
.\.venv\Scripts\futures-pipeline.exe phase9
.\.venv\Scripts\futures-pipeline.exe phase10
.\.venv\Scripts\futures-pipeline.exe phase11
```

The global options precede the subcommand when using the module directly:

```powershell
.\.venv\Scripts\python.exe -m futures_rebuild.pipeline --output reports/pipeline_audit/smoke.json smoke
```

Outputs are create-only. Choose a new path for each run.

## Audit commands

```powershell
.\.venv\Scripts\futures-master-audit.exe --invocation <frozen-invocation.json>
.\.venv\Scripts\python.exe -m pytest -q --junitxml=.pytest_tmp/full-suite.xml
.\.venv\Scripts\futures-meta-audit.exe --junitxml .pytest_tmp/full-suite.xml --suite-evidence-output .pytest_tmp/full-suite-evidence.json
.\.venv\Scripts\futures-retirement-audit.exe
```

On Windows, launch the full-suite child command through
`.\scripts\run_windows_host_root_pytest.ps1`. The launcher first proves
create/delete access at the repository drive root and does not start pytest on
failure. Its default child command is the exact full-suite command above.
Repository-local `basetemp` fallback is forbidden because prescribed synthetic
trees can exceed legacy `MAX_PATH`. A Codex invocation must therefore grant the
launcher Windows host-root write access; detaching a workspace-sandboxed child
does not grant that capability.

The Master Audit classifies one exact target without granting authority. The
Meta Audit checks its independently derived threat registry, Master coverage,
stage mappings, executable test nodes, and full-suite receipt. The retirement
audit verifies standalone closure without resolving or opening an external
repository.

## Cockpit workflow

```powershell
.\.venv\Scripts\python.exe -m futures_rebuild.live_cockpit --self-check
.\.venv\Scripts\python.exe -m futures_rebuild.live_cockpit --demo
.\.venv\Scripts\futures-high-risk-prepare.exe --operation cockpit-live-smoke --scope duration_seconds=120 --output reports/live_cockpit/bounded_live_smoke_result.json
```

There is no installed `futures-live-cockpit.exe` command. Packaging, if
separately approved, publishes `FuturesLiveCockpit/` with exactly
`FuturesLiveCockpit.exe` and `_internal/` at its top level.

The normal UI is observation-only. Provider-backed smoke, packaging,
installation, and activation are high-risk operations: prepare their bounded
scope in the repository, then let Codex execute only after one plain-language
confirmation. Existing shortcuts stay unchanged until the approved cutover has
passed its rollback verification.

## Approval gates

For day-to-day work, start with `CURRENT_WORKFLOW.md`. This outline defines the
research pipeline; it does not add a second operational workflow.

The project uses a two-tier workflow. Normal local work—code, documents,
tests, and non-research generated artifacts—continues from the user’s request
without a generated approval artifact; see `CURRENT_WORKFLOW.md` for normal
work, staging, local-commit, and high-risk procedures. Durable trial
declarations and immutable release validation remain required for real research
work; historic closure material is evidence only in `docs/LEGACY_WORKFLOWS.md`.

## Evaluation and model-trust standard

Before a model-trust, promotion, or sealing claim, require:

- exact raw/foundation coverage, causal identity, sessions, rolls, labels,
  features, and split evidence;
- an immutable pre-outcome trial declaration covering targets, features,
  models, seeds, hyperparameter budget, thresholds, costs, sizing, metrics,
  multiplicity, stopped branches, and finite stop rules;
- flat/no-trade, cost-only, simple trend, simple mean-reversion, and relevant
  causal regime/liquidity baselines under identical evaluation rules;
- gross-to-net PnL conservation, fees, spread/slippage/delay, carry/roll,
  capacity, margin/liquidation, concentration, shared liquidity, and portfolio
  interaction;
- dependence-aware uncertainty, effective independent breadth, temporal and
  parameter stability, negative controls, multiple-testing adjustment, and
  traditional-versus-satellite reporting;
- Phase 7 prediction integrity, Phase 8 all-41-market Databento economics
  audit/rulebook and portfolio/risk review, and
  Phase 9 statistical/adversarial evidence with all blockers preserved.

Passing a metric, a broad row count, or a favorable satellite result cannot
substitute for these gates.

## Bounded execution policy

Before a high-risk action, describe its command family, scope ceiling, expected
outputs, forbidden actions, and recovery boundary in the approval question.
For real research, the existing trial declaration, provenance, immutable-output,
and validation contracts still apply. On failure, preserve partial evidence and
last-known-good state; ask before retrying, deleting, or recovering.

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

## Reporting standard

Label material claims as `Verified`, `Inferred`, `Assumed`, or
`Not established`. Each report names its exact scope, evidence paths and hashes,
config/trial identity, command, result, limitations, stale-risk, and next gate.
Never present gross-only, synthetic, warning, failed, inferred, or incomplete
evidence as alpha, promotion, holdout, cockpit, paper, or live readiness.
At an approval boundary, report one concise reason and the plain-language scope
to approve. A handoff is optional and never asks the user to copy a continuation
prompt.

## Stop conditions

Stop before the boundary when an approval is missing, a hash or schema is stale,
an input is incomplete or ambiguous, an immutable destination exists, a profile
drifts, a secret may be exposed, a real trial is unregistered, a holdout could
be disclosed, an order path is reachable, or rollback cannot be proven. Report
the exact rejected item and smallest missing approval or input.
