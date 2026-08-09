# Futures intraday research project

## Objective

Operate a reproducible, point-in-time, bias-resistant research system with two
strictly separated Alpha lanes: the approved 41-market standard/full-contract
universe and an Apex-deployable integer-micro universe. Both use immutable data
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
  two-stage prelaunch-discovery successor. Discovery permits only an empty
  `partial` list or the exact single requested parent symbol, always requires
  empty `not_found`, derives the first mapping date, and then re-resolves parent
  and continuous symbology from that date with both status lists empty. It has
  20 definitions, a 375-call ceiling, and no download surface.
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
frozen.

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

### Apex integer-micro lane

The micro successor is currently prepared and synthetic-only. It is not
published, active, registered, or backed by an active source catalog.
Its prepared pointer remains beside the unpublished contract and profile; the
future active path `configs/active_micro_alpha_research_ladder.json` is absent.

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

Micro products use integer contracts only. Product-effective dates are
provider-confirmed during the metadata preflight; prelaunch coverage remains
explicit `PRODUCT_NOT_YET_EFFECTIVE_NO_EMPTY_DBN` evidence. No market inherits
its parent contract's calendar or economics implicitly. The current acquisition
scope is Tier 0/1 only: MES, MCL, MGC, and M6E. No micro equivalent of ZN is
invented; any future micro rates candidate requires official Apex eligibility,
provider availability, and economics verification before outcomes.

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

## Corrected Apex micro Phase 1A/1B/2 route

This route reuses the standard folder grammar without mixing catalogs:

```text
v2 metadata-only Databento preflight -> FAIL_CLOSED_METADATA_ONLY (2 calls; $0; no rows)
  -> v3 local preparation superseded before staging/execution
  -> v4 preflight -> FAIL_CLOSED_METADATA_ONLY (3 calls; valid nested range rejected)
  -> v5 preflight -> FAIL_CLOSED_METADATA_ONLY (4 calls; first broad-range symbology request rejected)
  -> v6 preflight -> FAIL_CLOSED_METADATA_ONLY (4 calls; local list-shape validator defect)
  -> v7 preflight -> FAIL_CLOSED_METADATA_ONLY (4 calls; over-strict success echo)
  -> v8 preflight -> FAIL_CLOSED_METADATA_ONLY (4 calls; broad prelaunch status rejected)
  -> immutable v9 two-stage prelaunch successor (20 definitions; at most 180 annual requests)
  -> data/dbn/<schema-folder>/<market>/<year>/<start>_<end>.dbn.zst [Phase 1A]
  -> adjacent <same-name>.manifest.json                              [Phase 1A]
  -> data/raw/<market>/<year>/<interval>/<release>/                  [Phase 1B definition + 1m]
  -> data/market_state/{status|statistics}/<market>/...              [Phase 1B diagnostics]
  -> data/outcome_sources/<market>/...                               [Phase 1B execution]
  -> data/causally_gated_normalized/...                             [Phase 2 1m features]
  -> separately certified micro catalog                            [not yet active]
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
provider-confirmed effective date, the latest year ends on the frozen complete
end-exclusive date, and full intervening years use January 1 boundaries.
Prelaunch intervals produce disposition records and no fabricated empty DBN.
Multi-year DBNs, wrong-year folders, hyphenated schema folders, duplicate
destinations, and alternate micro-root layouts fail closed.

The Phase 1A downloader is implemented and synthetic/adversarially tested but
has not executed. It uses one exact bounded interval per market/schema/year, writes
first to inactive staging, requotes every request at exactly $0 before the
first download, and then uses at most two isolated Databento download clients.
The worker queues stop scheduling after the first failure; an already-running
second request may finish into inactive staging and is preserved as evidence.
This bounded concurrency improves network utilization without sharing an SDK
client, retrying, overwriting, or weakening byte ceilings. It streams compressed
DBN bytes without iterating rows, verifies
size and SHA-256, creates an exact-query adjacent sidecar, refuses collisions,
and writes one terminal attempt record last. Empty, partial, failed, oversized,
or interrupted files remain inactive failure evidence. There is one attempt,
zero automatic retries, and no overwrite, resume, publication, catalog
activation, registration, evaluation, or trading path.

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
A new separate approval is required before v9 may contact Databento. Only a
passing report may freeze a deterministic
acquisition plan bound to the then-committed implementation HEAD. Metadata
approval never grants download authority.

The one-second source proves reported-trade-bar evidence only. It cannot prove
BBO availability, queue priority, guaranteed market-order execution, or precise
within-second tick ordering. Later Phase 2 contracts require entry after causal
availability, conservative same-bar ambiguity, explicit unfilled/no-trigger
states, independently scheduled baselines, locked stress costs, and explicit
missing or sparse checkpoints.

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
