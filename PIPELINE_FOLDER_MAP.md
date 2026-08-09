# Current pipeline and folder map

This is a topology guide, not authority. `CURRENT_WORKFLOW.md` controls normal
work and every real-data or publishing action remains separately controlled.

## Current implementation reality

| Classification | Surface | Reality | Authority |
| --- | --- | --- | --- |
| `CURRENT_REACHABLE` | Standard Alpha: `configs/active_alpha_research_ladder.json`; `data/active/catalog.json` | Active standard/full-contract ladder and catalog | Exact gateway authority required for real research |
| `CURRENT_REACHABLE` | `data/active/causally_gated_normalized/` | Only catalog-selected flattened standard-lane research view; 562 admitted market-years with adjacent certification sidecars | Resolve through `data/active/catalog.json`; direct globs forbidden |
| `CURRENT_REACHABLE` | `data/causally_gated_normalized/` | Content-addressed immutable Phase 2 release history with multiple preserved generations; not a second active view | Release inputs only through pinned manifests and catalog bindings |
| `CURRENT_REACHABLE` | `scripts/audit_standard_data_topology_source_safe.py`; `state/unpublished_evidence/standard_data_topology_source_safe_audit/report.json` | Source-safe PASS verifies catalog self-hash, foundation/DBN release bindings, 562 active sidecars, and referenced Phase 1B/2 paths without opening payloads | Does not recertify rows or grant research authority |
| `RETIRED` | `scripts/prepare_safe_cleanup_inventory_v4.py`; `state/unpublished_evidence/safe_cleanup_preparation_v4/plan.json` | Preserved no-delete preparation bound to pre-commit HEAD `558ee094...`; it became stale after the approved implementation commit and cannot authorize cleanup | Execution forbidden; no cleanup occurred |
| `PREPARED_NOT_EXECUTED` | `scripts/prepare_safe_cleanup_inventory_v5.py`; `state/unpublished_evidence/safe_cleanup_preparation_v5/plan.json` | Stable preserve-first cleanup policy with no frozen candidates or prepared-HEAD claim; exact literal cache census and execution HEAD are deliberately deferred until all prior writes finish | Separate exact cleanup approval required after a fresh census; all gates rerun immediately before/after mutation |
| `RETIRED` | `safe_cleanup_preparation` v1-v3 plans and supersession records | Preserved prepare-only drafts; v1/v2 had self-referential inventory drift and v3 used an unsuitable dynamic worktree binding | Execution forbidden; no cleanup occurred |
| `SYNTHETIC_ONLY` | `futures-pipeline`; `src/futures_rebuild/pipeline.py` | Generated mechanics fixtures only | No historical or alpha authority |
| `RETIRED` | `configs/apex_micro_tier01_databento_preflight_plan.json`; obsolete `febccafd...` preparation | Preserved MES/MGC/M6E/M6A bytes, classified `SUPERSEDED_PREPARATION — MICRO_TIER1_SCOPE_RECONCILIATION`; cannot execute as current | No provider or publication authority |
| `PREPARED_NOT_EXECUTED` | Corrected contract/profile/pointer under `state/unpublished_evidence/apex_micro_ladder_preparation_v2/`; architecture under `alpha_research_architecture_v2/` | Inactive MES/MCL/MGC/M6E successor; no active micro pointer or catalog | None |
| `PREPARED_NOT_EXECUTED` | `scripts/prepare_apex_micro_infrastructure.py`; `configs/apex_micro_product_reference_requirements.json`; `state/unpublished_evidence/apex_micro_preparation_supersessions/micro_tier1_scope_reconciliation.json` | Deterministic create-only successor preparation and byte-bound supersession classification | No provider or activation authority |
| `RETIRED` | `configs/apex_micro_tier01_databento_metadata_preflight_v2.json`; `src/futures_rebuild/micro_alpha_databento_preflight.py`; `state/unpublished_evidence/apex_micro_metadata_preflight_v2/report.json` | One approved metadata-only attempt executed and failed closed on `list_schemas` `ReadTimeout` after two calls; $0, zero retries/downloads/rows/DBNs; report and authorization use preserved | Consumed authorization cannot execute again; report grants no acquisition authority |
| `RETIRED` | `configs/apex_micro_tier01_databento_metadata_preflight_v3.json`; `state/unpublished_evidence/apex_micro_metadata_preflight_v3_supersession.json` | Unexecuted local preparation preserved after executor self-hash drift before staging; no provider access, authorization, or report | Execution forbidden; grants no authority |
| `RETIRED` | `configs/apex_micro_tier01_databento_metadata_preflight_v4.json`; `src/futures_rebuild/micro_alpha_databento_preflight_v4.py`; `state/unpublished_evidence/apex_micro_metadata_preflight_v4/report.json` | One approved metadata-only attempt failed closed after three calls when a valid nested schema-range response reached the flat-range parser; $0, zero retries/downloads/rows/DBNs | Consumed authorization cannot execute again; report grants no acquisition authority |
| `RETIRED` | `configs/apex_micro_tier01_databento_metadata_preflight_v5.json`; `src/futures_rebuild/micro_alpha_databento_preflight_v5.py`; `state/unpublished_evidence/apex_micro_metadata_preflight_v5/report.json` | One approved annual metadata-only attempt failed closed after four calls when the first `MES.FUT` parent symbology request used `2000-01-01` and received `BentoClientError`; $0, zero retries/downloads/rows/DBNs | Consumed authorization cannot execute again; provider message was not recorded; report grants no acquisition authority |
| `RETIRED` | `configs/apex_micro_tier01_databento_metadata_preflight_v6.json`; `src/futures_rebuild/micro_alpha_databento_preflight_v6.py`; `state/unpublished_evidence/apex_micro_metadata_preflight_v6/report.json` | One approved provider-range-safe attempt made four metadata calls and failed closed locally after the first resolve returned list-shaped `partial`; v6 incorrectly tested that list with set membership, producing `TypeError`; $0, zero retries/downloads/rows/DBNs | Consumed authorization cannot execute again; sealed report grants no acquisition authority |
| `RETIRED` | `configs/apex_micro_tier01_databento_metadata_preflight_v7.json`; `src/futures_rebuild/micro_alpha_databento_preflight_v7.py`; `state/unpublished_evidence/apex_micro_metadata_preflight_v7/report.json` | One approved list-shape-safe attempt made four metadata calls and failed closed locally in the combined pre-list exact response-echo check; local SDK-contract evidence identifies the empty-message expectation as the bounded v8 correction, while the sealed report records no provider value; $0, zero retries/downloads/rows/DBNs | Consumed authorization cannot execute again; report grants no acquisition authority |
| `RETIRED` | `configs/apex_micro_tier01_databento_metadata_preflight_v8.json`; `src/futures_rebuild/micro_alpha_databento_preflight_v8.py`; `state/unpublished_evidence/apex_micro_metadata_preflight_v8/report.json` | One approved success-echo-safe attempt made four metadata calls and failed closed on a nonempty status from the broad MES parent resolve; the sanitized classifier reported `symbols`, so no provider field value or unsupported partial/not-found claim is recorded; $0, zero retries/downloads/rows/DBNs | Consumed authorization cannot execute again; report grants no acquisition authority |
| `RETIRED` | `configs/apex_micro_tier01_databento_metadata_preflight_v9.json`; `src/futures_rebuild/micro_alpha_databento_preflight_v9.py`; `state/unpublished_evidence/apex_micro_metadata_preflight_v9/report.json` | One approved two-stage discovery attempt made four metadata calls and failed closed at the first MES resolve because the local validator required an opaque nonempty `partial` list to equal the requested-symbol singleton; the sealed report records only the field name, not list contents or cardinality; $0, zero retries/downloads/rows/DBNs | Consumed authorization cannot execute again; report grants no acquisition authority |
| `RETIRED` | `configs/apex_micro_tier01_databento_metadata_preflight_v10.json`; `src/futures_rebuild/micro_alpha_databento_preflight_v10.py`; `state/unpublished_evidence/apex_micro_metadata_preflight_v10/report.json` | One approved opaque-single-partial attempt made four metadata calls and failed closed at the first MES resolve because the exact-string `partial` list did not satisfy the local one-entry ceiling; the sealed report records neither contents nor exact cardinality; $0, zero retries/downloads/rows/DBNs | Consumed authorization cannot execute again; report grants no acquisition authority |
| `RETIRED` | `configs/apex_micro_tier01_databento_metadata_preflight_v11.json`; `src/futures_rebuild/micro_alpha_databento_preflight_v11.py`; `state/unpublished_evidence/apex_micro_metadata_preflight_v11/report.json` | One approved bounded opaque-partial attempt made four metadata calls and failed closed at the first MES resolve because the local validator guessed exact integer zero for the SDK-opaque application `status` field; the sealed report records only the field name; $0, zero retries/downloads/rows/DBNs | Consumed authorization cannot execute again; report grants no acquisition authority |
| `RETIRED` | `configs/apex_micro_tier01_databento_metadata_preflight_v12.json`; `src/futures_rebuild/micro_alpha_databento_preflight_v12.py`; `state/unpublished_evidence/apex_micro_metadata_preflight_v12/report.json` | One approved SDK-contract-safe status attempt made four metadata calls and failed closed at the first MES resolve because the local validator guessed an exact empty-or-OK allowlist for the SDK-opaque `message` field; the sealed report records only the field name; $0, zero retries/downloads/rows/DBNs | Consumed authorization cannot execute again; report grants no acquisition authority |
| `RETIRED` | `configs/apex_micro_tier01_databento_metadata_preflight_v13.json`; `src/futures_rebuild/micro_alpha_databento_preflight_v13.py`; `state/unpublished_evidence/apex_micro_metadata_preflight_v13/report.json` | One approved v13 SDK-opaque-message-safe predecessor made four metadata calls and failed closed at the first MES resolve because the local validator required `result` to have exactly the requested symbol as its sole key; the sealed report records only the field name; $0, zero retries/downloads/rows/DBNs | RETIRED / fail-closed result-group evidence; consumed authorization cannot execute again |
| `RETIRED` | `configs/apex_micro_tier01_databento_metadata_preflight_v14.json`; `src/futures_rebuild/micro_alpha_databento_preflight_v14.py`; `state/unpublished_evidence/apex_micro_metadata_preflight_v14/report.json` | One approved v14 provider-result-group-safe predecessor made five metadata calls, passed MES discovery/result-group validation, and failed closed because post-effective parent verification required the bounded opaque `partial` list to be empty; the sealed report records only the field name; $0, zero retries/downloads/rows/DBNs | RETIRED / fail-closed post-effective partial evidence; consumed authorization cannot execute again |
| `RETIRED` | `configs/apex_micro_tier01_databento_metadata_preflight_v15.json`; `src/futures_rebuild/micro_alpha_databento_preflight_v15.py`; `state/unpublished_evidence/apex_micro_metadata_preflight_v15/report.json` | One approved v15 gap-proof bounded-partial-safe attempt made six metadata calls, passed MES discovery and post-effective parent validation, then failed closed at the strict interval-bound gate for `MES.v.0`; the sealed report records only the field and price-free call context; $0, zero retries/downloads/rows/DBNs | RETIRED / fail-closed continuous-interval evidence; consumed authorization cannot execute again |
| `RETIRED` | `configs/apex_micro_tier01_databento_metadata_preflight_v16.json`; `src/futures_rebuild/micro_alpha_databento_preflight_v16.py`; `state/unpublished_evidence/apex_micro_metadata_preflight_v16/report.json` | One approved v16 interval-overlap-safe attempt made seven metadata calls, passed all three MES symbology gates, then failed closed because MCL parent expansion produced a bounded result-group key outside the locally assumed market-root prefix; the sealed report records only the field and price-free call context; $0, zero retries/downloads/rows/DBNs | RETIRED / fail-closed opaque-group-key evidence; consumed authorization cannot execute again |
| `RETIRED` | `configs/apex_micro_tier01_databento_metadata_preflight_v17.json`; `src/futures_rebuild/micro_alpha_databento_preflight_v17.py`; `state/unpublished_evidence/apex_micro_metadata_preflight_v17/report.json` | One approved v17 bounded opaque-group-key attempt made eight metadata calls, passed MCL parent discovery, then failed closed because parent-family intervals were incorrectly required to form one calendar-gap-free roll chain; $0, zero retries/downloads/rows/DBNs | RETIRED / fail-closed parent-family continuity evidence; consumed authorization cannot execute again |
| `PREPARED_NOT_EXECUTED` | `configs/apex_micro_tier01_databento_metadata_preflight_v18.json`; `src/futures_rebuild/micro_alpha_databento_preflight_v18.py` | Exact-scope immutable v18 parent-family-aware successor requires valid parent-family identities and query-boundary coverage without a false single-roll-chain claim, while continuous `<root>.v.0` alone retains gap-free clipped roll-continuity proof; all exact echo, field, bound, cost, disk, collision, no-download, and no-decode gates remain | Exact staging, commit, and new single-use metadata/provider approval required |
| `PREPARED_NOT_EXECUTED` | `src/futures_rebuild/micro_alpha_acquisition.py` | Phase 1A annual market-year create-only inactive-custody downloader implemented and adversarially tested; canonical `data/dbn/<schema>/<market>/<year>/` tree only; at most two isolated download clients, stop-after-first-failure scheduling, and no DBN output | Separate download approval absent; passing v18 preflight and committed HEAD required first |
| `SYNTHETIC_ONLY` | `tests/test_micro_alpha_*.py` | Corrected ladder, authorization, metadata, disk, collision, partial, cost, retry, custody, and no-decode mechanics | No provider, row, or download authority |
| `HISTORICAL_ROW_APPROVAL_REQUIRED` | Phase 1B/2 contracts in `src/futures_rebuild/micro_alpha_pipeline.py` | Decoder/causal routing contracts are prepared; Phase 1B/2 row processing has not executed | Separate row-read approval only after acquisition |
| `NOT_IMPLEMENTED` | `configs/active_micro_alpha_research_ladder.json`; `data/active/catalogs/apex_micro.json` | Intentionally absent until publication and Phase 2 certification | Cannot register micro research |
| `CURRENT_REACHABLE` | `src/futures_rebuild/certified_research_gateway.py` | Standard registration and one-use historical execution | Exact certified authority required |
| `CURRENT_REACHABLE` | `src/futures_rebuild/live_cockpit/`; `python -m futures_rebuild.live_cockpit` | Observation only | No order or position-changing authority |
| `RETIRED` | Versioned trial modules, old Phase 3-8 chain, registries, receipts, reports | Interpretation and provenance only | Cannot authorize current work |

## Current Alpha dependency flow

```text
active ladder pointer
  + active calendar pointer
  + active catalog
  + frozen mechanism and Tier 0 certificate
        |
        v
immutable readiness plan --separate row-read approval--> unpublished certificate
        |
        v
CertifiedResearchGateway registration --separate publication approval--> ES pilot
        |
        v
one-use economic execution --separate approval--> PASS or REJECT
```

No other public script or imported legacy helper is a current real-history
trial surface. Unknown or retired operation names fail closed.

## Prepared Apex micro dependency flow

```text
PROJECT_OUTLINE design
  -> obsolete MES/MGC/M6E/M6A plan                                 RETIRED / preserved
  -> corrected inactive MES/MCL/MGC/M6E ladder                     PREPARED_NOT_EXECUTED
  -> v2 metadata-only Databento preflight                          RETIRED / fail-closed timeout evidence
  -> v3 local preparation                                          RETIRED / pre-execution self-hash drift
  -> v4 metadata-only successor                                    RETIRED / fail-closed nested-range evidence
  -> v5 annual market-year successor                               RETIRED / fail-closed first symbology evidence
  -> v6 provider-range-safe successor                              RETIRED / fail-closed list-shape evidence
  -> v7 list-shape-safe successor                                  RETIRED / fail-closed success-echo evidence
  -> v8 success-echo-safe successor                                RETIRED / fail-closed broad-status evidence
  -> v9 two-stage prelaunch successor                              RETIRED / fail-closed opaque-partial evidence
  -> v10 opaque-single-partial successor                           RETIRED / fail-closed cardinality evidence
  -> v11 bounded opaque-partial-flag successor                     RETIRED / fail-closed status-semantic evidence
  -> v12 SDK-contract-safe status successor                        RETIRED / fail-closed message-semantic evidence
  -> v13 SDK-opaque-message-safe predecessor                       RETIRED / fail-closed result-group evidence
  -> v14 provider-result-group-safe predecessor                    RETIRED / fail-closed post-effective partial evidence
  -> v15 gap-proof bounded-partial-safe predecessor                RETIRED / fail-closed continuous-interval evidence
  -> v16 bounded interval-overlap-safe predecessor                 RETIRED / fail-closed opaque-group-key evidence
  -> v17 bounded opaque-group-key predecessor                      RETIRED / fail-closed parent-family continuity evidence
  -> immutable v18 parent-family-aware successor                   PREPARED_NOT_EXECUTED / approval absent
  -> exact audited acquisition plan                                NOT_IMPLEMENTED until preflight PASS
  -> Phase 1A DBN + sidecar inactive custody                       PREPARED_NOT_EXECUTED / download authority absent
  -> Phase 1B definition/status/statistics/1m/1s decoding          row-read approval required
  -> Phase 2 1m feature + separate 1s execution foundations        row-read approval required
  -> source certification
  -> micro catalog publication/activation                          NOT_IMPLEMENTED
  -> micro mechanism Tier 0 and later research                     blocked
```

Exact intended Phase 1A destination:
`data/dbn/<schema-folder>/<market>/<year>/<start>_<end>.dbn.zst` with an
adjacent `.manifest.json` sidecar. This is the existing standard/full-contract
DBN hierarchy, not a parallel micro tree. Exactly one pair is prepared for each
market x schema x calendar-year interval; partial launch and latest years are
explicit and multi-year DBNs are forbidden. The prepared source scope is MES, MCL, MGC,
and M6E across the required Databento Standard historical schemas: definition,
status, statistics, ohlcv-1m, and ohlcv-1s. No target micro DBN currently exists.

No micro phase is labeled complete. The v2, v4, v5, v6, v7, v8, v9, v10, v11, v12, v13, v14, v15, v16, and v17 metadata-only attempts
executed once each and produced only preserved fail-closed metadata evidence.
The v18 successor and Phase 1A downloader are implemented but unexecuted. Phase
1B/2 row processing is also unexecuted. The micro catalog is
inactive, registration is blocked, and download authority is absent. Apart
from the preserved price-free v2, v4, v5, v6, v7, v8, v9, v10, v11, v12, v13, v14, v15, v16, and v17 failure reports, the code proves mechanics
with synthetic records only.

## Active and protected folders

| Folder | Meaning |
| --- | --- |
| `data/active/` | Active hash-bound local research sources; resolve through the catalog only |
| `data/dbn/`, `data/raw/`, `data/causally_gated_normalized/` | Immutable source/release families; multiple content-addressed generations are expected and are not active by directory presence |
| `data/active/causally_gated_normalized/` | Only flattened standard-lane catalog-selected research view; resolve through `data/active/catalog.json` |
| `configs/` | Operational pointers plus immutable or predecessor contracts/plans |
| `state/trial_registry/`, `state/trial_events/` | Immutable trial genealogy and terminal evidence |
| `state/unpublished_evidence/` | Prepared or sealed evidence that is not publication authority |
| `state/authorization_uses/` | Consumed one-use authorization records |
| `manifests/` | Content-addressed source and research manifests |
| `reports/` | Human-readable audits and diagnostics; reports do not grant authority |
| `data/features/`, `data/outcomes/`, `data/predictions/`, `data/evaluations/` | Protected research artifacts; no direct current execution route |

Credentials and local provider configuration remain ignored. Their paths may
be counted for safety inventory, but their contents must never be inspected,
logged, staged, or packaged.

## Retired surfaces

The following are preserved evidence or synthetic-testable internals and are
not current production routes:

- `scripts/run_tier1_core_foundation.py`
- `scripts/run_tier1_phase5_split_plan.py`
- `scripts/prepare_tier1_phase6_wfa.py --run`
- `scripts/run_tier1_phase7_audit.py`
- the former Phase 8 opaque-token adapter and runner
- bracket evaluator and versioned bracket-successor executors
- V4-V12, Standard-only, Final, Authoritative, and other versioned historical
  registration/execution helpers

See `docs/LEGACY_WORKFLOWS.md` for preservation and interpretation rules.

## Validation commands

```powershell
.\.venv\Scripts\futures-pipeline.exe validate-profiles
.\.venv\Scripts\python.exe -m pytest -m current -q
.\.venv\Scripts\python.exe -m pytest -m high_risk -q
.\.venv\Scripts\python.exe -m compileall -q src scripts tests
.\.venv\Scripts\python.exe -m pip check
git diff --check
```

The current and high-risk lanes are source-safe synthetic checks. The legacy
and `local_evidence` lanes are reported separately and are never silently
counted as passing when deselected.
