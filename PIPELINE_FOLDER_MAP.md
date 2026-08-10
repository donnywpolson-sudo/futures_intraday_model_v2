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
| `CURRENT_REACHABLE` | `scripts/audit_data_topology_source_safe_v2.py`; `state/unpublished_evidence/data_topology_source_safe_audit_v2/report.json` | Successor metadata-only PASS inventories every top-level data root, binds repaired micro custody, and confirms release history versus catalog-selected active-view roles | Zero data-root cleanup candidates; no payload opened or cleanup authority granted |
| `RETIRED` | `scripts/prepare_safe_cleanup_inventory_v4.py`; `state/unpublished_evidence/safe_cleanup_preparation_v4/plan.json` | Preserved no-delete preparation bound to pre-commit HEAD `558ee094...`; it became stale after the approved implementation commit and cannot authorize cleanup | Execution forbidden; no cleanup occurred |
| `PREPARED_NOT_EXECUTED` | `scripts/prepare_safe_cleanup_inventory_v5.py`; `state/unpublished_evidence/safe_cleanup_preparation_v5/plan.json` | Stable preserve-first cleanup policy with no frozen candidates or prepared-HEAD claim; exact literal cache census and execution HEAD are deliberately deferred until all prior writes finish | Separate exact cleanup approval required after a fresh census; all gates rerun immediately before/after mutation |
| `PREPARED_NOT_EXECUTED` | `scripts/prepare_safe_cleanup_candidate_census_v6.py`; `state/unpublished_evidence/safe_cleanup_candidate_census_v6/census.json` | Post-commit metadata-only census froze 13 exact untracked, Git-ignored cache directories outside data/state/config roots; records filesystem counts but never opens DBN or Parquet payloads | Grants no delete/move authority; separate exact cleanup approval and immediate revalidation remain required |
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
| `RETIRED` | `configs/apex_micro_tier01_databento_metadata_preflight_v18.json`; `src/futures_rebuild/micro_alpha_databento_preflight_v18.py`; `state/unpublished_evidence/apex_micro_metadata_preflight_v18/report.json` | One approved v18 parent-family-aware attempt made 13 metadata calls, passed MES, MCL, and MGC symbology gates, then failed closed because M6E discovery still interpreted opaque `partial` presence as a prelaunch signal; $0, zero retries/downloads/rows/DBNs | RETIRED / fail-closed discovery-partial-semantic evidence; consumed authorization cannot execute again |
| `RETIRED` | `configs/apex_micro_tier01_databento_metadata_preflight_v19.json`; `src/futures_rebuild/micro_alpha_databento_preflight_v19.py`; `state/unpublished_evidence/apex_micro_metadata_preflight_v19/report.json` | One approved v19 attempt made 15 metadata calls, verified all four parent/continuous mapping surfaces, and failed closed because M6E was active at the 2010-06-06 provider dataset boundary and its earlier exact launch date remained unresolved; $0, zero retries/downloads/rows/DBNs | RETIRED / consumed authorization cannot execute again; mapping intervals are availability evidence, not launch-date evidence |
| `CURRENT_REACHABLE` | `state/unpublished_evidence/apex_micro_m6e_product_effective_date_source_v1/report.json` | Two official CME primary sources establish M6E listing/effective date 2009-03-22 and trade date 2009-03-23; one bounded $0 lookup, no Databento or data access | Unpublished source evidence only; grants no provider or download authority |
| `CURRENT_REACHABLE` | `state/unpublished_evidence/apex_micro_remaining_product_effective_dates_source_v1/report.json` | Official CME primary sources establish MES 2019-05-05/06, MCL 2021-07-11/12, and MGC 2010-10-03/04 effective/first-trade dates; bounded 9-request $0 lookup, no Databento or data access | Unpublished source evidence only; grants no provider or download authority |
| `PREPARED_NOT_EXECUTED` | `src/futures_rebuild/micro_alpha_product_effective_dates.py`; `tests/test_micro_alpha_product_effective_dates.py` | Fail-closed loader accepts only self-hashed official CME reports and explicitly forbids promoting Databento mapping dates to product-effective dates | Complete official four-market date scope is available to the prepared v21 successor |
| `RETIRED` | `configs/apex_micro_tier01_databento_metadata_preflight_v20.json`; `src/futures_rebuild/micro_alpha_databento_preflight_v20.py`; `state/unpublished_evidence/apex_micro_metadata_preflight_v20/report.json` | One approved v20 attempt reused sealed v19 metadata evidence and made 68 annual cost/size calls before the MES ohlcv-1s 2020 billable-size request timed out; $0, zero retries/downloads/rows/DBNs | RETIRED / consumed authorization cannot execute again; immutable timeout evidence |
| `CURRENT_REACHABLE` | `configs/apex_micro_tier01_databento_metadata_preflight_v21.json`; `src/futures_rebuild/micro_alpha_databento_preflight_v21.py`; `state/unpublished_evidence/apex_micro_metadata_preflight_v21/report.json` | One approved timeout-safe metadata-only run completed 20 full-range zero-cost proofs and 160 annual byte estimates in exactly 180 calls; `PASS_METADATA_ONLY`, end-exclusive 2026-08-09, $0, zero retries/downloads/rows/DBNs, and zero destination conflicts | Authorization consumed; report is price-free evidence for plan preparation only and grants no download authority |
| `RETIRED` | `src/futures_rebuild/micro_alpha_acquisition.py` | Preserved unexecuted predecessor downloader is bound to the older v7 report shape and cannot consume the sealed v21 PASS evidence as current | No acquisition authority; retained because v21 evidence hashes it |
| `RETIRED` | `src/futures_rebuild/micro_alpha_acquisition_v21.py`; exact plan/audit; consumed authorization; `state/unpublished_evidence/apex_micro_phase1a_acquisition_v21_failure/report.json` | One separately approved run completed 160 $0 cost calls and 36 downloads, then failed closed on the 7,200-second global ceiling; 36 hash-verified pairs are read-only failed-attempt staging evidence, with zero accepted/finalized pairs and zero final destinations | Authorization consumed; no retry, resume, promotion, decoding, or research-source authority |
| `RETIRED` | `src/futures_rebuild/micro_alpha_acquisition_v22.py`; exact plan/audit; v7 cleanup census; `state/unpublished_evidence/apex_micro_phase1a_acquisition_v22_supersession/report.json` | Unexecuted plan preparation exposed a self-referential worktree snapshot: reconstructing after its three create-only outputs appeared changed the v7 census; all artifacts are preserved and sealed as `SUPERSEDED_PREPARATION_SELF_REFERENTIAL_CENSUS` | No authorization, provider calls, downloads, or cleanup mutation; v22 cannot execute as current |
| `RETIRED` | `src/futures_rebuild/micro_alpha_acquisition_v23.py`; exact plan/audit; v8 cleanup census; `state/unpublished_evidence/apex_micro_phase1a_acquisition_v23_supersession/report.json` | Its plan and cleanup census reconstruct exactly, but the unexecuted audit self-hashed a volatile exact free-disk byte reading and therefore changed after unrelated filesystem writes; preserved as `SUPERSEDED_PREPARATION_VOLATILE_CAPACITY_SNAPSHOT` | No authorization, provider calls, downloads, or cleanup mutation; v23 is removed from the current preparatory-operation allowlist |
| `RETIRED` | `src/futures_rebuild/micro_alpha_acquisition_v24.py`; exact plan/audit; consumed authorization; v24 terminal; `state/unpublished_evidence/apex_micro_phase1a_acquisition_v24_verification_failure/report.json` | One separately approved run completed 160 cost calls and 160 downloads at $0 with zero retries, then its own final verification failed closed because all 320 read-only Windows staging-alias removals failed | Authorization consumed; immutable failure evidence preserved; custody was later corrected only by the separately approved v2 repair |
| `RETIRED` | `src/futures_rebuild/micro_alpha_custody_repair_v1.py`; immutable v1 plan; `state/unpublished_evidence/apex_micro_v24_custody_repair_v1_supersession/report.json` | Unexecuted preparation omitted mandatory execution-time implementation/evidence rechecks, per-sidecar identity bindings, pre-mutation DBN hash verification, and failure-path read-only proof; preserved as `SUPERSEDED_PREPARATION_INCOMPLETE_EXECUTION_BINDINGS` | No authorization, alias removal, provider call, DBN read, or terminal; v1 is removed from the current operation allowlist |
| `CURRENT_REACHABLE` | `src/futures_rebuild/micro_alpha_custody_repair_v2.py`; exact plan/audit; consumed authorization; terminal `ebb82d34...` | One approved no-network run removed all 320 exact staging aliases and verified 160 DBNs, 160 sidecars, and 1,849,575,228 DBN bytes before/after without decoding | `SUCCESS_INACTIVE_IMMUTABLE_CUSTODY_REPAIRED`; all finals single-link/read-only; no rerun authority |
| `SYNTHETIC_ONLY` | `tests/test_micro_alpha_*.py` | Corrected ladder, authorization, metadata, disk, collision, partial, cost, retry, custody, and no-decode mechanics | No provider, row, or download authority |
| `CURRENT_REACHABLE` | `src/futures_rebuild/micro_alpha_phase1b2_decoder.py`; `src/futures_rebuild/micro_alpha_phase1b2_execution.py`; `scripts/prepare_apex_micro_phase1b2_execution_v1.py` | Offline micro-only decoder/executor implements five schemas, exact annual source checks, 100,000-row batches, two-worker stop scheduling, write-time 64 GiB ceiling, explicit dispositions, inactive Phase 1B/2 outputs, certification, terminal-last evidence, and collision-checked 96-bit path aliases with full 256-bit identities retained in records | V3 completed all 120 inactive Phase 1B Parquets but failed before a completed Phase 2 output; its consumed authority cannot rerun |
| `HISTORICAL_ROW_APPROVAL_REQUIRED` | `src/futures_rebuild/micro_alpha_pipeline.py`; `src/futures_rebuild/micro_alpha_phase1b2_preparation.py`; `configs/apex_micro_phase1b2_prepare_only_contract_v1.json` | Exact 2018-2024 scope remains 120 source pairs / 1,232,883,585 compressed DBN bytes and 140 coverage cells; 120 preserved Phase 1B Parquets are inactive, uncertified inputs for a later successor | Additional derived-row diagnostics and the full Phase 2/certification successor each require their own immutable plan and exact approval |
| `RETIRED` | `configs/apex_micro_phase1b2_historical_execution_plan_v1.json`; `state/unpublished_evidence/apex_micro_phase1b2_execution_plan_v1/audit.json`; `state/unpublished_evidence/apex_micro_phase1b2_execution_plan_v1_supersession/report.json` | One approved execution request failed closed during central receipt verification because the exact source-foundation operation was absent from the preparatory allowlist | Receipt not consumed; zero authorization-use records, output roots, source hashes, rows, sealed-year access, provider calls, or retries; v1 cannot execute as current |
| `CURRENT_REACHABLE` | `src/futures_rebuild/research_gateway_policy.py`; exact operation `BUILD_APEX_MICRO_PHASE1B2_INACTIVE_FOUNDATION_V1_ONCE` | Central preparatory allowlist admits only the exact Phase 1B/2 inactive-foundation operation; unknown aliases fail closed | Does not itself grant row authority |
| `RETIRED` | `configs/apex_micro_phase1b2_historical_execution_plan_v2.json`; v2 audit; consumed authorization-use record; v2 terminal; `state/unpublished_evidence/apex_micro_phase1b2_execution_plan_v2_supersession/report.json` | One approved attempt verified all 120 source hashes, then failed before completing a decode because its first staged `.partial` path was 299 characters | One attempt consumed, zero retries, Parquet files, created bytes, completed decodes, Phase 2 outputs, sealed-year access, provider calls, publication, or activation; v2 cannot execute again |
| `RETIRED` | `configs/apex_micro_phase1b2_historical_execution_plan_v3.json`; v3 audit; consumed authorization-use record; 120 inactive Phase 1B Parquets; v3 terminal; `state/unpublished_evidence/apex_micro_phase1b2_execution_plan_v3_supersession/report.json` | One approved attempt proved the path fix, verified all sources, and completed all 120 Phase 1B outputs / 6,627,486,838 bytes, then failed closed before the first completed Phase 2 output | One attempt consumed, zero retries, Phase 2 outputs, certification reports, catalog candidates, sealed-year access, provider calls, publication, or activation; v3 cannot execute again |
| `CURRENT_REACHABLE` | `src/futures_rebuild/micro_alpha_phase1b2_phase2_diagnostic.py`; exact plan/audit; consumed authorization; report/terminal under `state/unpublished_evidence/apex_micro_phase1b2_phase2_diagnostic_v1/` | One approved run opened only the exact inactive M6E 2018 one-minute Phase 1B Parquet and created one separate inactive 17,093,314-byte causal diagnostic Parquet; `PASS_FIRST_INTERVAL_PHASE2_MATERIALIZATION`, one attempt, zero retries/provider calls | Proves only the materializer/path; the output is unpublished, inactive, not a certification release, and not for Git or research |
| `PREPARED_NOT_EXECUTED` | `src/futures_rebuild/micro_alpha_phase1b2_group_diagnostic.py`; `scripts/prepare_apex_micro_phase1b2_group_diagnostic_v2.py`; exact operation `DIAGNOSE_APEX_MICRO_PHASE2_FIRST_GROUP_V2_ONCE` | Stat-only planner binds exactly five M6E 2018 Phase 1B Parquets / 86,344,286 bytes; executor may reconstruct price-free identity/roll summaries and test group-disposition plus receipt serialization under one worker, 100,000-row batches, 900 seconds, one attempt, zero retries | Requires implementation commit, immutable plan/audit, and separate five-source derived-row approval; creates no Phase 2 Parquet and cannot open DBNs, a sixth Parquet, or 2025/2026 |
| `NOT_IMPLEMENTED` | Full Phase 2/certification successor plan after group diagnostic | Successor design waits for the bounded five-schema transition result | No current full row-read execution authority exists |
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
  -> v18 parent-family-aware predecessor                           RETIRED / fail-closed discovery-partial-semantic evidence
  -> immutable v19 opaque-partial-semantic-safe predecessor        RETIRED / fail-closed pre-dataset product-date evidence
  -> official CME product-effective-date evidence                  CURRENT_REACHABLE / all four markets sealed
  -> v20 launch-date-separated cumulative metadata successor       RETIRED / 68 calls / provider timeout
  -> v21 timeout-safe cumulative metadata successor                CURRENT_REACHABLE / PASS_METADATA_ONLY / 180 calls / $0
  -> v21-bound annual acquisition successor                       RETIRED / runtime fail-closed / 36 staged / 0 accepted
  -> v22 non-resuming annual acquisition successor                RETIRED / unexecuted self-referential census preparation
  -> v23 reconstruction-stable acquisition successor             RETIRED / unexecuted volatile-capacity audit preparation
  -> v24 volatile-capacity-safe acquisition successor            RETIRED / 160 downloads / verifier found 320 hard-link cleanup failures
  -> exact no-network custody repair successor                    CURRENT_REACHABLE / SUCCESS / authority consumed
  -> Phase 1A DBN + sidecar inactive custody                       CURRENT_REACHABLE / 160 + 160 / single-link / no decode
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
status, statistics, ohlcv-1m, and ohlcv-1s. All 160 target DBNs and adjacent
sidecars now exist only at their final read-only single-link paths. The v2
repair terminal verifies all 1,849,575,228 DBN bytes without decoding rows.

No micro row-processing phase is labeled complete. The v2, v4, v5, v6, v7, v8, v9, v10, v11, v12, v13, v14, v15, v16, v17, v18, v19, and v20 metadata-only attempts
executed once each and produced only preserved fail-closed metadata evidence.
The two official CME product-date reports are sealed for MES, MCL, MGC, and
M6E. V20 executed once and is preserved as fail-closed timeout evidence. The
timeout-safe cumulative v21 successor executed once and passed metadata-only.
The v21 Phase 1A downloader then executed once under a separate approval and
failed closed at its 7,200-second global runtime ceiling after 36 downloads.
Those complete staging pairs and terminal are preserved read-only, but zero
pairs were accepted or finalized and no target under `data/dbn` exists. Its
authorization is consumed and cannot authorize a retry. The v22 non-resuming
implementation was committed, but its first unexecuted plan/audit preparation
revealed that v7 recorded the worktree before its own three create-only outputs
appeared. The exact v22 artifacts are preserved and superseded; no authorization
was consumed and no provider call occurred. V23 corrected that worktree defect,
but its unexecuted audit self-hashed an exact live free-disk byte count and did
not reconstruct after unrelated filesystem writes. Its exact outputs are
preserved and superseded without provider access. V24 then executed once under
its own exact approval. All 160 cost checks and downloads completed at $0 with
zero retries, producing 160 DBNs, 160 sidecars, and 1,849,575,228 DBN bytes.
Its terminal also records 320 staging-cleanup failures: marking the hard-linked
final names read-only caused Windows to reject every staging-alias removal. The
canonical verifier therefore rejected the remaining two-link files. The exact
source-safe failure report is preserved and the consumed v24 authority cannot
retry. The separately approved v2 no-network repair then removed only those 320
aliases, verified every DBN hash and sidecar identity before/after, restored
read-only final state, and wrote terminal evidence last. Phase 1A inactive
custody is now complete. V3 produced 120 inactive Phase 1B Parquets but failed
closed before Phase 2; the one-source causal materializer diagnostic later
passed without certifying the lane. The micro catalog is inactive, registration is blocked, and all
download/repair authority is consumed. Apart
from the preserved price-free v2, v4, v5, v6, v7, v8, v9, v10, v11, v12, v13, v14, v15, v16, v17, v18, v19, and v20 failure reports and the v21 PASS report, the code proves downloader mechanics
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
