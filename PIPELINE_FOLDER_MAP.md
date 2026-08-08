# Current pipeline and folder map

This is a topology guide, not authority. `CURRENT_WORKFLOW.md` controls normal
work and every real-data or publishing action remains separately controlled.

## Current implementation reality

| Classification | Surface | Reality | Authority |
| --- | --- | --- | --- |
| `CURRENT_REACHABLE` | Standard Alpha: `configs/active_alpha_research_ladder.json`; `data/active/catalog.json` | Active standard/full-contract ladder and catalog | Exact gateway authority required for real research |
| `SYNTHETIC_ONLY` | `futures-pipeline`; `src/futures_rebuild/pipeline.py` | Generated mechanics fixtures only | No historical or alpha authority |
| `RETIRED` | `configs/apex_micro_tier01_databento_preflight_plan.json`; obsolete `febccafd...` preparation | Preserved MES/MGC/M6E/M6A bytes, classified `SUPERSEDED_PREPARATION — MICRO_TIER1_SCOPE_RECONCILIATION`; cannot execute as current | No provider or publication authority |
| `PREPARED_NOT_EXECUTED` | Corrected contract/profile/pointer under `state/unpublished_evidence/apex_micro_ladder_preparation_v2/`; architecture under `alpha_research_architecture_v2/` | Inactive MES/MCL/MGC/M6E successor; no active micro pointer or catalog | None |
| `PREPARED_NOT_EXECUTED` | `scripts/prepare_apex_micro_infrastructure.py`; `configs/apex_micro_product_reference_requirements.json`; `state/unpublished_evidence/apex_micro_preparation_supersessions/micro_tier1_scope_reconciliation.json` | Deterministic create-only successor preparation and byte-bound supersession classification | No provider or activation authority |
| `PREPARED_NOT_EXECUTED` | `configs/apex_micro_tier01_databento_metadata_preflight_v2.json`; `src/futures_rebuild/micro_alpha_databento_preflight.py` | Exact 20-definition, 51-call, metadata-only executor; synthetic tests only | Exact single-use metadata/provider approval absent |
| `PREPARED_NOT_EXECUTED` | `src/futures_rebuild/micro_alpha_acquisition.py` | Phase 1A create-only inactive-custody downloader implemented and adversarially tested; no provider run and no DBN output | Separate download approval absent; passing preflight and committed HEAD required first |
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
  -> metadata-only Databento preflight                             PREPARED_NOT_EXECUTED / approval absent
  -> exact audited acquisition plan                                NOT_IMPLEMENTED until preflight PASS
  -> Phase 1A DBN + sidecar inactive custody                       PREPARED_NOT_EXECUTED / download authority absent
  -> Phase 1B definition/status/statistics/1m/1s decoding          row-read approval required
  -> Phase 2 1m feature + separate 1s execution foundations        row-read approval required
  -> source certification
  -> micro catalog publication/activation                          NOT_IMPLEMENTED
  -> micro mechanism Tier 0 and later research                     blocked
```

Exact intended Phase 1A destination:
`data/dbn/<schema-folder>/<micro-root>/<year>/<interval>.dbn.zst` with an
adjacent `.manifest.json` sidecar. The prepared source scope is MES, MCL, MGC,
and M6E across the required Databento Standard historical schemas: definition,
status, statistics, ohlcv-1m, and ohlcv-1s. No target micro DBN currently exists.

No micro phase is labeled complete. The metadata preflight and Phase 1A
downloader are implemented but unexecuted; Phase 1B/2 row processing is also
unexecuted. The micro catalog is inactive, registration is blocked, and
download authority is absent. The code currently proves fail-closed mechanics
with synthetic records only.

## Active and protected folders

| Folder | Meaning |
| --- | --- |
| `data/active/` | Active hash-bound local research sources; resolve through the catalog only |
| `data/dbn/`, `data/raw/`, `data/causally_gated_normalized/` | Immutable source families and inactive releases |
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
