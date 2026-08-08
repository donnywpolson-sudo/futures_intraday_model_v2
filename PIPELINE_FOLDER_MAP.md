# Current pipeline and folder map

This is a topology guide, not authority. `CURRENT_WORKFLOW.md` controls normal
work and every real-data or publishing action remains separately controlled.

## Current reachable lanes

| Lane | Reachable surface | Data role | Authority |
| --- | --- | --- | --- |
| Synthetic mechanics | `futures-pipeline`; `src/futures_rebuild/pipeline.py` | Generated fixtures only | None |
| Active Alpha state | `configs/active_alpha_research_ladder.json`; bound registry contract/profile | Defines Tier 0, ES pilot, Tier 1-3, one 2025 holdout, forward monitoring | None by itself |
| Source/readiness preparation | Active catalog/calendar resolvers and immutable census-plan modules | Proves source compatibility without returns | Separate row-read approval required to execute |
| Trial registration/economic execution | `src/futures_rebuild/certified_research_gateway.py` | Exact certified registration and one-use historical execution | Exact gateway authority required |
| Cockpit | `src/futures_rebuild/live_cockpit/`; `python -m futures_rebuild.live_cockpit` | Observation only | No order or position-changing authority |
| Historical evidence | versioned trial modules, old Phase 3-8 chain, registries, receipts, reports | Interpretation and provenance only | Retired; cannot authorize current work |

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
