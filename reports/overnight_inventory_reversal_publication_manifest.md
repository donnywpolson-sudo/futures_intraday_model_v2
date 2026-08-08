# Overnight inventory reversal publication and consolidation manifest

Status: **PUBLISHED AND VERIFIED - NOT STAGED OR COMMITTED**

This manifest is limited to the closed overnight-inventory-reversal mechanism
and the preexecution-certification remediation. It does not register, design,
or execute another strategy.

## Terminal disposition

- Preserve unchanged:
  `state/unpublished_evidence/overnight_inventory_reversal/24772e41730b16bfdf3187d0c9e79b2491e6118962cfdffbc16a86d4e241169c/terminal_closure.json`.
- Publish additively, without replacing the preserved closure:
  `state/unpublished_evidence/overnight_inventory_reversal/24772e41730b16bfdf3187d0c9e79b2491e6118962cfdffbc16a86d4e241169c/terminal_closure_clarification.json`.
  The create-only destination is
  `state/trial_registry/overnight_inventory_reversal_terminal_closure/d4f97ae68be1dd0074dd20917d61fdb99a4da5e2c552239e8d401403223ea643.json`.
- Publish its additive terminal event:
  `state/unpublished_evidence/overnight_inventory_reversal/24772e41730b16bfdf3187d0c9e79b2491e6118962cfdffbc16a86d4e241169c/terminal_closure_clarification_event.json`.
  The create-only destination is
  `state/trial_events/overnight_inventory_reversal/935165e7e754270a36e5cfc15725c1ce9c58c4f53050bf1ecf1bc109169622d3.json`.
- Bind row-certified readiness evidence:
  `state/unpublished_evidence/overnight_inventory_reversal_fold_readiness_v2/24772e41730b16bfdf3187d0c9e79b2491e6118962cfdffbc16a86d4e241169c/fold_readiness_certificate.json`.
  The create-only destination is
  `state/trial_registry/overnight_inventory_reversal_fold_readiness/abc910ff3a7cc5d96c59b000d74b9751b5d2fab19409653befcfa8d03f85b440.json`.
- Final disposition remains `INCONCLUSIVE_DATA_OR_COVERAGE`; no economic
  evaluation occurred, attempt 1 of 1 is consumed, retry is unauthorized, and
  incremental rescue is forbidden.
- The additive clarification, readiness report, and terminal event were
  published create-only and verified byte-for-byte. The original closure and
  active pointer remain unchanged.

## Certification remediation

- `src/futures_rebuild/preexecution_fold_certification.py`
- `src/futures_rebuild/overnight_inventory_reversal_preexecution_census.py`
- `src/futures_rebuild/overnight_inventory_reversal_preexecution_census_v2.py`
- `src/futures_rebuild/overnight_inventory_reversal_closure_publication.py`
- `scripts/run_overnight_inventory_reversal_fold_census.py`
- `scripts/run_overnight_inventory_reversal_fold_census_v2.py`
- `scripts/publish_overnight_inventory_reversal_closure.py`
- `configs/overnight_inventory_reversal_fold_census_plan.json`
- `configs/overnight_inventory_reversal_fold_census_v2_plan.json`
- `reports/overnight_inventory_reversal_preexecution_audit.md`

These are additive controls and audit-only census boundaries. The serial plan
consumed one authorization and produced no report before its runtime limit;
its plan and use receipt remain immutable. The completed v2 successor changed
only market-stream concurrency and bounded termination and produced a
row-certified FAIL without economics. Neither alters the
registered mechanism, its parameters, sources, or preserved terminal bytes.

## Focused tests

- `tests/test_preexecution_fold_certification.py`
- `tests/test_overnight_inventory_reversal_preexecution_census.py`
- `tests/test_overnight_inventory_reversal_preexecution_census_v2.py`
- `tests/test_overnight_inventory_reversal_closure_publication.py`

The applicable suite also includes the preserved overnight registration,
execution, historical-boundary, and mandatory-baseline tests. Test results are
verification evidence only, not research evidence. The current results are 45
focused tests and 60 tests across the complete applicable remediation suite.
They include immutable registration/evidence binding, registration drift,
evidence substitution, cross-protocol substitution, and refusal before claim
consumption.

## Workflow and handoff

- `CURRENT_WORKFLOW.md`
- `CODEX_HANDOFF.md`

These document that row-certified, source-bound fold readiness is mandatory
before future registration and must be revalidated before an execution claim
is consumed.

## Repository boundary

No path in this manifest is staged or committed. Publication is complete;
staging and commit each remain separately authorized. Unrelated work and all
existing trial evidence remain outside this manifest and must be preserved.

## Exact repository-consolidation scope

The following 43 paths are the complete meaningful worktree scope for this
goal. They contain the preserved trial lineage, authorization uses, terminal
evidence, certification remediation, tests, and current workflow records. No
temporary output is included. Any staging operation must name these paths
explicitly and must refuse if the live worktree census differs:

```text
CODEX_HANDOFF.md
CURRENT_WORKFLOW.md
configs/overnight_inventory_reversal_fold_census_plan.json
configs/overnight_inventory_reversal_fold_census_v2_plan.json
configs/overnight_inventory_reversal_historical_execution_plan.json
configs/overnight_inventory_reversal_preoutcome_correction.json
configs/overnight_inventory_reversal_preregistration.json
reports/overnight_inventory_reversal_preexecution_audit.md
reports/overnight_inventory_reversal_publication_manifest.md
scripts/publish_overnight_inventory_reversal_closure.py
scripts/run_overnight_inventory_reversal_fold_census.py
scripts/run_overnight_inventory_reversal_fold_census_v2.py
src/futures_rebuild/overnight_inventory_reversal_closure_publication.py
src/futures_rebuild/overnight_inventory_reversal_execution.py
src/futures_rebuild/overnight_inventory_reversal_historical_execution.py
src/futures_rebuild/overnight_inventory_reversal_preexecution_census.py
src/futures_rebuild/overnight_inventory_reversal_preexecution_census_v2.py
src/futures_rebuild/overnight_inventory_reversal_preregistration.py
src/futures_rebuild/preexecution_fold_certification.py
src/futures_rebuild/tier1_mandatory_baseline_execution.py
state/authorization_uses/3b81b12f5288a7c6012827788a7574ee109f4f4d87a051c673aaad3b801e5312.json
state/authorization_uses/3d456eb595e5fd6196427408feb35a83d3db8838ef1d78428e4ebe66fde48e64.json
state/authorization_uses/6fc820d1327dce34af854b424e1b72ef9022985f1a3f4020e6c56eb0e6b223d0.json
state/trial_events/overnight_inventory_reversal/24772e41730b16bfdf3187d0c9e79b2491e6118962cfdffbc16a86d4e241169c.json
state/trial_events/overnight_inventory_reversal/24772e41730b16bfdf3187d0c9e79b2491e6118962cfdffbc16a86d4e241169c_pre_outcome_anchor_3d456eb595e5fd6196427408feb35a83d3db8838ef1d78428e4ebe66fde48e64.json
state/trial_events/overnight_inventory_reversal/620b6e4f3a4a7460b08e7b8dec834a7b1e5b5c5800dc0a68f3e4209b89709e28.json
state/trial_events/overnight_inventory_reversal/935165e7e754270a36e5cfc15725c1ce9c58c4f53050bf1ecf1bc109169622d3.json
state/trial_registry/overnight_inventory_reversal/24772e41730b16bfdf3187d0c9e79b2491e6118962cfdffbc16a86d4e241169c.json
state/trial_registry/overnight_inventory_reversal/620b6e4f3a4a7460b08e7b8dec834a7b1e5b5c5800dc0a68f3e4209b89709e28.json
state/trial_registry/overnight_inventory_reversal_fold_readiness/abc910ff3a7cc5d96c59b000d74b9751b5d2fab19409653befcfa8d03f85b440.json
state/trial_registry/overnight_inventory_reversal_terminal_closure/d4f97ae68be1dd0074dd20917d61fdb99a4da5e2c552239e8d401403223ea643.json
state/unpublished_evidence/overnight_inventory_reversal/24772e41730b16bfdf3187d0c9e79b2491e6118962cfdffbc16a86d4e241169c/terminal_closure.json
state/unpublished_evidence/overnight_inventory_reversal/24772e41730b16bfdf3187d0c9e79b2491e6118962cfdffbc16a86d4e241169c/terminal_closure_clarification.json
state/unpublished_evidence/overnight_inventory_reversal/24772e41730b16bfdf3187d0c9e79b2491e6118962cfdffbc16a86d4e241169c/terminal_closure_clarification_event.json
state/unpublished_evidence/overnight_inventory_reversal_fold_readiness_v2/24772e41730b16bfdf3187d0c9e79b2491e6118962cfdffbc16a86d4e241169c/fold_readiness_certificate.json
tests/test_mandatory_baseline_execution.py
tests/test_overnight_inventory_reversal_closure_publication.py
tests/test_overnight_inventory_reversal_execution.py
tests/test_overnight_inventory_reversal_historical_execution.py
tests/test_overnight_inventory_reversal_preexecution_census.py
tests/test_overnight_inventory_reversal_preexecution_census_v2.py
tests/test_overnight_inventory_reversal_preregistration.py
tests/test_preexecution_fold_certification.py
```
