# Universal preexecution gateway remediation

Status: **IMPLEMENTED AND SYNTHETICALLY VERIFIED - UNSTAGED**

## Policy value

- Concrete risk prevented: a future trial-specific writer or historical
  executor could bypass the row-certified fold gate and consume a one-use
  authorization before mandatory executability was proven.
- Decision improved: registration and historical execution now have one
  current technical surface whose identity and claim bind the exact trial,
  protocol, registration bytes, certificate, and readiness evidence.
- Why the simpler rule was insufficient: `CURRENT_WORKFLOW.md` required the
  certified writer and wrapper, but versioned and generic real-history helpers
  remained callable and the shared receipt boundary did not reject their
  operation names.

## Implemented control

- `CertifiedResearchGateway.register_trial` delegates to the create-only
  row-certified writer and returns the exact registration hash.
- `CertifiedResearchGateway.claim_historical_execution` reconstructs the
  receipt scope from the immutable registration and certificate, rejects scope
  substitution, revalidates evidence, and only then consumes the claim.
- The shared receipt verifier accepts one fixed certified trial-execution
  operation plus an exact allowlist of existing non-strategy preparatory
  operations. Unknown and retired trial operations fail before claim creation.
- The former generic `TrialRegistry` rejects real-history registration. V4's
  custom executor rejects before source access; all other identified historical
  executors cross the shared receipt verifier.
- V4-V12, Standard-only, Final, and Authoritative succession snapshots are
  explicitly legacy. One consumed overnight-census pre-execution snapshot node
  is legacy while the remaining census behavior tests stay high-risk.

## Verification

- Focused gateway, boundary, fold, documentation, and census tests: 82 passed,
  1 explicitly legacy node deselected.
- Current lane: 103 passed, 1,307 deselected.
- Complete high-risk lane: 643 passed, 766 deselected.
- Source audit: V4 was the only real-history executor without
  `OperationReceipt`; it now has an immediate retirement guard.
- No historical rows, research parameters, 2025 data, providers, network,
  credentials, publication, active data, or trading paths were accessed.

## Repository boundary

This remediation is local and unstaged. It changes code, tests, current
workflow documentation, and this non-research report only. Existing trial
registrations, predictions, outcomes, closures, authorization-use records,
readiness evidence, active pointer bytes, and immutable source releases are
unchanged.

## Exact repository-consolidation scope

```text
CODEX_HANDOFF.md
CURRENT_WORKFLOW.md
docs/LEGACY_WORKFLOWS.md
reports/universal_preexecution_gateway_remediation.md
src/futures_rebuild/boundary.py
src/futures_rebuild/certified_research_gateway.py
src/futures_rebuild/preexecution_fold_certification.py
src/futures_rebuild/research_gateway_policy.py
src/futures_rebuild/tier1_bracket_v4.py
src/futures_rebuild/trial.py
tests/conftest.py
tests/test_certified_research_gateway.py
tests/test_operational_documents.py
tests/test_preexecution_fold_certification.py
tests/test_repo_boundary.py
tests/test_workflow_lanes.py
```
