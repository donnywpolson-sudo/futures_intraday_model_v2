# Meta Audit of the Systematic Futures Master Audit

Version: 1.0.0
Mode: independent, evidence-only prompt-quality review

## Mission

Determine whether root `MASTER_AUDIT.md` is capable of detecting false
readiness claims for this project. The Meta Audit evaluates the audit
specification, matrix, classifier, examples, and tests. It never grants project
readiness and never substitutes for a Master Audit result.

## Mandatory blind-first method

1. Before reading `MASTER_AUDIT.md` closely, build an independent coverage
   standard from the repository's threat model, public interfaces, schemas,
   approval boundaries, immutable-release rules, data flow, research workflow,
   cockpit packaging, secret handling, and recovery requirements.
2. Enumerate plausible false-pass paths and assign each a severity:
   `Critical`, `High`, `Medium`, or `Low`, plus remediation priority
   `P0` through `P3`.
3. Only then read the Master Audit and map every independent requirement and
   false-pass path to exact Master Audit text, stage-matrix subchecks,
   classifier behavior, and executable tests.
4. Attempt adversarial mutations. At minimum test missing results, evidence-free
   pass/fail, stale hashes, escaping paths, pending or wrong universe receipt,
   target-state omissions, profile drift, secret exposure, forbidden runtime
   authority, and cockpit readiness with one required closure missing.
5. Reconcile contradictions without averaging them away. A producer assertion,
   document statement, or passing happy-path test is not independent proof.
6. Emit a traceability report with requirement, threat, severity, controlling
   text, enforcing code/config, test evidence, residual risk, and disposition.

## Minimum independent coverage standard

The blind standard must cover:

- exact evidence provenance, immutable releases, canonical schemas, hashes,
  reproducibility, dependency locking, and clean committed state;
- point-in-time universe admission, market/contract identity, sessions, rolls,
  source availability, missingness, and causal timing;
- feature/outcome/prediction isolation, chronological nested splits,
  purge/embargo, train-only fitting, and serving parity;
- complete trial genealogy, researcher degrees of freedom, multiplicity,
  optional stopping, baselines, negative controls, and finite stop rules;
- net economics, capacity, margin/liquidation, portfolio interaction,
  traditional-versus-satellite reporting, and statistical dependence;
- holdout escrow, indirect queries, disclosure budget, and access recovery;
- monitoring, abstention, state recovery, change control, and rollback;
- the 41-market cockpit's dependencies, assets/licenses, secret isolation,
  observation-only architecture, provider failures, cache/state behavior,
  package self-check, bounded live smoke, shortcuts, and rollback; and
- standalone operation with every external or legacy repository unavailable.

## Severity and closure

- `Critical/P0`: the audit can bless unauthorized data, holdout use, trading,
  secret disclosure, materially mutable evidence, or a false ready state.
- `High/P1`: a required readiness dimension can be omitted, spoofed, or accepted
  without independent evidence.
- `Medium/P2`: ambiguity or weak traceability could conceal a meaningful defect
  but another mandatory control is likely to catch it.
- `Low/P3`: clarity, maintainability, or defense-in-depth improvement.

Final Meta Audit status is:

- `SUPPORTABLE` only when every independently derived requirement is mapped,
  every required adversarial test behaves fail-closed, and no unresolved
  Critical/High or P0/P1 item remains;
- `BLOCKED` when a demonstrated false-pass path exists; or
- `INSUFFICIENT_EVIDENCE` when the review or test evidence is incomplete.

## Prohibited shortcuts

Do not:

- derive the coverage standard by paraphrasing the Master Audit;
- treat line-count, keyword presence, or schema validation as complete coverage;
- accept the classifier's own output as independent validation;
- downgrade a missing required check to not applicable without evidence;
- conceal limitations in prose outside the machine result; or
- infer authority from `SUPPORTABLE`.

## Required report

Publish one immutable, hash-bound report containing the blind coverage standard,
threat catalog, traceability matrix, adversarial test results, unresolved items,
classification, code/config/document identities, clean Git identity, reviewer
independence statement, and explicit non-authority statement.
