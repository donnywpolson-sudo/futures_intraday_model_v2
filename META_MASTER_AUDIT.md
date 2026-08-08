# Meta Audit of the Systematic Futures Master Audit

Version: 1.2.0
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

## Independent sources and separation

The blind standard must be built from project behavior, not from Master Audit
wording. Inspect the public commands, contracts, schemas, capability boundaries,
immutable publication mechanics, release readers, trial registry, holdout
guards, cockpit dependency graph, installer/shortcut behavior, recovery paths,
and tests before close-reading the Master.

Record when the same implementation produced both a claim and its supposed
proof. Such evidence needs an independent calculation, alternate reader,
adversarial fixture, or clean-room reproduction before it can close a High or
Critical threat. Reviewer identity, source order, frozen hashes, and any
conflict of interest belong in the final report.

## Minimum independent coverage standard

The blind standard must cover:

- exact evidence provenance, immutable releases, canonical schemas, hashes,
  reproducibility, dependency locking, and clean committed state;
- point-in-time universe admission, market/contract identity, immutable
  historical DBN observability, missing-is-not-closed handling, no synthetic
  open/close rows, current/forward CME captures, exact product mappings,
  activated calendar segments, rolls, source freshness, and causal timing;
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

## Required false-pass campaign

At minimum, attempt mutations that:

- remove one required result, evidence reference, release file, dependency,
  asset, license, market, or rollback artifact;
- replace a receipt, universe/profile identity, source release, implementation,
  clean Git identity, or shortcut target with a stale or internally consistent
  but wrong value;
- mark a required check not applicable, reuse one approval across authority
  classes, publish a manifest before payload closure, or substitute a producer
  success flag for independent evidence;
- inject future-known identity/session/roll state, feature access to outcomes,
  an unregistered real-data trial, a hidden holdout query, or satellite rescue;
- substitute a historical observability contract that fills time, calls
  unobserved periods closed, claims official CME authority, drops quarantines,
  or binds a wrong predecessor; or substitute a stale/incomplete
  current/forward CME capture, ambiguous product mapping, unknown schedule
  state, or no-op successor;
- expose a credential through Git, logs, reports, cache, package, install, or
  shortcut metadata;
- add an order/broker dependency, unbounded reconnect, cache mutation, corrupt
  state, missing abstention, external runtime import, or broken shortcut
  rollback to an otherwise passing cockpit.

For each mutation, identify the first expected failing precheck/subcheck and
verify the actual status, decision, and logical exit. A failure caught only by
an unrelated later control remains a traceability weakness.

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

The traceability matrix has one row per independently derived requirement and
records severity/priority, false-pass path, authoritative owner, exact Master
text, stage/gate/subcheck, implementation/config enforcement, executable test,
mutation result, evidence identity, residual risk, disposition, and remediation
owner. Missing rows or undocumented residual risk make the review
`INSUFFICIENT_EVIDENCE`.
