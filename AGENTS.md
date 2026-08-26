# Futures research operating guide

`CURRENT_WORKFLOW.md` is the only day-to-day workflow guide. This file keeps
the durable safety and research rules that apply to every task.

## Work normally

- A user request to change, fix, build, implement, or complete an outcome
  authorizes ordinary local work: inspect, edit, test, create non-research
  artifacts, stage exact task paths, and make a scoped local commit when needed
  to deliver that outcome.
- Keep working in the same task until it is complete or reaches a real high-risk boundary. Do not ask the user to copy a plan ID, hash, command, approval line, token, or continuation prompt.
- Preserve unrelated work, check repository identity and status before writing,
  and never use broad staging. If an intended task path overlaps a pre-existing
  user change, ask once before staging or committing it. A local commit never
  authorizes a push. Report unrelated problems instead of fixing or cleaning
  them up.

## Pause only for high risk

Ask once in plain language before provider or network use, a real-data read or
evaluation, publication or active-data mutation, installation or activation,
deletion or cutover, holdout or forward access, any trading or order path, or
a push. State the scope, bounded cost, outputs, and preservation or rollback
boundary. A conversational `Approve` is sufficient.

High-risk repository CLIs are prepare-only. They may describe the operation but
do not accept approval text or execute it directly. Codex records the approved
scope in its task report and performs the operation only within that approved
task.

## Keep research and cockpit safe

- Use only declared source families and verified immutable releases. Preserve
  accepted bytes; corrections are immutable successors.
- Real-data research needs a durable trial declaration. Synthetic tests prove
  mechanics, not alpha. Features, outcomes, predictions, and evaluation stay
  separated; splits are chronological with training-only transforms, purge, and
  embargo.
- Missing, stale, ambiguous, incomplete, or future-known input fails closed.
- The cockpit is observation-only: no broker, order, execution, or trading
  dependency. Missing or stale data causes an error or abstention, never a
  trade.
- Never copy, log, stage, package, or report credentials.

## Keep changes proportional

- For a localized task, inspect nearby code and make the smallest coherent
  change that satisfies the request and current contracts. Follow the existing
  architecture.
- Add an abstraction (including a one-implementation interface, forwarding
  wrapper, or speculative extension point), dependency, fallback/retry/cache/
  queue/telemetry/compatibility path, public API or schema change, migration,
  cross-cutting layer, or broad refactor only for a current requirement or
  invariant. If it materially expands the request, state the risk it prevents,
  the decision it improves, and why a simpler rule or existing-pattern change
  is insufficient; ask before proceeding. Necessary complexity for correctness,
  security, compatibility, or data integrity remains allowed.
- Use the narrowest verification that covers changed behavior and risk; run
  broader checks only when an affected contract or commit/release workflow
  requires them. Update only documentation or comments whose behavior,
  interface, procedure, or non-obvious invariant changed. Do not add unrelated
  test infrastructure or repeat unchanged checks without new evidence.
- Stop when the requested outcome and acceptance criteria are met, relevant
  checks pass, and one final diff review finds no accidental scope growth or
  known material correctness, security, or compatibility issue. Report optional
  improvements instead of implementing them.

## Historic material

`docs/LEGACY_WORKFLOWS.md` classifies retired modules, tests, and evidence.
They remain readable but are never instructions or command surfaces for new
work.

## Reporting

Use concise plain English. When useful, report `Status`, `Completed`, `Checks`,
and `Needs attention`. Mention only a real blocker or the single high-risk
confirmation now needed, and distinguish verified facts from assumptions.
