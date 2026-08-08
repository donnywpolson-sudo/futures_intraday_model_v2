# Futures research operating guide

`CURRENT_WORKFLOW.md` is the only day-to-day workflow guide. This file keeps
the durable safety and research rules that apply to every task.

## Work normally

- A user request authorizes ordinary local work: inspect, edit, test, and create
  non-research artifacts. Staging and local commits are separate actions; each
  requires explicit user authorization.
- Keep working in the same task until it is complete or reaches a real high-risk boundary. Do not ask the user to copy a plan ID, hash, command, approval line, token, or continuation prompt.
- Preserve unrelated work, check repository identity and status before writing,
  and never use broad staging. Stage only explicitly authorized paths. A local
  commit never authorizes a push.

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

## Historic material and policy value

`docs/LEGACY_WORKFLOWS.md` classifies retired modules, tests, and evidence.
They remain readable but are never instructions or command surfaces for new
work.

Before adding a policy control, state: (1) the concrete risk it prevents,
(2) the decision it improves, and (3) why an existing simpler rule is not
enough. Do not add the control if that case cannot be made.

## Reporting

Use concise plain English. When useful, report `Status`, `Completed`, `Checks`,
and `Needs attention`. Mention only a real blocker or the single high-risk
confirmation now needed, and distinguish verified facts from assumptions.
