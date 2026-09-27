# Bounded discovery implementation

Goal: find implementable futures strategies with economically meaningful net
returns, acceptable risk, and credible improvement over declared benchmarks on
unused evidence. Retain unsupported conclusions and stop within budget.

This is an implementation reference. `CURRENT_WORKFLOW.md` remains the only
day-to-day operating guide. This successor is **installed, with no research
operations enabled**. The [activation record](../state/unpublished_evidence/discovery_loop_v1/activation_v1/REPORT.md)
identifies the installed bytes, checks and rollback copies.

## What exists

`src/futures_rebuild/discovery.py` provides the small versioned study record,
append-only metadata history, batch declaration, reservations and shared result
projection. `discovery_attempt.py` adds durable fit charges to the existing
ES/NQ resource budget. There is no scheduler, provider, trading adapter, new
dependency, evidence migration or replacement statistical formula.

History relates question -> immutable scientific declaration -> candidates and
benchmarks -> operation attempts -> evaluations -> result. A study's scientific
ID cannot be renamed into a different history. A new scientific ID must retain
the recorded exposure, including globally disclosed diagnostics and conservatively
reserved access from failed attempts. Explicitly known result influences survive
descent. Unknown prior search remains unknown. These records cannot discover an
undisclosed manual look or prove that an owner-controlled filesystem was never
rewritten; the existing authority boundary and reviewed history prefix matter.

The import includes the retrospective question, ES/NQ transfer question, their
known development exposure, and ES/NQ's failed and replacement materialization
references. It is deliberately an incomplete historical census. Historical
resource consumption is unknown, not zero; imported attempts do not become new
studies or comparable Sharpe observations.

Every batch requires questions, rationale, exact development interval,
candidates/permitted variations, benchmarks, primary rule, stop conditions, and
all six limits: attempts, fits, seconds, memory, new output, additional cost.
Only zero-cost local work is supported. Economic batches also require explicit
execution, costs, sizing, capital treatment, benchmark policies, positive
owner-chosen economic hurdles and risk limits. No universal dollar hurdle exists.

The proposed central gateway guard runs before the original policy. It checks
the reviewed batch, exact operation binding, declaration bytes and persistent
history. It grants no receipt. Covered historical execution routes fail closed
when not listed in the new binding. Their existing study declarations and old
receipts are preserved, not silently rewritten or requalified.

An attempt reserves its entire maximum work before input access; a fit is charged
before the fitter. Failures do not refund reservations. Repeated IDs, exhausted
budgets and unresolved attempts cannot launch another attempt. New information
requires a reviewed successor batch. Reservations conservatively charge summed
runtime/output/fits and peak memory; they are not measured performance or an
independent statistical error budget. The existing cooperative runtime/memory
checks remain cooperative, not OS-enforced process limits.

## Results and metrics

Markdown and the cockpit use the same `display_rows` projection: question,
declaration/history/exposure, type/stage, completed/expected work, primary metrics
and benchmarks, uncertainty, economics, disposition and attempts. A terminal
attempt cannot be overwritten by a favorable diagnostic. Nothing in these
summaries grants promotion, confirmation, automatic continuation or trading.

- Failed execution retains its fatal record and terminal metadata; partial
  evidence is inconclusive. An interrupted process without terminal metadata
  stays visibly unresolved and cannot auto-retry.
- A complete valid primary failure closes the study as unsupported. Predictive
  improvement is only a predictive finding. Economic development candidates
  still stop at confirmation prerequisites.
- Missing metrics stay null with reasons. No expected observations cannot
  establish a scientific result. Incomplete work hides full-run metrics.
- The ES/NQ projection uses the existing primary block length 20, eight windows,
  sixteen fits and three declared comparisons. Availability follows its adopted
  scientific prerequisites; this does not claim a newly measured power study.
- `economic_summary` projects existing `l0_alpha_report/2` accounting and shared
  benchmark comparisons. It preserves net dollars, cost assumptions, dollar
  drawdown and exposure/diagnostics without modifying saved reports. Its decision
  remains inconclusive without all declared benchmarks and the declared
  evidential prerequisites. Report projection is not statistical qualification.
- Win rate includes break-even closed trades in its denominator; average loss
  is negative; profit factor is net winning dollars divided by absolute net
  losing dollars. Empty win/loss groups and zero-loss denominators are
  unavailable. Complete counts and gross-minus-cost-equals-net totals must
  reconcile before these diagnostics appear.
- CAGR, account Sharpe and percentage drawdown remain unavailable without a
  capital-return model. Dollar drawdown from closed trades is not intratrade,
  open-position or account-equity risk.

## Installed integration and checks

From the canonical root, using the existing environment:

```powershell
$env:PYTHONPATH = 'src'
.\.venv\Scripts\python.exe -B tests/test_discovery.py
.\.venv\Scripts\python.exe -B state/unpublished_evidence/discovery_loop_v1/activation_v1/verify_installed.py
```

The runtime test command runs eleven contracts. Two predecessor-only preparation
tests are explicitly skipped after installation; their full pre-adoption pass is
retained in review_v8. The installed verifier checks the actual gateway,
configuration, history, dependency bindings and cockpit metadata API.

The preserved `prepare_discovery.py`/`demo_discovery.py` commands target the
predecessor installation; do not rerun them against the installed successor or
overwrite adopted history. Their reviewed output is retained in review_v8.
The preparation command read explicit declarations and completed materialization
metadata only. It produces exact policy/reader/cockpit candidate files, patches,
history import and proposed configuration under that directory. It verifies
installed integration/configuration bytes did not change during preparation.
The fixed history destination is `state/discovery_history`; installation must
not overwrite an existing journal. Future batches extend that journal.

The prepared ES/NQ operation references the other task's completed
materialization. Before an evaluation attempt exists it can propose the existing
sixteen-fit evaluation and preserves
the scientific declaration, source plan and daily artifact locations. Its cap
is one attempt, sixteen fits, 3,600 seconds, 6 GiB working memory, 256 MiB new
output and $0. Historical duration is unmeasured; synthetic timing is not a
historical runtime estimate. No materialization rerun, fresh receipt or
historical evaluation is issued by preparation.

During this implementation another task started the historical evaluation.
The final preparation is therefore infrastructure-only: both proposed access
operation IDs are null and the discovery operation allowlist is empty. The
existing evaluation is now imported as complete and unsupported, using its
operation-matched terminal task metadata and validation record. The preparation
binds those metadata hashes; it did not inspect observations or recompute scores.
Missing terminal metadata remains unknown, and inconsistent metadata blocks the
import. It does not propose a
replacement or repeat. Preparation remains possible without interrupting that
writer or reusing its research approval.

Tests exercise the prepared receipt-to-reader route on ten generated partitions,
receipt reuse rejection, reservations, immutable decisions, history/exposure,
legacy reporting, economics availability and protected-scope rejection. Separate
generated no-edge and lead populations run the actual frozen ES/NQ fitter over
all eight windows: sixteen fits each. Both stop with no confirmation authority.
The static cockpit preview's four display examples are explicitly handwritten
synthetic examples; they are not historical ES/NQ results or calibration evidence.

## Remaining boundaries

The owner approved infrastructure installation with “activate”. Exact rollback
copies were retained and installed checks passed. Subsequent changes must bind
the current installation and preserve adopted history. Do not replay an old
preparation over newer work.

After adoption, historical evaluation still requires its own exact existing
gateway receipt. A completed batch closes, identifies a concrete evidence gap,
proposes a changed question with inherited exposure, or prepares confirmation
prerequisites; it never starts another batch itself. The next family and its
economic policies are owner-reviewed before result access.

The [qualification review](DISCOVERY_QUALIFICATION_REVIEW.md) records the
scientific blocker. Infrastructure activation grants no replacement method,
statistical qualification, sealed-period access, forward observation or trading.
Ordinary discovery accepts only 2018-2022 development;
the sealed 252 sessions and forward evidence remain outside this feedback loop.
