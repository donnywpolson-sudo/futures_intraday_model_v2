# Predictive successor V9: implementation and remaining work

2026-09-07. The user selected date-balanced inner-validation MSE, forecasts of
the actual contract observed at the decision, and endpoint-only one-hour labels.
The requested end-to-end historical implementation is **not yet complete**.

## Implemented

- Inactive V9 declaration, preserving V8 and previous configuration accounting.
  Sixty successor configurations are declared, not registered or charged twice
  by repeated preparation. The previous 65 unresolved references and penalty
  of 105 remain disclosed; the V8 historical-use decision is not reused as V9 authority.
- Existing 1m features and 5/10/16-column candidate packs, five-second decision
  delay, chronological splits, date-balanced fitting and regularization reused.
- Forecast opportunities separated from observable labels. Feature windows
  cannot cross contracts; endpoints must use the forecast contract and tick
  units. Missing intermediate bars no longer remove an otherwise valid label.
  Path-dependent diagnostics are omitted; they are not fabricated from endpoints.
- All forecast-eligible opportunities retained, including missing or invalid
  future outcomes. Empty dates and rejection reasons remain in the records.
  Late training labels are removed before the 252/505-date support check.
- Inner MSE selection with equal weight per scorable date and exact-score
  candidate-ID tie breaking. Outer comparisons use identical observed rows for
  the model, zero change and the frozen arithmetic training-mean benchmark.
- Eight outer/four inner folds and the predetermined last-inner-winner final
  refit. Candidate failures and partial runs are retained. Generated acceptance
  saves opportunity evidence, runs, final models and predictions, and checks
  exact prediction reproduction after loading each final model.

Implementation: `rolling_hour_predictive.py` and
`rolling_hour_predictive_preparation.py` in `src/futures_rebuild`.
The CLI has only `freeze`, `validate` and `synthetic`; none executes history.

## Validation

The focused V9 and existing V8 suites passed together: **19 tests**. Full saved
four-market generated-data acceptance passed: each market has 1,026 declared
dates, 7,049 observable forecast rows, eight outer folds and 120 candidate
outcomes. All four final synthetic models reproduced their 7,049 predictions
exactly after reload. These results establish mechanics, not historical alpha.

An independent saved-output inspection also passed for all four markets. It
recomputed inner candidate errors and winners, outer errors and training-mean
benchmarks, checked common candidate audit rows and label availability, and
reproduced all saved final predictions. There are 3,380 outer forecasts per
market. The runnable inspection is `.tmp/verify_predictive_v9.py`.

Evidence:
[acceptance report](../state/unpublished_evidence/rolling_hour_predictive_v1/2a12895f38477fcef72a922e4f4c89cffc13eb4073ebf04d8d78e91bf56e8ad5/report.json),
[independent inspection](../state/unpublished_evidence/rolling_hour_predictive_v1/2a12895f38477fcef72a922e4f4c89cffc13eb4073ebf04d8d78e91bf56e8ad5/independent_inspection.json).
Each market's subdirectory preserves the opportunity records, nested run,
final model and final predictions. V9 and its V8 predecessor bindings validate;
scoped diff and new-file whitespace checks pass. No files were staged or committed.
The 18 saved acceptance/inspection files occupy 927,556,309 logical bytes
(about 885 MiB); repeated inner-fold fit/audit identifiers account for part of
that evidence. Existing evidence was not removed or compacted.

The initial system-Python smoke attempt stopped at the existing XGBoost version
guard after dataset construction. Binding validation also rejected that runtime.
The existing project `.venv` supplies the declared versions; no dependencies
were installed or version checks relaxed. An initial empty-input unit test
exposed a different exception class; the scorer now rejects empty dates explicitly.
These were synthetic implementation checks, not historical candidate results.

## Remaining, in order

1. Establish the original schedule/version/publication evidence required by the
   retained session-quartile features and opportunity grid. The owned workbook
   named for New Year 2019 actually describes January 2020. Parsing all 192
   workbook/family combinations does not supply the missing January 2019 schedule
   or establish when revised schedules were knowable.
2. Certify applicable bar availability, instrument definitions and any consumed
   status timing against the actual 2018–2022 sources. The five-second delay is
   frozen behavior, not proof of historical delivery latency. No retrospective
   mapping may stand in for a contemporaneous contract observation.
3. Complete source certification and bind the successor-specific descriptive-use
   decision for registration. A separate source adapter and
   `CertifiedResearchGateway.prepare_predictive_readiness` now connect source
   products to the frozen fold-support checks. They require a consumed bounded
   readiness receipt and matching evidence files/certificate; no caller approval
   flag, plain lag assumption or retrospective-calendar flag admits a source.
   This is a readiness connection, not a historical execution authorization.
   See the [measured source-readiness report](../state/unpublished_evidence/rolling_hour_predictive_v1/source_readiness_v1/task_report.md).
4. After bounded historical-execution approval and readiness, run the frozen
   nested procedure and save/reload all four final historical models. Preserve
   every result, including disappointing scores; keep confirmation and forward
   data unread. Do not use synthetic success to bypass steps 1–3.

The free public-reference check included in the implementation request was
performed without credentials, paid acquisition or market observation reads.
CME's [January 2019 clearing advisory](https://www.cmegroup.com/tools-information/holiday-calendar/files/2019-new-years-advisory.pdf)
is available, but refers trading-hours questions to a separate calendar; it is
not the missing product-specific intraday schedule. Targeted searches for the
2019 New Year trading workbook did not recover a usable original. This is not
a claim that no archive or owner-held copy exists.

[Databento's OHLCV specification](https://databento.com/docs/schemas-and-data-formats/ohlcv)
confirms that the timestamp identifies the aggregation interval's beginning and
that intervals without trades produce no bar. Its discussion of bar construction
and publication does not establish a five-second historical delivery guarantee.
Neither reference closes the outstanding calendar-publication or availability gate.
The next required input is applicable original schedule/version evidence and
defensible historical availability evidence, followed by source certification;
no schedule or delay assumption was silently substituted to force readiness.

Original step 5 remains partial (historical source/admission integration).
Original step 6 remains partial (no historical run or historical final model).
Observed-only MSE is conditional descriptive evidence, not full-population skill
or profitability evidence, and development history remains previously inspected.
