# Profit-aligned research successor

The successor is implemented as inactive, synthetic-only mechanics. The current
preparation is `configs/fixed_hour_model_comparison_v6.json`; v3 through v5 are
preserved preparation revisions with the same statistical design and candidate
identities. Descriptive reporting, final model custody and complete-scenario validation were completed between
revisions. No test threshold, simulation scenario or power requirement changed.
It asks
whether a frozen ES, NQ, CL or GC strategy has positive mean daily net dollar
P&L. Ridge and XGBoost are both eligible. Benchmark superiority is descriptive;
failure does not establish that OHLCV has no edge.

## Measured result

**`BLOCKED_STATISTICAL_VALIDATION_FAILED`.** The [complete final report](../state/unpublished_evidence/profit_aligned_research_v1/1e1ec918e520a9aaf77ed38188cae5783cbdbd6587473c4575be771b9ea1fdcd/report.json)
contains all 28,000 replications: 2,000 for each of 14 declared scenarios.
Its declaration binds 22 implementation files and the numerical environment.
Ten of the twelve gated null families failed the required simultaneous upper
confidence limit. Selected results:

| Null scenario | False qualifications | Simultaneous 95% upper bound | Required maximum |
| --- | ---: | ---: | ---: |
| Independent normal | 3.70% | 5.21% | 2.50% |
| Serial dependence, AR(0.5) | 4.30% | 5.90% | 2.50% |
| Rare large losses | 29.70% | 32.98% | 2.50% |
| Partial null | 2.45% | 3.73% | 2.50% |

Strong-effect detection was 97.95%, with a simultaneous lower bound of 96.76%,
exceeding its 80% requirement. That power result cannot compensate for failed
false-positive control. A rare-loss window can omit the loss event and look
profitable even though the generating process has zero expected net P&L.

All 13 successor and 42 predecessor regression tests passed, including complete
four-market selection/replay, chronological prediction custody, bid/ask dollar
accounting, final-refit guards and incomplete-report rejection. The 17 preserved
predecessor implementation hashes are unchanged. No real-data research or
activation is authorized by these engineering results. A different statistical
procedure requires a new declaration and validation; the failed method was not
tuned after seeing these outcomes.

## Economics and selection

Each signal uses one standard contract, with no compounding or portfolio claim.
The primary score is the mean of **daily sums of stress net dollars**, across
all inner audit dates. Its sign agrees with total P&L over those dates. The old
per-opportunity daily average is not used for selection or fresh confirmation.
No losses are clipped, and a trade ledger must reconcile exactly to daily totals.

The nine packs, 29 features, 45 candidates per market, eight outer folds, four
inner folds, chronological embargo, train-only transforms, Ridge settings and
bounded XGBoost settings are preserved. Ties within $0.01/day use the existing
simplicity order. Complete development calendars are required; incomplete dates
fail instead of silently disappearing from a score. Declared zero-opportunity
dates in fresh confirmation contribute zero P&L and still consume a session.

Entry and exit use bid/ask quotes for the same actual contract. Long positions
buy the ask and sell the bid; shorts sell the bid and buy the ask. Net P&L
subtracts the greater of $10 and the verified round-trip fee, plus the existing
scenario-specific adverse allowance. It does not subtract the spread twice.
The decision threshold uses only the then-known full spread, fee and stress
allowance. Zero and negative prices are valid.

Quotes must have matching source and clock identities, completed intervals,
causal availability, acceptable age, positive displayed size and valid books.
Missing decision or entry quotes cause abstention. An entered trade with a
missing exit invalidates evidence. These are quote-based research economics;
they do not establish achievable fills, queue position, capacity or deployability.

## Development and fresh confirmation

Historical results remain development evidence. The final candidate comes from
the last outer fold's inner selection, not whichever outer result looked best.
The 2023-to-development-cutoff stress attempt is consumed before scoring and
must have positive whole-period stress P&L. Its unchanged candidate can then
receive a parameter-only refit using development labels available before the
protected cutoff. No post-refit performance score is produced.
The model freeze must follow training-label availability and precede the first
stress decision. Refit inputs must contain every original and stress sample in
order, preserving original fitting data and stress features exactly.

Fresh confirmation freezes four market-specific models, transformations, cost
policy, quote source and clock identities, session boundaries, and opportunity
schedules before 252 future sessions. Durable predictions precede outcomes.
Daily evaluation replays those predictions against saved raw quotes. Missing,
altered or immature evidence fails closed. There is no early success, horizon
extension, candidate replacement, retraining or reuse of historical holdout data.

Each market needs at least 500 executed trades and positive total/mean stress
P&L. The one-sided, null-centered studentized stationary bootstrap uses raw
daily dollars, HAC lag 5, mean block length 10, 9,999 draws and seed 62817.
Four Bonferroni slots remain allocated even when a market fails; failed slots
receive p=1. Campaign k receives 0.05/2^k, starting at k=1, with no reset or
reallocation. With the frozen resample count, campaign 7 onward cannot resolve
a small enough adjusted p-value and therefore cannot qualify.

Resampling can stop for futility after 62 exceedances: even the smallest possible
completed p-value would then fail every campaign threshold. That path reports
p=1 conservatively. Potential rejections receive all 9,999 draws. This shortcut
does not allow an early decision during the 252-session observation horizon.

## Validation and remaining boundaries

The declaration is written before Monte Carlo results. Fourteen scenarios each
run 2,000 replications through the production decision kernel: ordinary and
correlated nulls, serial dependence, heavy tails, volatility clustering, rare
large losses, negative drift, partial nulls, three power levels, and two explicit
robustness cases. Exact simultaneous 95% binomial intervals cover all reported
rates. Every supported null's familywise upper bound must be at most 2.5%; the
strong-effect detection lower bound must be at least 80%. Robustness cases are
reported separately. Passing synthetic tests is not evidence of alpha or a
distribution-free proof of bootstrap validity.

The fresh numerical route is separate from the unresolved historical census.
The predecessor's accounting floor is retained and the new 180 configurations
are deduplicated. These charges are not observed comparable trials or a proven
search upper bound. No historical qualification block is cleared.

All authority flags remain false. The synthetic file store demonstrates
create-only custody and replay; it is not an externally trusted, tamper-proof
allocator. A separately certified source/clock/fee/calendar adapter and trusted
campaign custody are still required before any authorized real-data run. There
is no installed real-data runner, active pointer change, provider access,
holdout read or trading path. A numerical `QUALIFIED_MECHANICS_ONLY` result always
has `qualification_ready=false` and `activation_ready=false`.
