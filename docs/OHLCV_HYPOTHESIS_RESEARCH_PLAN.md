# OHLCV hypothesis-led discovery

The current requested direction is the separate [minute-entry successor](ROLLING_HOUR_RESEARCH.md).
Its three existing packs are compared through one frozen position policy, with
matched contrasts reported descriptively. The hourly specification below remains
predecessor context; it is not a minute-cadence execution contract or authority.

Status: bounded model-comparison successor prepared inactive; research unexecuted.
This document organizes Phase 3 questions and Phase 5 feature comparisons.
The first implementation preserves the existing 29 formulas and nine packs and
compares Ridge with shallow XGBoost. It does not separately establish any of the
three explanations below. No policy activation or historical access is granted.
CURRENT_WORKFLOW.md controls procedure.

Current discovery means bounded feature-pack selection and learning relationships
within existing inputs. It does not generate new feature formulas or classify
each hour into verified continuation/reversal/neither labels. The single target
remains next-hour terminal change. The three questions below are research
motivations; their proposed matched comparisons and explicit stretch formula are
deferred, not implemented or answered by the current model comparison.

## The three questions

Continuation and reversal are competing explanations of next-hour movement.
Session context is a possible modifier of either, not necessarily a third model.
These are unproven predictive hypotheses, not claims about actual order flow.

### 1. Continuation: does the move keep going?

Question: does recent directional movement predict further movement in the same
direction over the next hour, depending on reported volume and volatility?

Start with existing 5/20/60-minute tick changes, signed trend efficiency,
relative volume, short-versus-long volatility, and price-direction-weighted
volume. These describe price and volume behavior; they do not identify aggressive
buyers or sellers.

Comparisons to freeze: the common baseline versus a compact continuation pack;
then matched versions with and without any proposed volume/volatility interactions.
Also retain the existing unconditional and simple momentum/reversal baselines.
Adding direction, volume and volatility separately to ridge does not by itself
make the direction coefficient vary with volume or volatility.

Evidence sought: reproducible later-period predictive and stress-net improvement,
not merely a positive fitted direction coefficient or a high in-sample correlation.
A failure to clear costs rejects the trading claim even if gross predictability
is visible. A non-significant result can be inconclusive, not proof of no effect.

### 2. Reversal: does a stretched move come back?

Question: does unusually displaced price tend to move back toward its recent
reference level over the next hour?

Existing inputs include location within the trailing range, recent tick changes,
bar shape and trailing volatility. They can describe some reversal behavior, but
the current library has no explicit distance-from-rolling-average feature.

For a future bounded successor, nominate one signed stretch feature:
distance of the last available close from the mean of the same 61 available
closes, divided by max(trailing population standard deviation of the 60 one-minute
price changes, one tick). Calculate differences in ticks, not log price returns,
so zero and negative futures prices remain valid. A positive value means price
is above that reference; it does not itself instruct the system to short.

Comparisons to freeze: otherwise identical packs without versus with that stretch
input. Do not add a collection of lookbacks, stretched-price thresholds or candle
patterns after inspecting results. A previous-minute reversal baseline alone
does not test the full stretched-price hypothesis.

Evidence sought: incremental out-of-sample value from stretch information and
predeclared reversal diagnostics, after costs and complete-family statistical
controls. A negative ridge coefficient alone is not sufficient evidence.

### 3. Conditional behavior: does context change either relationship?

Question: does the relationship between the recent pattern and next-hour movement
change with elapsed time within a verified open interval?

Reuse interval quartiles rather than introducing Asian/European/US session labels.
The existing context and context_interactions packs already provide a matched
comparison for adding six quartile-by-direction/price-volume interactions.
Their comparison tests that interaction bundle; it does not separately identify
which interaction is responsible.

To test whether stretch specifically changes with quartile, a future declaration
would need a matched stretch-with/without-context comparison and its exact bounded
interaction list. It is not already implemented by the current six interactions.
Keep all eligible Globex opportunities; do not retrospectively trade only the
best-looking quartile. Any segmented reports must be declared before results.

## Shared rules

- One terminal-change target, fixed 60-minute exits, and the existing stress-cost
  long/short/abstain rule. Diagnostic excursions do not influence fitting.
- Same 61-bar-eligible hourly schedule and identical samples across comparisons;
  do not improve results by silently removing difficult opportunities.
- ES, NQ, CL and GC qualify independently under complete-family controls.
- Preserve 2018-2022 nested chronological evaluation, training-only transforms,
  purge and embargo. Outer results cannot choose new features or thresholds.
- Retain conservative provisional costs, activity and quality gates, risk reports,
  the one-shot development stress test and the common sealed-holdout boundary.
- Run the declared comparisons as one bounded research family, not continuation
  first followed by result-informed reversal and context rescue attempts.
- Distinguish a tradable predictive model from an explanation of its behavior.
  One can pass while evidence for a particular explanation remains inconclusive.

## What changes now, and what remains

Now: nine packs × (three Ridge penalties + two XGBoost profiles) = 45 candidates
per market, 180 total. Count those once above the predecessor floor of 289.
No random forest, additional model families, new stretch formula, seed search,
early stopping, or importance-driven feature search is included.

Baseline → bounded pack/model comparison → inner-fold selection → outer-training
refit and freeze. Compare the complete procedure against independently selected
Ridge on the same outer samples. Require >0.01 tick/opportunity added stress-net
mean, positive one-sided 95% HAC lower bound and joint adjusted p<=0.05 across
28 declared market/baseline claims. Failure closes this route; no Ridge fallback.
This tests incremental value over Ridge, not whether any strategy is profitable.
Report economic performance and added-value evidence separately; failure of the
latter must not be described as proof that the former is absent.

Native model artifacts bind inputs, training identities, parameters and versions.
Synthetic checks prove software mechanics only. Explicit stretch comparisons,
historical readiness/access, measured expected costs and trading claims remain
separate, unapproved work; no profitability claim follows from preparation.
Historical research remains blocked pending resolution of predecessor census
recoverability under the frozen statistical rule; see CURRENT_WORKFLOW.md.
