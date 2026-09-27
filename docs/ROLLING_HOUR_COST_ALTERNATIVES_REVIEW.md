# Historical execution-cost alternatives: literature review

Reviewed 2026-09-07. Research recommendation only; no configuration, model,
source access or historical execution changed. Scope: the existing one-contract,
minute-entry, fixed-hour ES/NQ/CL/GC experiment over 2018-2022.

**Recommendation:** use an explicitly assumed, small set of market-specific
cost scenarios as the primary economic sensitivity analysis. Consider a causal
OHLC-based spread estimate as supporting evidence, not as reconstructed quotes
or a certified upper bound. Keep the forecast evaluation separate. This can
support an honest descriptive successor without purchasing old L1 data, but
does not make frozen v7 historically ready.

This recommendation is a synthesis for this repository, not a theorem or a
claim that any paper validates the complete proposed experiment.

## What the literature supports

| Approach | Evidence reviewed | Assessment for this project |
|---|---|---|
| EDGE: OHLC effective-spread estimator | Ardia, Guidotti and Kroencke (2024) develop a covariance-based estimator accounting for discrete trading and demonstrate minute-bar applications. | Most promising estimator to investigate using existing OHLCV-1m. It estimates a spread component, not strategy-specific execution shortfall. Futures validation remains necessary. [Published paper](https://jfe-journal.org/wp-content/uploads/Efficient-estimation-of-bid-ask-spreads-from-open.pdf) |
| Corwin-Schultz high-low estimator | Estimates spreads from one- and two-period ranges; its authors discuss overnight adjustments and infrequent trading. | Useful literature comparison, with material volatility/session assumptions. Do not treat the range itself as the spread. [Author paper, earlier conference version](https://users.nber.org/~confer/2009/mms09/Corwin_Schultz.pdf) |
| Abdi-Ranaldo close/high/low estimator | Adds close-price information and is evaluated against US-equity effective spreads. | A possible diagnostic comparator, not a reason to search several estimators for favorable economics. [Published abstract](https://academic.oup.com/rfs/article/30/12/4437/4047344), [author manuscript](https://ux-tauri.unisg.ch/RePEc/usg/sfwpfi/WPF-1604.pdf) |
| Roll / Hasbrouck transaction-price models | Hasbrouck estimates effective costs with a Bayesian model and warns that short-sample estimates can be dominated by the prior. | More estimation choices and assumptions than needed for the initial remediation. No automatic advantage from adding a sampler. [Hasbrouck, 2009](https://pages.stern.nyu.edu/~jhasbrou/Research/GibbsCurrent/HasbrouckJF.pdf) |
| Explicit cost scenarios and cost headroom | Proposed here to expose uncertainty instead of assigning missing historical costs a false precision. | Preferred primary safeguard. Results remain conditional on assumed costs; the scenario envelope is not a statistical confidence interval. |
| Carry recent-year spreads backward | Would require stability of the cost relationship across periods, contracts, trading conditions and strategy timing. | An unverified transport assumption. The user's recent data are not historical coverage and protected periods remain unopened. |
| Large-order impact/optimal-execution models | Almgren-Chriss models temporary/permanent impact and execution-risk tradeoffs with specified parameters. | Does not identify missing parameters from OHLCV automatically; unnecessary machinery for this one-contract research remediation. [Original paper, December 2000 manuscript](https://www.smallake.kr/wp-content/uploads/2016/03/optliq.pdf) |

EDGE's representative assumptions include uncorrelated fundamental returns,
uncorrelated price innovations and bid/ask noise, and uncorrelated, mean-zero
bid/ask noise. Price predictability and persistent order flow can violate these
assumptions. Its empirical comparisons are not a guarantee for our four futures
or for minutes selected by our model. [Paper, sections 2-3](https://jfe-journal.org/wp-content/uploads/Efficient-estimation-of-bid-ask-spreads-from-open.pdf)

The authors describe EDGE as estimating the **root-mean-square effective
spread** over the estimation sample. Intraday bars are supported, but short
windows can be noisy; a three-observation computational minimum is not useful
statistical support. Their guidance recommends regular grids retaining missing
values and cautions about negative-estimate handling and small-sample bias.
[Author implementation and FAQ](https://bidask.eguidotti.com/)

There is strong counterevidence to assuming that an elaborate spread estimate
is automatically accurate. Jahan-Parvar and Zikes find that popular daily-data
proxies can be noisy and upward biased, with variation driven by volatility.
That can make high-volatility periods look expensive even when the true spread
relationship differs. Their evidence concerns the examined estimators and
markets, not a blanket refutation of all OHLC estimators.
[Federal Reserve working paper](https://www.federalreserve.gov/econres/feds/when-do-low-frequency-measures-really-measure-transaction-costs.htm)

A newer preprint explicitly models serial dependence and compares spread
estimators in simulations. It reinforces the need to test dependence rather
than assume it away; it is not validation of a ready-made futures cost model.
[Brouty, Garcin and Roccaro, revised 2025](https://arxiv.org/abs/2407.17401)

## Futures evidence and historical anchors

Futures applications do exist: a CFTC-hosted study uses a Corwin-Schultz proxy
on one-minute E-mini S&P 500 and 10-year Treasury data from January 2015 to
August 2016. This demonstrates use in futures liquidity research, not a
validated reconstruction of our order fills or our 2018-2022 cost distribution.
[Trader Positions and Marketwide Liquidity Demand, sections III/V](https://www.cftc.gov/sites/default/files/idc/groups/public/@economicanalysis/documents/file/oce_trader_positions.pdf)

Two free exchange reports provide useful, explicitly limited historical anchors:

- CME's analysis of ES in early 2020 shows that trading volume, displayed depth
  and fill quality can give different impressions of liquidity. Thus a rule
  such as high volume implies low execution cost is insufficient.
  [CME, Assessing Liquidity](https://www.cmegroup.com/education/articles-and-reports/assessing-liquidity)
- CME reports an average top-level GC spread of 1.10 ticks for the full day and
  1.15 ticks during its specified Singapore morning sample, for the April 2020
  contract during February 2020. Those averages are useful checks on scale;
  they are not maxima, crisis-period estimates, all-contract coverage or
  executable quotes for our dates.
  [CME, Gold Futures and Options: APAC Liquidity](https://www.cmegroup.com/education/articles-and-reports/comex-gold-futures-and-options-a-look-at-apac-liquidity)

Public retrospective reports can inform today's research assumptions. Their
later publication does not make them information a historical decision-maker
possessed. They must not become future-known features or silently determine
date-specific historical entry decisions. No collection of isolated averages
establishes a complete historical cost schedule or a worst-case bound.

## Minimal proposed successor

1. **Separate what is known from what is assumed.** Bind tick values and fee
   schedules to actual contracts and applicable dates where records establish
   them. Mark unavailable fee components as assumptions. Distinguish total
   round-trip fees, spread crossing, additional slippage/latency, and non-fill
   risk. A spread proxy does not identify all four.

2. **Predeclare a small cost-scenario family.** Use base, adverse and severe
   allowances in ticks per side or per round trip, with the unit fixed
   explicitly. Market-specific ticks and fees supply meaningful differences.
   A few fixed session categories may be justified once the calendar is
   certified. Do not fit an independent knob for every market/year. Any
   time-varying modifier must follow a fixed rule using already available bars;
   it remains an assumption without a calibration target. Parameter values are
   not established by this literature review and must be frozen before results.

3. **Retain one profit-based inner selection rule.** Designate one adverse
   schedule in advance and continue maximizing mean daily simulated stress-net
   dollars. That preserves the broad objective but changes its numerical
   implementation. Do not select the cost schedule, estimator, candidate or
   outer-fold winner after seeing outer results. A worst-across-scenarios
   selector would be another objective change, not an automatic improvement.

4. **Replay costs where they actually affect the policy.** In current
   `rolling_hour_research.score_predictions`, costs affect the entry threshold
   and entries determine one-position occupancy. For each predeclared economic
   sensitivity scenario, replay the frozen fitted predictor through that
   scenario's thresholds and occupancy rules. Never refit or reselect using
   its outer result. Also report a fixed-trade-list cost sensitivity to isolate
   the arithmetic effect of higher charges; label the two analyses separately.

5. **Report the full economic range and cost headroom.** For a fixed trade list
   and one constant additional round-trip dollar charge, the zero-net charge
   equals total gross P&L minus included fees, divided by completed round trips.
   Retain negative values and report undefined when there are no trades. That
   is a descriptive tolerance, not an estimate of true costs. When costs alter
   entries, headroom requires replay; the simple division no longer describes
   the whole policy. Preserve zero-trade dates and all failed diagnostics.

6. **Make the spread estimator optional supporting evidence initially.** If
   investigated, use one specified EDGE implementation and a fixed trailing
   window from the same actual contract, containing only completed, available
   bars. Do not estimate costs from an entire outer month and feed them back
   into earlier decisions. Warm-up requirements, missingness, estimate
   instability and roll transitions must be explicit. A failed optional
   diagnostic need not erase an otherwise complete assumed-cost policy result.

No synthetic quote objects, exchange timestamps or release identities should
be fabricated to get assumed costs through v7's certified quote adapter.
An approved successor should represent cost assumptions explicitly, reusing
the existing chronological replay and accounting owners where appropriate.

The price reference also needs care: open prints are transaction prices, not
observed midquotes. Deducting a modelled half-spread at each end does not
reconstruct executable prices. Name the adjustment as an adverse allowance
relative to the open-print convention, and avoid double-counting components.
Do not assume passive fills or guaranteed execution because a bar touched a
price. No finite allowance is established here as a universal worst case.

## Validation before historical use

The following are proposed checks, not claims that they have passed:

- Synthetic known-spread cases with both predictable and unpredictable prices,
  serially dependent trade signs, volatility jumps, sparse trades, tick
  rounding, missing bars and contract rolls. Record estimator bias and failure
  rates for every predeclared case; do not search seeds for attractive results.
- Unit/clock checks for fees per side versus round trip, tick-to-dollar
  conversion, and training-only or decision-causal information. Keep the
  existing distinct-date support and all complete calendars.
- For a fixed trade list, increasing nonnegative charges must not improve P&L.
  This monotonicity need not hold when threshold changes alter the trade list;
  test those policy branches by replay instead.
- Preserve full grids and difficult observations. Log-based spread methods
  require positive prices. WTI futures did trade negative in April 2020; whether
  the project's selected contract encountered them is unverified. Do not shift
  prices, silently drop those dates, or infer applicability from the market
  name. [CFTC account](https://www.cftc.gov/PressRoom/PressReleases/8315-20)
- A separately authorized, eligible quote sample could later compare the
  spread proxy to matched effective-spread evidence and examine errors by
  market, session and volatility. A recent-year check would validate only that
  sample; it would not establish historical transport. Protected holdout and
  forward observations cannot be used to calibrate this development procedure.

## Books consulted and access limitations

The public material for Larry Harris's *Trading and Exchanges*, chapter 21,
identifies liquidity and transaction-cost measurement as the relevant topic.
Joel Hasbrouck's *Empirical Market Microstructure*, chapters 14-15, explicitly
separates retrospective cost measurement from prospective execution strategies;
the author's detailed contents include benchmark prices and selection effects.
I checked publisher/author contents and available descriptions, not the full
paywalled book chapters. Specific estimator findings above come from accessible
papers, rather than unverified recollection of those books.
[Harris chapter](https://academic.oup.com/book/52292/chapter/421090015),
[Hasbrouck book](https://academic.oup.com/book/52241),
[Hasbrouck author contents](https://pages.stern.nyu.edu/~jh4/EMM%20Book/TOC%20Dream.htm)

## Decision and permitted claims

The most defensible no-purchase route is **conditional economic sensitivity
under frozen assumptions**, with a spread proxy contributing diagnostics if it
survives validation. It can produce an honestly labelled descriptive research
model; it cannot certify the actual missing historical execution costs.

For the forecast question itself, date-balanced MSE against zero change and
the eligible training mean remains interpretable independently of transaction
costs. Because the candidate is still selected economically, it must not be
called the most accurate forecaster. Choosing candidates by forecast accuracy
instead would need its own explicitly approved objective change.

Approve a cost-assumption successor before implementation changes the selector
or replay. Preserve v7 and predecessor trial charges; account for the material
successor once, including cost choices if used for selection. Calendar
publication/applicability, contract-selection provenance, historical-use
accounting and applicable execution authorization still need resolution.
Predictive inference and the failed profitability qualification remain separate.

No provider purchase, quote download, protected-data read, new historical
evaluation or code implementation was performed for this review.
