# Inactive research pilot — Milestone 1

Status: INACTIVE; generated-data engineering only. No historical or protected reader,
registry integration, activation, qualification method, or trading interface exists here.
The roadmap is the revised “Futures Research Pipeline Overhaul: Historical Relevance,
Tiered Testing, and Economic Qualification” plan in the owner task. This document
records its implemented Milestone 1 subset and the owner's three clarifications.

## Clarifications incorporated

1. Fitting, inner selection, and outer evaluation have separate declarations in
   research_pilot.protocol(). Fitting compares exactly EQUAL_SESSION and DECAY_365_25.
   Inner selection pools scorable dates across four preceding quarters and minimizes
   equal-date mean within-date MSE. Outer evaluation independently pools scorable
   outer dates with that same explicitly declared aggregation. Neither uses the
   recency fitting weights. Zero and equal-session training-mean benchmarks are
   scored on the same available forecast/target population with the same evaluation
   weights. The mean benchmark remains equal-session even for the recency model.
2. Generated tests exercise research_pilot.fit(), predict(), evaluate(), run_pilot()
   and Portfolio directly. There is no parallel demonstration fitter/replay engine.
   Closed-form Ridge and P&L oracles are independently specified in the tests.
3. January 1, 2023 through the existing development cutoff is a fixed development
   slice. Its evidence does not establish current deployment applicability.
   The pilot reports that slice separately and never qualifies deployment.

No data boundary changes: development ends strictly before 2025-07-13T22:00:00Z.
The existing holdout remains July 14, 2025–July 13, 2026, 252 project trade dates,
manifest ad1baab30b52fdda95e72777a4930ea18bd78037925cc36a27594f484c876549.
The copied dates/identity are metadata, not permission to read observations.

## Small implementation surface and reuse

- src/futures_rebuild/research_pilot.py: one in-memory successor module.
- tests/test_research_pilot.py: clearly generated study, calendar, observations and checks.
- This document and one compact unpublished evidence packet.

Existing discovery.bind/identity/validate_study supply canonical metadata identity
and study-history shape. Existing SyntheticOnlyPermit, numeric validators and exact
Decimal tick conversion are reused. A synthetic declaration is not an access
capability or evidence that real provenance has been verified. It is never appended
to the actual discovery journal.

The normalized weighted Ridge equation preserves the sound arithmetic of
l0_alpha_research.fit_model. Its short linear algebra is adapted here because the
existing wrapper binds frozen membership, profiles and weights. Editing that wrapper
would change legacy experiment identity. The old split guard assumes expanding
membership; the successor owns its rolling source windows without weakening it.
The old replay wrapper initializes an isolated market; it cannot represent the
new shared persistent book. Existing P&L code that requires nonzero entry notional
is unsuitable for zero-price dollar accounting; its exact tick conversion is reused.

No existing source/configuration, active pointer, source release, registry,
predecessor artifact, or legacy validator is edited. No exports or pipeline dispatch
activate this module.

## Protocol and chronology

prepare() separates chronological stage, claim, readiness, unavailable qualification
and deployment. Missing study values produce MISSING_INPUTS. Non-predictive
study kinds have an explicit UNIMPLEMENTED_STUDY_KIND disposition. Their real
selection/economic qualification paths are outside this milestone.

The synthetic study supplies its model penalty, support minimum, clock opportunities,
market route, cost assumptions and risk allocation. No numeric fixture is a real
strategy recommendation. A real study cannot opt into this pilot by setting a flag:
the current interface accepts synthetic-only declarations and inputs.

Calendar:
- Explicit, contiguous Pacific civil accounting dates, each OPEN/CLOSED/UNKNOWN.
- Accounting dates are not asserted to be exchange sessions or settlements.
- UNKNOWN is rejected. Closed dates are explicit; absent observations do not close dates.
- Outer periods align with calendar quarters, with an exact exclusive final endpoint.
- A date-only endpoint means Pacific midnight; a timestamp keeps its exact instant.
- Activation is the first declared decision of the quarter.
- The preceding complete OPEN accounting date is excluded. Its start is the cutoff.
- Each fit spans [five calendar years before cutoff, cutoff).
- Leap-day subtraction maps February 29 to February 28 when necessary.
- Four prior calendar-quarter validation windows each have their own earlier fit.
  The last validation window is truncated at the outer selection cutoff.
- Training labels must be known strictly before fit cutoff. Validation labels must
  be known strictly before outer selection cutoff. Longer outcome intervals fail
  maturity eligibility; the one-session policy floor does not override causality.
- Inner source membership can extend earlier than the final outer five-year fit.
- The schedule retains UTC nanoseconds and Pacific activation/cutoff displays.
- All declared inner/outer clock opportunities must be represented before running.
  Missing features remain explicit abstentions; missing outcomes remain explicit
  unresolved dispositions. The pilot never invents observations to fill gaps.

No actual calendar or source availability is verified by generated examples.
The fixture opens only Mondays to keep the test small. This is deliberately not
a claim about CME trading dates or historical coverage.

## Fitting and selection arithmetic

For session d with n_d eligible training rows:
a_d = 1 for equal fitting, or 2^(-calendar_age_d/365.25) for recency.
W_d = a_d / sum(a); row weight w_di = W_d/n_d. Weights sum to one.

Training-only means, scales, intercept and Ridge all use these same weights.
Exact constant and duplicate columns are removed using training rows only.
For standardized feature matrix Z:

    minimize sum_i w_i (y_i - intercept - Z_i beta)^2 + lambda * beta' beta
    intercept = sum_i w_i*y_i
    beta = solve(Z' W Z + lambda*I, Z' W (y-intercept))

The intercept is unpenalized. The target is actual-contract exit-minus-entry ticks;
it is not a probability forecast. The fixture model does not establish directional
calibration, net expectancy or incremental alpha.

Report fit lower/cutoff boundaries, eligible dates/rows, maximum date weight and
inverse sum of squared date weights. Weight concentration is not an independent
sample-size estimate.

Selection compares exactly two policies by pooled equal-date MSE. Exact ties prefer
equal-session fitting. No tolerance-based tie, horizon search, or penalty search
is added. Outer and recent reports show all three predictive scores, forecast
coverage, missing targets and unscorable dates. Availability-limited scores remain
descriptive and cannot qualify a strategy.

A model is valid only within its scheduled outer/inner evaluation interval.
A late supplied ready-at timestamp causes abstention for the scheduled interval;
it cannot enable a result-driven late activation. Missing support/history stops
the in-memory pilot explicitly. There is no automatic fallback to an old model.
Portfolio tests verify that a failed update stops new entries while existing
positions retain their original exit identity.

## Shared risk and persistent reference accounting

run_pilot saves each forecast record before target attachment, then uses one
Portfolio for the entire ordered evaluation stream. No quarter/year reset occurs.
Forecast records bind the fitted model and decision identity. Source-release
identity changes when supplied source content changes even if numerically
irrelevant future outcomes leave coefficients/predictions unchanged. Cost/allocation
changes do not change the predictive fitting identity. The complete run additionally
binds its economic study and input contents. No persistent result cache or
cross-version artifact-reuse service is implemented.

Policy is explicitly synthetic:
- Long/short/flat from predicted move versus the declared round-trip cost threshold.
- Fixed planned entry/exit, one position per market, integer quantity.
- Exact contemporaneous signals sorted by declared market priority and forecast ID.
- Shared quantity and reserved-risk limits applied before entry and P&L.
- Pending entries reserve capacity. New model identities affect new entries.
- Originating entry/exit, quantity, contract, costs, cash, marked equity and
  drawdown persist across quarter/year/model updates.
- Scheduled exits precede simultaneous new decisions; entry must be strictly later
  than decision, and exit strictly later than entry.
- A reversal is a charged exit followed by a new, separately timed entry.
- Round-trip fee and slippage deductions occur once on close. An optional open mark
  includes the same estimated liquidation costs without charging them twice.
- No open position is assigned a convenient next quote. Missing exact entry/exit
  references, late references, or missing requested marks stop complete accounting
  and preserve unresolved exposure. There is no outcome-dependent entry filter.
- Dollar P&L uses signed tick changes times exact tick value and quantity.
  Zero/negative prices are allowed. A different exit contract cannot create a
  continuous-contract roll profit. Contract economics are explicit and immutable.

Risk reservation is a fixture allocation unit, not verified stop-loss, margin,
capital adequacy, funded-account rules or an intrabar drawdown bound.
Only supplied marks and completed trade events inform reported drawdown.
Unmarked intermediate risk is unknown. The pilot reports no annualized account
return or risk-adjusted economic qualification.

Later-stage doubles are explicitly labeled: generated source/calendar provenance,
supplied model ready-at times, and reference prices treated as fills with assumed
costs. Real models, forecasts, selection, aggregation and portfolio accounting
are not doubles. Specialist/broad-route tests call only the transition interface;
they do not execute a 4/16/41-market campaign. No queue, fill, exchange-calendar,
protected-reader, inference or deployment claim is made.

## Verification and acceptance

tests/test_research_pilot.py covers the real generated learning path, independent
arithmetic, weight concentration, benchmark/aggregation separation, full opportunity
counts, nested chronology, DST/leap/partial dates, immature-label and late-feature
sentinels, future-outcome numeric invariance, model expiry/tampering, missing support,
protected-role rejection, specialist/broad route dispositions, shared risk,
cross-year persistent state, missing execution/marks, negative/zero prices, contract
units and impossible same-timestamp entry/exit.

Known signal: x=(-1,0,1), y=4x, lambda=1. The actual nested successor produces
outer MSE 8/3 against zero/training-mean MSE 32/3 on both generated markets.
This validates a known mechanism in software, not market predictability.

Independent oracle: y=3+2x produces intercept 3, standardized coefficient sqrt(2/3),
raw slope 1 and forecasts (2,3,4); fitted objective is 4/3. A separate two-date
case proves recency changes the fitted intercept/slope while the mean benchmark
stays equal-session. Equal-date scoring of losses (9,1,1,1) is 3 even when row
and quarter counts differ.

Independent reference accounting: long +4 ES ticks = 5000 cents gross, less
1350 cents fixture costs = 3650 cents. Short +2 ticks = 2500-1350=1150 cents.
Combined net 4800 cents; total costs 2700 cents; supplied adverse mark drawdown
2600 cents. Shared limits admit ES and abstain NQ before any P&L is available.

The exact commands, measured resources, checks and integrity comparison are in
state/unpublished_evidence/research_pilot_milestone_1_v1. Focused legacy split,
synthetic-permit and futures-economics tests are included. No historical result is
reproduced and no replacement inference method is tested.

## Stop and next dependency

Milestone 1 completion establishes software mechanics only within the tested scope.
Qualification is UNAVAILABLE_NO_ADOPTED_METHOD; predictive, economic, incremental
and deployment claims remain separate and unqualified.

The single next dependency is one concrete, owner-reviewed real study declaration:
a substantive hypothesis and required market/claim scope, representable inputs,
source/calendar evidence, decision/evaluation policy, resource bounds and exact
access/adoption prerequisites. It must identify whether its claim-specific inference
is available or blocked. This is a dependency for separately scoped Milestone 2,
not authority to open data, search for a statistical method, or implement it now.


## Separate ES close integration — version 0.1.0-proposed

The owner subsequently authorized the synthetic-only integration task bound to
state/unpublished_evidence/es_close_anchored_continuation_v1_proposed/DECLARATION.md,
SHA-256 4a1ea185dc0aa36b8ead679cbdf7bee464920ef5b1d1d2e972e9cae34887acc7.
That preparation packet and its disclosed prior access incident remain unchanged.
The M1 description and protocol above still describe M1; this is a separate path.

research_pilot.close_protocol() declares ES_CLOSE_WLS_V1. prepare(), fit(),
predict() and run_pilot() explicitly recognize that version while retaining
zero-argument protocol(), existing Ridge and Portfolio behavior.
close_prepare() has no cost/risk/Ridge dependency; it requires the exact
declaration, ES-only Tier 0 route and explicitly synthetic support settings.
It never grants source admission, historical access, inference or deployment.

close_inputs() transforms supplied CashDay, CloseContract and CloseBar records,
using an exact raw-record SyntheticOnlyPermit. It validates that permit before
deriving a digest-bound transformed permit with the same synthetic-only flags.
There is no file reader. Source/calendar identities and causal input provenance
are retained, and full source/derived fixture identities remain distinct from
a fit's mature training identity. The existing contract specification and exact
tick arithmetic are reused; no adjusted prices or invented contract substitutes.

The event is defined in America/New_York under the owner's explicit instruction,
stored in UTC and displayed in Pacific. It uses prior cash-session 15:59-minute
close, current 15:28-minute close, 15:29:05 decision, and 15:30/16:00 minute opens.
Outcomes mature no earlier than 16:01:05. SHORT is not skipped to find a previous
FULL session; UNKNOWN does not become a holiday/closure. Calendar exclusions,
missing inputs, unknown contracts and unobservable outcomes remain distinct.
Future endpoint defects cannot change the already-constructed input disposition.
The M1 integer Outcome clock uses source-end as an internal ordering sentinel for
unknown maturity, with null references and explicit null ledger maturity; that
sentinel is never asserted to be actual observation availability.

The real _close_wls kernel performs one-predictor weighted-standardized least
squares without penalty or sign constraint. Constant/rank-deficient inputs are
unavailable; the predictor is never silently dropped. Five-year fitting, four
inner quarters, the one-session separation floor, pooled common-session MSE and
selected-only outer fitting use the existing chronology and weight arithmetic.
Training, selection and outer aggregation remain separately declared. Each
candidate's failures/membership and adaptive selection are retained.

close_score() reports equal-session loss/paired benefit, training-mean/zero and
weight-matched intercept diagnostics, modal-sign comparison, class confusion and
coverage. A zero target/feature stays visible. No new probability calibration,
test statistic, effect threshold or fitted zero deadband is introduced. Exact
signs of near-zero floating forecasts remain visible in directional diagnostics.

CloseState provides bound JSON-round-trippable model/update state, including
failed updates and expiry. It has no order or position path. The existing
Portfolio tests are later-stage reference-accounting doubles and do not constitute
integration of the separately gated continuation policy.

Focused check results, resource measurements, exact command, preservation and
limitations are in state/unpublished_evidence/es_close_anchored_continuation_integration_v1.
Historical role/calendar/contract/read and predictive qualification prerequisites
remain unresolved. Costs, h, capital/risk and economics remain unbound. The fixed
recent-development subset and existing final-252 boundary are unchanged.
