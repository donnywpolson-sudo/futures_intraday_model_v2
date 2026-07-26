# Futures rebuild constitution

Version: `1.0.0-m1`
Status: binding for this repository

## 1. Purpose and claims

The project is a research and manual decision-support system for CME futures. It does not promise alpha and has no order-placement authority. Existing historical results are discovery evidence, never a reset holdout. A candidate may become trusted only through a predeclared historical discovery protocol followed by genuinely prospective confirmation.

## 2. Independence

This repository, its environment, data vault, ledgers, bundle registry, locks, and outputs are independent from every other project. The legacy repository is a forensic source only. Active code may not import it or recursively discover it.

## 3. Data authority

The only intended provider is the user's existing Databento `GLBX.MDP3` archive. Milestone 1 performs no provider call or download. `configs/source_contract.json` identifies the exact legacy folders and permissible roles. Every DBN family is preserved as an immutable source release. Definition, one-minute OHLCV, status, and statistics are the canonical research inputs; one-second OHLCV and trades are diagnostic inputs; hourly and daily OHLCV are cross-check inputs unless a future charter explicitly changes that role.

An immutable source release is never edited. New dates, provider corrections, schema changes, and code changes create successor release IDs. Publication is staging, complete validation, content hashing, then atomic promotion. Overwrite, symlink, junction, hard-link, and legacy-path fallbacks are forbidden. The legacy `data/raw` and `data/causally_gated_normalized` trees are comparison evidence only: v2 regenerates both stages from accepted DBN releases. The six known anomaly families (KE 2019/2021/2023/2024 and SR1/SR3 2020) remain explicitly quarantined until anomaly-specific acceptance evidence passes; they are never silently dropped or waived.

The logical active DAG is one-way:

```text
dbn -> validated_actual_contract -> causally_normalized
    -> feature_release
    -> outcome_release
    -> registered_evaluation
    -> sealed_bundle
    -> prospective_predictions -> matured_outcomes
```

Feature and outcome branches are peers. Neither is built from the other.

## 4. Bitemporal and bar timing

Every market-data or metadata row must retain:

- `event_at`: when the market event occurred.
- `available_at`: when the complete information was usable by the system.
- `source_received_at`: provider receipt timestamp when available.
- `decision_at`: the predeclared decision timestamp.
- `source_release_id` and source epoch.

For a bar, `event_at` is bar start and `bar_end = event_at + interval`. `available_at` cannot precede `bar_end`; `decision_at` cannot precede `available_at`. An entry cannot occur at or before the decision timestamp. Missing bars mean no reported trade bar, not zero return.

## 5. Instrument identity and rolls

A continuous symbol is only a selector. Databento `instrument_id` is not globally or permanently unique. Trusted rows key actual contract identity by provider dataset, `publisher_id`, `instrument_id`, and `instrument_id_date_utc`, while separately retaining `exchange_session_date`, raw symbol, and definition release as provenance; exchange, currency, multiplier, and minimum tick remain required economics. A definition is eligible only when both its effective time and system-availability time are no later than the decision.

Roll selection at decision time can use only selection observations whose `available_at <= decision_at`. Eligibility cannot use the next realized contract change or `bars_until_roll`. Expiry, first-notice, halt, limit, and liquidity eligibility must be based only on facts available at the decision time. Labels, costs, predictions, and P&L use actual contracts and explicit roll economics.

Databento DBN metadata may contain retrospective continuous-symbol mapping intervals with future interval end dates. Those mappings are reconciliation evidence only. They cannot be features, eligibility inputs, roll-boundary inputs, or sample-selection inputs. The composite identity carried by each bar plus its as-of-available instrument definition is authoritative; changing a future metadata mapping interval must not change any earlier row, feature, or eligibility decision.

## 6. Schema firewalls

Features contain only decision-time-known values. Outcome rows contain future realized values and maturity status. Prediction rows contain forecasts and abstention reasons, never outcomes. Scoring joins predictions to matured outcomes in a later, separate process.

## 7. Research firewall

Synthetic smoke proves mechanics only. Every real-data evaluation must have a registered charter before it reads evaluation data. The charter pins hypothesis, releases, target, feature policy, metric, benchmark, costs, folds, trial number, and decision rule. Any semantic change creates a new trial. Evaluator records are append-only; a failed primary gate cannot be rescued by secondary metrics.

No historical period already inspected is called pristine. Final confirmation is prospective. Underpowered evidence is `INCONCLUSIVE`, not a pass.

## 8. Model bundle and inference

A sealed bundle pins the artifact, feature schema and order, preprocessing, estimator, calibration, thresholds, training cutoff, release IDs, code, configuration, environment, and dependency hashes. Reload parity is required.

Inference receives a predict-only capability. Objects exposing `.fit()` are rejected. Inference cannot read outcomes, evaluation reports, or training roots. It cannot place orders. Stale inputs, schema mismatch, release mismatch, unknown identity, incomplete bars, or bundle failure produce abstention. Predictions are appended before outcomes and never overwritten.

## 9. Operations

Publication and ledgers use one-writer leases, fsync, atomic replacement, integrity chains, idempotent retry, and explicit recovery. Provider outage, entitlement loss, schema drift, stale watermarks, clock error, insufficient disk, and corrupted releases fail closed. No automatic retraining, source substitution, or threshold changes are allowed.

## 10. Completion

`REBUILD_COMPLETE` requires exact migration lineage, zero open structural handoff errors, clean-room reproduction, recovery/restore evidence, and all causal/adversarial suites. `HISTORICAL_RESEARCH_READY` additionally requires a valid registered research engine. Neither state implies alpha.
