# Offline simulation and model inspection

## OOS session library and outcomes

The source viewer supports a bounded saved ES session library. Preparing the
declaration is metadata-only; generating real results requires approval of that
declaration through the existing task receipt. There is no research-execution
button or model loader in the viewer.

```powershell
$env:PYTHONPATH = Join-Path (Get-Location) 'src'
.\.venv\Scripts\python.exe -m futures_rebuild.historical_simulation --prepare-library reports\oos_session_library_v1\new_spec.json --output FuturesLiveCockpit\ReplayRuns\new-oos-library
```

Choose unused specification and output paths. The fixed scope is the saved ES
primary Ridge model's 63 candidate weekdays, January 26–April 22, 2021. The
prepared scope discloses separate feature and target admission requests and the
reader's annual scans. After approval the task calls
`historical_model_inspection.execute_library_authorized(root, receipt)`. Each
session must pass history, contract, outcome, and retained-forecast checks.
Expected data exclusions are explained; integrity/parity failures stop the batch.
Only a fully accounted-for batch publishes `library.json`; no automatic retry.

The approved library is complete: **61 playable sessions, 20,130 forecasts and
20,130 outcomes**. February 15 and April 2 were excluded by the admission reader
with `GAP_DUPLICATE_OR_OUT_OF_ORDER`; no prices were filled or substituted.
Double-click `FuturesLiveCockpit/Futures Cockpit - Offline Replay.lnk` to open the
library, initially paused on January 26. Alternatively:

```powershell
.\.venv\Scripts\python.exe -m futures_rebuild.live_cockpit --replay-run FuturesLiveCockpit\ReplayRuns\es-primary-oos-library-v1\library.json
```

**Open saved run** accepts `result.json` or `library.json`. Choose a session or
**Random session**; random selection ignores forecasts/results and avoids the
current session when alternatives exist. Excluded dates have reasons and are not
playable. Each selection starts paused at zero; the initial session is the earliest
included date. Opening a library validates its saved files; subsequent switching
loads only the selected saved result.

**Next prediction** advances the clock and selects the next recorded forecast.
Click a reached candle or use **Inspect reached bar** to inspect earlier inputs.
The neutral dot denotes a selected forecast, not an order. **Jump to outcome**
advances to that forecast's maturity while retaining its selection.

The original target is the next-minute opening price to the opening price 60
minutes later, in ticks. These are price references, not fills. Its outcome is
available only after the exit-reference minute closes plus the declared five-second
delay: a 06:30:05 prediction first resolves at 07:32:05, not at 07:30. Each session
includes 60 context bars, 330 decision bars, and 62 outcome-tail bars. Signed
error is **actual minus predicted**. The replay backend sends no future outcome
values. Pending outcomes say **Outcome pending**; older saved runs explicitly
state that outcome comparison is unavailable.

The data is **historical OOS development data**, previously examined by the
closed study, not untouched test data. There is no adopted trading policy,
position simulation, P&L, model fitting, or profitability claim. Schemas
`offline-simulation/1` and `/2` remain supported; `/3` stores outcome events at
their availability bars. Libraries use `offline-session-library/1` and contain
validated relative result paths and content hashes, never executable content.

## Historical model inspection

The original January 26, 2021 run remains preserved: 390 real ESH1 minute bars and
330 predictions verified against the retained forecasts. Open
`FuturesLiveCockpit/ReplayRuns/es-2021-01-26-primary/result.json` using **Open
saved run**, or use the source launch command below. The first 60 bars provide
lookback context; predictions start at 06:30:05 Pacific. Generating the run again
is unnecessary: replay reads the saved result only.

Historical inspection uses the saved ES primary Ridge model from the first
evaluation window of the completed four-market screen. It does not refit it.
The study remains closed with no supported edge. It supplied predictions, not
an adopted trading policy: **No trading policy — trades and P&L unavailable.**

The bounded first session is January 26, 2021. Its 330 scheduled decisions run
06:30:05–11:59:05 Pacific, with 61 completed bars per feature window. The saved
display includes 390 bars starting at 05:29 Pacific, including lookback context.
The seven original feature formulas, fitted scaling, and Ridge weights are
reused. The target is next-minute open to the opening price 60 minutes later,
in ticks; it is not a predicted next-bar close or a demonstrated fill.

For a separately authorized future run, prepare a specification from the
repository root without reading prices (choose a new, unused output directory):

```powershell
$env:PYTHONPATH = Join-Path (Get-Location) "src"
.\.venv\Scripts\python.exe -m futures_rebuild.historical_simulation --prepare-historical reports\historical_model_inspection_v1\new_spec.json --output FuturesLiveCockpit\ReplayRuns\es-2021-01-26-new-inspection
```

`--historical-spec <spec.json> --output <same directory>` validates an existing
specification; it also remains prepare-only. A specification is a durable
diagnostic declaration, not historical execution authority. After the owner
approves its concrete scope, the approved task calls
`historical_model_inspection.execute_authorized(root, receipt)` with the existing
one-use operation receipt. The CLI and cockpit cannot issue that receipt.

The unchanged admission reader verifies and scans the ES 2021 annual release
internally, retaining only the declared session and lookback for inference.
The saved first-window forecast batch is read to compare only this session's
predictions. No outcomes, fitting, alternative models, protected periods,
providers, or paid services are used. Missing/delayed inputs, contract changes,
stale bindings, or forecast mismatches stop the run without a complete result.
An unsuccessful attempt does not automatically select another day or retry.

When the approved task has produced `result.json`, open it using **Open saved
run** in the source cockpit's replay mode, or launch:

```powershell
$env:PYTHONPATH = Join-Path (Get-Location) "src"
.\.venv\Scripts\python.exe -m futures_rebuild.live_cockpit --replay-run FuturesLiveCockpit\ReplayRuns\es-2021-01-26-primary
```

**Step one bar** advances the recorded clock. Expand **Inspect predictions and
inputs** to view exact predictions and features. **Inspect reached bar** lets you
inspect earlier reached records without changing the playback cursor. The
inspector shows feature values in model order, raw prediction, target horizon,
eligibility reason, input-window start and availability time. Reasons explain
eligibility; they are not invented causal explanations of a forecast.
The compact forecast is rounded to two decimals for display; inspection retains
the exact saved value. **Run details and assumptions** contains the model identity,
training cutoff and input-availability assumption. Replay hides the unused market
sidebar and fixed range/interval controls so the chart has more room.
Context bars have no predictions. No future outcomes or trading statistics are
manufactured. Replay speed changes display timing only. The installed executable
and the existing synthetic example remain unchanged.

Historical predictions use `offline-simulation/2`; synthetic trade simulations
retain `offline-simulation/1`. Both are validated data-only bundles. Historical
trade policies are not implemented for this prediction-only saved model.

## Synthetic mechanics example

This source-only workflow uses generated prices and a scripted mechanics test.
It requires no subscription, credentials, provider connection, or new dependency.
It proves software mechanics only. It does not test a viable trading model or
establish profitability. The installed cockpit executable is unchanged.

The saved synthetic example remains available through **Open saved run** at
`FuturesLiveCockpit/ReplayRuns/synthetic-example/result.json`. It contains
generated prices, not real historical data. The Offline Replay shortcut now
opens the real historical ES inspection using the existing Python environment
without opening a terminal. `FuturesLiveCockpit.exe` remains the installed
observation-only live-market cockpit.

From the project root in PowerShell, use the existing environment:

```powershell
$env:PYTHONPATH = Join-Path (Get-Location) "src"
.\.venv\Scripts\python.exe -m futures_rebuild.historical_simulation --demo --output tmp\my_synthetic_run
.\.venv\Scripts\python.exe -m futures_rebuild.live_cockpit --replay-run tmp\my_synthetic_run
```

Choose a **new output directory for each run**. Existing directories are rejected.
Only a complete `result.json` is replayable; a failed/interrupted run leaves no
complete result. Unsupported schemas and malformed bundles are rejected.

The viewer starts paused with no bars visible. Use **Step one bar**, **Play**,
**Pause**, **Restart**, and the **Playback rate** selector (1, 10, or 100 bars/sec).
The chart, decisions, fills and account values advance together. The final
summary appears only at completion; restarting hides it. Expand **Simulated
fills and test assumptions** for the fill log. Timestamps use the cockpit's
Pacific display formatter; saved timestamps are UTC. Preferences are stored
separately under the cockpit app-data directory's `replay` subdirectory.

## Assumptions and expected result

The fixture contains 60 completed one-minute bars for one ES-like synthetic
contract. Starting balance is $10,000, position is limited to -1/0/+1 contract,
tick size is 0.25, point value is $50, commission is $2/contract/side, and adverse
slippage is one tick. These are test assumptions, not verified brokerage costs.

Each completed bar produces a target or explicit abstention (`None`). A changed
target fills at the **next bar's open**, with adverse slippage. Reversals trade
two contract sides. Abstention retains the position. An unfilled final decision
is cancelled; an open position is marked at the final close without a made-up
exit. Drawdown is the distance below peak equity at each completed bar, not
intrabar drawdown. The simulation does not model an exchange queue or liquidity.

Realized and unrealized P&L include slippage. Equity subtracts commissions;
`total_costs` reports commissions plus slippage for information and is **not
deducted a second time**. Prices and account values use exact decimal arithmetic.

The scripted fixture should show these six fill events:

| Side | Quantity | Price |
| --- | ---: | ---: |
| Buy | 1 | 4999.50 |
| Sell | 2 | 4998.50 |
| Buy | 1 | 4998.75 |
| Sell | 1 | 4999.00 |
| Buy | 2 | 4999.50 |
| Sell | 1 | 4998.50 |

Its four closed legs total -$137.50 after slippage, with $16 commissions,
$100 included slippage, $116 total costs, no open position, and **$9,846.50
final equity**. These values are independently asserted by the focused tests.

## Interfaces and boundaries

`historical_simulation.Bar` is a normalized immutable bar: fixed contract,
UTC `opened_at`/`closed_at`, exact decimal OHLC strings and integer volume.
Bars must be contiguous, chronological, tick-aligned one-minute intervals;
duplicates, gaps, nonfinite values and inconsistent OHLC are rejected. One run
is bounded to one synthetic session of at most 1,440 bars.

`simulate(bars, decide, strategy=..., settings=..., synthetic=True)` accepts a
Python callable. It receives an immutable `Context` containing only the completed
bar prefix and current `Account`. Return integer -1, 0, +1, or `None`; exceptions
stop the run. `no_trade` and `scripted_test` are mechanics fixtures, not candidate
models. The command line only generates the built-in fixture: there is no raw
data browser, dynamic model loader or executable bundle content.

`simulation_records.py` owns the versioned data-only bundle validation.
`live_cockpit/replay.py` reads validated saved records and moves a display cursor;
it never imports the simulator or reruns a model. Live/demo/research-report
workflows retain their existing entry points. Replay cannot combine with them.

Other historical sessions, discovery/evaluation, and other model adapters require
a separate bounded task under the existing research controls. The synthetic
flag is a caller declaration, not evidence that external data is authorized.
Broker connections, packaging, installation and trading are outside this work.

## Focused verification

```powershell
.\scripts\run_windows_host_root_pytest.ps1 -PytestArguments @("tests/test_historical_simulation.py", "-q")
```

The native viewer uses the already-installed WebView2 runtime and existing
offline safeguards. A WebView2 initialization failure is a runtime failure;
it does not invalidate the saved run. No runtime installation or repair is
performed by either command.

Native replay was verified on 2026-09-25 using the source command above from a
normal Windows host process, with the existing offline WebView2 safeguards.
The earlier restricted test launch failed during WebView2 initialization;
the host launch required no runtime repair or installation. Run the viewer
from ordinary PowerShell when the automation sandbox cannot initialize it.
