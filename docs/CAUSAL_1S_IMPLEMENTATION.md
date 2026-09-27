# Inactive 1s implementation and integration review

This source-only candidate implements the 1s build, independent certification,
compact storage, and timeframe-specific resolver. Execution is restricted to
generated synthetic data. It grants no historical, research, publication, or
activation authority. The existing 1m producer and single-file resolver retain
their semantics.

## Source custody

The isolated repository is `C:\Users\donny\.codex\visualizations\2026\09\11\01a08ed0-e241-7c11-98d9-95c5bb5d4af6\s`.
Its source baseline is commit `43f9519`, captured from canonical HEAD
`33287f1bfed176762812a39ec0d3a0b6d59c39e8` plus the existing tracked changes and
necessary untracked source/config/test files. Three byte-identical capture passes
covered 1,244 files, totaling 24,262,446 bytes. The adjacent `baseline.json`
records every captured file; its SHA-256 is
`9b9f436346454eaefaa5202ba5a4d265d98628fd19192569614c5521aca4327c`.

This is an independent Git repository, not a shared worktree. No historical data,
canonical Git metadata, virtual environment, state tree, or report tree was copied.
The canonical installed Python/dependencies were used read-only with bytecode
writes disabled. The baseline commit records inherited source, not this task's
authorship. The implementation diff starts after that commit.

## Interfaces and invariants

`causal_1s.Spec` binds the exact market/window/source identities, implementation,
Python/dependency versions, and inactive timeframe contract. The market set is the existing 41 standard
markets. Primary 1s bars end one second after their source timestamp and become
eligible five seconds later. That delay remains an assumption, not measured
provider delivery latency. Eligibility is strictly before the exclusive declared
end, which cannot exceed `2025-07-13T22:00:00Z`.

`build_candidate(destination, spec, source_factory, rulebook, limits)` consumes
ordered UTC days. `certify_candidate(...)` invokes the source factory twice,
reconstructs all five logical tables independently, and compares every persisted
row and complete declared-day coverage. It writes no second expected-data bundle.
The replay module does not call the producer. Both use the existing validated
record, definition-index, economics, and compact Parquet primitives; this is
independent transformation logic, not a second independent implementation of
those foundational primitives.

Definitions must be received by bar start and belong to that day's instrument-ID
index. Prices, including valid negative prices, remain unadjusted. Gaps and empty
days are explicitly unavailable/unknown, never filled. Native 1m is a reference,
not a substitute 1s source. A comparison requires a complete aligned interval
and matching publisher/contract; disagreements preserve both inputs. Native
hour/day references and status/statistics support remain separate evidence.

`SyntheticDbnSources` exercises the native DBN decoder. It verifies the exact
inventory and every required DBN metadata sidecar before opening any DBN, rejects
schema substitutions, overlapping source intervals, and protected coverage,
and decodes each selected file once per pass. Existing before/after integrity
hashing adds file reads; one decode is not a promise of one physical disk read.
Sidecars are required source
bindings; they are not OHLCV bars or another normalized timeframe. Generated
`synthetic/` paths and the inactive contract are mandatory. There is no provider
client, historical-admission issuer, or activation command in this addition.

## Storage and active-view organization

Choose a fresh candidate root under `data/causally_gated_normalized/` when
eventual admission permits a real candidate. In synthetic tests only, the layout
below exists under the test's private root:

```text
data/causally_gated_normalized/<candidate>/
  attempt.json, candidate.json, certificate.json
  runs/<attempt>.start.json, runs/<attempt>.finish.json
  <YYYY-MM>.seal.json
  ES/<YYYY>/1s/<YYYY-MM>/candidate/
    <the existing five compact Parquet table filenames>
data/active/
  catalog.json                         # existing 1m catalog
  causally_gated_normalized/ES/<YYYY>/<YYYY>.parquet[.manifest.json]
  timeframes.json                     # metadata index, synthetic/unadopted
  causally_gated_normalized/ES/<YYYY>/1s/manifest.json
```

Five Parquet files are kept open for one month, with aligned UTC-day row groups.
Empty days remain in coverage metadata; an entirely empty month has no payload
files. Hashes use the existing compact physical encoding and group-local evidence
references. A sealed month is reused only after its input identity, prior state,
coverage, and payload hashes match. An interrupted unsealed month is preserved
by a same-volume rename before rebuilding that month. No rollback data is deleted.

`prepare_reference` and `prepare_index` return prospective metadata in memory.
They do not publish it. The prospective 1s view references existing normalized
payloads and creates no duplicate payload copy. The manifest binds candidate,
certificate, completed resource-accounted certification, coverage, year, ordered
monthly membership, and file hashes. The index binds the unchanged 1m catalog.

`active_data_view.resolve_partitions(..., timeframe='1s', purpose='SYNTHETIC_TEST')`
returns the exact ordered tuple of monthly observation paths. Other 1s purposes,
status certification, missing/duplicate selections, mixed cadence, and drift fail
closed. `timeframe='1m'` delegates to the existing resolver, even without an index.
The original `resolve(...)` still returns one Path. Neither API guesses a cadence
or falls back from 1s to 1m. The legacy active-tree validator permits only the
exact registered metadata extension and does not read 1s payloads during 1m
resolution. Existing active payload names and bytes need no migration.

## Resource controls and reproduction

`Limits` requires explicit write, process-memory, and elapsed-time budgets. It
also bounds free-space reserve, daily source rows, Parquet group bytes, and file
creation. Writes include Arrow headers/footers and run records. Retries consume
the cumulative write allowance; an unfinished attempt pessimistically consumes
its declared remaining allowance. The operation lease is exclusive and is never
automatically stolen. Missing durable failure accounting is a block, not a
successful checkpoint.

Resource checks occur at input batches, day boundaries, hash operations, and
payload writes. Memory/runtime checks can observe an overshoot within a bounded
operation; they are not an OS-enforced memory reservation or preemptive timer.
Daily state is bounded independently of total history. Reports distinguish exact
application writes, instrumented payload-hash reads, OS process transfer counters, peak
process memory, and retained payload bytes. None measures SSD NAND wear or
provider throughput. Synthetic measurements are not a 41-market historical
capacity estimate.

Run the generated-data rehearsal from the isolated repository using its existing
Python dependencies, with bytecode writes disabled:

```powershell
python -B scripts/rehearse_causal_1s_synthetic.py --output <new-private-output-directory> --days 2
```

The harness accepts 1, 2, or 4 generated ES days, defaults to 86,340 bars/day, and
uses ceilings of 512 MiB cumulative application writes, 3 GiB process memory,
30 minutes per operation, and a 64 GiB free-space floor. It retains outputs and
refuses an existing destination. These ceilings are for this synthetic exercise,
not an approved historical run. The small terminal rehearsal JSON and stdout
are outside the producer's application-write counter and are separately visible.

## Integration review

The only existing implementation file changed is `active_data_view.py`: one
guarded metadata-extension check and the explicit timeframe wrapper. The 1m
producer, existing independent 1m certifier, compact codec, and ES reader are
unchanged. New modules reuse those dependencies without modifying them.

The final read-only source comparison and patch applicability result are saved
beside this checkout in `integration_audit.json`. No shared-checkout integration
has occurred. The other task, **Map current data stages**, completed its inactive
ES reader; its source and tests are preserved in the captured baseline. No message,
interrupt, reset, or commit was sent to that task.

At integration time, recheck that task's status and compare the affected source
hashes again. Apply only this implementation diff to a source baseline that still
matches, then run the focused 1s, active-view, compact-codec, 1m canary, and ES-reader
checks on the actual integrated source. Keep existing dirty/untracked work and
all accepted data bytes. A clean patch check alone is not a successful integration.

Prepare the ES historical pilot only after those actual integration checks pass.
Its exact admitted 1s source/sidecar inventory, development window, proposed
destination, measured resource allowance, and current primary-source policy must
be reviewed then. This software candidate does not reinterpret the current
reference-only 1s source role as primary-data authority. Historical admission,
reads, and active publication remain separate high-risk actions. No ES historical
pilot, activation packet, or real-data command has been prepared here.
