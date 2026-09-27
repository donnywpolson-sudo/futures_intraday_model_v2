# Native 1s lifecycle

The 1s software supports monthly builds, independent two-pass reconstruction,
and manifest-backed publication. The installed gateway still denies historical
1s execution. Software tests and prepared operations do not normalize historical
data or authorize publication, activation, or research.

`causal_1s_native.prepare(root, market='ES', month='2024-03')` returns the exact
operation using registered metadata, verified sidecars and file sizes. It does
not open DBN or Parquet payloads. The default pilot is the whole March 2024 UTC
month (February 29 at 4 p.m. through March 31 at 5 p.m. Pacific, exclusive).
The output interval differs from physical read scope: all selected annual files
are read in each of three passes, with integrity hashing before and after decoding.

`prepare_adoption` returns the proposed gateway source and exact inactive-scope
configuration. Those are review artifacts. After the owner approves that bounded
adoption and historical operation, `execute_once` consumes the existing external
receipt, rechecks preparation, and starts one supervised worker. There is no
execution CLI or automatic retry. A fresh operation refuses an existing output
directory. `prepare(..., resume=True)` creates a distinct scope requiring a new
approval and exact prior source/code/semantics; it reuses verified month seals.

The worker uses the existing offline decoder and five-table codec. The producer
and independent reconstruction remain separate. A Windows job limits worker
commit memory and kills its process tree if the supervisor closes; the parent
enforces the total elapsed-time limit, including blocked input/output. Resource
checks run before writes. Standard output/error are discarded rather than allowed
to grow; bounded failure metadata and retained partial output explain failures.
Filesystem allocation, application writes, and process I/O are separate measures;
none measures SSD NAND wear. The allocation ceiling is conservatively bounded by
the cumulative write cap plus 64 KiB per file; execution rejects larger clusters.

Completed month seals permit exact recovery without rewriting completed payloads.
A failed historical attempt requires a newly reviewed resume operation; its
receipt and partial files remain. A successful native supervisor receipt is
required before its candidate can be proposed for publication.

`prepare_publication` accounts for an explicitly enumerated market/month set.
Every declared pair must be certified, unavailable, or quarantined. Mixed source
releases, duplicate pairs, and missing pairs fail. `publish` requires a separate
external publication receipt bound to the exact proposal and limits. Under the
existing publication lock it moves each five-file directory on the same volume,
installs immutable manifests, verifies membership and hashes, and replaces the
active index last. Interrupted moves can be resumed with the consumed receipt;
predecessor drift fails. Previous index bytes and immutable manifests remain.

Payloads and monthly manifests have one durable owner:

```
data/causally_gated_normalized/ES/2024/1s/2024-03/<spec-id>/
    observations.parquet
    missingness.parquet
    roll.parquet
    quality.parquet
    cadence.parquet
    manifest.json
data/active/timeframes.json
```

This refines the audited plan's proposed per-year active manifest layout. Only the
atomic index is added inside `active`; it explicitly references the immutable
monthly manifests. Otherwise installing manifests before the index would make
the existing exact-tree 1m validator reject an interrupted publication. There are
no additional payload copies, hard links, or annual 1s files.

`resolve_partitions(..., timeframe='1m', ...)` preserves the annual 1m interface.
Explicit `1s` returns ordered monthly observation paths after checking the index,
coverage dispositions, cohort, certificates, all five payload hashes, observation
schema and cadence. Synthetic releases allow `SYNTHETIC_TEST`; native releases
allow `LINEAGE_AUDIT` only. Status-qualified or research use requires separate
admission and is refused. A year containing unavailable/quarantined declared
periods is not returned as a complete certified year.

The first real operation remains ES only. Expanding to all 41 standard markets
requires complete inventory-based bounds plus measured ES results and separately
approved scopes. Missing historical coverage and actual resource use cannot be
established by generated-data tests.

For an approved longer range, `through_month` extends the operation while keeping
monthly storage and continuous carried state. Use one declared contiguous range
per market, rather than treating each month as an unrelated canary. Availability
must precede the operation's exclusive end; earlier monthly storage boundaries
do not discard observations or reset roll state. The final July 2025 prefix ends
at the development cutoff; a crossing physical source is still refused.
