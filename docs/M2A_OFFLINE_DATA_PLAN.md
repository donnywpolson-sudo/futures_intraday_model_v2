# M2A offline data plan

M2A is implemented but has not been executed against the full legacy archive.

## Frozen boundaries

- Authoritative seed: all eight existing DBN families, exactly 8,040 files and 25,007,876,004 bytes.
- Canonical research inputs: definition, one-minute OHLCV, status, and statistics.
- Diagnostics: one-second OHLCV and trades.
- Cross-checks: hourly and daily OHLCV.
- Legacy Phase 1B comparison: exactly 530 direct `{market}/{year}.parquet` files. The 132 `_refresh_*` files are excluded.
- Legacy Phase 2 comparison: exactly 518 direct `{market}/{year}.parquet` files.
- Legacy raw and causal files never become authoritative; v2 regenerates them.
- Phase 1A schema exceptions and market-session policies are carried as evidence requiring v2 revalidation.
- The user-named legacy `FuturesLiveCockpit.exe` is pinned as a generated charting-artifact capsule only (13,636,273 bytes; SHA-256 `1cdc1210f884a157f93d8b2741ae36846bbbb6ac2cf4f45dbf36f647de6bbdf3`). It is never executed or ported, is not a data or inference source, and cannot satisfy any rebuild or research-readiness gate.

## Migration behavior

The default command is a read-only hash plan with concise family summaries. It enforces all expected counts, the authoritative byte total, the direct Phase 1B/Phase 2 byte totals, and exact byte/SHA pins for every singleton evidence file. `--detailed` includes per-file records. The safer review mode, `--detailed-inventory-output`, writes that complete canonical index atomically and without overwrite to a direct child of `state/migrations`, while printing only its small identity/totals summary.

The exact controlled-copy manifest is checked in and pinned by canonical manifest and source-scope hashes, but execution remains disabled by the separate pending approval artifact. `--approval-draft-output` can turn one reviewed inventory artifact into a canonical non-authorizing state draft; it does not modify the tracked approval config. Only deliberate review plus `apply_patch` of that exact config can authorize the hash-copy. Execution also requires the exact manifest/config paths, one allowlisted legacy root, the exact active staging/publication/state roots, the approved manifest and inventory hashes, and a hash-scoped `CONTROLLED_REBUILD_NON_ALPHA` operation receipt. Migration approval cannot authorize history evaluation or candidate sealing. The inventory is recomputed and matched before any lock or data write. Execution rejects Windows-normalized collisions, reserved device names, trailing dots/spaces, alternate-data-stream syntax, receipt-name collisions, and any extra staging path. It uses one writer, a manifest/inventory/implementation-bound checkpoint, per-file source-before/source-after verification, resume, verified idempotent skips, and orphan-temp quarantine. A killed writer's lease is never silently stolen: only a separately token-scoped recovery receipt may quarantine a sufficiently old same-host lease whose PID the OS proves dead, and both the original lock bytes and a canonical recovery receipt are retained. After every planned destination is reverified and the immutable receipt is durable, every legacy source is rehashed once more as the final substantive check before atomic rename to `data/vault/source_snapshots/{content-addressed snapshot ID}`. Consumers reverify the exact receipt schema/version/status, exact tree, every hash and total, and the directory content address. No active configuration may consume `.staging` or a merely checkpointed copy.

## DBN validation behavior

The offline catalog uses installed `databento==0.78.0` without constructing a client. For every exact-layout `.dbn.zst`, it requires one sidecar and checks provider, dataset, schema, market, exact start/end nanoseconds, declared path, size, SHA-256, encoding, compression, status, positive sampled publisher/instrument identities, and sampled event bounds. Its safe default consumes only the decoder's first bounded chunk; the sidecar SHA still authenticates every compressed byte. A full stream scan uses bounded chunks and requires explicit sorted family filters plus a total compressed-byte ceiling. Full decode of canonical research families belongs to M2B rather than inventory, while diagnostic families need a separate resource-justified gate. A sidecar's acquisition-client version remains provenance and is not required to equal the offline decoder version; locally decoded compatibility, schema, and bytes are the gates.

The source-selection manifest preserves every exact contract file and never searches for or chooses a recursively newest file. The eight known 6M June 2026 broad/narrow overlaps remain copied, but the broad file alone is authoritative for the interval; each exception is allowed only when both full file hashes and a sorted full-record overlap proof match `configs/dbn_overlap_resolutions.json`. An unlisted, unused, duplicate, or changed resolution fails closed. The exact anomaly families `KE` 2019/2021/2023/2024 and `SR1`/`SR3` 2020 are quarantined until separate content-addressed causal-acceptance releases exist.

Databento `.v.0` is pinned to previous-trading-day volume rank 0 with original unadjusted prices. Retrospective continuous-symbol mapping intervals are reconciliation-only; their end dates never become features or eligibility inputs. The bar's actual `instrument_id` plus a definition effective no later than the bar event and received/available no later than the decision is authoritative. Definition versions are date-independent, while the UTC instrument namespace and exchange-session dates are derived at bar resolution. One definition can therefore govern multiple dates without treating future metadata as known.

Databento OHLCV has no `ts_recv`. Historical OHLCV availability is modeled explicitly as interval end plus a pinned conservative latency; archive/source retrieval time is recorded separately and never relabeled as provider receipt time. Feature, label, return, and P&L intervals cannot cross the resolved actual-contract segment. An unexpected future change remains `ROLL_UNRESOLVED` in the outcome-coverage denominator. Active prediction or P&L paths also require one verified actual-contract economics record; legacy `configs/costs.yaml` is exact hash-copy reconciliation evidence only and never economics authority.

## Deferred execution

No full hash inventory, legacy DBN decode, catalog output, copy, promotion, or provider operation has run. The overlap policy has synthetic coverage but has not yet been proved against the real archive. The next execution gate is generation and human review of the non-authorizing detailed-inventory artifact. The tracked approval remains pending; any later copy still requires its own deliberate patch and the exact reviewed hashes.
