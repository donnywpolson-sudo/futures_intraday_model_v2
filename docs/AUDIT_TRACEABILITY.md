# Audit traceability

Every release blocker maps to an enforceable contract and acceptance evidence. `M1` means implemented in the deterministic core; later gates remain blocked.

| Audit finding | Required correction | Implementation | Acceptance evidence | State |
|---|---|---|---|---|
| New folders do not reset experiment history | Existing history is discovery; prospective confirmation only | Constitution, trial charter | Charter rejects unregistered evaluation | M1 |
| Real-data smoke is another trial | Separate mechanical smoke from registered evaluation | `trial.py` | Synthetic trial/firewall tests | M1 |
| Hashes alone do not prove causal correctness | Event, available, received, and decision time | `time_contracts.py` | Timing rejection and UTC tests | M1 |
| Databento bar timestamp is interval start | Require completion and processing before decision | `BarObservation` | Same-bar decision rejection | M1 |
| Continuous symbol is not executable identity | Persist actual composite contract and definitions | `identity.py`, schemas | Identity validation tests | M1 |
| Future realized roll can condition earlier eligibility | As-of ledger uses only observations available by decision | `AsOfRollLedger` | Future-observation mutation test | M1 |
| Feature/outcome/prediction leakage | Physically distinct schemas; forbidden feature names | `schemas.py` | Poison-column tests | M1 |
| Repeated repairs can overfit | Pre-registration and immutable trial identity | `trial.py` | Semantic-change/new-ID tests | M1 |
| WFA fold model is not a deployable artifact | Seal complete bundle metadata and verify bytes | `bundle.py` | Tamper/reload tests | M1 |
| Inference can fit or place orders | Predict-only capability, no order interface, abstention | `inference.py` | `.fit()` spy rejection and abstention tests | M1 |
| Predictions can be rewritten or contaminated with outcomes | Immutable hash-chained record files, prediction-only schema | `ledger.py` | Duplicate/tamper/chain tests | M1 |
| Publication may partially succeed or collide | Lease, staging validation, fsync, atomic rename, no overwrite | `release.py`, `locking.py` | collision/idempotence/recovery tests | M1 |
| Legacy roots can leak into active work | Exact allowlist and copy-only migration | `migration.py`, migration config | stray/symlink/manifest tests | M1 |
| Refresh backups can silently enter comparison data | Direct `{market}/{year}.parquet` filter with frozen exclusion counts | `migration.py`, migration config | direct-layout and exclusion test | M2A |
| Source changes after review but before copy | Require reviewed manifest and independently pinned recomputed inventory before any write | `migration.py` | wrong-inventory/no-write test | M2A |
| Interrupted copy can produce partial or ambiguous state | Manifest-bound checkpoint batching, lease, resume, destination verification | `migration.py` | receipt, uncheckpointed-batch, and post-rename crash tests | M2A |
| Checkpointed staging can be mistaken for accepted data | Exact reverify, immutable receipt, atomic content-addressed publication, consumer-side full verification | `migration.py` | publication, tamper, and second-run verification tests | M2A |
| DBN sidecars or decoded metadata can disagree with bytes | Offline pair validation, exact metadata boundaries, bounded decode, and exact-layout catalog | `dbn_catalog.py` | synthetic DBN hash/metadata/bounds/unsupported-schema tests | M2A |
| Broad and narrow provider files overlap | Preserve both; authorize one interval only through pinned file hashes and full-record subset equality | `dbn_catalog.py`, overlap contract | overlap selection and tamper tests | M2A |
| Recursive newest selection can substitute source files | All exact files selected; uncontracted duplicate logical coverage fails | `dbn_catalog.py` | ambiguous-coverage test | M2A |
| Retrospective mapping intervals expose future roll ends | Bar instrument ID is authoritative; mappings reconciliation-only | `identity.py`, `dbn_catalog.py` | future-mapping mutation invariant | M2A |
| Databento instrument IDs can be reused across publishers or dates | Composite dataset/publisher/instrument/UTC-namespace-date identity plus separate exchange-session date and as-of definition provenance | `identity.py`, schemas | composite identity and future-definition mutation tests | M2A |
| Legacy cockpit can be mistaken for trusted live inference | Pin as non-active generated evidence; never port or execute | migration manifest, M2A plan | exact size/hash migration gate | M2A |
| Existing Phase 1B to Phase 2 evidence is structurally inconsistent | Regenerate against canonical promoted identities; zero waivers | M2 data gate | Zero-error reconciliation report | Blocked |
| Missing/no-trade, sessions, DST, halt, limit, expiry and roll behavior | Explicit causal fixtures and policies | M2/M3 data code | Adversarial data suite | Blocked |
| Row-level samples exaggerate evidence | Date/session clusters, purged nested WFA, block/HAC uncertainty | M4 research engine | Synthetic statistical validation | Blocked |
| Historical success is not final proof | Seal candidate then collect prospective evidence | M5 operator | As-received prediction and outcome ledgers | Blocked |

## M2A frozen audit closure

The current synthetic contract suite additionally closes these audit paths while
leaving the real data foundation and historical research blocked:

| Failure mode | Enforced correction | Evidence state |
|---|---|---|
| Repository code mints its own candidate/history authorization | External receipts require RSA PKCS#1-v1.5/SHA-256 under a pinned public key; no matching private key or signer is stored | Synthetic forgery and scope tests pass; production signer intentionally absent |
| A migration approval is reused as research authority | Migration, candidate, and real-history classifications and scopes are distinct | Cross-classification tests pass; `copy_authorized` remains false |
| OHLCV archive time is mislabeled as provider receive time | OHLCV rejects provider `ts_recv`; availability is interval end plus pinned latency and retrieval is separate | Synthetic timing tests pass |
| `.v.0` or mapping ends leak future roll information | `.v.0` is previous-trading-day volume rank zero on unadjusted prices; the bar's actual instrument ID is authoritative and mapping ends are reconciliation-only | Policy-hash and future-mutation tests pass |
| Definitions selected with later knowledge identify earlier bars | Definition effective time must precede the bar event; receive/availability must precede the decision | Delayed-decision and future-definition tests pass |
| UTC-midnight or in-session mapping changes blend economics | Every actual-identity change creates a hard contract segment | Segment and same-session midnight tests pass |
| Future contract changes disappear from outcomes | Cross-contract labels are `ROLL_UNRESOLVED` with no return and exact coverage preserves one outcome per prediction | Outcome coverage tests pass |
| Trial history is guessed as exact or zero | Real history requires a preregistered conservative penalty strictly above the observed floor, with rationale and evidence hashes | Unresolved and conservative-census tests pass; real history remains unauthorized |
| Runtime dependency drift changes a supposedly sealed predictor | Interpreter, packages, dependency receipt, runtime config/code tree, and exact NumPy Windows wheel are hash-bound | Dependency and reload-parity tests pass |

These are architecture and synthetic-mechanics findings only. They do not prove
the legacy bytes, decode real DBN history, establish alpha, or satisfy
`HISTORICAL_RESEARCH_READY`.
