# Local filesystem complexity manifest

Generated from path and file metadata only on 2026-08-08. No credential value,
price row, provider payload, or 2025 payload was opened.

## Census

- Files outside `.git`: 101,998
- Total bytes: 280,505,772,839
- Ignored paths enumerated by Git: 100,196; Git also reported inaccessible
  overlong archive subtrees, so this is a lower bound for Git visibility.
- Active-data files: 1,125
- Immutable DBN/raw/causal-source files: 16,816
- Feature/outcome/prediction/evaluation artifacts: 1,444
- Test/cache residue paths: 5,831
- Archive/backup/staging-named paths: 3,494
- Absolute paths at least 240 characters: 1,703
- Longest observed absolute path: 297 characters
- One actual credential-bearing path is present by name: `api.env`. Four other
  credential/secret-named matches are source code, audit output, or bytecode;
  none was opened for secret content.

Categories overlap and are classification aids, not deletion counts.

## Binding-preserving classification

1. Active dependencies: `data/active/`, the active catalog, the active
   calendar pointer, and exact files named in the current readiness plan.
   Leave them in place until a separately approved immutable successor changes
   every affected binding.
2. Immutable inactive evidence: DBN, raw, causal releases, manifests, trial
   genealogy, receipts, and sealed unpublished evidence. Preserve by default.
3. Duplicate or superseded archives: the top-level
   `futures_intraday_model_v2_data_archive/`, `FuturesLiveCockpit.backup-*`,
   and historic staging trees. They are cleanup candidates only after an exact
   active-binding and unique-content audit.
4. Reproducible residue: `.pytest*`, `__pycache__`, `*.pyc`, and build caches.
   They are deletion candidates only under a separately approved exact path
   manifest.
5. Protected configuration: `api.env` and any installed credential locator.
   Keep ignored; never read, log, stage, package, relocate with research data,
   or include in deduplication content scans.

## Safe future remediation order

1. Do not relocate anything before the current readiness plan is resolved.
2. Export the exact active-binding path set and exclude it from cleanup.
3. Address reproducible cache residue first.
4. Audit archives for unique immutable evidence using manifests, not price-row
   inspection.
5. If relocation is still useful, create an immutable catalog successor and
   validate every changed hash/path before pointer cutover with rollback.

No deletion, move, archive, deduplication, rewrite, pointer change, or active
data mutation is authorized by this manifest.
