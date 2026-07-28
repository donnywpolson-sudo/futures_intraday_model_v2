# Codex handoff

## Current state

- Repository: `C:\Users\donny\Desktop\futures_intraday_model_v2`
- Branch: `codex/standalone-v2-completion`
- Pre-closure HEAD: `3b8fee583c80e37aeae521c1e1cf042ac407e30d`
- Session 1 milestone commits:
  - cockpit activation: `2ea5964741b6567877fc824fdbb0a955a1a0ac06`;
  - calendar and empirical foundation:
    `d2ef4e1a385ae459041aa321b23bccc9d8982e87`;
  - non-authorizing causal predecessor:
    `42dac0a5b874b190aece0523e4da36b15871d084`;
  - steady-state audit and operating documents:
    `e0cee4129e5072afb0df0c6f2b5afcdf5d9097c7`.
- Accepted 41-market DBN release:
  `086282eaef7b36a61626f88d93d06c93b87c1cb3407c936d065d0d1b9d98599e`
  (4,491 DBNs plus 4,491 sidecars).
- Accepted schema-5 foundation predecessor:
  `78806ef01714c72f6da537c1b6e6f8b2e903b14728822b0daa31b4c6c75a8909`;
  manifest SHA-256:
  `bfa86450b77ab4b19c6e17e641aab6684895d721188497a664eb4b37ab4c8ce9`;
  683 immutable intervals.
- Active official current/forward CME calendar index:
  `2b1ec84769b655dd0e73789522126aa1650ae2209210dd5c4c49d0ae67bc423c`;
  pointer:
  `ca0044c4a6bf197a62a9d5b01896226d2b4c4295aa0f6873d3b6908844b45dcc`.
- The observation-only cockpit smoke and separately approved activation passed;
  prior installation and rollback metadata remain preserved.

## Empirical historical-observability successor

- The user selected immutable Databento DBN observability instead of continuing
  the incomplete public historical CME archive search.
- Approved semantic plan:
  `reports/foundation/dbn_empirical_historical_observability_successor_plan.json`
  - plan ID:
    `e990d135fce49cd809c4b122c72f3077ef49216eb9d691bf677e87582a4493c2`
  - plan SHA-256:
    `73f9f3c83366913b2873667ae4545079d13b3546923f19a75019779819ccf0dd`
  - approval receipt ID:
    `43dc2fd562ef2ae513534bb7c2b2768251c678ed0d69627e4f82e5a38044cebc`
- The policy is offline and non-authorizing:
  - actual decoded DBN rows only;
  - no fill, interpolation, synthetic open/close, or closed-state inference;
  - unobserved time is missing, not closed;
  - session rollover groups trade dates but is not trading-hours authority;
  - no official historical CME open/close/halt/holiday claim;
  - the activated official CME calendar remains current/forward cockpit
    scheduling authority only.
- Derived predecessor census:
  - 683 physical intervals across 41 markets;
  - 650 available market-years;
  - 644 research-admissible market-years;
  - 568 pre-status intervals;
  - 115 status-scope physical intervals representing 82 market-years
    (41 markets for 2025 and 2026; 2026 has immutable source splits);
  - 562 research-admissible pre-status candidate market-years;
  - 198 research-admissible discovery market-years;
  - 41 locked holdout and 41 forward-only market-years;
  - six quarantined market-years remain observable but not
    research-admissible.
- Accepted schema-7 empirical-observability foundation:
  `637f16b3c23c9f2215858f49754965738fe9c00095661d7a29d6877d566ae5e3`;
  manifest SHA-256:
  `969079b6576417658ede21e63b00b9d2211856157d01ef10c3ebb0d77cca2ad9`.
  Its publication approval receipt is
  `f995fc23e007dbb4b153b043767c4327f2ed42cd75c36906f1e6d9992f5ca002`.
  Existing schema-4/5/6 foundations remain reproducible but fail readiness
  with `HISTORICAL_OBSERVABILITY_CONTRACT_NOT_BOUND`.
- This semantic operation authorizes no network/provider request, historical
  payload read, foundation publication, readiness publication, outcomes,
  models, predictions, holdout access, staging, commit, or push.

## Contracts and closure evidence

- Use the pinned environment: `.\.venv\Scripts\python.exe` (Databento 0.78.0).
  System Python currently resolves Databento 0.79.0 and is not authoritative.
- Universe/cohort contract SHA-256:
  `86e38f8732bd05b10d0faa2eec93e0bd0db6aeac3e56a13fbaed8e69555035d4`.
- Operational profile SHA-256:
  `df406dcd31826bfdb83d1237ecee1c41db668bf27bfd0d04127df893a9c64954`.
- Active current/forward calendar:
  - index release:
    `2b1ec84769b655dd0e73789522126aa1650ae2209210dd5c4c49d0ae67bc423c`;
  - index manifest SHA-256:
    `51ad8a036eadcb9f11831ede6dcb65575c2e796deb4bc03ad27f87c3fb7d25a7`;
  - policy SHA-256:
    `4c3420d301390d7cb6f7602f61a00abe9d6b09ae2defb2b19e916ca948aee6d5`.
- Data-inventory baseline is DBN release
  `086282eaef7b36a61626f88d93d06c93b87c1cb3407c936d065d0d1b9d98599e`,
  manifest SHA-256
  `c2584d5e1a65103f8651a871de6f704ac31ec2c2f7ec5c2e1a941aae6a4dc8fd`,
  with 4,491 DBNs, 4,491 sidecars, 8,982 files, and 25,592,717,852 bytes.
- The pinned full suite passed before final receipt generation: 600 passed,
  one existing Node-only skip, zero failed, and zero errors.
- Final closure requires create-only reports at:
  - `reports/audits/final/foundation_ready.json`;
  - `reports/audits/final/historical_research_ready.json`;
  - `reports/audits/final/observation_cockpit_ready.json`;
  - `reports/audits/final/meta_master_audit.json`;
  - `reports/audits/final/retirement_readiness.json`.
  The first three and Meta Audit must be `SUPPORTABLE`; retirement must be
  `LEGACY_RETIREMENT_READY`. Treat any absent or different result as a blocker.
- `data/active` is absent. No current writer, provider operation, publication,
  recovery process, or active lock exists. Historical staging and recovery
  artifacts are retained as inert evidence.
- Preserve every prior calendar/archive attempt and failure artifact as inert
  evidence. Do not resume the CME/Internet Archive historical search.
- `configs/causal_market_year_materialization_approval.json` remains
  `PENDING_EXACT_HASH_BOUND_APPROVAL`; it is predecessor evidence only and
  authorizes no materialization, cutover, or publication.
- No remote push and no legacy deletion.

## Session 2

After all final reports above pass and `git status --short` is empty, continue
in the session named `Review data path format`:

1. Read `AGENTS.md`, `PROJECT_OUTLINE.md`, this handoff, and live repository
   evidence.
2. Bind Session 2 to schema-7 foundation release
   `637f16b3c23c9f2215858f49754965738fe9c00095661d7a29d6877d566ae5e3`.
3. Reconcile the actual 650/562/6/41/41/198 counts; do not force a planning
   snapshot.
4. Treat the pending causal-materialization plan as non-authorizing predecessor
   evidence and stop at its next exact approval boundary.
5. Do not mutate `data/active`, read outcomes, fit models, access holdout or
   forward rows, call a provider, push, trade, or delete the legacy repository
   without separate exact authority.
