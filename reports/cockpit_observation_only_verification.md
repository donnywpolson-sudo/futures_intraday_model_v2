# Cockpit observation-only verification

Source-only verification on 2026-08-08 found no broker dependency and no
submit/place/send/cancel/modify order or open/close-position call in the
`futures_rebuild.live_cockpit` package. The package receives market data and
renders observation state; its UI explicitly disables trading controls.

The supported source launch is:

```powershell
.\.venv\Scripts\python.exe -m futures_rebuild.live_cockpit --self-check
.\.venv\Scripts\python.exe -m futures_rebuild.live_cockpit --demo
```

There is no installed `futures-live-cockpit.exe` entrypoint in `pyproject.toml`.
The self-check is coded to inspect credential presence only, but it also writes
a temporary local cache probe. It was not launched during this remediation;
source-safe synthetic/static tests are the evidence used here. Demo launch,
provider smoke, credential use, packaging, installation, and desktop cutover
remain separate approval boundaries.

The cockpit does not block the Alpha readiness census because no shared broker,
order, or position-changing surface exists.
