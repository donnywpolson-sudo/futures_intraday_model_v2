# MFF execution boundary

FuturesLiveCockpit is observation-only. It contains no broker adapter,
Tradovate transport, credential store, order assistant, account reconciliation,
or order command surface.

MFF Evaluation and Rapid EOD Sim Funded remain `MANUAL_ONLY`; order entry and
account-state reconciliation occur outside the app. MFF Live API capability
remains unconfirmed.

`configs/mff_execution_capability_evidence.json` is preserved as bounded
capability evidence only. It grants no provider, account, order, trading, or
production authority.

Execution code may return only for a current requirement, verified entitlement,
explicit approval, and a separate safety review. Cockpit packages continue to
reject execution modules.
