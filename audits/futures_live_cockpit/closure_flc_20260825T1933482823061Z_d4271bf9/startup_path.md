# Startup path

The canonical desktop entry is the bare executable:

`C:\Users\donny\Desktop\futures_intraday_model_v2\FuturesLiveCockpit\FuturesLiveCockpit.exe`

The packaged host loads bundled HTML/CSS/JavaScript assets, creates the WebView2 window, and starts engine/cache initialization off the UI thread. Demo data is generated lazily for the selected market, rendering is coalesced, retained demo history is bounded, and shutdown joins initialization before closing the cache.

`--self-check` validates the package without opening the provider. `--demo` provides a provider-free GUI smoke path. A bare launch is the configured live observation path and may open the repository credential source; it must not be used for offline audit work.
