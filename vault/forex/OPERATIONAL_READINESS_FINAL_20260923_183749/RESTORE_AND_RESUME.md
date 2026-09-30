# Verified operational checkpoint

Git commit: `5e7cb25c467bfb47887c89953b16f5b50d698528` on private `shaggyga/tradez`, branch `forex`.
Bundle: `forex-operations-5e7cb25c467b.bundle`; SHA256 `bdad10907b8e6d02522c24a8abb5bef7eaabce859a431c8ea10e96195d08af8a`.

Verify SHA256, then run:

```powershell
git -c core.longpaths=true clone --branch forex <bundle-path> forex
```

Use the local machine's Vault path with `tools/forex_preflight.py`. Read CURRENT_STATUS.md
and the live queue/pointers first. The three registered saved-artifact archives can be
retrieved byte-for-byte; do not refit an existing run because local weights are absent.

58 contract tests passed, six independent gate cases and twelve publication guard
cases passed. The fresh restored checkout matched every one of 4,595 tracked files
and the fetched GitHub commit. Root and restored preflight both passed, including the
three saved-artifact archives. These are offline integrity checks, not model runtime,
forecast or trading qualification. No models were loaded or fitted.

Current services are degraded according to the main packet's read-only runtime report.
Service recovery and G's machine qualification remain separate. Shared ownership is
advisory; uncertain freshness blocks potentially duplicate work. Research is paused.
Exact next scientific queue item: `macro_currency_meter_numeric_evidence_join_v2`.
