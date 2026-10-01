# Exact quote receipts

Queue step `retained_management_quote_receipts_v1`; Vault packet `EXACT_QUOTE_RECEIPTS_20261001`.

The offline quote boundary is implemented and verified. It is opt-in and has **not been enabled in a running collector**. No broker calls, collector restarts, fits, forecasts, orders or account actions occurred. The old float cache and current running pipeline remain unchanged. Management is not activated.

## What changed

`trad/oanda_exact_quote_receipts_v1.py` preserves original bid/ask strings and the nanosecond RFC3339 provider clock before conversion to floats. Each receipt binds its raw payload hash, actual local receive time, session identity, connection generation, instrument and explicit tradeability. The parser rejects ambiguous tradeability, crossed prices, future market clocks and invalid numeric representations.

An optional `raw_price_observer` on the existing `MultiPriceStream` receives raw PRICE bytes before the old converter. It receives an empty generation boundary on reconnect. The observer is not supplied by the existing dedicated quote worker, so this checkpoint does not roll out collection. A failing observer increments a visible error counter without changing legacy float quotes.

The new observer uses the existing coalescing SQLite/WAL publisher, with four retained snapshots and at most 128 declared pairs. It publishes no more than once per second for ordinary ticks, with immediate boundary/refusal updates. Payloads are bounded to 256 KiB and individual raw messages to 16 KiB. This is a latest-observation sidecar, not a tick archive. Each new process gets a distinct session ID. Malformed updates clear affected quote eligibility; unknown malformed messages clear the full sidecar map. Reconnects cannot reseed exact receipts from old float caches.

The existing transport now exposes its already-recorded write-start timestamp to readers. This is explicitly not a commit-completion claim. The receipt reader sets manager availability to its actual read-completion clock and verifies market <= receive <= snapshot creation <= read. When a SQL write timestamp exists it must also fit that ordering. JSON fallback has no asserted SQL publication timestamp. Age is always measured from the original market timestamp; rereading does not refresh it.

The existing management `quote_at` and USD conversion functions remain authoritative and unchanged. Receipt eligibility does not establish position state, financing, sizing, expected continuation value or order authority.

## Reuse and evidence

The September9 `oanda_research_quote_receipt_v1.py` already validates cache observations for three pairs. Its bounded JSON, time and decimal validation is reused. It cannot recover original price precision after float conversion; its original source, tests and historical evidence are preserved. The new module captures earlier in the existing stream path instead of replacing that history. The original transport and accounting engines are reused.

Read-only discovery found an existing raw EUR/USD archive written by `oanda_capture_extras.py`: `trad/data/eurusd_feed_20260927_week/messages-20260927T191205Z-c04fb8ea.jsonl`. A bounded first-record/tail read retained 64 distinct PRICE records, their original decimal/time strings and recorded receive clocks. Other-task capture files and processes were untouched. The source file may continue growing; the evidence records sampled size/offset and slice hash, not a full-file seal. Normalized archived payload JSON is not claimed to preserve original wire whitespace.

All 64 records parsed. At the actual recorded inspection clock all were stale and correctly refused by the manager. Each report covers the same declared 68-pair inventory: the other 67 pairs have explicit missing raw evidence. This is parser/causality evidence, not a 68-pair live activation or a forecasting experiment.

The deterministic command produced the same report on a second invocation (`completed_verified`) and after extraction into a new folder and a fresh process. The restored process refused reads from the original workspace and Vault before application imports. Capsule: `quote_receipts_capsule.zip`, SHA-256 `2a20fe1bc1d602c941c66b9ce474f0e2f4c721a63162d1cf54a4827cd66c73f0`; 11 members, 37,912 bytes. The frozen report SHA is `9d8090c024984886e7008567e58aba37aa39c732f23db1dd033d47e0b632111f`.

## Verification and review

137 tests passed, including new exact-decimal and actual-consumer tests plus existing receipt, transport, dedicated stream and executor tests. They cover stale/future/nontradeable/crossed refusal; resealed wrong receipt/session/generation; inventory and clock ordering; direct/inverse USD gain/loss conversion; callback delivery before float conversion; reconnect/startup separation; invalid update removal; bounded four-snapshot storage; JSON-lock fallback; and the actual read availability clock. The stream test uses a supplied fake HTTP response and never opens a broker connection.

Same-task review completed; independent review was not performed. Engineering readiness is limited to the offline receipt boundary. Forecast evidence is unchanged, policy evidence remains unqualified, and demo authorization is not granted. Deployment must bind the active process/session/generation and observe hook/publisher health before using the sidecar; asynchronous publication does not promise instantaneous cross-process generation switching.

## Reproduce and operate

Use the existing locked Python environment. The capsule contains `INPUT.json`, exact source closure, `EXPECTED.json` and its own manifest. Verify the archive SHA and extracted manifest first, then:

```powershell
python -I -B <extracted>/replay.py C:/Users/zmoor/Documents/forex C:/Users/zmoor/OneDrive/thevault/projects/forex
```

The two roots are paths that restored execution must not read. The runner verifies its source/input hashes, executes the real consumer and compares its output hash. On another machine pass the corresponding original roots. It is a same-host relocation check, not proof that a second machine was tested.

The project command is:

```powershell
python -I -B tools/forex_quote_receipt_replay.py --input <INPUT.json> --sha256 492866a1432fd8bcb0907806b292026da2f601a29b0d0a582cbb1e909fb20093 --output <new-report.json>
```

Repeating with the identical output verifies it without replacing bytes. An altered/partial output fails `existing_output_mismatch`: preserve it and investigate; do not claim resume or overwrite it. Source/input mismatch fails before replay. Missing, stale or invalid rows remain explicit refusals. The capture clocks are fixed for reproduction and never represented as fresh now.

For future opt-in integration, create `ExactQuoteReceiptPublisher(separate_path, sorted_pairs)`, supply it as `raw_price_observer` to `MultiPriceStream`, and close it after the stream stops. Its `flush()` supports a finite offline batch. Read using `read_receipts(separate_path, instruments=sorted_pairs)`. Keep the existing float cache at its original path. This interface is implemented and tested, but service configuration and rollout are outside this offline checkpoint.

Local evidence: `evidence/quote_receipts_20261001`; immutable review packet and capsule in the live Vault. Git contains source and a dated Vault knowledge snapshot. Rollback source predecessor: `7113f3dad0133422d360758474b2161f8c8b285c`; preserve captured evidence.

## Exact next work

`retained_management_forecast_receipts_v1`: use the existing saved retained forecast publications and immutable issue bodies to build a separately named, observation-only receipt adapter accepted under its real input tier. Preserve exact origin/terminal/model/input identities, original first observation and outcome state. Reuse the native curve publication/consumption contracts; do not relabel retained forecasts as synthetic, invent prior availability, refit models or recompute saved projections. Qualify exact same-terminal continuation separately from new-entry forecasts, test refusal/restart/restore through the consumer, and publish the complete gate.

Remaining beyond that step: quote sidecar live rollout and health/session reconciliation, common-terminal conditional-value qualification, explicit economic/state policy, unqualified exact horizon targets and global model superiority. Quote receipt repair alone does not complete position management.
