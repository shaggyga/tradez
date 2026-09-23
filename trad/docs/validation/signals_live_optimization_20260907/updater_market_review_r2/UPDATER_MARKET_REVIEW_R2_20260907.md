# Independent updater and market-display review — revision 2

Three verified malformed-input paths were corrected before runtime reload:

- The market overview accepted an exact `1e400` price and marked it current while converting it to Infinity. Prices and derived display values are now bounded and finite. Decimal calculations use an independent context; a nonzero result that underflows a display float is unavailable rather than falsely flat. Deeply recursive JSON is caught without crashing the dashboard.
- The recovery parser accepted midpoint OHLC outside the corresponding bid/ask OHLC, despite each component satisfying its own range checks. All four midpoint values must now lie between the actual bid and ask values.
- The gap scanner previously trusted `datetime` when `time` disagreed and did not verify row instrument/timeframe. It now rejects contradictory clocks, duplicate/ambiguous or missing required columns, malformed CSV and mismatched instrument/M1 identity before creating a reservation or requesting data.

Validation: 79 focused tests passed in 3.66 seconds. These include the new adversarial cases and existing immutable receipt, crash/rollback, retry budget, bounded scan, dry-run, CSV byte-preservation and freshness tests. The offline harness blocks network, worker creation and writes outside this evidence directory. All numerical examples are synthetic fixtures or in-memory reproductions.

Prior evidence under `updater_repair` remains intact; the revision-2 receipt binds the prior receipt hash and the final five source/dependency hashes. The original six registered study source hashes were also confirmed unchanged.

Runtime reload and actual API/process verification remain with root. This subtask did not call a broker, modify a live archive/database or start/reload any process. The scanner now requires explicit instrument, granularity and close columns plus an explicit timestamp; unsupported schemas are reported as recovery errors. Atomic CSV insertion continues to assume the updater is the sole writer.
