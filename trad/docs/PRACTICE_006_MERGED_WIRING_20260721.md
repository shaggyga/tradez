# Practice -006 Merged Aggressive Lane

## Purpose

Practice account `-006` is an additional sample-generation account. It combines
the major GPT checkpoint used by `-002` with the live consolidated all-signal
snapshot produced by `-007`. It does not replace or mutate either source
account.

The strategy lab remains the only heavy 68-pair, 208-lane producer. The merged
relay consumes its compact snapshot and reprices every candidate immediately
before an order, avoiding a second copy of the large outcome pipeline.
The snapshot exposes the top 32 consolidated pair rows to this relay; `-007`'s
own execution thresholds and selected signal are unchanged.

## Runtime Components

- `oanda_practice_merged_006_gpt.py`: pins the major GPT checkpoint to practice
  account key `OANDA_ACCOUNT_ID_DUM3`, verified to resolve to suffix `-006`.
- `oanda_practice_merged_signal_relay.py`: consumes
  `practice_007_signal_snapshot_v1.json`, applies the aggressive gate, fetches a
  current OANDA quote, and delegates sizing and exits to `PracticeExecutor`.
- `start_oanda_practice_merged_006.ps1`: starts both workers under independent
  Event 7 guards.
- `practice_006_merged_order.lock`: serializes account entries across both
  workers. The GPT order path performs a second broker duplicate check while
  holding this lock.

## Aggressive Policy

- Maximum open positions: 8 across the account.
- Target/high/hard margin: 68% / 76% / 82%.
- Per-trade margin allocation: 8% to 16% of NAV.
- Per-trade stop risk: 0.6% to 1.8% of NAV.
- Entry cooldown: 2 seconds; identical signal repeat delay: 90 seconds.
- Prediction horizon is retained for attribution; relay-owned positions use a
  supported managed hold no longer than 900 seconds.
- Profits remain open-ended. Trailing activation and profit-lock checks run at
  250 ms, with spread-aware minimum distances.
- Minimum executable projected net: +0.05 pips after the source cost model.
- Ordinary unvalidated signals are allowed for paper sampling when confidence,
  projected net, and positive family consensus pass.
- At most two negative-calibration exploration positions may be open. They need
  at least 50% current confidence, +0.10 projected net pips, and an agreement
  edge of at least three families. Persistent negative historical warmup is
  always rejected. Exploration signals are low-sized and explicitly tagged.

## Validation And Initial State

Focused merged-lane tests: 9 passed. Existing strategy/executor regression
tests: 64 passed. Total focused deployment run: 73 passed.

A no-order connectivity run resolved `-006`, initialized against the OANDA
practice endpoint, and made zero order attempts. The account started at balance
and NAV `49.2972`, with zero open trades and zero margin used.

At initial deployment, the GPT launch call returned OpenAI HTTP 429
`insufficient_quota`. The process remains alive for later scheduled retries, but
the GPT half cannot emit a fresh decision until API quota is available. The
technical relay is independent and remains active. Its first fresh snapshots
contained no candidate with a positive instantaneous executable edge that also
passed the aggressive/exploration constraints, so no initial relay order was
forced.

## Storage Guard

Windows emitted new disk Event 7 records through `425799` on 2026-07-20. Every
source and merged worker was restarted with `425799` as its baseline and is
required to stop on the next Event 7. The D volume being mounted and reported
healthy by `Get-Volume` is not evidence that the physical disk is repaired.

## Run

```powershell
powershell.exe -NoProfile -ExecutionPolicy Bypass `
  -File D:\forex\trad\start_oanda_practice_merged_006.ps1
```

No credential is stored in this package. The launcher and wrappers resolve the
existing local credential keys at runtime and refuse an account suffix other
than `-006`.
