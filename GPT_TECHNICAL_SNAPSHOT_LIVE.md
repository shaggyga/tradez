# GPT Technical Snapshot Live

Live GPT account manager that sends a ranked technical snapshot of potential
OANDA FX positions to GPT, then lets GPT manage the live account through the
existing guarded advisor execution engine.

## Files

- Bot wrapper: `oanda_gpt_technical_snapshot_account_manager.py`
- Safe starter: `start_gpt_technical_snapshot_live.ps1`
- Live data: `data/forex_gpt_manager/account_gpt_technical_snapshot_live/`
- Registry role: `gpt_tech_snapshot_live`
- Latest technical snapshot audit:
  `data/forex_gpt_manager/account_gpt_technical_snapshot_live/latest_technical_snapshot.json`
- Technical snapshot ledger:
  `data/forex_gpt_manager/account_gpt_technical_snapshot_live/technical_snapshot_ledger.jsonl`
- Local status report:
  `data/forex_gpt_manager/account_gpt_technical_snapshot_live/latest_local_status.json`

## Credentials

The lane resolves credentials from the ignored local `creds` file:

- `OPENAI_API_KEY`
- `OPENAI_MODEL`
- `OANDA_LIVE_API_KEY`
- `OANDA_ACCOUNT_LIVE_GPT_TECHNICAL`, `OANDA_ACCOUNT_LIVE_GPT_TECH`,
  `OANDA_ACCOUNT_ID_GPT_TECH_LIVE`,
  `OANDA_ACCOUNT_ID_GPT_TECHNICAL_LIVE`, `OANDA_ACCOUNT_LIVE_TECH`,
  `OANDA_ACCOUNT_ID_TECH_LIVE`, `OANDA_ACCOUNT_ID_LIVE_TECH`, or
  `OANDA_ACCOUNT_LIVE_MAIN`

The local creds file currently includes `OANDA_ACCOUNT_LIVE_TECH`, so this lane
prefers the dedicated live technical account over `OANDA_ACCOUNT_LIVE_MAIN`.
Do not run this manager at the same time as another live manager on the same
account unless that is intentional.

## Safety

- The wrapper forces `OANDA_ENV=live`.
- OpenAI web search is disabled; GPT receives broker prices, candles,
  indicators, open trades, and potential-position templates.
- Local event scout orders are disabled; entries come from GPT decisions only.
- New live technical opens require stop-loss and take-profit geometry.
- GPT can only open the exact instrument and direction supplied in the current
  technical potential-position snapshot.
- Existing stop, spread, margin, duplicate-action, and open-trade recheck
  guardrails are inherited from `oanda_advisor_account_manager_auto.py`.
- The wrapper acquires
  `data/forex_gpt_manager/account_gpt_technical_snapshot_live/account_process.lock`
  for real runs so duplicate snapshot managers cannot act on the account.
- Live broker writes require `FOREX_ALLOW_LIVE=True` and either
  `--i-understand-live-risk` or `FOREX_LIVE_CONFIRM`.
- If `oanda_gpt_prod_live_account_manager.py` is already running on
  `OANDA_ACCOUNT_LIVE_MAIN`, this wrapper blocks live execution by default
  only when the snapshot lane resolves to that same account.
- If `oanda_tech_prod_live_account_manager.py` is already running on
  `OANDA_ACCOUNT_LIVE_TECH`, this wrapper blocks live execution by default
  when the snapshot lane resolves to that same account.
  Set `FOREX_TECH_LIVE_ALLOW_SHARED_ACCOUNT=True` only if you intentionally want
  both managers acting on the same account.
- After a tech-account handoff, the snapshot lane defaults to manage-only while
  existing exposure is still high. It may tighten, partial-close, close, or
  otherwise manage supplied open trades, but fresh `OPEN`/`SCALE_IN` orders are
  converted to `WATCH` until the handoff limits clear.

Accepted confirmation values:

```text
FOREX_LIVE_CONFIRM = "GPT_TECH_SNAPSHOT_LIVE_REAL_MONEY"
```

For compatibility with the existing live GPT wrapper, this value is also
accepted:

```text
FOREX_LIVE_CONFIRM = "GPT_PROD_LIVE_REAL_MONEY"
```

## Commands

Print resolved config without revealing API keys:

```powershell
.\data\oanda_training_manager\.research_py313\Scripts\python.exe .\oanda_gpt_technical_snapshot_account_manager.py --print-config --once
```

Read the live account and write one monitor row without placing orders:

```powershell
.\data\oanda_training_manager\.research_py313\Scripts\python.exe .\oanda_gpt_technical_snapshot_account_manager.py --monitor-now --dry-run --once
```

Run one GPT technical scan without broker execution:

```powershell
.\data\oanda_training_manager\.research_py313\Scripts\python.exe .\oanda_gpt_technical_snapshot_account_manager.py --scan-now --dry-run --once
```

Write a local status report without calling OANDA or OpenAI:

```powershell
.\data\oanda_training_manager\.research_py313\Scripts\python.exe .\oanda_gpt_technical_snapshot_live_status.py
```

Run the safe starter in status-only mode:

```powershell
.\start_gpt_technical_snapshot_live.ps1 -StatusOnly
```

Start the lane only if the resolved live account is not already locked by
another live manager:

```powershell
.\start_gpt_technical_snapshot_live.ps1 -Execute
```

Arm a 10-hour handoff that waits for the resolved live account lock to clear,
then starts the lane:

```powershell
.\start_gpt_technical_snapshot_live.ps1 -Execute -WaitForReady -DurationHours 10 -IntervalSeconds 120
```

Run one live broker-executing GPT technical scan:

```powershell
.\data\oanda_training_manager\.research_py313\Scripts\python.exe .\oanda_gpt_technical_snapshot_account_manager.py --scan-now --execute --i-understand-live-risk --once
```

Run continuously on the live account:

```powershell
.\data\oanda_training_manager\.research_py313\Scripts\python.exe .\oanda_gpt_technical_snapshot_account_manager.py --execute --i-understand-live-risk
```

Optional snapshot tuning can be set in env or creds:

- `FOREX_TECH_SNAPSHOT_CANDLE_GRANULARITY`, default `M15`
- `FOREX_TECH_SNAPSHOT_CANDLE_COUNT`, default `160`
- `FOREX_TECH_SNAPSHOT_MAX_CANDIDATES`, default `50`
- `FOREX_TECH_LIVE_MIN_SCORE`, default `62`
- `FOREX_TECH_LIVE_MIN_DIRECTION_EDGE`, default `6`

Potential positions are tagged `OPEN_ELIGIBLE` only when score, spread,
spread-to-stop, and directional-edge gates pass. Candidates that fail these
gates remain visible to GPT as `WATCH_ONLY` with zero executable risk.

Optional cadence overrides:

- `FOREX_TECH_LIVE_SCAN_INTERVAL_MINUTES`, default `30`
- `FOREX_TECH_LIVE_SCAN_MINUTE_OFFSET`, default `15`
- `FOREX_TECH_LIVE_MIN_MINUTES_BETWEEN_GPT_SCANS`, default `30`
- `FOREX_TECH_LIVE_LOCAL_MONITOR_INTERVAL_MINUTES`, default `5`
- `FOREX_TECH_LIVE_LOOP_SLEEP_SECONDS`, default `30`
- `FOREX_TECH_LIVE_SCHEDULE_WINDOW_MINUTES`, default `5`

Optional live risk overrides:

- `FOREX_TECH_LIVE_HARD_MAX_RISK_PCT_PER_TRADE`, default `1.0`
- `FOREX_TECH_LIVE_HARD_MAX_TOTAL_NEW_RISK_PCT_PER_SCAN`, default `2.0`
- `FOREX_TECH_LIVE_MAX_RISK_PCT_PER_TRADE`
- `FOREX_TECH_LIVE_MAX_TOTAL_NEW_RISK_PCT_PER_SCAN`
- `FOREX_TECH_LIVE_MAX_OPEN_TRADES`
- `FOREX_TECH_LIVE_MAX_NEW_TRADES_PER_SCAN`
- `FOREX_TECH_LIVE_TARGET_MARGIN_USED_PCT`
- `FOREX_TECH_LIVE_MAX_MARGIN_USED_PCT`

Optional handoff manage-only controls:

- `FOREX_TECH_LIVE_HANDOFF_MANAGE_ONLY_ENABLED`, default `True`
- `FOREX_TECH_LIVE_HANDOFF_FRESH_OPEN_MAX_OPEN_TRADES`, default `3`
- `FOREX_TECH_LIVE_HANDOFF_FRESH_OPEN_MAX_MARGIN_USED_PCT`, default is the
  lane target margin percentage, currently capped conservatively around `45`

Optional live profit guard controls:

- `FOREX_TECH_LIVE_PROFIT_GUARD_ENABLED`, default `True`
- `FOREX_TECH_LIVE_PARTIAL_PROFIT_PIPS`, default `5`
- `FOREX_TECH_LIVE_FULL_PROFIT_PIPS`, default `12`
- `FOREX_TECH_LIVE_MAX_LOSS_PIPS`, default `9`
- `FOREX_TECH_LIVE_PROFIT_PARTIAL_CLOSE_PCT`, default `50`
- `FOREX_TECH_LIVE_TIGHTEN_PROFIT_PIPS`, default `3`
