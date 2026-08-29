# GPT prod live account setup

This sets up `gpt_prod` for OANDA live without changing the existing practice
account manager.

## Files

- Live wrapper: `oanda_gpt_prod_live_account_manager.py`
- Live logs/data: `data/forex_gpt_manager/account_gpt_prod_live/`
- Existing practice GPT prod remains: `oanda_advisor_account_manager_auto.py`

## Credentials

Put live credentials in your local ignored `creds` file or environment.  Do not
reuse the practice token/account names for live.

Required in `creds`:

```text
OANDA_LIVE_API_KEY = "..."
OANDA_ACCOUNT_LIVE_MAIN = "..."
```

Accepted aliases:

```text
OANDA_API_KEY_LIVE = "..."
OANDA_LIVE_API_TOKEN = "..."
OANDA_API_TOKEN_LIVE = "..."
OANDA_ACCOUNT_ID_GPT_LIVE = "..."
OANDA_LIVE_ACCOUNT_ID_GPT = "..."
OANDA_ACCOUNT_ID_LIVE = "..."
OANDA_LIVE_ACCOUNT_ID = "..."
```

## Safe connectivity checks

Print resolved config without secrets:

```powershell
& '.\..venv\Scripts\python.exe' .\oanda_gpt_prod_live_account_manager.py --print-config --once
```

Read the live account and write one monitor row without placing orders:

```powershell
& '.\..venv\Scripts\python.exe' .\oanda_gpt_prod_live_account_manager.py --monitor-now --once
```

Run a GPT scan against the live account in no-execution mode:

```powershell
& '.\..venv\Scripts\python.exe' .\oanda_gpt_prod_live_account_manager.py --scan-now --once
```

Run one watch-only breaking-news check:

```powershell
& '.\..venv\Scripts\python.exe' .\oanda_gpt_prod_live_account_manager.py --news-watch-now --once
```

## Enabling live orders

Live orders require all three gates:

1. live credentials are present;
2. `FOREX_ALLOW_LIVE=True`; and
3. either pass `--i-understand-live-risk` or set:

```text
FOREX_LIVE_CONFIRM = "GPT_PROD_LIVE_REAL_MONEY"
```

Optional, if you want the wrapper to execute without passing `--execute` each
time:

```text
FOREX_LIVE_EXECUTE = True
```

Example one-shot live scan with execution enabled:

```powershell
& '.\..venv\Scripts\python.exe' .\oanda_gpt_prod_live_account_manager.py --scan-now --once --execute
```

## Default live behavior

The live wrapper mirrors the current `gpt_main` behavior by default:

- same GPT prompt plus a small live-identification note;
- same all-pair GPT universe;
- aggressive live risk/margin caps, while preserving broker/account guardrails;
- same scan schedule and scan-on-launch behavior;
- same duplicate suppression, broker rechecks, stop-loss requirement, spread
  caps, weekend/reopen handling, and `SCALE_IN` restrictions.

The live wrapper now defaults to an aggressive deployment profile. It should
turn the best viable GPT candidate into a small controlled live order instead
of repeatedly staying flat on vague caution. Hard blockers still win: stop-loss,
spread, margin, duplicate exposure, broker recheck, event/news contradiction,
and account failsafe checks remain active.

These live-specific defaults can still be overridden in `creds` without
changing the practice manager:

```text
FOREX_LIVE_WATCH_ACCOUNTABILITY_MIN_CONFIDENCE = 55.0
FOREX_LIVE_MAX_RISK_PCT_PER_TRADE = 3.0
FOREX_LIVE_MAX_TOTAL_NEW_RISK_PCT_PER_SCAN = 10.0
FOREX_LIVE_MIN_RISK_PCT = 0.25
FOREX_LIVE_MAX_OPEN_TRADES = 12
FOREX_LIVE_MAX_NEW_TRADES_PER_SCAN = 5
FOREX_LIVE_TARGET_MARGIN_USED_PCT = 75.0
FOREX_LIVE_MAX_MARGIN_USED_PCT = 90.0
FOREX_LIVE_MISSED_ENTRY_FALLBACK_OPEN_ENABLED = True
```

## Breaking-news watch

The live wrapper checks the web for major new FX catalysts every five minutes
while FX is open and every fifteen minutes while it is closed. News can only
create a timed watch. It cannot submit an order. While a watch is active, the
wrapper checks a limited set of affected pairs on M1 every minute. A confirmed
move triggers the full GPT portfolio review; execution still requires matching
technical direction, explicit technical confirmation, a stop, expected R of at
least 1.0, and all normal broker/account checks.

Defaults:

```text
FOREX_LIVE_NEWS_WATCH_ENABLED = True
FOREX_LIVE_NEWS_CHECK_INTERVAL_SECONDS = 300
FOREX_LIVE_NEWS_CLOSED_CHECK_INTERVAL_SECONDS = 900
FOREX_LIVE_NEWS_LOOKBACK_MINUTES = 20
FOREX_LIVE_NEWS_MIN_SEVERITY = 70
FOREX_LIVE_NEWS_REQUIRE_VERIFIED_CITATION = True
FOREX_LIVE_NEWS_REQUIRE_CORROBORATION = True
FOREX_LIVE_NEWS_TECH_SCAN_INTERVAL_SECONDS = 60
FOREX_LIVE_NEWS_MAX_ACTIVE_WATCHES = 30
FOREX_LIVE_NEWS_MAX_WATCHED_PAIRS = 18
FOREX_LIVE_NEWS_MIN_EXPECTED_R = 1.0
```

Watch records and source timing are written under the live data directory:

```text
latest_news_watch.json
news_watch_ledger.csv
news_watch_ledger.jsonl
latest_news_technical_watch.json
```

## Live profit guard

The live wrapper has a local profit guard that runs during monitor passes and
before GPT scans. It can tighten stops, take partial profit, fully close a
winner, or close a loser before a wider stop is reached. Defaults are
intentionally conservative:

```text
FOREX_LIVE_PROFIT_GUARD_ENABLED = True
FOREX_LIVE_PROFIT_GUARD_TIGHTEN_PIPS = 3.0
FOREX_LIVE_PROFIT_GUARD_PARTIAL_PIPS = 5.0
FOREX_LIVE_PROFIT_GUARD_FULL_CLOSE_PIPS = 12.0
FOREX_LIVE_PROFIT_GUARD_MAX_LOSS_PIPS = 9.0
FOREX_LIVE_PROFIT_GUARD_MIN_PROFIT_USD = 0.03
FOREX_LIVE_PROFIT_GUARD_PARTIAL_CLOSE_PCT = 50.0
FOREX_LIVE_PROFIT_GUARD_LOCK_RATIO = 0.35
FOREX_LIVE_PROFIT_GUARD_MIN_LOCK_PIPS = 1.0
```

## Always-on live process

For always-on live operation, start it hidden with stdout/stderr redirected to
`data/runtime_logs/`.

The wrapper follows the same scan-on-launch behavior as `gpt_main`.  Add
`--no-scan-on-launch` if you want to start the process without an immediate GPT
scan.

## Important behavior

- Breaking-news discovery and watched-pair movement scanning are enabled.
  Direct event/scout local order trading remains disabled; only the full GPT
  decision path can request an order after technical confirmation.
- Existing open live trades are still monitored and can be managed by GPT if
  execution is enabled.
- The local profit guard runs during live monitor passes and before GPT scans
  to protect winners and cut losers.
- `SCALE_IN` is allowed only for an already profitable same-direction winner
  with a protective stop.
- Strategy behavior mirrors `gpt_main`, but live deployment is intentionally
  more aggressive about converting viable WATCH candidates into reduced-risk
  orders.
- Missed-entry fallback opens are enabled by default. The local missed-entry
  audit may promote a high-confidence GPT WATCH candidate into a reduced-risk
  live OPEN after retry exhaustion when broker/account checks pass.
