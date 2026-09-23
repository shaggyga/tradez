# Authority and reuse map — September 21, 2026

This is the WP0 map for the existing Forex project. It maps the design’s
logical components to inspected local components; it does not rename or
replace the live project. The current user authorized offline research and
explicitly deferred GPT/advisor comparisons. Existing services remain
research-only and unchanged.

| Capability | Existing path | Owner/process | Current contract and evidence | Defect or gap | Decision | Change scope | Verification |
| --- | --- | --- | --- | --- | --- | --- | --- |
| Authority and restore | `RECOVERY_CHECKPOINT_20260921` | Vault | Hash-verified source/history checkpoint and isolated restore | OneDrive cloud sync unverified | reuse | Vault handoff only | Restore receipt; checkpoint manifest |
| Long M1 history | `inputs/long_m1_68.zip` | Offline only | 68 Parquet members, 50,321,016 rows; raw identities preserved | Receipt/first-seen timing unavailable | reuse | Read-only staging | ZIP/preflight and footer survey receipts |
| Native M1 history | `inputs/native_m1_68.zip` | Offline only | Separate newer 68-pair CSV vintage | 26,824 stored spread-unit errors; no merge precedence | quarantine | Excluded until derived correction contract | Native source trace |
| Instrument pip map | `trad/config/pair_local_operational_v2_20260913.json` | Shared metadata | Hash-bound 68-instrument sizes | Legacy rotation used JPY-only shortcut | repair | Isolated Stage B only | 11 focused tests passed |
| Labels/outcomes | `oanda_rolling_technical_labels_v1.py` | Offline label service | Exact-minute path labels, bid/ask endpoint proxy, explicit states | Bar-end and receipt time are reconstruction assumptions | reuse | Wrapped by Stage C settlement | Existing label source hash; 48h settlement receipt |
| Replay clock | `stage_c_all68_20260921/all68_global_clock.py` | Offline only | Deterministic instrument/time ordering and full coverage records | Raw timestamp semantics are not observed availability | extend | Isolated Stage C | Four synthetic clock/policy tests |
| Reference accounting | `stage_c_all68_20260921/all68_execution_accounting.py` | Offline only | Side prices, delayed fills, conversion direction and financing fixtures | No historical conversion/financing feed bound yet | extend | Isolated Stage C | Four synthetic accounting tests |
| Forecast/ledger baseline | Stage C neutral artifacts | Offline only | 24h shared no-change tape and two independent no-trade ledgers | No fitted directional all-68 baseline yet | extend | Stage C only | 781,184 forecasts and linked settlement |
| Existing rotation | `oanda_primary_forecast_rotation_bot.py` | Existing live code | P0/P1 predecessor policy source exists | Pip units repaired only in isolated copy; policy needs common offline valuation horizon | wrap | No live import or execution | Stage B regression evidence; source trace |
| Existing broker-style replay | `oanda_broker_style_portfolio_replay.py` | Existing source | Historical predecessor to inspect/reuse | Not yet bound to common Stage C forecast tape | unresolved | Read-only inspection before reuse | Pending policy-contract review |
| Rolling technical features/models | `oanda_rolling_technical_*` modules | Existing research services | Existing causal feature/label and worker family | Must isolate source/data cutoffs for campaign fit | reuse | Adapter in Stage C | Existing selected contract evidence |
| Recovered model evidence | Audit lineage records | Offline evidence | H1 opportunity and EUR/USD movement findings retained | No artifact admitted as all-68 directional model | wrap | Admission boundary | `MODEL_ADMISSION_BOUNDARY.md` |
| Macro/news | Existing official/news collectors | Research-only services | Source and transport evidence retained | Point-in-time macro feature census not yet attached to campaign | unresolved | Separate WP6 | Do not use placeholders as observed inputs |
| Advisor/GPT | No active Stage C adapter | Deferred | User requested hold | No implementation or calls authorized for this branch | quarantine | None | No advisor/API activity |
| Broker/order interface | Existing OANDA code/processes | Active research-only stack | `can_place_orders: false` | Current research health is not broker/trading evidence | quarantine | No start/restart/changes | Process snapshot and runtime config |

## Design decisions

1. The recovery checkpoint and actual source map take precedence over design
   path suggestions. Stage C is a bounded adapter layer, not a parallel live
   system.
2. All historical decisions use raw minute timestamp plus a stated 60-second
   bar-end reconstruction. Original receipt timing remains unknown.
3. Long and native source vintages remain separate. The initial baseline uses
   long history only.
4. The next unblocked work package is WP3: an isolated all-68 baseline with a
   24-hour and eligible multiday target path, explicit feature/training/fit
   cutoffs, outcome maturity, and resumable artifacts.
5. GPT/advisor work is deferred. No project component may call an advisor or
   external API for this campaign.
