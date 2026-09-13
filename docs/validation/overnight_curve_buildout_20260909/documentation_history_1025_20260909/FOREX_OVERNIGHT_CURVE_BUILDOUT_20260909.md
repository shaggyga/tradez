# September 9 curve and position-management buildout

Interim record at 08:28 UTC / 04:28 Eastern. Work continues in the current task until 13:00 UTC / 09:00 Eastern. Final verification, accumulated outcomes and the portable evidence index are pending. Dashboard appearance is deferred; backend forecast availability is being repaired. No order, promotion or account authority is enabled.

## What changed

The existing second-ridge engine now has a separately registered research path from real S5 inputs to its 13 native forecast horizons, immutable publication, independent consumption, original-target outcomes and USD-valued management candidates. Reference bar labels, effective price times, observed availability, issue, publication and consumption remain distinct. Missing candles are not filled; expired targets are not moved.

Five isolated virtual management arms are running against the same observed GBP/USD quotes and original hourly curve: the unchanged legacy momentum reference, USD momentum manager, USD curve manager, no-trade and curve hold without rotation. Each has a hypothetical $2,500 notional limit. They are separate scenarios, not five positions in the user's account. These experiments use actual later bid/ask observations after durable decisions, integer units and explicit assumed slippage. No broker fills occur.

A new prospective M1 risk study started at 08:14 UTC, with its first reference at 08:16. It issues both fixed empirical and volatility-scaled distributions for signed terminal movement, absolute movement, range, variation and long/short favorable/adverse excursions at 15, 30 and 60 minutes. Every pair has 72 nodes; all three first-cycle chains were issued, durably published and independently consumed. The first offline audit verified all 216 nodes with zero issues; all outcomes were still pending at that audit. These are uncalibrated distributions, not demonstrated confidence levels or a sizing policy.

## Recovered features: implementation is not predictive acceptance

The project already had 227/220-field models, a 795-input intrahour engine, a 643-field moving-average grid, second-ridge curves, blurb/factor datasets and position controls. The buildout preserves those distinctions; it does not call the current narrow H1 vector the entire project.

The MA audit found two future-data dependencies: a fallback scale calculated over the full series and a no-cross age that used the final series length. The separate causal helper fixes both and unifies live and batch calculations. In retained real archives, 40,754 of 147,715 eligible feature rows changed. Appending future data changed 13 of 36 inspected original rows and none of the repaired rows. This establishes feature correctness, not the cause of prior losses.

The predeclared historical comparison was unfavorable to the wider models. Corrected MA643 and combined667 both had worse final-test mean absolute error than compact24 and zero change in all nine pair/horizon cells. Compact24 also lost to zero change. All cells had nine test dates, below the predeclared ten-date requirement for block intervals. These models were not activated or tuned against that same final test.

Separate companions now describe true 1/15/60-minute currency movement, entry-time news context versus vetted direction, causally admissible response memory, and USD-valued net/gross currency exposure. They remain outside the live model and sizing rules. The inspected news sample had four recent context members and no vetted forward topic. Historical factor/response samples lacked the independent observation and response-known clocks required for causal memory; those clocks were not invented.

## First completed management episode

The first hourly episode completed at its original 07:43:50 UTC target. The independent verifier checked all 60 scheduled steps, found no missing slots or inconsistencies, and confirmed all five arms were flat. Exact retained quote-based attribution gives:

| Separate arm | Gross price movement, USD | Spread cost, USD | Assumed slippage, USD | Realized USD | Round trips |
| --- | ---: | ---: | ---: | ---: | ---: |
| Legacy momentum | -0.175285 | 9.941775 | 1.54944770285 | -11.66650770285 | 31 |
| USD momentum | 0.884415 | 4.791135 | 0.74970533365 | -4.65642533365 | 15 |
| USD curve | -0.958880 | 1.584810 | 0.24992200590 | -2.79361200590 | 5 |
| Hold original curve | -0.746415 | 0.304095 | 0.04998446375 | -1.10049446375 | 1 |
| No trade | 0 | 0 | 0 | 0 | 0 |

The USD curve arm lost less than the momentum arms, but more than hold or no-trade. The older policy's 31 round trips were particularly costly. The USD momentum arm's positive gross movement was insufficient to cover costs. This supports further investigation of turnover and entry/exit value; one overlapping hourly episode does not establish a profitable replacement. No policy was selected or altered from this result.

Cost attribution uses original entry/exit bid and ask, quote identities, clocks and integer units. Signed midpoint movement minus both half-spreads and both registered 0.1-bps slippage charges reconciles realized USD exactly. Hypothetical liquidation marks are not counted as executed action legs. Financing, commissions and market impact are not modeled.

## Publication reliability

The ten-minute exact-byte transport probe retained 555 coherent envelopes and 45 unavailable observations. All 45 failures were reproduced by the same producer defect: the saved summary retained a mutable `last_attempt` dictionary; later completion changed that dictionary, causing the heartbeat to hash bytes that were never published. Increasing a reader cache cannot repair a nonexistent publication.

A new immutable publication boundary freezes exact bytes and requires a matching actual readback before heartbeat fields can be derived. The historical producer remains frozen for its existing cohort. A separately reviewed read-only observer now verifies original committed ledger contracts, activations, forecasts and publication/consumption receipts directly. Its 08:13 UTC pass verified all 68 ledgers and found 66 current H1 forecast pairs, with TRY/JPY and USD/TRY lacking publication. Original producer transport diagnostics remain separate. A backend API integration is being implemented; the reader does not claim current input readiness, predictive success or order authority.

## Efficiency and validation

Lossless shared news storage used about 60.8% fewer bytes on three retained captures, but read slower than whole-file gzip. It remains inactive. This was a storage saving, not a speed improvement.

The first optimized offline curve evaluator reproduced the same 198-curve retained comparison and reduced evaluation time from 263.61 to 102.37 seconds. A larger live inventory exposed nonlinear work from overlapping captures and repeated decoding. A further exact-result optimization is being investigated; the original evaluation continues, and no scorer or cohort is rewritten.

Relevant focused checks include: 184 combined M1 capture/contract/outcome/worker tests; 266 observed-management preactivation tests; 21 quote-cost attribution tests; 35 causal-MA tests; 47 cross-window tests; 121 entry-news tests; 66 currency-exposure tests; and 37 ledger-observer tests with one Windows symlink-privilege skip. Counts refer to their named suites and overlap with earlier component runs; they are not added into an invented independent-test total. Independent source reviews and retained real-input/evidence checks accompany the components.

## Still required

- Complete the remaining two fixed paper episodes and retain their original-target verification and cost decomposition.
- Score accumulated prospective curves and M1 risk distributions, keeping pending, missing and failed outcomes explicit.
- Reassess current joint price/news outcomes and predictor dependence on identical original decisions and quotes.
- Finish reviewed backend ledger observation, then verify actual endpoint behavior and main-worker health.
- Verify all frozen source closures, document remaining gaps and publish credential-free records plus a source-only vault snapshot.
- Collect more independent sessions before any calibration, turnover-policy, wider-feature or after-cost acceptance claim.

Local evidence currently resides at `C:/Users/zmoor/Documents/forex/overnight_curve_buildout_20260909`. Raw runtime ledgers, source news, credentials and fitted recovery artifacts remain separate from the vault. The final evidence index will distinguish exported records from machine-local dependencies. Current source-level success does not establish accurate forecasts or trading readiness.
