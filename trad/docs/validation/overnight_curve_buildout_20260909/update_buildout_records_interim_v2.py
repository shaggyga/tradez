"""Retain the earlier report and consolidate dated implementation evidence."""
from pathlib import Path
import hashlib
import json
import time

BASE = Path(__file__).resolve().parent
PROJECT = BASE.parent / 'trad'
BACKUP = BASE / 'documentation_history_1025_20260909'

REPORT = '''# September 9 curve and position-management buildout

Interim consolidation at approximately 10:25 UTC / 06:25 Eastern. Work continues in this task until 13:00 UTC / 09:00 Eastern. Final runtime verification, the last retained outcome evaluations and vault publication are pending. Dashboard appearance is deferred. Research is running with orders, account authorization and promotion disabled.

## Result so far

The project has a working research path from the existing second-ridge engine through its 13 native curve horizons to explicit USD management candidates, observed paper episodes and independently verified outcomes. It also has separately registered risk distributions, causal feature repairs and a live ledger-backed forecast view. These are substantive engineering improvements. They have not established a profitable predictor or position policy.

The larger completed samples remain unfavorable. In the 08:33–08:35 UTC joint-v3 reassessment, 2,182 completed original outcomes had 48.53% correct direction and a mean -4.7681 net bps after spread. On exactly the same outcomes, price-only had 50.37% direction and -4.8305 bps; neutral-news had 52.84% and -4.5698 bps. Combined news/price was therefore not consistently better. Its MAE of 4.0317 bps also exceeded zero-change MAE of 3.7961 bps, and Brier score 0.2521 exceeded the fair-coin baseline of 0.25. These overlapping observations are not independent trials.

The recovered second-ridge pilot's 09:41–09:53 UTC evaluation verified 1,154 original curve chains, 747 retained captures and all 156 pair/horizon/view/convention cells with zero evidence issues. There were 24,732 scored node views; that is not an independent sample count. Pending and unavailable targets remain explicit. Only four of the 39 cells in each view/convention had positive mean net movement. For example, native official-M EUR/USD H1 had 166 outcomes, 37.35% direction and -2.4287 net bps. Its earlier favorable 71-outcome snapshot is retained as dated history, not presented as the current result. More outcomes are still maturing.

## Three completed observed management episodes

All three predeclared GBP/USD hourly episodes completed at their original targets. The independent verifier checked 178 scheduled steps, 2,727 retained files and the frozen 20-source closure, found zero issues or missing steps, and confirmed all five arms were flat. Each arm is an isolated hypothetical $2,500 notional scenario reset per episode; these are not positions in the user's account.

| Separate arm | Gross midpoint movement, USD | Spread cost, USD | Assumed slippage, USD | Net virtual USD | Round trips |
| --- | ---: | ---: | ---: | ---: | ---: |
| Hold original curve | 3.159030 | 0.949320 | 0.14990121470 | 2.05980878530 | 3 |
| No trade | 0 | 0 | 0 | 0 | 0 |
| USD curve manager | -1.630180 | 3.493840 | 0.54980004780 | -5.67382004780 | 11 |
| USD momentum manager | 3.733610 | 16.107080 | 2.44909228100 | -14.82256228100 | 49 |
| Legacy momentum reference | 2.258465 | 26.949565 | 4.09855263215 | -28.78965263215 | 82 |

The curve manager lost less than matched momentum in every episode but lost in all three and underperformed unchanged curve holding overall. Its $9.14874223320 relative advantage comprises $14.51253223320 lower costs and $5.363790 worse gross movement. Lower turnover explains the relative result; it does not establish directional skill. These are three consecutive episodes on one pair and date, with unknown independent sample size.

Costs reconcile exactly from original entry/exit bid and ask, integer units and the registered 0.1-bps hypothetical slippage per leg. Liquidation marks are not counted as trades. Financing, commissions, market impact and actual broker fills are not modeled. The all-episode figure retains the distinction between the original official-M candle reference and later available bid/ask midpoint quotes; its horizontal terminal estimate is an endpoint annotation, not an invented predicted path.

Episode two exposed a specific semantic gap: after price fell below the unchanged original terminal estimate, the manager treated the distance back to that estimate as a new long opportunity. It was still the old forecast. A new, inactive direction-admission companion rejects an entry whose rebased direction contradicts its original issued direction, while retaining a separate signed continuation value for an existing position. It passed 40 tests and classified all 59 original episode-two contexts without inventing alternative fills or PnL. This is a necessary consistency check, not a tested replacement policy.

## Curves, risk and the next management connection

The new interfaces preserve reference bar labels, effective price times, observation availability, issue, publication, independent consumption and original target clocks separately. Missing candles are not filled and expired targets are not moved. USD candidate values use explicit units, conversions and costs; native pip counts are not used as cross-pair dollar rankings.

A separate M1 risk study started at 08:14 UTC. It publishes both fixed empirical and volatility-scaled distributions for signed terminal movement, absolute movement, range, variation and long/short favorable/adverse excursions at 15, 30 and 60 minutes. There are 72 nodes per pair. Its training-only artifacts were retained before prospective scoring. These are uncalibrated full-horizon distributions. OHLC envelopes do not establish barrier order or executable stop fills.

A new pure curve/risk attachment validator replays both original issue/publication/consumption contracts and checks instrument, effective reference price/time/convention, original target window, source identities and actual downstream read clocks. It passed 55 tests and independent review. The actual current S5 curve and M1 risk chains were correctly refused: their reference times/prices and target windows did not match. No approximate clock rounding or fabricated common event was used. Full-horizon marginals are explicitly not conditional remaining-position risk.

The next implementation requires a new common issue grid for curves and risk, fresh forecast updates at position decisions, and a separately registered continuation/exit/rotation policy with matching executable costs. Existing stop, trailing, profit, time, curve-opposition and partial-reduction code remains available for reuse. Currency exposure diagnostics now value net and gross base/quote risk in USD, but they do not invent account margin, covariance estimates or an active multi-pair allocator.

## Existing wide features and news

The inventory already includes 227/220-field models, a 795-input intrahour engine, a 643-field moving-average grid, second-ridge curves and blurb/factor datasets. The current narrow H1 study is a separate path, not the whole project.

The MA audit found two future-data dependencies: a fallback scale calculated from the full series and a no-cross age based on final series length. A separate causal helper fixes both and unifies live and batch calculations. In real retained archives, 40,754 of 147,715 eligible rows changed. Appending future data changed 13 of 36 inspected original rows and none of the repaired rows. This establishes feature correctness without attributing prior trading losses.

The predeclared historical comparison did not support activating wider models: corrected MA643 and combined667 had worse final-test MAE than compact24 and zero change in all nine pair/horizon cells; compact24 also lost to zero change. Validation choices were retained before the final test, which contained nine dates, below the fixed ten-date threshold for block intervals. The failed wider models were not tuned against that final test or activated.

Separate companions describe true 1/15/60-minute currency movement, entry-time news context versus vetted direction, causally admissible response memory and currency exposure. They remain outside the live predictor and position rules. The inspected news sample contained four recent context members and zero vetted forward topics. Historical factor/response samples lacked independent observation and response-known clocks needed for causal memory; those clocks were not invented. Missing evidence is distinct from neutral sentiment.

## Forecast visibility and runtime reliability

The ten-minute exact-byte transport probe reproduced all 45 unavailable observations as a mutable producer-summary alias: later completion changed a shared dictionary, causing the heartbeat to name bytes that were never published. A new immutable publication boundary freezes bytes and requires actual matching readback before heartbeat publication. Historical producer sources remain frozen for their original cohort.

The running read-only ledger endpoint and table now verify original committed forecasts and publication/consumption receipts directly, independently of that producer envelope. The 09:40 live check served 65 current original forecasts and 67 verified ledgers across 68 pairs. The table preserves target expiry, quote/receipt age and original identity; a stalled main request cannot leave an old forecast displayed as current indefinitely. Old producer diagnostics remain visible separately. This is a functional availability repair, not a claim of input readiness or prediction success.

A subsequent 20-minute live endpoint probe validated 77 polls covering 66 distinct reports, with no HTTP/projection errors and every pair visited. It found 40 per-pair failures in 26 reports; 39 were followed by a later verified forecast, while the final failure lacked a later observation. Each report used only 0.547–2.828 seconds of its eight-second overall allowance. A separately versioned bounded retry candidate passed 105 tests and independent review. Its actual-ledger evaluation and runtime binding are in progress; no improvement from that candidate is claimed yet.

The two obsolete joint-v1/v2 workers were retired after independently reconciling all 4,050 original forecasts to outcomes or explicit exclusions, with no future or unresolved obligations. Their ledgers and registered sources were retained. The main joint-v3 study, pair-local comparison workers, quote/news collection and original source closures remained intact. The 09:40 operational audit found all 15 expected main workers, plus then-running paper/pilot/risk research. Paper completed naturally afterward. The first reload attempt encountered an exiting-wrapper identity race; its failure and separately reviewed successful recovery are both retained.

Five historical news-clock errors and 13 original producer errors remained in the observed counters. A ten-minute passive probe saw no recurrence; a longer bounded read-only probe continues until 12:45 UTC. The original failing news inputs were not retained, so recovered current news does not establish their cause. News-capture capacity remains a monitored gap; no evidence deletion was added.

## Efficiency and verification

The optimized offline curve evaluator preserved the original scorer and exact retained evidence. On the same 198-curve comparison, the first optimization reduced elapsed time from 263.61 to 102.37 seconds; a further version took 91.08 seconds. A wider test substantially improved repeated rolling-selection work without materially changing total runtime, so that component speedup is not presented as a whole-system claim. The full latest evaluation completed in 686.02 seconds on a larger, different inventory. Retiring two obsolete workers removed those CPU consumers; before/after process samples are observational, not a controlled overall-throughput benchmark.

Lossless shared news storage used approximately 60.8% fewer bytes on three retained captures but read slower than whole-file gzip. It remains inactive.

Focused source-bound suites, independent reviews, real-input checks, adverse clock/identity tests and original-target verifiers accompany the changes. Named test suites overlap and are not added into an invented independent-test total. Frozen cohorts are not rewritten to make successors pass. Raw runtime ledgers, news, credentials and fitted recovery artifacts remain machine-local dependencies.

## Remaining work and evidence

- Finish and verify the bounded retry endpoint change; retain original failure reasons and actual attempt clocks.
- Complete the final retained curve/risk evaluations and runtime/news checks, with pending or missing outcomes explicitly counted.
- Publish the curated credential-free records, source snapshot and a verified evidence index to the vault.
- Build a fresh common-grid curve/risk cohort before any management attachment, then compare a fresh-update policy with fixed hold, no-trade and the existing manager under identical costs.
- Gather independent sessions for after-cost, magnitude and calibration acceptance. Broader point-in-time feature/news and co-movement integration still requires new matched prospective comparisons.

Local evidence is at `C:/Users/zmoor/Documents/forex/overnight_curve_buildout_20260909`. This interim report is not the final morning runtime receipt or a declaration of trading readiness.
'''

PENDING = '''## In progress — September 9 curve and management buildout

Work continues in this task until 09:00 Eastern (13:00 UTC). The [consolidated buildout report](docs/FOREX_OVERNIGHT_CURVE_BUILDOUT_20260909.md) records implementation, original-outcome measurements and remaining gaps as of approximately 10:25 UTC. Dashboard appearance is deferred; orders, account authorization and promotion remain disabled.

Completed engineering: recovered 13-horizon second-ridge research curves; separate issue/publication/consumption and original-target contracts; USD management candidates and exact quote-cost attribution; three independently verified five-arm paper episodes; causal MA repair; entry-news, cross-window, currency-exposure and direction-admission companions; exact-event curve/risk attachment validation; prospective M1 risk distributions; an active ledger-backed forecast view; and retirement of two obsolete joint workers after all 4,050 original obligations were resolved. Existing wide models and blurb/factor datasets remain mapped for reuse.

Measured limits: the 2,182-outcome current joint study remained negative after spread and did not consistently improve on matched price-only/neutral-news baselines. The larger second-ridge evaluation also remained mostly negative. The curve manager lost $5.67 over three isolated $2,500 GBP/USD paper episodes, versus $14.82 for matched momentum and a $2.06 gain for fixed hold. Its relative advantage came from lower costs, not better gross direction. Corrected MA643/combined667 failed all nine final-test comparisons against compact24 and zero change. These results do not support predictive promotion.

Priority queue:

1. Finish reviewed bounded per-pair retry deployment and live verification; preserve original producer errors separately.
2. Finish mature curve/risk outcomes, frozen-source checks, runtime verification and credential-free source/evidence publication by the morning handoff.
3. Register a new shared curve/risk issue schedule and fresh-update management comparison. Current S5/M1 clocks and windows cannot be silently joined; full-horizon risk is not conditional remaining risk.
4. Test entry/continuation/rotation and existing position controls against hold/no-trade with identical executable costs. The static-terminal rebasing defect now has an inactive admission guard, not an accepted new trading policy.
5. Extend causal news/blurb/response memory and co-movement inputs only with real point-in-time evidence and new matched comparisons. Collect independent sessions for magnitude, after-cost and calibration acceptance.
6. Investigate recurrent news-clock failures only when actual failing evidence is captured; monitor finite news/history capacity. Do not delete evidence or treat context-only news as directional confidence.

The earlier September 9 interim report/pending text is retained in the overnight evidence history. September 8 and older entries below retain their original dated claims.

'''


def main():
    paths = [PROJECT/'docs/FOREX_OVERNIGHT_CURVE_BUILDOUT_20260909.md', PROJECT/'FOREX_PENDING_IMPROVEMENTS.md']
    original = [p.read_bytes() for p in paths]
    pending_text = original[1].decode('utf-8-sig')
    marker = '## Current — September 8 repaired-news research deployed; predictive acceptance pending'
    assert pending_text.count(marker) == 1
    BACKUP.mkdir(exist_ok=False)
    entries = []
    for p,raw in zip(paths,original):
        out=BACKUP/p.name
        with out.open('xb') as h:h.write(raw)
        entries.append(dict(path=str(p),retained_path=str(out),sha256=hashlib.sha256(raw).hexdigest(),bytes=len(raw)))
    payloads = [REPORT, '# Forex pending improvements\n\n'+PENDING+pending_text[pending_text.index(marker):]]
    for p,before,text in zip(paths,original,payloads):
        assert p.read_bytes()==before
        p.write_text(text,encoding='utf-8',newline='\n')
    receipt=dict(schema_version='overnight_interim_documentation_consolidation_v2',actual_epoch=time.time(),before=entries,
                 after=[dict(path=str(p),sha256=hashlib.sha256(p.read_bytes()).hexdigest(),bytes=p.stat().st_size) for p in paths],
                 vault_published=False,runtime_changed=False)
    target=BACKUP/'DOCUMENTATION_CONSOLIDATION_RECEIPT_20260909.json'
    target.write_text(json.dumps(receipt,indent=2)+'\n',encoding='utf-8')
    print(json.dumps(dict(path=str(target),sha256=hashlib.sha256(target.read_bytes()).hexdigest())))


if __name__=='__main__':main()
