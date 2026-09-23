"""Update the five owned narrative files; retain exact draft bytes and diffs."""
import datetime
import difflib
import hashlib
import json
from pathlib import Path

BASE = Path(__file__).resolve().parent
ROOT = BASE.parent / 'trad'
HISTORY = BASE / 'documentation_before_final_001'
OUT = BASE / 'documentation_final_draft_001'
P = 'validation/overnight_curve_buildout_20260909/'

REPORT = '''# September 9 curve and position-management buildout

Draft finalization at 12:10 UTC / 08:10 Eastern. This report distinguishes completed engineering and dated observations from the final main-study, pilot, risk and runtime checks still due before the 13:00 UTC handoff. Research orders, account authorization and promotion remain disabled. Dashboard appearance is deferred.

## Outcome and scope

The existing second-ridge engine now has an independently verifiable research path through its 13 native horizons, USD management candidates, observed paper episodes and original-target evaluation. Separate risk distributions, causal feature repairs, existing-manager correctness fixes and a ledger-backed forecast view have been added. The measured main forecast and active paper-management results remain negative after costs; engineering verification is not predictive acceptance.

The four frozen research closures remain distinct: joint-v3 (20 sources), second-curve pilot (14), observed paper management (20), and M1 risk distributions (8). Sources overlap between closures. The later inactive manager/channel/chain helpers do not retroactively change those studies or their outcomes.

## Dated prediction results

The 08:33–08:35 UTC joint-v3 reassessment contains 2,182 completed original outcomes: 48.53% correct direction and -4.7681 mean net bps after spread. On exactly the same decisions/endpoints, the retained matched price-only comparator has 50.37% direction and -4.8305 bps; neutral-news ablation has 52.84% and -4.5698 bps. These are pre-issue diagnostics on joint decisions, not independent price-v2 schedules. Combined price/news is not consistently better. Joint MAE 4.0317 bps exceeds zero-change MAE 3.7961 bps. The final main-study snapshot is still pending.

The fixed-bin probability diagnostic includes all 2,182 completed rows, all 68 pairs and all ten deciles, including empty bins and abstentions. Thirteen exact-flat outcomes remain label zero under the original up-versus-not-up event. Original Brier reconciles exactly to 0.2521189983, worse than the fixed 0.25 comparator. The 0.5–0.6 bin has 1,139 observations, mean issued probability 0.5270 and observed up fraction 0.4644. This is descriptive evidence of uncalibrated probabilities, not fitted recalibration or an independent-trial estimate. The source-bound suite passed 68 tests. [Probability evidence](validation/overnight_curve_buildout_20260909/joint_v3_reassessment_v1/calibration_v1/actual_calibration_002/JOINT_PROBABILITY_BINS_20260909.json).

The recovered pilot evaluation at 11:25:40–11:38:38 UTC verified 1,154 original curve chains and 1,058 retained sources, with one separately retained HTTP 504 source failure and zero evidence issues. All 156 pair/horizon/view/convention cells are presented: 27,964 scored node views, 1,166 pending, 620 provider-missing, 250 insufficient-coverage and four withheld. These overlapping counts are not independent N. For example, native official-midpoint EUR/USD H1 has 179 outcomes, 34.64% direction and -2.7780 mean net bps; other cells differ and remain visible. Retained-training-target and nominal-exact views keep distinct event conventions. The final pilot pass is still pending. [Complete current pilot table](validation/overnight_curve_buildout_20260909/prospective_pilot/complete_results_v3_002/COMPLETE_PILOT_HORIZON_RESULTS_20260909.md).

## Three completed paper-management episodes

All three predeclared GBP/USD hourly episodes reached their original targets. Independent verification checked 178 scheduled steps, 2,727 files and the frozen 20-source closure with zero issues or missing steps; all five arms ended flat. Each arm is an isolated hypothetical $2,500 scenario reset per episode, not a user-account position.

| Separate arm | Gross midpoint USD | Spread USD | Assumed slippage USD | Net virtual USD | Round trips |
| --- | ---: | ---: | ---: | ---: | ---: |
| Hold original curve | 3.159030 | 0.949320 | 0.14990121470 | 2.05980878530 | 3 |
| No trade | 0 | 0 | 0 | 0 | 0 |
| USD curve manager | -1.630180 | 3.493840 | 0.54980004780 | -5.67382004780 | 11 |
| USD momentum manager | 3.733610 | 16.107080 | 2.44909228100 | -14.82256228100 | 49 |
| Legacy momentum reference | 2.258465 | 26.949565 | 4.09855263215 | -28.78965263215 | 82 |

The curve manager lost in all three episodes and underperformed fixed holding. Its $9.14874223320 advantage over matched momentum came from $14.51253223320 lower costs despite $5.363790 worse gross movement. This does not establish directional skill. Costs reconcile from original bid/ask endpoints, integer units and registered hypothetical 0.1-bps slippage per leg. Financing, commissions, impact and real fills are absent; liquidation marks are not trades. [Final paper acceptance](validation/overnight_curve_buildout_20260909/observed_management/OBSERVED_THREE_EPISODE_FINAL_ACCEPTANCE_20260909.json).

Episode two showed that rebasing an unchanged old terminal estimate could reverse its apparent entry direction. An inactive admission guard preserves the original issued direction, while a separate continuation channel retains the signed value for an incumbent. It does not invent guarded-policy fills or PnL.

## Existing managers and the new inactive connection

The advisor and technical account managers already contain entry, hold, exit, reduce, rotation and protective controls. Their separately reviewed reconciliation correction now distinguishes observed-open, observed-absent and unknown state; uncertain lookups do not authorize a flip, confirmed reductions retain actual remaining units, and protective updates require original fresh tradeable quote evidence. Ambiguous transport acceptance is not a fill, and dry-run counts are separate. The final 86-case suite uses AST-extracted methods and fake transport. These managers were not started or imported as running account processes, and their changes are outside all four frozen research closures. [Manager transition](validation/overnight_curve_buildout_20260909/management_correctness_audit_v1/ACCOUNT_RECONCILIATION_SOURCE_TRANSITION_V2_20260909.json).

The separate channel helper passed 39 tests: new entries are an admitted subset of the same prepared continuation estimates, so removing an invalid new entry does not erase opposite-signed or zero incumbent information. The new original-chain bridge reconstructs curve/publication/consumption contracts and preserves target, price convention and exact original float-clock evidence in a separately labelled decimal compatibility record. Its integrated 315-case run includes overlapping contract/channel tests; 86, 39, 37 and 315 must not be summed. Both helpers remain unregistered and inactive. Caller-observed I/O, source authority, actual state and any future execution remain separate obligations. [Bridge contract and tests](validation/overnight_curve_buildout_20260909/replay/chain_bridge_v1/CURVE_CHAIN_MANAGEMENT_BRIDGE_CONTRACT_20260909.md).

At 11:42:25 UTC, a bounded diagnostic rebuilt the latest retained GBP/USD official-M H3 chain and original raw input/model result, paired it with a newly observed local quote and an explicitly hypothetical flat state, and produced one entry and one continuation candidate. Its unchanged reference was 08:53:50, issue 08:54:01.252507 and nominal target 11:53:50, retaining the original 0–7-second target approximation. The diagnostic selected short 1,843 base units under the existing $2,500 scenario. This was compatibility inspection, not a fresh-information forecast, account action, virtual fill or performance test. [Actual retained-chain limits](validation/overnight_curve_buildout_20260909/replay/chain_bridge_v1/ACTUAL_RETAINED_H3_BRIDGE_DIAGNOSTIC_20260909.md).

## Risk, wider features and news

The separate M1 risk study publishes two fixed training-only methods for terminal move, absolute move, range, variation and long/short favorable/adverse excursions at 15/30/60 minutes: 72 nodes per pair. Final retained outcomes are still pending. Full-horizon uncalibrated marginals are not conditional remaining-position risk, and OHLC envelopes cannot establish stop/barrier order or executable fills. The 55-test attachment validator correctly refused the actual S5/M1 chains because original reference times/prices and target windows differed. A future shared issue grid and fresh forecast updates require a new registered comparison; clocks are not rounded into compatibility.

The inventory already contains 227/220-feature models, a 795-input engine, a 643-field MA grid and blurb/factor datasets. The narrow H1 study is not the whole project. Separate causal MA fixes remove full-series fallback scaling and final-length no-cross age: 40,754 of 147,715 eligible retained rows changed. Appending future data changed 13 of 36 inspected old rows and none of the repaired rows. This establishes feature correctness, not a cause for trading losses.

The frozen historical comparison rejected wider-model promotion: corrected MA643 and combined667 had worse final-test MAE than compact24 and zero change in all nine pair/horizon cells; compact24 also lost to zero change. There were only nine test dates, below the fixed ten-date interval threshold. No retuning or activation followed those failed final tests.

Cross-window currency movement, entry-news eligibility, response memory and USD exposure companions remain separate from active prediction/position rules. The dated 10:34:13 news sample contained five recent context members and zero vetted directional topics, plus 27 post-move and 36 aged-out members. Missing historical observation/response-known clocks were not reconstructed. The final news/runtime check is pending; that older sample is not current telemetry.

## Forecast visibility and bounded reliability

All 45 unavailable observations in the ten-minute exact-byte probe matched a mutable producer-summary alias: heartbeat hashes could name bytes never published. Historical producer sources remain frozen. A new immutable boundary requires exact written/readback bytes, while the active independent ledger reader verifies original committed forecasts and their publication/consumption receipts directly; it never declares the mismatched old heartbeat valid.

The original V1 endpoint probe had 77 polls/66 distinct reports, with 40 pair failures in 26 reports, 39 later recoveries and one right-censored failure. V2 uses a full first pass and at most one fresh eligible retry inside the same eight-second budget; clock/identity/integrity failures are not retried. Its 10:45:04–11:05:04 UTC probe had 77 polls/67 reports: all 19 first failures received fresh retries and recovered forecasts, with zero final failures and 66 forecasts on every poll. Every report verified all 68 ledgers. These are direct within-report recoveries in a separate uncontrolled window, not a controlled V1/V2 gain estimate. Two existing browser tabs could add concurrent polling. [Completed V2 probe](validation/overnight_curve_buildout_20260909/ledger_endpoint_reliability_probe_v1/LEDGER_ENDPOINT_V2_RELIABILITY_ASSESSMENT_20260909.json).

The 10:33 dashboard-only reload preserved all 33 other process identities. Actual HTTP and served JavaScript matched reviewed source; original forecast targets and expiry timers remain enforced even if the main fetch hangs. Producer mismatch/error diagnostics remain separate. Appearance changes remain deferred.

TRY/JPY and USD/TRY are registered, activated models with no publications in the dated 10:34 audit. Their last original readiness-log samples had 17 and 40 mature exact-H1 joint rows against 48 required, observed at 10:22:09 and 10:26:05. Later quotes were tradeable. These are retained attempt/readiness facts, not recomputed current progress; the minimum was not relaxed. Persisting pre-attempt refusals remains useful future work.

Old joint-v1/v2 workers were retired only after all 4,050 original obligations reconciled to outcomes or explicit exclusions. Sources and ledgers remain preserved. The 09:40 audit found 15 expected main workers; the paper process later completed naturally. Final current process/news/source checks remain pending. Thirteen old producer errors and five historical news-clock errors were observed previously; missing original failure inputs prevent causal attribution from later recovery.

## Verification, portability and remaining work

On the same 198-curve fixture, evaluator versions reduced elapsed time from 263.61 to 102.37 and 91.08 seconds while preserving exact verification. Larger inventories and different workloads have separate timing receipts. Lossless shared news storage used approximately 60.8% fewer bytes on three retained captures but read slower than whole gzip; it remains inactive. No retention deletion was introduced.

The explanatory dictionary was rebuilt after an advisor source change. Exact AST review found its two referenced ATR functions unchanged; only one inspected hash and references 1942→1950 / 1972→1980 changed. Build/check and 34 tests passed. The Sep8 validation and original document bytes remain historical; the [Sep9 metadata validation](validation/overnight_curve_buildout_20260909/feature_dictionary_source_refresh_v1/FEATURE_DICTIONARY_METADATA_REFRESH_VALIDATION_20260909.json) binds current generated JSON/MD. Vault alias `FEATURE_DICTIONARY_VALIDATION_CURRENT.json` still carries the dated Sep8 hashes and must not implicitly validate the refreshed documents.

Named test suites overlap; no invented total is used. The portable validation will embed copied-member hashes, current source dependencies and all four frozen closures. Original raw ledgers/news/account observations and fitted recovery artifacts remain external dependencies. A valid source archive does not reconstruct missing fitted weights, raw clocks or a live account.

Remaining work: complete final main/pilot/risk/runtime checks and publication; register a genuinely shared issue grid and fresh-update management comparison; obtain independent sessions for magnitude, after-cost and calibration acceptance; preserve future refusal/clock evidence and monitor capacity. No execution or promotion is authorized. The final source/record publication receipts, not this draft, will establish what reached the vault.
'''

CURRENT_QUEUE = '''## Current — September 9 finalization, draft 12:10 UTC

The [consolidated buildout report](docs/FOREX_OVERNIGHT_CURVE_BUILDOUT_20260909.md) separates completed engineering, dated measured results and final observations still pending before 13:00 UTC. Orders, account authorization and promotion remain disabled; appearance work is deferred.

Completed: recovered 13-horizon research chains; three verified paper episodes; causal MA repair; independent ledger visibility and a completed V2 retry probe; inactive manager reconciliation correction (86 tests), separate channels (39) and chain integration (315 overlapping tests); fixed-bin probability diagnostics (68); and metadata-only dictionary refresh (34). Current V2 observation: 19 fresh retry recoveries, zero final failures, 66 forecasts on every poll. This does not establish profitable prediction.

The dated 2,182-outcome joint study remains negative after spread and worse than fixed Brier/zero-change magnitude baselines. Curve management lost $5.67 across three isolated paper episodes, against -$14.82 matched momentum and +$2.06 fixed hold. Wider MA families failed all nine final-test cells. No inactive helper has counterfactual or prospective success assigned to it.

1. Finish final main, pilot, risk, runtime/news/source verification and portable evidence publication; preserve missing and pending outcomes explicitly.
2. Register a shared curve/risk issue grid with fresh decision-time updates before a new management comparison. Current old full-horizon estimates and S5/M1 target conventions cannot be silently joined.
3. Compare entry, continuation, hold/exit/rotation and existing controls against fixed hold/no-trade under identical executable costs and independent sessions. Inactive reconciliation/channel/bridge fixes are not activation permission.
4. Extend causal news/blurb/response memory and co-movement only with original knowledge clocks and matched prospective comparisons; assess magnitude, costs and descriptive probability calibration together.
5. Persist pre-attempt refusals and actual failing clock inputs; monitor finite news/history capacity without deleting evidence or weakening maturity/freshness guards.

The current dictionary's Sep9 metadata receipt is [separate from historical Sep8 validation](docs/validation/overnight_curve_buildout_20260909/feature_dictionary_source_refresh_v1/FEATURE_DICTIONARY_METADATA_REFRESH_VALIDATION_20260909.json). The original validation alias retains its old hashes.

## Dated queue history

The entries below retain their original dates and wording; they are not the current queue or telemetry.

'''

LOG = '''

## 2026-09-09 12:10 UTC — completed engineering; final observations still pending

Prepared the finalization draft in docs/FOREX_OVERNIGHT_CURVE_BUILDOUT_20260909.md and refreshed pending/README/vault orientation, preserving earlier dated text and exact before bytes. Final main/pilot/risk/runtime measurements and publication are still pending; no final operational or profitability claim is made here.

The completed V2 reader probe has 77 polls/67 distinct reports, 19 first-pass failures followed by 19 fresh forecast recoveries, zero final failures and 66 forecasts per poll. It preserves original producer errors separately and does not turn an uncontrolled before/after window into a causal gain estimate. The existing-manager reconciliation correction passed 86 fake-transport cases without activating account managers. Separate channels passed 39 and the bridge integration 315 overlapping cases; they remain inactive. The retained H3 bridge diagnostic used a newly observed quote and hypothetical flat state, not an account or new prediction.

Fixed-bin probability analysis passed 68 tests and reconciles the same 2,182 original outcomes, including 13 flats, to Brier 0.2521189983 versus 0.25. The three completed paper curve-management episodes remain net negative. The dictionary metadata refresh passed build/check and 34 tests; its original Sep8 validation/hashes remain historical, with new Sep9 current-metadata verification linked separately. No model, scorer, registered cohort, execution authorization or publisher mapping changed in this documentation pass.
'''


def sha(raw):
    return hashlib.sha256(raw).hexdigest()


def main():
    assert not OUT.exists()
    history = json.loads((HISTORY / 'BEFORE_FINAL_DOCUMENTATION_HISTORY_20260909.json').read_bytes())
    originals = {}
    for item in history['records']:
        path = Path(item['original_path']); raw = path.read_bytes()
        assert sha(raw) == item['sha256'] and raw == Path(item['retained_path']).read_bytes()
        originals[path] = raw
    pending = ROOT / 'FOREX_PENDING_IMPROVEMENTS.md'
    old_pending = originals[pending].decode('utf-8')
    assert old_pending.startswith('# Forex pending improvements')
    tail = old_pending[old_pending.index('\n') + 1:].lstrip('\r\n')
    readme = originals[ROOT / 'README.md'].decode('utf-8')
    anchor = '**Vault-independent orientation:**'
    insert = '''**September 9 buildout:** [current consolidated report](docs/FOREX_OVERNIGHT_CURVE_BUILDOUT_20260909.md) records recovered curves, exact paper costs, inactive manager/channel/bridge repairs, risk research and ledger visibility. Final observations/publication remain pending in the 12:10 UTC draft. Main forecasts and active paper management have not passed after-cost acceptance; orders and promotion remain disabled.

The current dictionary has a [Sep9 metadata validation](docs/validation/overnight_curve_buildout_20260909/feature_dictionary_source_refresh_v1/FEATURE_DICTIONARY_METADATA_REFRESH_VALIDATION_20260909.json). The Sep8 validation retains its original historical hashes; it does not validate the refreshed JSON/MD implicitly. Exact referenced ATR function math is unchanged.

'''
    assert readme.count(anchor) == 1
    readme = readme.replace(anchor, insert + anchor)
    readme = readme.replace('Prior joint cohorts and price-only models remain separate comparisons.', 'Prior joint cohorts retain their completed evidence; the two obsolete joint workers were retired after their original obligations resolved. Price-only comparison studies remain separate.')
    guide = originals[ROOT / 'FOREX_AUDIT_START_HERE.md'].decode('utf-8')
    anchor = 'The latest narrative records describe research collection with orders and promotion disabled.'
    assert guide.count(anchor) == 1
    guide = guide.replace(anchor, '''Start current work with [the September 9 overnight buildout](OVERNIGHT_CURVE_BUILDOUT_CURRENT.md) and [its publication-bound validation](OVERNIGHT_CURVE_BUILDOUT_VALIDATION_CURRENT.json). The 12:10 UTC narrative is a finalization draft: later outcome/runtime checks and final publication are still pending. Its engineering improvements do not establish profitable prediction or management. The validation's embedded evidence/source index identifies exact portable members; source ZIP paths start at the project root, without `trad/`.

The latest narrative records describe research collection with orders and promotion disabled.''')
    guide = guide.replace('`FEATURE_DICTIONARY_VALIDATION_CURRENT.json` records documentation checks and source hashes.', '`FEATURE_DICTIONARY_VALIDATION_CURRENT.json` is explicitly the historical September 8 validation for its original hashes. The refreshed dictionary\'s September 9 metadata verification is source ZIP member `docs/validation/overnight_curve_buildout_20260909/feature_dictionary_source_refresh_v1/FEATURE_DICTIONARY_METADATA_REFRESH_VALIDATION_20260909.json`, indexed by the overnight validation. Only one source hash and two line locations changed; the referenced feature functions are unchanged. Do not use the old alias to imply verification of the new JSON/Markdown bytes.')
    payloads = {ROOT / 'docs/FOREX_OVERNIGHT_CURVE_BUILDOUT_20260909.md': REPORT.encode(),
        pending: ('# Forex pending improvements\n\n' + CURRENT_QUEUE + tail).encode(),
        ROOT / 'FOREX_PROJECT_LOG.md': originals[ROOT / 'FOREX_PROJECT_LOG.md'] + LOG.encode(),
        ROOT / 'README.md': readme.encode(), ROOT / 'FOREX_AUDIT_START_HERE.md': guide.encode()}
    assert set(payloads) == set(originals)
    OUT.mkdir()
    records = []
    for path, raw in payloads.items():
        assert path.read_bytes() == originals[path]
        path.write_bytes(raw)
        assert path.read_bytes() == raw
        (OUT / path.name).write_bytes(raw)
        diff = ''.join(difflib.unified_diff(originals[path].decode().splitlines(True), raw.decode().splitlines(True),
                                          fromfile=str(path) + ' (before)', tofile=str(path) + ' (draft)'))
        (OUT / (path.name + '.diff.txt')).write_text(diff, encoding='utf-8', newline='\n')
        records.append(dict(path=str(path), before_sha256=sha(originals[path]), sha256=sha(raw), bytes=len(raw)))
    result = dict(status='draft_finalization_waiting_final_bound_observations', created_utc=datetime.datetime.now(datetime.timezone.utc).isoformat(),
        records=records, old_pending_history_preserved=True, project_log_original_prefix_preserved=True,
        no_source_mapping_or_runtime_changes=True)
    raw = (json.dumps(result, sort_keys=True, indent=2) + '\n').encode()
    (OUT / 'DOCUMENTATION_DRAFT_20260909.json').write_bytes(raw)
    print(json.dumps(dict(receipt_sha256=sha(raw), **result)))


if __name__ == '__main__':
    main()
