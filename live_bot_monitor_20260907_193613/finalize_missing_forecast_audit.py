"""Compose captured read-only evidence; no further runtime queries."""
from pathlib import Path
from datetime import datetime,timezone
from collections import Counter
import hashlib,json
OUT=Path(__file__).resolve().parent
def load(p):return json.loads(p.read_text(encoding='utf-8-sig'))
def bind(p):return {'path':str(p),'sha256':hashlib.sha256(p.read_bytes()).hexdigest()}
inputs_path=OUT/'MISSING_FORECAST_INPUTS_SNAPSHOT_20260907.json'
ledger_path=OUT/'MISSING_FORECAST_LEDGER_BASELINE_20260907.json'
thb_path=OUT/'USD_THB_RIDGE_ELIGIBILITY_DIAGNOSIS_20260907.json'
inputs=load(inputs_path);ledger=load(ledger_path);thb=load(thb_path)
assert inputs['read_failures']==0 and len(inputs['rows'])==68 and ledger['status']=='passed'
primary={};rows=[];few=[]
for row in inputs['rows']:
    pair=row['instrument'];historical=ledger['pairs'][pair]
    if row['last_completed_bar_age_seconds']>900:group='stale_archive_endpoint'
    elif row['current_own_consecutive_bars']<61:group='insufficient_consecutive_own_minutes'
    elif row['mature_ridge_training_rows']<24:group='insufficient_mature_ridge_labels'
    else:group='input_minima_met'
    primary.setdefault(group,[]).append(pair)
    if (row['last_completed_bar_age_seconds']<=900 and 1<=row['missing_of_last61_slots']<=5
            and row['api_row']['status']!='forecast'):few.append(pair)
    rows.append({'instrument':pair,'api_status':row['api_row']['status'],'api_reason':row['api_row'].get('reason'),
        'current_primary_input_condition':group,'current_own_bars':row['current_own_consecutive_bars'],
        'current_missing_of_last61':row['missing_of_last61_slots'],'current_mature_ridge_rows':row['mature_ridge_training_rows'],
        'quote_eligibility_at_separate_snapshot':row['quote_now_status'],
        'quote_reason_at_separate_snapshot':row.get('quote_now_reason'),
        'ever_publication_count':historical['counts']['publication'],
        'latest_attempt':historical['latest_attempt'],'latest_diagnostic':historical['latest_diagnostic'],
        'latest_publication':historical['latest_publication'],'current_bucket_attempt_exists':historical['current_bucket_attempt_exists']})
assert {k:len(v) for k,v in primary.items()}=={'insufficient_consecutive_own_minutes':57,'input_minima_met':7,'stale_archive_endpoint':3,'insufficient_mature_ridge_labels':1}
restoration={
 'insufficient_consecutive_own_minutes':'The selected pair must have 61 actual consecutive completed minute closes at a fresh archive endpoint and enough mature training windows. A single missing minute breaks this feature window. Legitimate provider recovery can restore a truly returned missing candle; otherwise a later uninterrupted run is required. No imputation, compressed time or backdated forecast is valid under this contract.',
 'stale_archive_endpoint':'EUR_TRY, TRY_JPY and USD_TRY need current tradeable stream quotes and fresh actual provider candles. Their endpoints are hours old while the all-pair collector continues; this audit does not establish the reason those instruments stopped producing forward data.',
 'insufficient_mature_ridge_labels':'USD_THB meets the current61-bar condition but has10 of24 mature stride-three Ridge training rows. Each row requires121 consecutive actual prices spanning the feature and exact H1 label window. The current contract requires both model families, so state-space output alone is withheld. More valid matured history is needed; separate-family issuance requires a separately specified study.',
 'fresh_quote_requirement':'A current tradeable stream quote observed within60 seconds is required. A new quote can clear this gate, but at the independently captured quote observation all24 quote-ineligible pairs also had input blockers.',
 'already_attempted_current_bucket':'USD_HUF grew from53 bars at its20:00 attempt to64 at20:09, with253 mature Ridge rows and an eligible quote. That20:00–20:15 attempt bucket was already consumed, so its next scheduled opportunity is20:15, conditional on inputs and quote still qualifying. This is a cadence delay, not a stuck-worker finding.',
 'expired_previous_forecast':'Sixteen previously publishing pairs have passed their latest original H1 target. The dashboard correctly stops presenting those estimates as active. They require a new eligible prospective attempt; extending or relabeling the old forecast would misstate its horizon.'}
report={'schema':'missing_forecasts_audit_v1_20260907','status':'read_only_diagnosis_completed',
    'written_utc':datetime.now(timezone.utc).isoformat(),'input_observation_window':[inputs['started_utc'],inputs['finished_utc']],
    'ledger_observation_window':[ledger['started_utc'],ledger['finished_utc']],
    'api_display_groups_at_input_snapshot':inputs['api_group_counts'],
    'primary_current_input_groups':{k:{'count':len(v),'pairs':v} for k,v in primary.items()},
    'quote_snapshot_ineligible':sum(r['quote_now_status']=='ineligible' for r in inputs['rows']),
    'quote_ineligible_and_input_blocked':sum(r['quote_now_status']=='ineligible' and not r['all_minimum_input_conditions_now'] for r in inputs['rows']),
    'current_missing_pairs_with_only1_to5_missing_calendar_slots':few,
    'ever_published_pair_count':len(ledger['ever_published_pairs']),
    'never_published_pair_count':68-len(ledger['ever_published_pairs']),
    'ever_published_now_missing':ledger['ever_published_now_missing'],
    'all_previous_missing_forecasts_have_original_targets_passed':all(ledger['pairs'][p]['latest_publication']['target_passed_at_ledger_read'] for p in ledger['ever_published_now_missing']),
    'fresh_quote_without_current_bucket_attempt':ledger['fresh_ledger_quote_without_current_bucket_attempt'],
    'ready_now_but_missing_forecast':inputs['ready_now_but_no_displayed_forecast'],
    'reader_or_worker_assessment':{'archive_read_failures':0,'registered9_source_hashes_unchanged':True,
       'current_input_capture_exceptions':'No source-read or timestamp-validation failure found. Rejections reflect missing own-minute windows, stale endpoints or Ridge-label sufficiency.',
       'scheduler_backlog_found':False,'ready_after_failed_attempt_cadence_delay_found':['USD_HUF'],
       'general_feed_outage_explains_missing_forecasts':False},
    'restoration_by_group':restoration,'all68_pair_details':rows,
    'candidate_future_improvements_not_implemented':[
       'Display current input counts alongside last-attempt counts, and show both input and quote blockers when they overlap.',
       'For a new registered scheduler, assess input readiness before consuming a bucket and align eligible attempts with completed archive updates.',
       'Evaluate a separately registered sparse-minute model using actual elapsed time and explicit gaps, or a separately validated shorter-feature model; never silently fill or compress missing prices.',
       'Evaluate separately registered per-family publication if state space should be available while Ridge lacks training labels.'],
    'evidence':[bind(inputs_path),bind(ledger_path),bind(thb_path),bind(OUT/'COLLECTION_20260907_2002.json'),bind(OUT/'audit_missing_inputs.py')],
    'scope_limits':['Counts are timestamped observations, not promises about the rest of the hour; quote reasons change as individual pairs update.',
       'Input minima alone do not establish numerical fit success or predictive accuracy.',
       'Provider absence is proven only for named retained responses at their recorded observation times.',
       'The original EUR scoring duplicate-reference issue is separate from the independent EUR pair model current-minute blocker.'],
    'runtime_changes':False,'source_or_vault_writes':False,'model_fits':0,'forecast_issuance':0,'orders':0,'broker_requests':0}
target=OUT/'MISSING_FORECASTS_AUDIT_20260907.json'
with target.open('x',encoding='utf-8') as handle:json.dump(report,handle,indent=2,allow_nan=False)
notes='''At 20:09:17–24 UTC the dashboard showed 7 active forecast pairs, 30 pairs labeled for insufficient own minutes, 30 labeled for quote freshness, and USD_THB missing Ridge. These display reasons overlap the underlying input problems and change as quotes arrive.

Independent checks of all68 archives found 57 fresh endpoints without61 consecutive real minute closes,3 stale TRY endpoints,1 pair lacking enough mature Ridge labels, and7 meeting minimum inputs. All archive reads were coherent; no registered source changed. At a separately timestamped quote snapshot24 pairs lacked an eligible quote, and all24 also lacked required inputs. A broad stream or worker outage does not explain this pattern.

The61 requirement is a rolling uninterrupted feature window, not a one-time warm-up since connection or market open. Fourteen currently missing pairs have only1–5 missing slots in their last61. USD_JPY has a single missing minute at19:46; a stored provider response observed20:07:16 specifically omitted it and its hash verifies. That one absent minute leaves only20 consecutive bars at the observed endpoint. EUR/USD has7 consecutive bars and8 gaps in its last61; its latest verified recovery response omitted19:52. Unobserved missing minutes are not automatically classified as provider omissions.

USD_THB has148 current bars but only10 of24 required matured Ridge training examples; the exact model needs121 real consecutive prices for each H1-labeled example sampled every3 minutes. The paired-family publication rule discards the otherwise available state-space result. The specific diagnosis is in USD_THB_RIDGE_ELIGIBILITY_DIAGNOSIS_20260907.json. No current numerical fit was run by this audit.

USD_HUF is the clear scheduling limitation:53 bars at20:00 became64 by20:09, with253 valid training rows and an eligible quote. The once-per15-minute attempt rule waits for the20:15 opportunity, subject to continuing eligibility. There was no fresh-quote pair missing its current-bucket attempt and no scheduler backlog found. GBP/USD remains displayed from an earlier valid forecast even though a subsequent gap now blocks replacement; active forecasts last until their original H1 target.

Twenty-three pairs have published before. Sixteen of them currently have no active forecast, and every one has passed its latest original H1 target: AUD_NZD, CHF_JPY, EUR_AUD, EUR_HUF, EUR_NOK, EUR_SEK, EUR_USD, EUR_ZAR, GBP_AUD, GBP_CHF, GBP_PLN, HKD_JPY, USD_HUF, USD_JPY, USD_SEK and USD_ZAR. They have not been erased; their estimates expired without an eligible replacement. Forty-five pairs have never published in this study.

Legitimate recovery is new qualifying quotes, actual missing provider candles when they become available, or enough later uninterrupted and matured history. The useful next engineering changes are clearer current-versus-last-attempt diagnostics, a new scheduler that reserves an attempt only when inputs are ready, and separately registered gap-aware or per-family models. Those changes require their own validation; the current registered rules must remain intact. Filling flat candles, compressing missing time, extending expired horizons, or weakening requirements under the same study would not repair the evidence.

No source, model, ledger, order or vault change was made. The original EUR scorer duplicate-reference defect is separate from the independent pair model's current-minute gap. Root continues the requested live monitoring hour.
'''
with (OUT/'MISSING_FORECASTS_NOTES_20260907.md').open('x',encoding='utf-8') as handle:handle.write(notes)
print(json.dumps({'audit':str(target),'sha256':bind(target)['sha256'],'primary_counts':{k:len(v) for k,v in primary.items()},'few_gap_missing_pairs':len(few),'ever_published':len(ledger['ever_published_pairs']),'expired_without_active_replacement':len(ledger['ever_published_now_missing'])}))
