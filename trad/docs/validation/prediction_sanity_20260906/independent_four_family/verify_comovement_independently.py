"""Check root co-movement artifacts from frozen review JSON only.

Standard-library correlations and a power-iteration eigenvalue calculation;
dictionary interval-membership checks instead of root's run-length indexing.
No runtime sources, CSVs, databases, original helper imports or mutations.
"""
from collections import Counter
from datetime import datetime, timezone
import hashlib
import itertools
import json
import math
from pathlib import Path
import statistics

OUT = Path(__file__).resolve().parent
REVIEW = OUT.parent
FROZEN = REVIEW/'comovement'
PAIRS = ('EUR_USD','GBP_USD','AUD_USD','NZD_USD','USD_JPY','USD_CHF','USD_CAD')


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def average(values):
    return math.fsum(values)/len(values)


def correlation(xs,ys):
    if len(xs)<3:
        return None
    mx,my = average(xs),average(ys)
    dx,dy = [x-mx for x in xs],[y-my for y in ys]
    denominator = math.sqrt(math.fsum(x*x for x in dx)*math.fsum(y*y for y in dy))
    return math.fsum(x*y for x,y in zip(dx,dy))/denominator if denominator else None


def close(actual,expected,tolerance=1e-9):
    if actual is None or expected is None:
        assert actual is expected,(actual,expected)
        return 0.0
    difference = abs(actual-expected)
    assert difference <= tolerance,(actual,expected,difference)
    return difference


def power_eigenvalue(matrix):
    vector = [1/math.sqrt(len(matrix))]*len(matrix)
    for iteration in range(10000):
        image = [math.fsum(row[j]*vector[j] for j in range(len(vector))) for row in matrix]
        norm = math.sqrt(math.fsum(x*x for x in image))
        updated = [x/norm for x in image]
        if max(abs(x-y) for x,y in zip(vector,updated))<1e-15:
            vector=updated
            break
        vector=updated
    else:
        raise AssertionError('power iteration did not converge')
    image = [math.fsum(row[j]*vector[j] for j in range(len(vector))) for row in matrix]
    eigenvalue = math.fsum(x*y for x,y in zip(vector,image))
    residual = max(abs(image[i]-eigenvalue*vector[i]) for i in range(len(vector)))
    assert residual < 1e-12
    return eigenvalue,residual,iteration+1


def main():
    model_path = FROZEN/'model_output_comovement.json'
    currency_path = FROZEN/'currency_comovement.json'
    candle_path = FROZEN/'captured_week_candles.json'
    models = json.loads(model_path.read_text())
    currency = json.loads(currency_path.read_text())
    candles = json.loads(candle_path.read_text())
    paired = json.loads((OUT/'independently_paired_endpoints.json').read_text())
    previous_independent = json.loads((OUT/'independent_four_family_results.json').read_text())
    families = tuple(models['families'])
    assert set(families)==set(previous_independent['family_metrics'])
    assert models['n_matching_decisions']==len(paired)==368
    observed_unanimous,observed_three,mean_prob_losses,mean_prediction_errors = [],[],[],[]
    correctness = Counter()
    two_two = 0
    pair_reports = []
    for row in paired:
        samples = [row['families'][family] for family in families]
        buy_count = sum(x['side']==1 for x in samples)
        actual = row['actual_signed_move_pips']
        vote_side = 1 if buy_count>2 else -1 if buy_count<2 else 0
        hit = int(vote_side*actual>0)
        if buy_count in (0,4): observed_unanimous.append(hit)
        if buy_count != 2: observed_three.append(hit)
        if buy_count == 2: two_two += 1
        mean_prob_losses.append((average([x['probability_up'] for x in samples])-int(actual>0))**2)
        mean_prediction_errors.append(abs(average([x['expected_signed_pips'] for x in samples])-actual))
        correctness[sum(x['correct'] for x in samples)] += 1
    def consensus(hits):
        return {'n':len(hits),'hits':sum(hits),'direction_accuracy_flats_miss':sum(hits)/len(hits),'coverage':len(hits)/len(paired)}
    unanimous,three = consensus(observed_unanimous),consensus(observed_three)
    assert unanimous==models['unanimous'] and three==models['three_or_four_agree']
    assert two_two==models['two_two_ties']
    assert {str(k):v for k,v in sorted(correctness.items())}==models['correct_models_per_decision']
    close(average(mean_prob_losses),models['mean_probability_brier'])
    close(average(mean_prediction_errors),models['mean_signed_prediction_mae'])
    close(average([abs(row['actual_signed_move_pips']) for row in paired]),models['zero_move_mae'])
    for root in models['pairs']:
        a,b = root['families']
        xs = [row['families'][a] for row in paired]
        ys = [row['families'][b] for row in paired]
        record = {
            'families':[a,b],
            'probability_correlation':correlation([x['probability_up'] for x in xs],[y['probability_up'] for y in ys]),
            'expected_move_correlation':correlation([x['expected_signed_pips'] for x in xs],[y['expected_signed_pips'] for y in ys]),
            'probability_error_correlation':correlation([x['probability_up']-x['actual_up'] for x in xs],
                                                       [y['probability_up']-y['actual_up'] for y in ys]),
            'same_direction_count':sum(x['side']==y['side'] for x,y in zip(xs,ys)),
            'both_direction_correct_count':sum(x['correct'] and y['correct'] for x,y in zip(xs,ys)),
            'both_direction_wrong_count':sum(not x['correct'] and not y['correct'] for x,y in zip(xs,ys)),
        }
        for key in record:
            if key=='families':continue
            close(record[key],root[key])
        pair_reports.append(record)

    assert sha(candle_path)==currency['captured_candles_sha256']
    assert len(candles)==7 and {row['pair'] for row in candles}==set(PAIRS)
    stamp = lambda value: datetime.fromisoformat(value.replace('Z','+00:00')).timestamp()
    window_start,window_end = map(stamp,currency['window_bar_starts_utc'])
    assert window_start==1788048000 and window_end==1788566400
    prices={}
    for row in candles:
        values = {}
        for bar in row['bars']:
            t,value = bar['bar_start_epoch'],bar['close']
            assert type(t) is int and t%60==0 and window_start<=t<window_end
            assert math.isfinite(value) and value>0
            assert t not in values
            values[t] = value
        assert list(values)==sorted(values)
        prices[row['pair']]=values
    common = set(prices[PAIRS[0]])
    for pair in PAIRS[1:]:common.intersection_update(prices[pair])
    ordered = sorted(common)
    assert len(ordered)==currency['common_bars']==6904
    directions = {pair:1 if pair.endswith('_USD') else -1 for pair in PAIRS}
    def contiguous(t,past,future=0):
        return all(t+offset*60 in common for offset in range(-past,future+1))
    def ret(pair,t,minutes):
        return math.log(prices[pair][t]/prices[pair][t-minutes*60])*directions[pair]
    independent_matrices={}
    matrix_max_error=0
    for minutes in (1,60):
        selected = [t for t in ordered if contiguous(t,minutes) and (minutes==1 or (t+60)%3600==0)]
        vectors = {pair:[ret(pair,t,minutes) for t in selected] for pair in PAIRS}
        matrix=[[correlation(vectors[a],vectors[b]) for b in PAIRS] for a in PAIRS]
        root = currency['matrices'][str(minutes)]
        assert len(selected)==root['n']
        for i in range(7):
            for j in range(7):
                matrix_max_error=max(matrix_max_error,close(matrix[i][j],root['correlation'][i][j]))
        value,residual,iterations=power_eigenvalue(matrix)
        share=value/math.fsum(matrix[i][i] for i in range(7))
        close(share,root['first_principal_component_standardized_variance_share'])
        independent_matrices[str(minutes)]={'n':len(selected),'correlation':matrix,'pc1_share':share,
                                            'power_iteration_residual':residual,'power_iterations':iterations}
    lagged=[]
    lag_max_error=0
    for past in (1,5,15,60,120):
        selected=[t for t in ordered if (t+60)%3600==0 and contiguous(t,past,60)]
        future=[math.log(prices['EUR_USD'][t+3600]/prices['EUR_USD'][t]) for t in selected]
        for pair in PAIRS:
            value=correlation([ret(pair,t,past) for t in selected],future)
            root=next(x for x in currency['lagged_correlations_all_fixed_cells']
                      if x['past_return_minutes']==past and x['predictor_pair']==pair)
            assert root['n']==len(selected)
            lag_max_error=max(lag_max_error,close(value,root['correlation']))
            lagged.append({'predictor_pair':pair,'past_minutes':past,'n':len(selected),'correlation':value})
    gaps=[(b-a)//60-1 for a,b in zip(ordered,ordered[1:]) if b-a>60]
    result={
        'schema_version':'independent_comovement_arithmetic_verification_v1',
        'generated_utc':datetime.now(timezone.utc).isoformat(),
        'status':'all_consensus_average_correlation_alignment_and_gap_checks_match',
        'inputs_sha256':{path.name:sha(path) for path in [model_path,currency_path,candle_path,
                                                        OUT/'independently_paired_endpoints.json',REVIEW/'comovement_diagnostic.py']},
        'script_sha256':sha(Path(__file__)),
        'method':'Standard-library arithmetic, correlations, independent interval membership checks and power-iteration PCA; no root helper imports or production reads.',
        'model_comparison':{'unanimous':unanimous,'three_or_four_agree':three,'two_two_ties':two_two,
            'mean_probability_brier':average(mean_prob_losses),'mean_signed_prediction_mae':average(mean_prediction_errors),
            'pairs':pair_reports},
        'currency_comparison':{'common_bars':len(ordered),'actual_first_bar_utc':datetime.fromtimestamp(ordered[0],timezone.utc).isoformat(),
            'actual_last_bar_utc':datetime.fromtimestamp(ordered[-1],timezone.utc).isoformat(),
            'gaps_between_common_bars':len(gaps),'missing_minutes_inside_gaps':sum(gaps),
            'longest_gap_minutes':max(gaps),'continuous_segments':len(gaps)+1,
            'matrices':independent_matrices,'maximum_matrix_cell_absolute_difference':matrix_max_error,
            'lagged_correlations':lagged,'maximum_lagged_cell_absolute_difference':lag_max_error},
        'review_findings':[
            'No arithmetic, sign-orientation, alignment, interval-boundary, gap-handling or consensus-count error found.',
            'Positive orientations consistently mean non-USD appreciation: JPY/CHF/CAD are reciprocally oriented via negated USD-base log returns.',
            'Return endpoints are completed M1 bar closes (bar starts plus 60 seconds), not observed-availability timestamps.',
            'Hourly returns use UTC hour-end endpoints and cannot cross missing common bars; lead-lag cells additionally require uninterrupted entire past and future intervals.',
            'Unanimous/three-or-four agreement subsets are descriptive emitted-direction slices. Equal probability and expected-return averages are different diagnostics.',
        ],
        'limitations':[
            'A common USD denominator and overlapping FX exposures can mechanically create shared movements; 71.19% PC1 share is descriptive standardized variance, not explained future EURUSD returns or independent information.',
            'Simultaneous currency return correlation is not lead-lag predictive evidence. The fixed lead-lag table is also retrospective, has small hourly sample counts and no out-of-sample validation.',
            '6,739 one-minute returns and 87 contiguous hourly returns are observations, not independent experimental trials. The common-bar selection drops missing intervals and may condition on availability.',
            'The six-calendar-day window contains approximately five market days; Sunday before open is empty. Actual retained trading timestamps are reported explicitly.',
            'Captured close-only JSON can verify arithmetic and alignment but cannot independently prove original CSV headers, quote completeness, exact original file consistency or historical data availability.',
            'The models comparison retains known publication and endpoint-clock defects. Consensus scores do not establish realizable trade performance.',
            'PCA uses the whole inspected window. Consensus and equal averages were inspected after observing data; no new model, weighting or threshold was trained or activated.',
        ],
        'runtime_started':False,'proof_eligible':False,'account_eligible':False,
    }
    with (OUT/'INDEPENDENT_COMOVEMENT_VERIFICATION_20260906.json').open('x',encoding='utf-8') as handle:
        json.dump(result,handle,indent=2,allow_nan=False)
        handle.write('\n')
    print(json.dumps({'status':result['status'],'unanimous':unanimous,'three_or_four_agree':three,
        'mean_probability_brier':average(mean_prob_losses),'currency_eur_gbp_m1':independent_matrices['1']['correlation'][0][1],
        'pc1_m1_share':independent_matrices['1']['pc1_share'],'matrix_max_error':matrix_max_error,
        'lag_max_error':lag_max_error,'gaps':len(gaps),'common_bars':len(ordered)},indent=2))


if __name__=='__main__':main()
