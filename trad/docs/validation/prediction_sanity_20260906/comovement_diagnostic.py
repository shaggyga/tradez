"""Descriptive co-movement audit; no fitting, runtime imports or trading actions."""
from pathlib import Path
from datetime import datetime, timezone
from collections import defaultdict
import csv
import hashlib
import io
import itertools
import json
import math
import os
import sys

OUT=Path(__file__).resolve().parent/'comovement'
ROOT=Path(__file__).resolve().parent.parent/'trad'
OUT.mkdir(parents=True,exist_ok=False)
sys.dont_write_bytecode=True
for name in ('OMP_NUM_THREADS','OPENBLAS_NUM_THREADS','MKL_NUM_THREADS'):os.environ[name]='1'
import numpy as np

def stamp(s):return datetime.fromisoformat(s.replace('Z','+00:00')).timestamp()
def sha(raw):return hashlib.sha256(raw).hexdigest()
def write(name,value):
    with (OUT/name).open('x',encoding='utf-8') as f:json.dump(value,f,indent=2,allow_nan=False);f.write('\n')
def corr(x,y):
    if len(x)<3 or np.std(x)==0 or np.std(y)==0:return None
    return float(np.corrcoef(x,y)[0,1])

source=ROOT/'docs/validation/fixed_evaluation_20260906/selected_candidate_rows.json'
raw=source.read_bytes();rows=json.loads(raw)
groups=defaultdict(list)
for row in rows:
    assert sha(row['forecast_json'].encode())==row['payload_sha256']
    groups[row['entry_time']].append(row)
families=sorted({r['family'] for r in rows})
paired=[]
for when,group in sorted(groups.items()):
    if len(group)!=4 or len({r['family'] for r in group})!=4 or not all(r['outcome'] for r in group):continue
    endpoints={(r['entry_bid'],r['entry_ask'],r['pip'],r['outcome']['exit_time'],r['outcome']['exit_bid'],r['outcome']['exit_ask']) for r in group}
    if len(endpoints)!=1:continue
    by={r['family']:r for r in group};r=group[0];o=r['outcome']
    actual=((o['exit_bid']+o['exit_ask'])/2-(r['entry_bid']+r['entry_ask'])/2)/r['pip']
    paired.append({'time':when,'actual_pips':actual,'up':int(actual>0),
                   'side':[1 if by[f]['direction']=='buy' else -1 for f in families],
                   'prob':[by[f]['probability_up'] for f in families],
                   'expected':[json.loads(by[f]['forecast_json'])['expected_signed_pips'] for f in families]})
assert len(paired)==368
P=np.array([r['prob'] for r in paired]);S=np.array([r['side'] for r in paired]);Y=np.array([r['up'] for r in paired])
A=np.array([r['actual_pips'] for r in paired]);E=np.array([r['expected'] for r in paired]);H=(S*A[:,None]>0)
comparisons=[]
for i,j in itertools.combinations(range(4),2):
    comparisons.append({'families':[families[i],families[j]],'n':len(paired),
        'probability_correlation':corr(P[:,i],P[:,j]),'expected_move_correlation':corr(E[:,i],E[:,j]),
        'probability_error_correlation':corr(P[:,i]-Y,P[:,j]-Y),
        'same_direction_count':int(np.sum(S[:,i]==S[:,j])),
        'same_direction_rate':float(np.mean(S[:,i]==S[:,j])),
        'both_direction_correct_count':int(np.sum(H[:,i]&H[:,j])),
        'both_direction_wrong_count':int(np.sum(~H[:,i]&~H[:,j]))})
vote=np.sum(S,axis=1)
def consensus(mask):
    side=np.sign(vote[mask]);n=int(np.sum(mask));hits=int(np.sum(side*A[mask]>0))
    return {'n':n,'hits':hits,'direction_accuracy_flats_miss':hits/n if n else None,'coverage':n/len(paired)}
model_result={'classification':'already_inspected_archival_diagnostic_not_causal_or_executable_proof',
    'source':str(source.relative_to(ROOT)),'source_sha256':sha(raw),'n_matching_decisions':len(paired),
    'families':families,'pairs':comparisons,'unanimous':consensus(abs(vote)==4),
    'three_or_four_agree':consensus(abs(vote)>=2),'two_two_ties':int(np.sum(vote==0)),
    'mean_probability_brier':float(np.mean((np.mean(P,axis=1)-Y)**2)),
    'constant_half_brier':0.25,'mean_signed_prediction_mae':float(np.mean(abs(np.mean(E,axis=1)-A))),
    'zero_move_mae':float(np.mean(abs(A))),
    'correct_models_per_decision':{str(i):int(np.sum(np.sum(H,axis=1)==i)) for i in range(5)},
    'limitations':['Six previously inspected UTC days with overlapping one-hour outcomes; no independent-sample significance claim.',
        'Equal averaging and agreement slices are retrospective diagnostics, not registered strategies or validated improvements.',
        'Outcome error correlation includes the shared realized target; it is descriptive, not independent proof of duplicate features.',
        'Archived entry/publication and target-clock defects remain; metrics do not establish realizable trading results.']}
write('model_output_comovement.json',model_result)

PAIRS=('EUR_USD','GBP_USD','AUD_USD','NZD_USD','USD_JPY','USD_CHF','USD_CAD')
start=stamp('2026-08-30T00:00:00+00:00');end=stamp('2026-09-05T00:00:00+00:00')
series={};bindings=[];captured=[]
for pair in PAIRS:
    p=ROOT/'data/oanda_training_manager/candles'/f'{pair}_M1.csv'
    before=p.stat();raw=p.read_bytes();after=p.stat()
    assert (before.st_size,before.st_mtime_ns)==(after.st_size,after.st_mtime_ns),'source changed during read'
    values={};previous=None
    for row in csv.DictReader(io.StringIO(raw.decode('utf-8-sig'))):
        t=stamp(row['time']);assert stamp(row['datetime'])==t
        assert row['instrument']==pair and row['granularity']=='M1' and t%60==0
        if not start<=t<end:continue
        close=float(row['close']);assert math.isfinite(close) and close>0
        assert t not in values and (previous is None or t>previous),'duplicate or unordered bars'
        values[int(t)]=close;previous=t
    assert values,'empty archive window'
    series[pair]=values
    bindings.append({'pair':pair,'path':str(p.relative_to(ROOT)),'source_sha256':sha(raw),
                     'source_bytes':len(raw),'window_rows':len(values),
                     'source_observed_utc':datetime.now(timezone.utc).isoformat()})
    captured.append({'pair':pair,'bars':[{'bar_start_epoch':t,'close':c} for t,c in values.items()]})
write('captured_week_candles.json',captured)
common=sorted(set.intersection(*(set(series[p]) for p in PAIRS)))
prices=np.array([[series[p][t] for p in PAIRS] for t in common])
times=np.array(common,dtype=np.int64)+60 # closed-bar endpoint, not availability certificate
logs=np.log(prices)
orientation=np.array([1 if p.endswith('_USD') else -1 for p in PAIRS])
runlen=np.ones(len(common),dtype=int)
for i in range(1,len(common)):
    if times[i]-times[i-1]==60:runlen[i]=runlen[i-1]+1
def return_matrix(minutes,nonoverlap=False):
    inds=np.flatnonzero(runlen>minutes)
    if nonoverlap:inds=inds[times[inds]%(minutes*60)==0]
    return (logs[inds]-logs[inds-minutes])*orientation,inds
matrices={}
for minutes in (1,60):
    ret,inds=return_matrix(minutes,nonoverlap=minutes==60)
    matrix=np.corrcoef(ret,rowvar=False)
    eigenvalues=np.linalg.eigvalsh(matrix)
    matrices[str(minutes)]={'return_minutes':minutes,'utc_anchored_nonoverlapping':minutes==60,
        'n':len(inds),'correlation':matrix.tolist(),
        'first_principal_component_standardized_variance_share':float(eigenvalues[-1]/sum(eigenvalues))}
lagged=[]
for past in (1,5,15,60,120):
    # Fixed hourly UTC decision grid; future60m and all predictor history must be contiguous.
    inds=np.array([i for i in range(past,len(times)-60)
                   if times[i]%3600==0 and runlen[i+60]>=past+61],dtype=int)
    future=logs[inds+60,0]-logs[inds,0]
    pred=(logs[inds]-logs[inds-past])*orientation
    for j,pair in enumerate(PAIRS):
        lagged.append({'predictor_pair':pair,'past_return_minutes':past,'future_eurusd_minutes':60,
                       'n':len(inds),'correlation':corr(pred[:,j],future)})
currency_result={'classification':'descriptive_inspected_week_comovement_not_predictive_validation',
    'window_bar_starts_utc':['2026-08-30T00:00:00+00:00','2026-09-05T00:00:00+00:00'],
    'common_bars':len(common),'pair_order':list(PAIRS),
    'orientation':'Positive means non-USD currency strengthening versus USD. USD_JPY/USD_CHF/USD_CAD log returns negated.',
    'source_bindings':bindings,'captured_candles_sha256':sha((OUT/'captured_week_candles.json').read_bytes()),
    'matrices':matrices,'lagged_correlations_all_fixed_cells':lagged,
    'limitations':['Only six already inspected UTC days; correlations do not establish a stable relation or a forecast edge.',
        'Midpoint candle returns exclude bid/ask costs, slippage and execution availability.',
        'No data filling; gaps remove returns whose entire interval is not consecutive.',
        'PCA uses this whole descriptive window; no fitted factor is credited as known before a forecast.',
        'Lead-lag table retains every fixed pair/window; no best cell chosen, no causal claim, no model activated.']}
write('currency_comovement.json',currency_result)
print(json.dumps({'model_pairs':comparisons,'unanimous':model_result['unanimous'],
    'three_or_four_agree':model_result['three_or_four_agree'],'ties':model_result['two_two_ties'],
    'mean_probability_brier':model_result['mean_probability_brier'],
    'currency_common_bars':len(common),'currency_matrices':matrices,
    'lagged_correlations':lagged},allow_nan=False))
