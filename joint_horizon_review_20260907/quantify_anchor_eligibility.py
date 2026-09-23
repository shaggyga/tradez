"""Read-only timestamp and causal-news reconstruction audit; no fitting or orders."""
import collections,datetime as dt,hashlib,json,sys,time
from pathlib import Path
sys.dont_write_bytecode=True
ROOT=Path(r'C:\Users\zmoor\Documents\forex\trad');sys.path.insert(0,str(ROOT))
import oanda_causal_forecast_inputs_pair_v2 as price_inputs
import oanda_pair_local_models_v2 as price_model
import oanda_causal_forecast_inputs_joint_news_v1 as joint_inputs
import oanda_joint_price_news_models_v1 as joint_model
WORK=Path(__file__).resolve().parent
PAIRS=('EUR_DKK','GBP_HKD','GBP_PLN','GBP_SGD')
def sha(raw):return hashlib.sha256(raw).hexdigest()
def write_new(path,value):
    with path.open('x',encoding='utf8') as handle:json.dump(value,handle,indent=2,allow_nan=False)
def run():
    started=time.time();registry=json.loads((ROOT/'config/joint_price_news_study_v2_20260907.json').read_bytes())
    latest=max((ROOT/'data/oanda_training_manager/joint_price_news_study_v2/news_captures').glob('*.json.gz'),key=lambda p:p.stat().st_mtime_ns)
    raw=latest.read_bytes();descriptor={'news_capture_sha256':latest.name.removesuffix('.json.gz'),'news_capture_path':str(latest)}
    news=joint_inputs._load_news(descriptor)
    with (WORK/'RETAINED_NEWS_CAPTURE.json.gz').open('xb') as handle:handle.write(raw)
    rows_by_pair={};sets={};summaries={};all_anchors=set()
    for pair in PAIRS:
        pip=registry['pairs'][pair]['pip_size']
        capture=price_inputs.capture_inputs(ROOT/'data/oanda_training_manager/candles',pair,pip_size=pip)
        if capture['status']!='ready':raise ValueError('current_price_capture_not_ready:'+pair+':'+str(capture['reasons']))
        price_inputs.validate_capture(capture,instrument=pair,pip_size=pip)
        write_new(WORK/(pair+'_PRICE_CAPTURE.json'),capture)
        rows,_=price_inputs._verified_rows(capture,instrument=pair,pip_size=pip);cutoff=capture['reference_start_epoch']
        epochs=list(rows);sessions=price_model._sessions(epochs)
        exact=[t for t in epochs if t+3600<=cutoff and t+3600 in rows and sessions[t]==sessions[t+3600] and price_model._window_ready(price_model._window(epochs,sessions,t))]
        covered=[t for t in exact if news['history_start_epoch']<=t+60<=news['first_observed_epoch']]
        sets[pair]={'all_minutes':covered,'three_minute_grid':[t for t in covered if t%180==0],
                    'fifteen_minute_grid':[t for t in covered if t%900==0]}
        rows_by_pair[pair]=(capture,rows);all_anchors.update(covered)
        summaries[pair]={'pip_size':pip,'retained_real_rows':len(rows),'price_cutoff_epoch':cutoff,
            'price_observed_epoch':capture['first_observed_epoch'],'price_capture_sha256':capture['source_capture_sha256'],
            'same_session_exact_h1_valid_feature_anchors_full_price_history':len(exact),
            'full_price_three_minute_anchors':sum(t%180==0 for t in exact),
            'causal_news_covered_counts':{k:len(v) for k,v in sets[pair].items()},
            'excluded_before_news_history_count':sum(t+60<news['history_start_epoch'] for t in exact),
            'excluded_after_news_capture_count':sum(t+60>news['first_observed_epoch'] for t in exact)}
    print(json.dumps({'stage':'raw_eligibility','news_history_start_epoch':news['history_start_epoch'],'news_capture_observed_epoch':news['first_observed_epoch'],'pairs':summaries,'unique_news_reconstruction_anchors':len(all_anchors)}),flush=True)
    if len(all_anchors)>4096:raise ValueError('bounded_anchor_audit_required')
    frames={}
    for i,t in enumerate(sorted(all_anchors),1):
        frame=joint_inputs._frame(news,t+60)
        if frame['available_max_epoch']>t+60:raise ValueError('frame_future_availability')
        frames[t]=frame
        if i%200==0:print(json.dumps({'stage':'news_frame_replay','completed':i,'total':len(all_anchors)}),flush=True)
    for pair in PAIRS:
        features={t:joint_inputs._pair_features(frames[t],pair,t+60)[0] for t in sets[pair]['all_minutes']}
        groups={}
        for name,anchors in sets[pair].items():
            values=[features[t] for t in anchors]
            nonzero=sum(v[1]>0 for v in values);patterns=len({tuple(v[:4]) for v in values});vetted=sum(v[5]>0 for v in values)
            groups[name]={'anchors':len(anchors),'nonzero_context_rows':nonzero,'distinct_context_patterns':patterns,
                'vetted_rows':vetted,'passes_existing_48_12_8_counts':len(anchors)>=48 and nonzero>=12 and patterns>=8,
                'anchor_epochs':anchors,'feature_matrix_sha256':sha(joint_inputs._encoded(values)),
                'maximum_used_news_available_epoch':max((frames[t]['available_max_epoch'] for t in anchors),default=None)}
        summaries[pair]['replayed_news_feature_counts']=groups
    source_names=('oanda_causal_forecast_inputs_joint_news_v1.py','oanda_joint_price_news_models_v1.py',
                  'oanda_causal_forecast_inputs_pair_v2.py','oanda_pair_local_models_v2.py','oanda_news_causal_aggregation_guard_v1.py')
    result={'status':'passed','started_utc':dt.datetime.fromtimestamp(started,dt.timezone.utc).isoformat(),
        'finished_utc':dt.datetime.now(dt.timezone.utc).isoformat(),'pairs':summaries,
        'news_capture':{'original_path':str(latest),'file_sha256':sha(raw),'capture_sha256':descriptor['news_capture_sha256'],
                        'observed_epoch':news['first_observed_epoch'],'history_start_epoch':news['history_start_epoch'],'history_rows':len(news['history'])},
        'source_bindings':{n:sha((ROOT/n).read_bytes()) for n in source_names},'helper_sha256':sha(Path(__file__).read_bytes()),
        'news_reconstruction_anchor_count':len(frames),'model_fits':0,'forecast_publications':0,'runtime_writes':False,'orders_enabled':False,
        'limitations':['All anchors use actual observed closes and exact same-session t+3600 endpoints; no bars, price targets or article availability times were filled.',
            'Every retained candidate replays the frozen point-in-time news frame at t+60; only the study-specific15minute anchor subsampling was relaxed for this audit.',
            'Counts do not prove independent samples or predict improved accuracy. More overlapping labels require adjusted shrinkage and chronological purged validation.',
            'The48/12/8 minimum counts are preserved in the comparison; unseen-vetted-feature and current-freshness rules are separate checks.',
            'Price sources were independently observed after the existing immutable news capture. Only historical anchors within the captured news coverage were considered.']}
    write_new(WORK/'ANCHOR_ELIGIBILITY_AUDIT_20260907.json',result)
    print(json.dumps({'stage':'complete','path':str(WORK/'ANCHOR_ELIGIBILITY_AUDIT_20260907.json'),'pairs':{p:{k:{a:b for a,b in v.items() if a!='anchor_epochs'} for k,v in s['replayed_news_feature_counts'].items()} for p,s in summaries.items()}}),flush=True)
if __name__=='__main__':run()
