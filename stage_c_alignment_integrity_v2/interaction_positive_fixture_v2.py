"""Balanced interaction-only synthetic mechanism, distinct from market evidence."""
import math
from contracts import fingerprint
from matched_campaign_models_v2 import contract as original_contract,fit_pair as baseline_fit,predict as baseline_predict,FEATURES
from matched_view_models_v2 import fit_pair
from rich_family_models_v2 import predict

def build():
    start=1704067200;cut=start+64*86400;group='synthetic_interaction_only';pairs=['AUD_USD','EUR_USD','GBP_USD','USD_JPY'];corners=[(-1.,-1.),(-1.,1.),(1.,-1.),(1.,1.)]
    c={**original_contract(),'training_start':start,'fit_cutoffs':[cut],'evaluation_asof':start+80*86400,'groups':[group]}
    obs=[];labels=[];views={};names=FEATURES+['synthetic_axis_product']
    target={'target_id':'technical_endpoint_midpoint_elapsed_15m','horizon_seconds':900}
    for day in range(72):
        origin=start+day*86400+60
        for index,pair in enumerate(pairs):
            a,b=corners[(index+day)%4];rid=f'{pair}:{origin}';values=[a,b]+[0.]*24
            row={'record_id':rid,'instrument':pair,'origin_epoch':origin,'available_epoch':origin,'features':values};obs.append(row)
            labels.append({'record_id':rid,'target_id':target['target_id'],'label_end_epoch':origin+900,'available_epoch':origin+900,'value':a*b})
            views[rid]={group:{'record_id':rid,'original_record_sha256':fingerprint(row),'feature_names':names,'values':values+[a*b],'shared_legacy_population_eligible':True}}
    base,tree=baseline_fit(obs,labels,pairs,15,cut,c);meta,models=fit_pair(obs,labels,views,group,names,15,cut,base,c)
    held=[r for r in obs if r['origin_epoch']>=cut];bf,_=baseline_predict(base,tree,held,procedure='frozen',c=c);af,_=predict(meta,models,held,views,procedure='frozen',c=c)
    lookup={o['record_id']:o['value'] for o in labels};metrics=[]
    for family,rows in [('marginal_baseline',bf),('explicit_interaction',af)]:
        for method in ('ridge','recovered_hgb'):
            selected=[r for r in rows if r['method']==method];squares=[(r['forecast']['prediction']-lookup[r['record_id']])**2 for r in selected]
            metrics.append({'family':family,'method':method,'rows':len(squares),'mse':math.fsum(squares)/len(squares),'predictions':[r['forecast']['prediction'] for r in selected]})
    train=[r for r in obs if r['origin_epoch']<cut];cross=[math.fsum(r['features'][j]*lookup[r['record_id']] for r in train)/len(train) for j in (0,1)]
    baseline=next(x for x in metrics if x['family']=='marginal_baseline' and x['method']=='ridge');interaction=next(x for x in metrics if x['family']=='explicit_interaction' and x['method']=='ridge')
    expected=(20/(len(train)+20))**2
    if cross!=[0.,0.] or baseline['mse']!=1. or not math.isclose(interaction['mse'],expected,rel_tol=1e-12,abs_tol=1e-14):raise ValueError('interaction_only_positive_fixture_not_detected')
    return {'scope':'synthetic_algorithm_positive_control_not_market_evidence','axis_meaning':'balanced_synthetic_bits_not_actual_technical_feature_values','training_rows':len(train),'held_forward_rows':len(held),'fit_cutoff':cut,'marginal_feature_outcome_cross_moments':cross,'marginal_screen_used':False,
        'ridge_interaction_mse_oracle':expected,'metrics':metrics,'synthetic_learner_fits':4,'baseline_fit_id':base['fit_id'],'interaction_fit_id':meta['fit_id'],'training_population_sha256':meta['training_population_sha256'],'model_hashes':meta['model_sha256'],'positive_control_detected':True,'independent_market_confirmation':False}
