"""Full declared report grid, deliberately synthetic forecasts; no estimators."""
from copy import deepcopy
import pytest
from contracts import fingerprint
from rich_dependence_report_v2 import build

class Reader:
    def __init__(self):
        self.data={};groups=['compact38_cost2','compact50_cost2','full228_cost2'];horizons=[15,60,240,720,1440,2880,7200];procedures=['frozen','adaptive'];methods=['ridge','recovered_hgb']
        origins=list(range(1721606460,1722038400,21600));pairs=['EUR_USD','GBP_USD'];asof=1722556800
        self.configuration={'universe':pairs,'block_lengths':[4,8,20],'replicates':100,'seed':20260922}
        c={'groups':groups,'horizon_minutes':horizons,'procedures':procedures,'methods':methods,'evaluation_asof':asof};self.data['family','experiment_contract.json']=c
        base=[];bs=[];cs=[];inventory=[];old=[]
        controls={'zero':0.,'history_mean':2.,'ridge':1.,'recovered_hgb':2.5}
        target=lambda h:f'technical_endpoint_midpoint_elapsed_{h}m'
        record=lambda pair,t:pair+':'+str(t)
        def f(pair,t,h,value):return {'instrument':pair,'decision_epoch':t,'available_epoch':t+2,'target_id':target(h),'prediction':value}
        for pair in pairs:
            self.data['rich','outcomes_'+pair+'.json']={'outcomes':[{'record_id':record(pair,t),'target_id':target(h),'value':2.,'available_epoch':t+h*60,'label_end_epoch':t+h*60} for t in origins for h in horizons]}
        support=fingerprint(sorted(record(pair,t) for t in origins for pair in pairs))
        for h in horizons:
            for cutoff in [1721606400,1721779200]:
                old.append({'target':{'target_id':target(h)},'fit_cutoff':cutoff,'ready_epoch':cutoff+30,'fit_id':fingerprint(['old',h,cutoff]),'training_population_sha256':fingerprint(['population',h,cutoff]),'training_rows':100})
            for procedure in procedures:
                for method,value in controls.items():
                    base.extend({'record_id':record(pair,t),'method':method,'procedure':procedure,'forecast':f(pair,t,h,value)} for t in origins for pair in pairs)
                    bs.append({'target_id':target(h),'procedure':procedure,'method':method,'rows':40,'support_sha256':support,'mae_bps':abs(value-2),'mse_bps2':(value-2)**2})
                for group in groups:
                    rows=[]
                    for method in methods:
                        value=3. if method=='ridge' else 2.25
                        rows.extend({'record_id':record(pair,t),'group':group,'method':method,'procedure':procedure,'forecast':f(pair,t,h,value)} for t in origins for pair in pairs)
                        cs.append({'group':group,'target_id':target(h),'procedure':procedure,'method':method,'rows':40,'support_sha256':support,'mae_bps':abs(value-2),'mse_bps2':(value-2)**2})
                    self.data['family',f'forecasts_{group}_{h}_{procedure}.json']=rows
            for group in groups:
                for cutoff in [1721606400,1721779200]:
                    inventory.append({'group':group,'target':{'target_id':target(h)},'fit_cutoff':cutoff,'ready_epoch':cutoff+30,'fit_id':fingerprint([group,h,cutoff]),'training_population_sha256':fingerprint(['population',h,cutoff]),'training_rows':100,'model_sha256':{m:fingerprint([group,h,cutoff,m]) for m in methods},'fitted_transform':{'sha256':fingerprint([group,h,cutoff,'transform']),'raw_width':40,'transformed_width':50}})
        self.data['baseline','forecasts.json']=base;self.data['baseline','scores.json']=bs;self.data['family','scores.json']=cs
        self.data['family','model_inventory.json']=inventory;self.data['baseline','model_inventory.json']=old;self.data['rich','lineage_summary.json']={'scope':'synthetic_fixture_not_real_prior_evidence'}
    def read(self,alias,name):return self.data[alias,name]
    def source(self,alias,name,record):return {'dependency':alias,'payload':name,'original_record_sha256':fingerprint(record),'synthetic':True}

def test_full_synthetic_grid_uses_joint_indices_and_all_attempts():
    reader=Reader();before=deepcopy(reader.data);out=build(reader,reader.configuration);assert reader.data==before
    r=out['run_report.json'];assert r['paired_comparisons']==336 and r['origin_panel_rows']==6720 and r['block_sensitivity_rows']==1008 and r['models_fitted']==0
    assert r['assessment_origin_counts']==[20] and r['assessment_date_counts']==[5] and r['effective_sample_size'] is None
    assert out['attempt_ledger.json']['new_fixed_model_fits']==84 and out['attempt_ledger.json']['reused_baseline_fits']==28
    assert len({x['model_id'] for x in out['attempt_ledger.json']['attempts']})==112
    rows=out['block_sensitivity.json']
    for length in (4,8):assert len({x['indices_sha256'] for x in rows if x['block_length_origins']==length})==1
    assert all(x['interval'] is None for x in rows if x['block_length_origins']==20)
    zero=next(x for x in out['paired_comparison_scope.json'] if x['method']=='ridge' and x['control']=='zero')
    assert zero['paired_deltas']=={'mae_bps':-1.,'mse_bps2':-3.}

def test_original_score_corruption_refused():
    reader=Reader();reader.data['family','scores.json'][0]['mae_bps']+=1
    with pytest.raises(ValueError,match='scalar_score_reconciliation'):build(reader,reader.configuration)

def test_model_attempt_collision_refused():
    reader=Reader();reader.data['family','model_inventory.json'][1]=reader.data['family','model_inventory.json'][0]
    with pytest.raises(ValueError,match='exact_current_attempt_inventory'):build(reader,reader.configuration)
