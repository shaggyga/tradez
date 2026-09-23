"""Native owner/publication fixtures with real SQLite and original clock policy.

Only opaque input computation is a spy; no numerical model is fitted/imported.
Exact M1 capture, store, publication, clock validation and score replay are real.
"""
from pathlib import Path
from types import SimpleNamespace
from copy import deepcopy
import ast,datetime as dt,email.utils,hashlib,json,math,tempfile,threading,unittest
from unittest.mock import patch
import oanda_causal_forecast_ledger_joint_news_v5 as ledger
import oanda_fixed_forecast_evaluation_joint_news_v3 as ev
import native_m1_outcome_v1 as o
import native_m1_ledger_v1 as store
from test_native_outcome_store_v1 import policy,line
HERE=Path(__file__).parent;TRAD=HERE.parents[2]/'trad'

def helpers():
    path=TRAD/'test_oanda_causal_forecast_ledger_joint_news_v3.py';tree=ast.parse(path.read_text(encoding='utf-8'))
    nodes=[n for n in tree.body if isinstance(n,(ast.ClassDef,ast.FunctionDef)) and n.name in ('Clock','contract_fixture','quote','ready_attempt')]
    env={'deepcopy':deepcopy,'SCHEMA':ledger.SCHEMA,'PROTOCOL_SCHEMA':ev.PROTOCOL_SCHEMA,'digest':ledger.digest,'ledger_module':ledger}
    exec(compile(ast.Module(nodes,type_ignores=[]),str(path),'exec'),env)
    path=TRAD/'oanda_local_news_sentiment_repair_v2.py';tree=ast.parse(path.read_text(encoding='utf-8'))
    nodes=[n for n in tree.body if isinstance(n,ast.FunctionDef) and n.name in ('encoded','digest','epoch','validate_clock_state')]
    clock_env={'math':math,'json':json,'hashlib':hashlib,'dt':dt,'email':email,
        'CLOCK_CONTRACT':'repaired_news_synchronized_host_attestation_v2_20260912'}
    exec(compile(ast.Module(nodes,type_ignores=[]),str(path),'exec'),clock_env)
    return env,clock_env

class NativeOwner(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.root=Path(self.tmp.name);self.fx,self.clock_env=helpers()
        self.clock=self.fx['Clock']();self.session=object();self.news=object();self.history=object();self.lock=threading.RLock()
        self.cutoff=int(self.clock.value)-60;self.rows={self.cutoff:1.1};self.events=[]
        def eligible(s,c):
            assert s is self.session and c is self.news and self.lock._is_owned();self.events.append('eligible')
        def validate(capture,**kw):
            assert kw['session'] is self.session and kw['news_capture'] is self.news and kw['history_share'] is self.history
            assert self.lock._is_owned();self.events.append('validate')
        def health(s,c,*,clock):
            eligible(s,c);at=clock()
            return {'health_proof_sha256':'f'*64,'observed_epoch':at,'readback_observed_epoch':at,
                'scope':'current_health_only_retained_input_expiry_guards_still_required','research_only':True,
                **{k:False for k in ('can_place_orders','can_promote','can_authorize','account_eligible','proof_eligible','joint_model_consumption_proven','execution_eligible')}}
        def read_json(path,limit):
            raw=Path(path).read_bytes();assert len(raw)<=limit;value=json.loads(raw)
            return value,{'path':str(path),'sha256':hashlib.sha256(raw).hexdigest(),'raw_utf8':raw.decode(),'value':value}
        self.io=SimpleNamespace(_session=lambda s:{'lock':self.lock},_eligible=eligible,encode=o.encode,
            reobserve_before_issue=health,read_json=read_json,strict_health_json=json.loads,
            repair=SimpleNamespace(validate_clock_state=self.clock_env['validate_clock_state']))
        owner=SimpleNamespace(news_io=self.io,validate_capture=validate,
            price_inputs=SimpleNamespace(_verified_rows=lambda p:(self.rows,None)))
        self.patcher=patch.object(ledger,'_bound_inputs',lambda:owner);self.patcher.start()
        self.contract=self.fx['contract_fixture']();p=self.contract['evaluation_protocol']
        cohort='joint_price_news_native_v1_20260913.EUR_USD.ridge_price_news_v1.fixture'
        self.contract['cohorts']={'ridge_price_news_v1':cohort};p['cohorts']=deepcopy(self.contract['cohorts'])
        p.update(native_target_recipe='exact_completed_M1_close_at_origin_bar_start_plus_3600',
            native_outcome_recipe=o.SCORE_RECIPE,executable_quote_policy=None,baselines=['fair_coin','zero_move','no_trade'],
            native_outcome_policy=policy())
        self.clockpath=self.root/'clock.json';self.refresh_clock()
        self.contract.update(native_clock_path=str(self.clockpath),native_candle_path=str(self.root/'EUR_USD_M1.csv'),native_outcome_policy=policy(),
            native_source_bindings=ledger.native_source_bindings())
        self.owner=ledger.CausalForecastLedger(self.root/'owned.sqlite',self.contract,clock=self.clock,activate=True)
        self.a,self.q,self.c,self.r=self.fx['ready_attempt'](self.owner,self.clock)
        self.c.update(reference_start_epoch=self.cutoff,max_bar_close_epoch=self.cutoff+60,
            price_capture={'first_observed_epoch':self.c['first_observed_epoch']-.5},feature_decision_epoch=self.c['first_observed_epoch'],
            news_capture_descriptor={'schema_version':'revision_news_fixed_io_descriptor_v1_20260913','capture_sha256':'e'*64,'capture_path':str(self.root/'capture.json')},
            news_context_sha256='b'*64,source_bindings={'fixed':'a'*64},training_news_points=[],
            history_share_descriptor={'schema_version':'fixture','history_share_sha256':'1'*64,'context_sha256':'b'*64,
                'capture_sha256':'e'*64,'source_generation_sha256':'c'*64})
        self.c['current_news_point']={'decision_epoch':self.c['feature_decision_epoch'],'context_sha256':'b'*64,
            'coverage_usable':True,'source_generation_sha256':'c'*64,'transport_timing_sha256':'d'*64,'features':[0.]*8}
        self.r['predictions']['ridge_price_news_v1']['diagnostics']['training_label_maturity_max_epoch']=self.cutoff+60
        self.c['source_capture_sha256']=ledger.digest({k:v for k,v in self.c.items() if k!='source_capture_sha256'})
        self.r['source_capture_sha256']=self.c['source_capture_sha256']
        self.csv=self.root/'EUR_USD_M1.csv'
    def tearDown(self):self.owner.close();self.patcher.stop();self.tmp.cleanup()
    def refresh_clock(self):
        state={'generated_utc':dt.datetime.fromtimestamp(self.clock(),dt.timezone.utc).isoformat(),'status':'ok',
            'timestamp_normalization_trusted':True,'host_clock_synchronized':True,'clock_discontinuity_active':False,
            'broker_clock_lead_sec':0.,'broker_clock_sample_count':32,'source_age_sec':0.,'source_fresh':True}
        self.clockpath.write_text(json.dumps(state),encoding='utf-8')
    def issue(self,**kw):return self.owner.issue(self.a,self.c,self.r,session=self.session,news_capture=self.news,history_share=self.history,**kw)
    def mature(self):
        self.clock.value=float(self.cutoff+4000);self.refresh_clock()
        self.csv.write_text('datetime,close,complete\n'+line(self.cutoff,1.1)+line(self.cutoff+3600,1.1002),encoding='utf-8')
    def test_issue_target_and_delta_use_source_anchor(self):
        identity=self.issue();self.owner.consume_publications();value=self.owner.verified_native_publication()
        self.assertEqual(value['decision_id'],identity);self.assertEqual(value['target_epoch'],self.cutoff+3660)
        self.assertNotEqual(value['target_epoch'],self.q['market_epoch']+3600)
        self.assertEqual(value['forecasts'][0]['native_anchor']['origin_mid_hex'],(1.1).hex())
        self.assertTrue(value['publication_verified']);self.assertTrue(value['consumption_verified'])
    def test_all_three_original_opaque_handles_required(self):
        with self.assertRaises(AssertionError):self.owner.issue(self.a,self.c,self.r,session=self.session,news_capture=self.news,history_share=object())
        self.assertEqual(self.owner.counts()['forecasts'],0)
    def test_full_native_source_to_score_and_export(self):
        self.issue();self.owner.consume_publications();self.mature()
        self.assertTrue(self.owner.native_source_due())
        self.assertEqual(self.owner.observe_native_source(self.csv,clock_path=self.clockpath)['status'],'admitted_exact_source')
        self.assertEqual(self.owner.settle(),1);self.assertFalse(self.owner.native_source_due())
        data,protocol=self.owner.export_evaluation();result=ev.evaluate(data,protocol)
        self.assertEqual(result['native_scores']['n'],1);self.assertEqual(result['status_counts'],{'scored':1})
        self.assertIsNone(result['account_return']);self.assertEqual(result['collection_counts']['outcomes'],1)
    def test_idle_never_opens_csv(self):
        self.issue();self.owner.consume_publications()
        with patch.object(o,'capture_source',side_effect=AssertionError('no_due_read')):
            self.assertEqual(self.owner.observe_native_source(self.csv,clock_path=self.clockpath)['status'],'idle_no_unresolved_mature_target')
    def test_late_consumption_preserves_unknown_without_poll(self):
        self.issue();self.mature();self.owner.consume_publications()
        self.assertIsNone(self.owner.verified_native_publication());self.assertFalse(self.owner.native_source_due())
        data,protocol=self.owner.export_evaluation();result=ev.evaluate(data,protocol)
        self.assertEqual(result['forecast_count'],1);self.assertEqual(result['status_counts']['unknown'],1)
    def test_source_clock_stale_failure_then_newproof_recovers(self):
        self.issue();self.owner.consume_publications();self.mature();self.clock.advance(100)
        self.assertEqual(self.owner.observe_native_source(self.csv,clock_path=self.clockpath)['status'],'refused')
        self.refresh_clock();self.assertEqual(self.owner.observe_native_source(self.csv,clock_path=self.clockpath)['status'],'admitted_exact_source')
        self.assertEqual(self.owner.settle(),1)
    def test_export_recomputes_score_and_never_uses_live_targetquote(self):
        self.issue();self.owner.consume_publications();self.mature();self.fx['quote'](self.owner,self.clock,bid='9',ask='10')
        self.owner.observe_native_source(self.csv,clock_path=self.clockpath);self.owner.settle()
        data,p=self.owner.export_evaluation();score=data['native_scores'][0]['outcome']['score']
        self.assertEqual(score['target_mid_hex'],(1.1002).hex());self.assertEqual(ev.evaluate(data,p)['native_scores']['n'],1)
    def test_native_news_expiry_and_precommit_remain_binding(self):
        self.c['news_expires_epoch']=self.clock.value-1;self.r['news_expires_epoch']=self.c['news_expires_epoch']
        with self.assertRaisesRegex(ValueError,'expiry'):self.issue()
        self.assertEqual(self.owner.counts()['forecasts'],0)
    def test_reopen_schema_and_capacity_census(self):
        self.owner.close()
        self.owner=ledger.CausalForecastLedger(self.root/'owned.sqlite',self.contract,clock=self.clock)
        self.assertEqual(self.owner.counts()['forecasts'],0)
    def test_closed_world_native_model_pins_before_db_write(self):
        bad=deepcopy(self.contract);bad['native_source_bindings']['native_m1_outcome_v1.py']='0'*64
        path=self.root/'bad.sqlite'
        with self.assertRaisesRegex(ValueError,'source_contract'):ledger.CausalForecastLedger(path,bad,clock=self.clock,activate=True)
        self.assertFalse(path.exists())
    def test_evaluator_refuses_probability_and_native_target_tamper(self):
        self.issue();self.owner.consume_publications();data,p=self.owner.export_evaluation()
        d=data['publications'][0]['forecast'];arm={**d['forecasts'][0],'committed_available_epoch':data['publications'][0]['consumed_epoch']}
        self.assertEqual(ev.forecast_errors(arm,d,p),[])
        for key,value in [('probability_up',.5),('target_epoch',arm['target_epoch']+60),('reference_mid','1.2')]:
            with self.subTest(key=key):
                changed={**arm,key:value};self.assertTrue(ev.forecast_errors(changed,d,p))
