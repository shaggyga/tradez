"""Cold real operational owners with synthetic market evidence and a fit-only spy."""
from concurrent.futures import Future
import copy,csv,hashlib,json,math
from pathlib import Path
import pytest
import test_core_operational_v2 as fixture
import revision_joint_inputs_v3 as joint
import oanda_joint_price_news_forecast_study_v7 as worker
import oanda_causal_forecast_ledger_joint_news_v5 as ledger_module
import oanda_fixed_forecast_evaluation_joint_news_v3 as evaluator
import native_fixture_contract_v1 as contract_fixture
io=joint.news_io

class Clock:
    def __init__(self,minute):self.value=fixture.epoch(minute)
    def __call__(self):self.value+=.001;return self.value

class InlineExecutor:
    """Only scheduling transport is inline; production functions and futures stay real."""
    def __init__(self,*a,**k):pass
    def submit(self,function,*args,**kwargs):
        future=Future()
        try:future.set_result(function(*args,**kwargs))
        except BaseException as exc:future.set_exception(exc)
        return future
    def shutdown(self,*a,**k):pass

def registry(root,clock_path,pins):
    pairs={};dependencies=joint.original.dependency_versions()
    for pair in ('EUR_USD','GBP_USD'):
        metadata={'output_instrument':pair,'pip_size':.0001,'model_source_sha256':joint.BOUND_NUMERIC_SHA,
          'source_bindings':joint._bindings(),'dependency_versions':dependencies}
        contract=contract_fixture.make_contract(ledger_module,evaluator,metadata,clock_path,root/(pair+'_M1.csv'))
        family=joint.FAMILY;cohort='joint_price_news_native_v1_20260913.'+pair+'.'+family+'.'+worker.COHORT_SCOPE+'.fixture'
        contract.update(contract_id=cohort,source_bindings=pins,cohorts={family:cohort})
        contract['evaluation_protocol'].update(contract_id=cohort+'.evaluation',cohorts={family:cohort})
        pairs[pair]={'pip_size':.0001,'families':{family:{'contract':contract,'contract_sha256':worker.digest(contract)}}}
    return {'schema_version':worker.REGISTRY_SCHEMA,'registry_id':'joint_price_news_study_v7_20260913',
      'collection_enabled':True,'research_only':True,'source_bindings':pins,'dependency_versions':dependencies,'pairs':pairs,
      **{key:False for key in worker.INERT_FLAGS}}

def write_quote(path,clock):
    minute=(clock.value-fixture.epoch(0))/60
    value={'producer':'practice_007_dedicated_quote_stream','generated_utc':fixture.at(minute).isoformat(),
      'coverage':{'retained_last_known_instruments':[]},'quotes':{pair:{'instrument':pair,'source':'stream','tradeable':True,
        'time':fixture.at(minute).isoformat(),'pip':'.0001','bid':'1.40000000000000001','ask':'1.40020000000000001'}
        for pair in ('EUR_USD','GBP_USD')}}
    path.write_text(json.dumps(value),encoding='utf-8')

def test_actual_worker_quote_to_native_outcome_with_blocked_news(tmp_path,monkeypatch):
    source,config,transport_runner=fixture.make(tmp_path);owner=None
    try:
        for ordinal,origin in enumerate([15*i for i in range(48)]+[765]):
            fixture.add_article(source,ordinal,origin+.1+ordinal*.001)
            assert fixture.transport.run_cycle(transport_runner,clock_provider=fixture.proof_clock(origin+.5))['status']=='ready'
        fixture.health(config,766.1);session=io.create_session(config);clock=Clock(766.1)
        history_calls=[];historical=io.historical_features
        def one_history(*a,**k):history_calls.append((a,k));return historical(*a,**k)
        monkeypatch.setattr(io,'historical_features',one_history)
        news,share,capture_sha,serial=worker.capture_shared_owned(session,fixture.PAIRS,clock=clock)
        assert len(history_calls)==1
        manifest=json.loads((Path(__file__).parent/'SOURCE_KIT_INVENTORY_003.json').read_text(encoding='utf-8'))
        pins={row['name']:row['sha256'] for row in manifest['files']}
        price_root=tmp_path/'prices';price_path=fixture.prices(price_root)
        registered=registry(price_root,config['clock_path'],pins)
        registered.update(news_io_config_sha256=hashlib.sha256(io.encode(config)).hexdigest())
        registry_path=tmp_path/'registry.json';registry_path.write_bytes(worker.encoded(registered))
        assert worker.load_registry(registry_path)==registered
        quote_path=tmp_path/'quote.json';clock.value=fixture.epoch(766.15);write_quote(quote_path,clock)
        monkeypatch.setattr(worker,'ThreadPoolExecutor',InlineExecutor)
        owner=worker.PairRunner(registered,tmp_path/'study',price_root,quote_path,session=session,clock=clock,activate=True)
        owner.news_capture=news;owner.history_share=share
        clock.value=fixture.epoch(766.2);owner.poll_quotes()
        assert owner.current['GBP_USD'][joint.FAMILY]['bid']=='1.40000000000000001'
        now=clock();assert owner.schedule_capture(now,int(now//900))
        owner.finish_work()
        assert owner.states['EUR_USD']['capture'] is None and owner.states['EUR_USD']['capture_error']
        good=owner.states['GBP_USD']['capture'];assert good['readiness_status']=='ready',good
        assert len(good['news_frames'])==48 and good['reference_start_epoch']==int(fixture.epoch(765))
        assert owner.states['GBP_USD']['history_share'] is share
        module=joint._model();prediction_calls=[]
        def predict(rows,cutoff,**kwargs):
            readiness=module.family_readiness(rows,cutoff,**{k:v for k,v in kwargs.items() if k!='families'})
            assert readiness[joint.FAMILY]['ready'];origins=sorted(map(int,good['news_frames']))
            prediction_calls.append((copy.deepcopy(rows),cutoff,copy.deepcopy(kwargs)))
            diagnostics={'training_row_start_epochs_by_pair':{'GBP_USD':origins},
              'training_news_feature_cutoff_max_epoch':max(origins)+60,'training_label_maturity_max_epoch':max(origins)+3660,
              'matched_price_only_expected_pips':.25,'neutral_news_ablation_expected_pips':-.1}
            return {joint.FAMILY:(.5,.6,diagnostics)},readiness
        monkeypatch.setattr(module,'predict_with_readiness',predict)
        clock.value=fixture.epoch(766.3);now=clock();assert owner.schedule_fit(now,int(now//900))
        owner.finish_work();slot=owner.states['GBP_USD']['families'][joint.FAMILY];ledger=slot['ledger']
        assert ledger.counts()['forecasts']==1,slot['reason']
        assert ledger.counts()['publication']==ledger.counts()['consumption']==1
        assert len(prediction_calls)==1 and len(history_calls)==1
        publication=worker.verified_publication(ledger);arm=publication['forecasts'][0]
        assert publication['reference_epoch']==fixture.epoch(766) and publication['target_epoch']==fixture.epoch(826)
        assert publication['reference_quote_role']=='current_context_not_native_model_anchor'
        rows,_=joint.price_inputs._verified_rows(good['price_capture'])
        assert arm['native_anchor']['origin_mid_hex']==rows[int(fixture.epoch(765))].hex()
        assert float(arm['reference_mid'])!=1.4001 and arm['probability_up']==.6
        assert arm['news_revision_evidence']['news_capture_descriptor']['schema_version']==io.DESCRIPTOR
        # New transport failure invalidates the old three-handle generation; forecasts remain retained.
        failed=fixture.transport.run_cycle(transport_runner,clock_provider=fixture.proof_clock(766.5),
          stop_requested=lambda:(_ for _ in ()).throw(ValueError('owned transport failure after forecast')))
        assert failed['status']=='failed'
        fixture.health(config,766.6)
        with pytest.raises(io.HealthFailure):io.reobserve_before_issue(session,news,clock=lambda:fixture.epoch(766.6))
        owner.observe_news_failure();assert owner.news_capture is None and owner.history_share is None
        assert ledger.counts()['forecasts']==1
        # Source ingestion/settlement remains independent of news and current quotes.
        original=price_path.read_bytes();price_path.write_bytes(original+b'broken,partial\n')
        clock.value=fixture.epoch(826.2);fixture.health(config,826.2,status='error')
        for _ in range(3):owner.score()
        assert slot['native_outcome_status']=='refused'
        assert slot['native_outcome_reason'] and slot['scored_outcomes']==0
        refused_reason=slot['native_outcome_reason']
        price_path.write_bytes(original)
        with price_path.open('a',encoding='utf-8',newline='') as stream:
            writer=csv.writer(stream)
            for minute in range(766,826):
                writer.writerow([fixture.at(minute).isoformat(),'GBP_USD','M1','true',1.2+.000001*(minute+20)+.00001*math.sin(minute/15)])
        clock.value+=31;fixture.health(config,(clock.value-fixture.epoch(0))/60,status='error')
        for _ in range(3):owner.score()
        assert slot['scored_outcomes']==1,slot
        assert ledger.counts()['outcomes']==1 and owner.news_capture is None
        dataset,protocol=ledger.export_evaluation();score=evaluator.evaluate(dataset,protocol)
        assert score['native_scores']['n']==1 and score['forecast_count']==1
        assert score['account_return'] is None and score['preissue_native_comparisons']
        owner.publish_status(force=True)
        summary=worker.build_summary(registered,owner.states,clock(),published_only=False) if False else owner.summary
        report={'status':'actual_owned_worker_publication_native_outcome_recovery_passed','observations':49,
          'history_pairs':68,'history_batches':len(history_calls),'training_contexts':48,
          'prediction_calls':len(prediction_calls),'actual_fits':0,'temporary_activated_native_ledgers':2,
          'original_market_data':False,'real_runtime_activation':False,'forecast_count':1,'native_outcomes':1,
          'missing_pair_error':owner.states['EUR_USD']['capture_error'],'native_refusal_reason':refused_reason,
          'native_origin_epoch':publication['reference_epoch'],'native_target_epoch':publication['target_epoch'],
          'native_origin_mid_hex':arm['native_anchor']['origin_mid_hex'],'source_capture_sha256':good['source_capture_sha256'],
          'original_news_capture_sha256':capture_sha,'original_failure_serial':serial,
          'failure_serial_after_transport_failure':io.session_status(session)['failure_serial'],
          'news_blocked_during_outcome_recovery':owner.news_capture is None,'native_score':worker.jsonable(score),
          'summary_published':owner.published_summary is not None}
        (tmp_path/'FULL_OPERATIONAL_PROOF.json').write_text(json.dumps(report,indent=2),encoding='utf-8')
    finally:
        if owner is not None:owner.close()
        fixture.transport.close_runner(transport_runner)

def test_exact_quote_representation_helpers_are_original_owners():
    import oanda_fixed_forecast_evaluation_joint_news_v1 as original
    assert worker.loads_exact_prices is original.loads_exact_prices and worker.jsonable is original.jsonable
    parsed=worker.loads_exact_prices('{"bid":1.40000000000000001,"clock":1.25}')
    assert str(parsed['bid'])=='1.40000000000000001'
