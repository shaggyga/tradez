from pathlib import Path
from copy import deepcopy
import json,sys
import pytest
R=Path(__file__).resolve().parents[1];sys.path[:0]=[str(R/'tools'),str(R/'tests')]
import forex_retained_management_contract as m
import test_retained_combined_observation as old


def fixture(tmp_path):
 meta,d=old.old.fixture(tmp_path);d=json.loads(json.dumps(d).replace('AAA_AAB','EUR_USD'));reg=d['registry.json'];reg['connections'][0].update(kind='legacy26_matched',arm='ridge',feature_names=['x'])
 reg['connections'].append({**reg['connections'][0],'id':'old_saved','horizon_minutes':7})
 rid=m.prior.sha(m.prior.encoded(reg));meta['registry_sha256']=rid
 current=d['current.json'];current.pop('payload_sha256');current.update(registry_sha256=rid,connections=reg['connections'])
 for f in current['forecasts']:f['registry_sha256']=rid
 original=[]
 for a in d['anchors.json']:
  f=json.loads(a['body'])['forecast'];f.update(connection='old_saved',horizon_minutes=7,target_epoch=10300,registry_sha256=rid);original.append(f)
 pub={**deepcopy(current),'generated_epoch':9882.,'forecasts':original};before=m.prior.sha(m.prior.encoded(pub))
 for a,f in zip(d['anchors.json'],original):
  a.update(id=m.prior.tracking.prediction_key(f),registry=rid,connection='old_saved',horizon=7,target=10300,body=json.dumps(dict(forecast=f,publication_epoch=9882.,publication_sha256=before,first_observed_epoch=9883.)))
 current['coverage'] += [dict(connection='old_saved',instrument=p,status='unavailable') for p in reg['pairs']]
 after=m.prior.sha(m.prior.encoded(current));d={k:v for k,v in d.items() if not k.startswith('issue_')}
 d.update({'issue_'+before+'.json':pub,'issue_'+after+'.json':deepcopy(current)});current['payload_sha256']=after
 path=tmp_path/'qualified_input';pin=old.old.old.save(path,meta,d);c=m.combined.forecasts.VerifiedCapture.load(path,pin)
 (tmp_path/'ordinary').mkdir();_,_,r=old.fixture(tmp_path/'ordinary');r.update(capture_manifest_sha256=pin,requested_targets={b['connection']+'/'+b['instrument']:10300 for b in c.population})
 r['quote_snapshot']['instruments']=sorted(c.registry['pairs']);r['quote_snapshot']['quotes'].pop('AAA_AAB');raw=json.dumps(dict(type='PRICE',instrument='EUR_USD',time='1970-01-01T02:46:41Z',bids=[dict(price='1.1000')],asks=[dict(price='1.1002')],tradeable=True)).encode();r['quote_snapshot']['quotes']['EUR_USD']=m.combined.quotes.parse_price(raw,received_epoch=10001.5,generation=2,session_id='fixture');old.seal_snapshot(r)
 rows,_=m.combined.observe(c,r);row=next(x for x in rows if x['instrument']=='EUR_USD' and x['connection']=='saved');key='saved/EUR_USD'
 q=dict(schema=m.SCHEMA,source_bindings=m.sources(),evidence_tier='synthetic_fixture',combined_request=r,contracts={},evidence_records={})
 def add(v):
  h=m.prior.sha(m.prior.encoded(v));q['evidence_records'][h]=v;return h
 anchor=c.receipts['first_observed','old_saved','EUR_USD']
 state=dict(status='OPEN',pending_orders=[],state_asof_epoch=10002,base_units=100,side=1,provenance='synthetic_fixture',account_currency='USD',position_id='synthetic_position',entry_thesis_sha256=add({'thesis':'fixture'}),entry_information_sha256=add({'information':'fixture'}),invalidation_rule='hard risk first',paid_costs_usd='1',mfe_usd='2',mae_usd='1',exposure_usd='110',liquidation_wealth_usd='10000',liquidation_convention='current_executable_liquidation_net_close_cost',instrument='EUR_USD',entry_receipt_sha256=anchor['receipt_sha256'],entry_epoch=9884)
 statepin=add(state);riskpin=add(dict(state_sha256=statepin,decision_epoch=10002,hard_exit_required=False,maximum_notional_usd='1000',available_margin_usd='100',required_margin_usd='10',maximum_loss_usd='50'))
 branches={}
 for action,gross in [('HOLD','4'),('EXIT','0'),('REPLACE','8')]:
  b=dict(action=action,state_sha256=statepin,common_terminal_epoch=10300,decision_epoch=10002,baseline_wealth_usd='10000',continuation_policy='cash_through_terminal' if action=='EXIT' else 'declared_fixture',economic_convention='increment_from_current_liquidation_future_costs_once',sunk_costs_recharged=False,spread_in_gross=True,slippage_convention='separate_future_cost',future_costs_usd=dict(fees='0',slippage='0',financing_debit='1' if action!='EXIT' else '0',financing_credit='0'),financing_scope='through_common_terminal_including_rollover',economic_evidence_sha256=add({'assumption':'synthetic known costs'}),gross_quote_increment=gross,quote_currency='USD',receipt_sha256=row['receipt_sha256'],input_hash=row['candidate']['input_hash'],model_definition_sha256=row['candidate']['model_definition_sha256'],quote_id=row['quote_id'],value_method='fresh_conditional_continuation',value_available_epoch=10002,value_reference_epoch=10000,base_units=100,side=1,notional_usd='110',required_margin_usd='10',worst_loss_usd='20')
  branches[action]=add(b)
 q['contracts'][key]=dict(state_sha256=statepin,risk_sha256=riskpin,incumbent=key,common_terminal_epoch=10300,branches=branches)
 return c,path,q,key


def replace_record(q,contract,field,changes):
 oldpin=contract[field];v={**q['evidence_records'][oldpin],**changes};pin=m.prior.sha(m.prior.encoded(v));q['evidence_records'][pin]=v;contract[field]=pin


def test_actual_join_and_cost_controls(tmp_path):
 c,_,q,key=fixture(tmp_path);rows,report=m.inspect(c,q);r=next(x for x in rows if x['slot']==key)
 assert r['status']=='declared_contract_consistent',r
 assert [r['values'][a]['net_usd'] for a in ['HOLD','EXIT','REPLACE']]==['3','0','7']
 assert r['switch_advantage_usd']=='4' and not r['action_eligible'] and not r['manager_activation']
 assert report['population']==136 and report['statuses']['prerequisites_missing']==135


@pytest.mark.parametrize('change', ['missing_state','pending','stale_state','target','anchor','risk','missing_cost','rebased','future','sunk','identity','margin','float_cost','missing_cash','bad_value_clock','tamper'])
def test_actual_consumer_refuses_missing_and_mismatched_contracts(tmp_path,change):
 c,_,q,key=fixture(tmp_path);s=q['contracts'][key];branches=s['branches']
 if change=='missing_state':s['state_sha256']='missing'
 elif change=='pending':replace_record(q,s,'state_sha256',{'pending_orders':['exit']})
 elif change=='stale_state':replace_record(q,s,'state_sha256',{'state_asof_epoch':10001})
 elif change=='target':s['common_terminal_epoch']=10301
 elif change=='anchor':replace_record(q,s,'state_sha256',{'entry_receipt_sha256':'absent'})
 elif change=='risk':replace_record(q,s,'risk_sha256',{'state_sha256':'wrong'})
 elif change=='missing_cost':replace_record(q,branches,'HOLD',{'future_costs_usd':{}})
 elif change=='rebased':replace_record(q,branches,'HOLD',{'value_method':'original_minus_realized'})
 elif change=='future':replace_record(q,branches,'HOLD',{'value_available_epoch':10003})
 elif change=='sunk':replace_record(q,branches,'HOLD',{'sunk_costs_recharged':True})
 elif change=='identity':replace_record(q,branches,'REPLACE',{'input_hash':'wrong'})
 elif change=='margin':replace_record(q,branches,'REPLACE',{'required_margin_usd':'10000'})
 elif change=='float_cost':replace_record(q,branches,'HOLD',{'gross_quote_increment':4.0})
 elif change=='missing_cash':del branches['EXIT']
 elif change=='bad_value_clock':replace_record(q,branches,'HOLD',{'value_available_epoch':9999})
 elif change=='tamper':q['evidence_records'][branches['HOLD']]['gross_quote_increment']='99'
 if change=='tamper':
  with pytest.raises(ValueError,match='evidence_record_hash'):m.inspect(c,q)
 else:
  rows,_=m.inspect(c,q);r=next(x for x in rows if x['slot']==key);assert r['status']=='prerequisites_refused',r


def test_risk_veto_no_action_and_strict_population(tmp_path):
 c,_,q,key=fixture(tmp_path);s=q['contracts'][key];replace_record(q,s,'risk_sha256',{'hard_exit_required':True})
 rows,_=m.inspect(c,q);r=next(x for x in rows if x['slot']==key)
 assert r['hard_exit_required'] and not r['optional_rotation_permitted_by_risk'] and not r['action_eligible']
 q['contracts']['bogus']=s
 with pytest.raises(ValueError,match='contract_population'):m.inspect(c,q)


def test_entrypoint_resume_identity_and_payload_corruption(tmp_path):
 _,path,q,_=fixture(tmp_path);req=tmp_path/'REQUEST.json';raw=m.prior.encoded(q);req.write_bytes(raw);pin=m.prior.sha(raw);out=tmp_path/'out'
 assert m.run(path,req,pin,out,'full')['status']=='completed'
 assert m.run(path,req,pin,out,'resume',max_new=1)['status']=='checkpointed'
 assert m.run(path,req,pin,out,'resume',resume=True)['status']=='completed'
 for p in (out/'full').glob('rows_*.json'):assert p.read_bytes()==(out/'resume'/p.name).read_bytes()
 assert (out/'full/REPORT.json').read_bytes()==(out/'resume/REPORT.json').read_bytes()
 with pytest.raises(ValueError,match='request_external_hash'):m.run(path,req,'0'*64,out,'bad')
 (out/'full/rows_0000.json').write_bytes(b'[]')
 with pytest.raises(Exception):m.run(path,req,pin,out,'full')
