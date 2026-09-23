"""Synthetic prospective-clock curve chains fed through the actual policy engine.

The recovered receipt schema calls its clock-ordered issuance current_research;
the enclosing signed input context explicitly identifies synthetic qualification.
No publication, consumption or market forecast here is claimed historically observed.
"""
from copy import deepcopy
from decimal import Decimal
from contracts import fingerprint
from native_policy_input_v2 import load_native,prepare_frame,TIER
from policy_fixture_v2 import fixture as old_fixture
from reference_accounting_adapter_v2 import DEFAULT_TRAD

SOURCES={'synthetic_model_definition':fingerprint({'model':'declared_native_terminal_scenarios_not_fitted'}),
 'synthetic_feature_definition':fingerprint({'features':'declared_prospective_clock_fixture'})}

def packet(pair,epoch,target,mid,move,trad_root=DEFAULT_TRAD,*,conditioning_epoch=None):
    c,_=load_native(trad_root);reference=epoch-2 if conditioning_epoch is None else conditioning_epoch
    horizon=target-reference
    policy=c.make_policy(native_horizons_sec=[horizon],maximum_reference_age_sec=30,maximum_build_sec=10,
      maximum_issue_delay_sec=10,maximum_publication_delay_sec=5,maximum_decision_age_sec=30,minimum_remaining_sec=0)
    prepared=c.prepare_curve(instrument=pair,pip_size='0.0001',forecast_cohort='synthetic_native_policy',
      model_sha256=SOURCES['synthetic_model_definition'],feature_version='synthetic_current_information_v1',source_bindings=SOURCES,
      input_capture_sha256=fingerprint({'pair':pair,'reference':reference}),input_available_epoch=reference,
      reference_epoch=reference,reference_label_epoch=reference,reference_price=str(mid),reference_price_kind='synthetic_mid_quote',bar_duration_sec=0,
      model_fitted_epoch=1,computation_started_epoch=reference,computed_epoch=reference,points=[{'horizon_sec':horizon,
       'target_epoch':target,'target_label_epoch':target,'model_id':'declared_synthetic_terminal_model','predicted_signed_pips':str(move)}],
      policy=policy,computation_sha256=fingerprint({'pair':pair,'reference':reference,'move':str(move)}),
      input_context={'input_tier':TIER,'conditioning_kind':'fresh_conditional_terminal_expectation','conditioning_epoch':reference,'thesis_version':'native-two-day-v1'})
    curve=c.issue_curve(prepared,expected_source_bindings=SOURCES,clock=lambda:epoch-1)
    publication=c.publication_receipt(curve,persisted_bytes_sha256=c.content_hash(curve),publication_started_epoch=epoch-1,
      expected_source_bindings=SOURCES,clock=lambda:epoch-1)
    consumption=c.consume_curve(curve,publication,expected_source_bindings=SOURCES,clock=lambda:epoch-1)
    return {'curve':curve,'publication':publication,'consumption':consumption,
      'remaining_financing_usd_per_base_unit':{'long':'-0.00005','short':'0.00002'}}

def fixture(trad_root=DEFAULT_TRAD):
    contract,frames=old_fixture(trad_root)
    from policy_continuation_v2 import PolicyReplay
    config=PolicyReplay(contract,trad_root=trad_root).book.config
    result=[]
    for old in frames:
        if old['kind']!='decision':result.append(old);continue
        f={k:deepcopy(v) for k,v in old.items() if k!='candidates'}
        f.update(native_input_tier=TIER,native_contract={'maximum_conditioning_age_seconds':2,'expected_source_bindings':SOURCES},native_packets=[])
        for row in old['candidates']:
            q=f['quotes'][row['instrument']];mid=(Decimal(q['bid'])+Decimal(q['ask']))/2
            f['native_packets'].append(packet(row['instrument'],f['epoch'],f['target_epoch'],mid,row['expected_move_pips'],trad_root))
        result.append(prepare_frame(f,config,trad_root))
    return contract,result
