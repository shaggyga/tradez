from copy import deepcopy
from decimal import Context, Decimal, localcontext
import hashlib
import json

import pytest

import oanda_currency_exposure_projection_v1 as e


def inputs():
    pairs={'EUR_USD':('1.0998','1.1002'), 'GBP_USD':('1.2498','1.2502'), 'USD_JPY':('149.98','150.02')}
    quotes={p:dict(instrument=p,quote_id='q.'+p,bid=b,ask=a,market_epoch=999.25,
                  available_epoch=999.75,tradeable=True,source_record_sha256='b'*64)
            for p,(b,a) in pairs.items()}
    metadata={p:dict(instrument=p,base_currency=p[:3],quote_currency=p[4:],
                    pip_size='0.01' if p=='USD_JPY' else '0.0001',unit_increment=1) for p in pairs}
    limits=dict(maximum_positions=8,maximum_currency_direction_positions=8,
        maximum_gross_position_notional_usd='1000000',maximum_quote_age_sec=5,
        maximum_currency_gross_usd={c:'1000000' for c in e.CURRENCIES},
        maximum_currency_abs_net_usd={c:'1000000' for c in e.CURRENCIES},
        maximum_currency_gross_leg_share={c:'1' for c in e.CURRENCIES})
    return dict(quotes=quotes,metadata=metadata,limits=limits,scenario_id='episode1.curve',
        decision_epoch=1000.,observed_epoch=1001.,expected_source_bindings=e.source_bindings())


def position(pair='EUR_USD',side=1,units=1000,id='p1',scenario='episode1.curve'):
    return dict(position_id=id,scenario_id=scenario,instrument=pair,side=side,base_units=units,
        source_record_sha256='c'*64,opened_epoch=900.,state_available_epoch=950.)


def candidate(pair='GBP_USD',side=1,units=1000):
    return dict(candidate_id='candidate1',scenario_id='episode1.curve',instrument=pair,side=side,
        base_units=units,source_record_sha256='d'*64,available_epoch=999.)


def project(rows=None,addition=None,**changes):
    args=inputs();args.update(changes)
    return e.project_exposure([position()] if rows is None else rows,addition,**args)


def test_three_pair_currency_orientation_and_two_leg_attribution():
    out=project([position(),position('GBP_USD',id='p2'),position('USD_JPY',id='p3')])
    book=out['after']; c=book['currencies']
    assert c['EUR']['net_signed_units']=='1000'
    assert c['GBP']['net_signed_units']=='1000'
    assert c['JPY']['net_signed_units']=='-150000.00'
    assert Decimal(c['USD']['net_signed_units'])==Decimal('-1350')
    assert c['USD']['long_position_count']==1 and c['USD']['short_position_count']==2
    assert Decimal(book['conservative_gross_currency_leg_attribution_usd'])>Decimal(book['conservative_gross_position_notional_usd'])
    assert book['within_supplied_limits'] is True and len(book['positions'])==3
    assert out['actual_account_positions_observed'] is False


def test_long_and_short_each_have_opposite_currency_risk_legs():
    long=project()['after']['positions'][0]['legs']
    short=project([position(side=-1)])['after']['positions'][0]['legs']
    assert [r['factor_id'] for r in long]==['currency:EUR:long','currency:USD:short']
    for a,b in zip(long,short):
        assert Decimal(a['signed_risk_units'])==-Decimal(b['signed_risk_units'])
        assert a['conservative_absolute_usd_risk_equivalent']==b['conservative_absolute_usd_risk_equivalent']


def test_equal_offset_keeps_gross_and_position_counts_visible():
    out=project([position(),position(side=-1,id='p2')])['after']
    for ccy in ('EUR','USD'):
        row=out['currencies'][ccy]
        assert Decimal(row['net_signed_units'])==0 and Decimal(row['conservative_abs_net_usd'])==0
        assert Decimal(row['conservative_gross_usd'])>0
        assert row['long_position_count']==row['short_position_count']==1
    assert out['position_count']==2 and Decimal(out['conservative_gross_position_notional_usd'])>0


def test_unequal_offset_is_quantified_instead_of_boolean_hedge():
    out=project([position(units=1000),position(side=-1,units=400,id='p2')])['after']['currencies']
    assert out['EUR']['net_signed_units']=='600'
    assert out['EUR']['gross_units']=='1400'
    assert Decimal(out['USD']['net_signed_units'])==Decimal('-660')


def test_jpy_inverse_conversion_uses_reciprocal_mid_and_purchase_side():
    row=project([position('USD_JPY')])['after']['positions'][0]
    leg=row['legs'][1]
    with localcontext(Context(prec=192)):
        expected=Decimal('150000')/Decimal('149.98')
        assert Decimal(leg['conservative_absolute_usd_risk_equivalent'])==expected
    assert Decimal(leg['signed_mid_usd_risk_equivalent'])==Decimal('-1000')
    assert leg['conversion']['instrument']=='USD_JPY'
    assert row['conservative_position_notional_usd']=='1000'


def test_candidate_before_after_reveals_additive_usd_concentration():
    args=inputs();args['limits']['maximum_currency_direction_positions']=1
    args['limits']['maximum_currency_gross_usd']['USD']='2000'
    out=e.project_exposure([position()],candidate(),**args)
    assert out['before']['within_supplied_limits'] is True
    assert out['after']['position_count']==2 and out['after']['within_supplied_limits'] is False
    kinds={(b['limit'],b['currency']) for b in out['after']['limit_breaches']}
    assert ('maximum_currency_direction_positions','USD') in kinds
    assert ('maximum_currency_gross_usd','USD') in kinds
    assert out['candidate_mode']=='additive_same_scenario_what_if'


def test_limits_equal_boundary_allowed_then_one_unit_over_blocked():
    args=inputs();args['limits']['maximum_gross_position_notional_usd']='1100.2'
    assert e.project_exposure([position()],**args)['after']['within_supplied_limits'] is True
    assert e.project_exposure([position(units=1001)],**args)['after']['within_supplied_limits'] is False


def test_net_limit_and_leg_share_do_not_use_account_nav():
    args=inputs();args['limits']['maximum_currency_abs_net_usd']['EUR']='1000'
    args['limits']['maximum_currency_gross_leg_share']['EUR']='0.4'
    book=e.project_exposure([position()],**args)['after']
    kinds={b['limit'] for b in book['limit_breaches']}
    assert 'maximum_currency_abs_net_usd' in kinds and 'maximum_currency_gross_leg_share' in kinds
    assert not any('nav' in key.lower() or 'margin' in key.lower() for key in book)


def test_flat_no_candidate_needs_no_quotes_and_has_explicit_zero_exposure():
    args=inputs();args['quotes']={};out=e.project_exposure([],**args)
    assert out['before']==out['after']
    assert out['after']['position_count']==0 and out['after']['within_supplied_limits'] is True
    assert all(r['conversion'] is None and r['valuation_status']=='no_exposure' for r in out['after']['currencies'].values())


def test_pure_repeatability_and_low_decimal_context_parity():
    args=inputs();rows=[position('USD_JPY'),position(id='p2')];add=candidate();before=deepcopy((args,rows,add))
    normal=e.project_exposure(rows,add,**args)
    with localcontext(Context(prec=3)):
        low=e.project_exposure(rows,add,**args)
    assert low==normal and (args,rows,add)==before
    body={k:v for k,v in normal.items() if k!='projection_sha256'}
    assert normal['projection_sha256']==hashlib.sha256(json.dumps(body,sort_keys=True,separators=(',',':'),allow_nan=False).encode()).hexdigest()


@pytest.mark.parametrize('field,value',[
    ('side',0),('side',True),('side','buy'),('base_units',0),('base_units',-1),('base_units',1.5),
    ('base_units','1000'),('base_units',True),('base_units',10**9+1),('instrument','EUR_JPY'),
    ('instrument','USD_EUR'),('scenario_id','episode1.momentum'),('source_record_sha256','fake'),
    ('state_available_epoch',1000.01),('opened_epoch',951),('state_available_epoch',False),
])
def test_no_position_is_silently_dropped_or_reinterpreted(field,value):
    bad=position(id='p2');bad[field]=value
    with pytest.raises(ValueError): project([position(),bad])


def test_duplicate_identity_and_cross_arm_candidate_refused():
    with pytest.raises(ValueError,match='duplicate'):project([position(),position()])
    add=candidate();add['scenario_id']='episode1.momentum'
    with pytest.raises(ValueError,match='cross_scenario'):project(addition=add)
    add=candidate();add['candidate_id']='p1'
    with pytest.raises(ValueError,match='duplicate'):project(addition=add)


@pytest.mark.parametrize('field,value',[
    ('tradeable',False),('tradeable',1),('bid',1.1),('bid','NaN'),('ask','Infinity'),('bid','0'),
    ('bid','-1'),('bid','1e0'),('bid','1.'),('bid','0.'+'0'*24+'1'),('ask','1.0'),
    ('market_epoch',1000.01),('available_epoch',1000.01),('market_epoch',994.99),
    ('available_epoch',998),('source_record_sha256',''),('quote_id',''),
])
def test_every_quote_requires_exact_source_prices_and_decision_availability(field,value):
    args=inputs();args['quotes']['EUR_USD'][field]=value
    with pytest.raises(ValueError):e.project_exposure([position()],**args)


def test_later_observer_cannot_unlock_previously_unavailable_quote():
    args=inputs();args['observed_epoch']=1200.;args['quotes']['EUR_USD']['available_epoch']=1001.
    with pytest.raises(ValueError,match='not_available'):e.project_exposure([position()],**args)


def test_fractional_clock_boundary_is_not_rounded_to_integer():
    args=inputs();args['quotes']['EUR_USD'].update(market_epoch=995.,available_epoch=1000.)
    assert e.project_exposure([position()],**args)['status']=='complete_diagnostic'
    args['quotes']['EUR_USD']['market_epoch']=994.999999
    with pytest.raises(ValueError):e.project_exposure([position()],**args)


def test_missing_mark_or_required_conversion_fails_whole_projection():
    args=inputs();del args['quotes']['USD_JPY']
    with pytest.raises(ValueError,match='missing_quote'):e.project_exposure([position(),position('USD_JPY',id='p2')],**args)


@pytest.mark.parametrize('mutation', ['unit_increment','pip','identity','scope','extra','position_extra','quote_extra','binding'])
def test_scope_metadata_and_closure_are_exact(mutation):
    args=inputs();rows=[position()]
    if mutation=='unit_increment':args['metadata']['EUR_USD']['unit_increment']=.1
    if mutation=='pip':args['metadata']['USD_JPY']['pip_size']='0.0001'
    if mutation=='identity':args['metadata']['EUR_USD']['base_currency']='USD'
    if mutation=='scope':args['metadata'].pop('USD_JPY')
    if mutation=='extra':args['metadata']['EUR_USD']['account']='fake'
    if mutation=='position_extra':rows[0]['orders_enabled']=True
    if mutation=='quote_extra':args['quotes']['EUR_USD']['future_bid']='100'
    if mutation=='binding':args['expected_source_bindings'][e.__name__+'.py']='0'*64
    with pytest.raises(ValueError):e.project_exposure(rows,**args)


@pytest.mark.parametrize('mutation',['missing_limit','negative','float_limit','share','currency','count','age','bool_count'])
def test_caps_are_explicit_bounded_and_never_defaulted(mutation):
    args=inputs();lim=args['limits']
    if mutation=='missing_limit':del lim['maximum_positions']
    if mutation=='negative':lim['maximum_currency_gross_usd']['USD']='-1'
    if mutation=='float_limit':lim['maximum_gross_position_notional_usd']=10.0
    if mutation=='share':lim['maximum_currency_gross_leg_share']['USD']='1.1'
    if mutation=='currency':del lim['maximum_currency_abs_net_usd']['JPY']
    if mutation=='count':lim['maximum_positions']=33
    if mutation=='age':lim['maximum_quote_age_sec']=61
    if mutation=='bool_count':lim['maximum_positions']=True
    with pytest.raises(ValueError):e.project_exposure([position()],**args)


def test_zero_limit_is_restrictive_not_disabled():
    args=inputs();args['limits']['maximum_positions']=0
    assert e.project_exposure([position()],**args)['after']['within_supplied_limits'] is False
    assert e.project_exposure([],**args)['after']['within_supplied_limits'] is True


def test_bounded_positions_and_input_bytes():
    rows=[position(id='p'+str(i)) for i in range(33)]
    with pytest.raises(ValueError,match='count_bound'):project(rows)
    args=inputs();args['quotes']['EUR_USD']['extra']='x'*(e.MAX_INPUT_BYTES+1)
    with pytest.raises(ValueError,match='byte_bound'):e.project_exposure([position()],**args)


def test_output_never_authorizes_any_action_even_when_all_limits_pass():
    out=project()
    assert out['after']['within_supplied_limits'] is True
    for flag,value in e.FLAGS.items():assert out[flag] is value
    assert out['broker_requests']==0 and out['runtime_writes'] is False
    assert 'order' not in out and 'actions' not in out
