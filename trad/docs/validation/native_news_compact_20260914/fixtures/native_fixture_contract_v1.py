"""Owned integration fixture only; never emit a deployed registry from this.

Uses the actual capture's immutable numerical/dependency identity and the fixed
ledger's source-binding method. All validation remains production-owned.
"""
from copy import deepcopy

def make_contract(ledger,evaluator,capture,clock_path,candle_path):
    pair=capture['output_instrument'];pip=capture['pip_size'];family='ridge_price_news_v1'
    cohorts={family:'joint_price_news_native_v1_20260913.'+pair+'.'+family+'.owned_integration_fixture'}
    policy={'schema_version':'native_joint_m1_outcome_policy_v1_20260913',
        'maximum_retained_source_bytes':8*1024**2,'maximum_source_observations':100,
        'maximum_source_blobs':100,'maximum_metadata_bytes':8*1024**2,'maximum_score_attempts':100,
        'maximum_target_records':100,'target_selection':'first_actual_admitted_exact_native_close',
        'score_recipe':'original_binary64_target_minus_origin_divided_by_pip_v1'}
    shared={'family':family,'instrument':pair,'pip_size':pip,'input_timeframe':'M1','horizon_sec':3600,
        'cohorts':cohorts,'model_version':'sha256:'+capture['model_source_sha256'],
        'feature_version':'sha256:'+capture['source_bindings']['revision_joint_inputs_v3.py'],
        'quote_max_age_sec':60,'maximum_entry_delay_sec':60,'maximum_target_quote_delay_sec':60,
        'maximum_input_age_sec':900,'maximum_news_age_sec':300}
    protocol={**deepcopy(shared),'schema_version':evaluator.PROTOCOL_SCHEMA,'contract_id':'owned_native_evaluation_fixture',
        'proof_eligible':False,'account_eligible':False,'collection_enabled':False,
        'historical_start_utc':'2026-01-01T00:00:00+00:00','historical_end_utc':'2028-01-01T00:00:00+00:00',
        'native_outcome_policy':deepcopy(policy),'baselines':['fair_coin','zero_move','no_trade'],
        'native_target_recipe':'exact_completed_M1_close_at_origin_bar_start_plus_3600',
        'native_outcome_recipe':policy['score_recipe'],'executable_quote_policy':None}
    return {**shared,'schema_version':ledger.SCHEMA,'contract_id':'owned_native_collection_fixture',
        'research_only':True,'can_place_orders':False,'can_promote':False,'can_authorize':False,
        'account_eligible':False,'proof_eligible':False,'historical_rows_imported':False,
        'evaluation_protocol':protocol,'numeric_model_source_sha256':capture['model_source_sha256'],
        'dependency_versions':deepcopy(capture['dependency_versions']),'cadence_sec':900,'maximum_build_sec':120,
        'native_clock_path':str(clock_path),'native_candle_path':str(candle_path),'native_outcome_policy':policy,
        'native_source_bindings':ledger.native_source_bindings()}
