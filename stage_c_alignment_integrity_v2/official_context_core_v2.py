"""Inspect retained disabled official context without manufacturing macro values."""
from pathlib import Path
from datetime import datetime,timezone
import importlib,json,sys,hashlib
from contracts import fingerprint

FIELDS=('actual','expectation','surprise','stance','change','observed_reaction')
def unavailable(reason):return {'value':None,'status':'unavailable','reason':reason}
def load_adapter(retained):
    root=(Path(retained)/'src').resolve();name='forex_system.ingestion.official_fact_adapter_v5'
    if name in sys.modules and not Path(sys.modules[name].__file__).resolve().is_relative_to(root):
        raise ValueError('official_context_import_root_mismatch')
    sys.path.insert(0,str(root));module=importlib.import_module(name)
    if not Path(module.__file__).resolve().is_relative_to(root):raise ValueError('official_context_import_root_mismatch')
    return module

def pair_packet(snapshot,universe):
    """Only original context references; missing semantics are never neutral votes."""
    if len(universe)!=68 or len(set(universe))!=68:raise ValueError('exact_68_pair_universe_required')
    currencies=sorted({c for p in universe for c in p.split('_')})
    if len(currencies)!=21 or any(len(p.split('_'))!=2 for p in universe):raise ValueError('exact_21_currency_universe_required')
    result=[];status=snapshot['status'];s=snapshot.get('original')
    for pair in universe:
        sides={}
        for role,currency in zip(('base','quote'),pair.split('_')):
            facts=[x for x in s['facts'] if x['currency']==currency] if s else []
            events=[x for x in s['upcoming_events'] if x['currency']==currency] if s else []
            sides[role]={'currency':currency,'context_status':'retained_policy_context_only' if facts else 'no_policy_context_at_cutoff',
                'fact_ids':[x['fact_id'] for x in facts],'event_version_ids':sorted({x['event_version_id'] for x in events}),
                'clock_status':s['event_clock_provenance']['state'] if s else snapshot['reason'],
                'fields':{name:unavailable('fixed_v5_does_not_supply_'+name) for name in FIELDS}}
        result.append({'instrument':pair,'decision_cutoff_utc':snapshot['cutoff'],'snapshot_status':status,'snapshot_id':s['snapshot_id'] if s else None,
            'sides':sides,'directional_difference':unavailable('no_qualified_base_quote_semantic_values'),
            'shared_event_count_is_independent_sample_count':False,'forecast_eligible':False,'execution_eligible':False,'can_place_orders':False})
    return result

def validate_packet(packet,snapshot,universe):
    if packet!=pair_packet(snapshot,universe):raise ValueError('official_pair_context_reconstruction_mismatch')

def evidence_summary(audit_root,expected):
    root=Path(audit_root)
    def read(name):
        raw=(root/name).read_bytes()
        if len(raw)>262144 or hashlib.sha256(raw).hexdigest()!=expected[name]:raise ValueError('official_audit_consumed_bytes_changed')
        return json.loads(raw)
    sla=read('official_document_sla_v1.json');macro=read('macro_surprise_v1.json');consensus=read('macro_consensus_prospective_v1.json')
    matches=[x for x in sla['documents'] if 'opinion_2026/opi260731.pdf' in str(x.get('document_key',''))]
    if len(matches)!=1:raise ValueError('exact_retained_boj_sla_row_required')
    row=matches[0]
    return {'evidence_grade':'retained_derived_snapshots_not_original_transport_receipts','current_live_health_checked':False,
        'boj_august_case':{'snapshot_generated_utc':sla['generated_utc'],'original_sla_record':row,
            'record_sha256':fingerprint(row),'raw_two_version_receipts_recovered':False,'remembered_1600_case_identified':False,
            'interpretation':'title_listing_and_later_body_clocks_are_distinct; neither backdates_context_or_proves_profit'},
        'numeric_population':{k:macro[k] for k in ('generated_utc','release_count','actual_count','actual_and_consensus_count','causal_actual_and_consensus_count','causal_consensus_observation_count','standardized_surprise_count','status')},
        'expectation_collection':{'generated_utc':consensus['generated_utc'],'status':consensus['status'],'cohort':consensus['cohort'],
            'provider_called':False,'credentials_read':False},
        'predictive_admission':'none_from_these_derived_population_snapshots','known_limitations':['missing_original_incident_receipts','325_minute_claim_unresolved','separate_remembered_incident_unresolved','movement_selected_case_not_all_release_study','macro_actuals_are_not_verified_surprises']}

def build(retained,audit_root,configuration,audit_hashes):
    module=load_adapter(retained);originals=[];pairs=[]
    for cutoff in configuration['cutoffs']:
        # Only the expected absent historical clock is coverage, not an integrity bypass.
        try:
            s=module.OfficialFactAdapterV5().as_of(cutoff);module.validate_official_fact_v5_snapshot(s)
            view={'cutoff':cutoff,'status':'retained_context_reconstructed','original':s}
        except module.OfficialFactAdapterV5Error as e:
            if str(e)!='immutable_event_clock_v2_snapshot_missing':raise
            view={'cutoff':cutoff,'status':'unavailable_before_archived_clock','reason':str(e),'original':None}
        originals.append(view);packet=pair_packet(view,configuration['universe']);validate_packet(packet,view,configuration['universe']);pairs.extend(packet)
    summaries=[]
    for v in originals:
        s=v['original'];summaries.append({'cutoff':v['cutoff'],'status':v['status'],'policy_context_records':s['fact_count'] if s else 0,
            'unique_policy_document_urls':len({f['source_url'] for f in s['facts']}) if s else 0,
            'scheduled_currency_records':s['upcoming_event_count'] if s else 0,
            'unique_event_versions':len({e['event_version_id'] for e in s['upcoming_events']}) if s else 0,
            'clock_ready_for_cutoff':s['event_clock_provenance']['clock_ready_for_cutoff'] if s else False,
            'causal_consensus_count':s['causal_consensus_count'] if s else 0})
    report={'status':'verified_retained_official_context_recovery','cutoff_summaries':summaries,'pair_context_rows':len(pairs),'universe_count':68,'currency_count':21,
        'macro_semantic_numeric_values_admitted':0,'models_fitted':0,'active_databases_opened':False,'network_calls':0,
        'retained_manifest_independent_review_state':'pending','historical_2024_campaign_macro_admission':False,
        'engineering_ready':False,'forecast_evidence_status':'retained_context_only_no_predictive_macro_evidence','policy_evidence_status':'not_evaluated','demo_authorization_status':'not_granted','independent_review':False}
    return {'official_original_snapshots.json':originals,'official_pair_context.json':pairs,'macro_evidence_summary.json':evidence_summary(audit_root,audit_hashes),'run_report.json':report}
