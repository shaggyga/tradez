"""Compare raw receipt retention with original canonical detail protection."""
import ast,hashlib,json,types
from macro_receipt_population_v2 import require


def source_order(raw,first_line):
    tree=ast.parse(raw.decode('utf-8-sig'));calls={}
    for n in ast.walk(tree):
        if isinstance(n,ast.Call) and isinstance(n.func,ast.Name):calls.setdefault(n.func.id,[]).append(first_line+n.lineno-1)
    record=min(calls['record_source_observation']);select=min(calls['select_source_observation']);bind=min(calls['bind_active_source_version'])
    require(record<select<bind,'unexpected_collector_observation_selection_order')
    return {'record_raw_observation_line':record,'select_projection_observation_line':select,'bind_projection_line':bind,
        'raw_recording_precedes_projection_selection':True,'whole_collector_executed':False}


def build(blobs):
    read=lambda n:json.loads(blobs[n]);plan=read('STORAGE_PLAN.json')
    for n,h in plan['payloads'].items():require(hashlib.sha256(blobs[n]).hexdigest()==h,'storage_predecessor_pin_mismatch')
    capture=read('STORAGE_CAPTURE.json');require(hashlib.sha256(blobs['upsert_fragment.py']).hexdigest()==capture['fragment_sha256'],'upsert_fragment_pin_mismatch')
    order=source_order(blobs['upsert_fragment.py'],capture['fragment_first_line'])
    ledger=types.ModuleType('pinned_original_ledger');exec(compile(blobs['source_ledger.py'],'<pinned-original-ledger>','exec'),ledger.__dict__)
    rows=read('retained_versions.json');versions={r['version_id']:r for r in rows};content={}
    for vid,r in versions.items():
        require(hashlib.sha256(r['content_json'].encode()).hexdigest()==r['content_sha256'],'storage_content_identity_mismatch');content[vid]=json.loads(r['content_json'])
    comparisons=[]
    for d in read('replacement_text_diffs.json'):
        if len(d['before_version_ids'])!=1 or len(d['after_version_ids'])!=1:continue
        old,new=d['before_version_ids'][0],d['after_version_ids'][0];a,b=content[old],content[new]
        downgrade=bool(a.get('detail_enriched')) and not bool(b.get('detail_enriched'))
        # Only the original early detail guard is exercised with real frozen
        # content. Do not manufacture full active-version readiness metadata.
        accepted=ledger.select_observation({'eligible':True},a,b) if downgrade else None
        comparisons.append({'event_id':d['event_id'],'before_version_id':old,'after_version_id':new,
            'before_detail_enriched':bool(a.get('detail_enriched')),'after_detail_enriched':bool(b.get('detail_enriched')),
            'before_characters':len(a.get('summary','')),'after_characters':len(b.get('summary','')),
            'before_action_candidate':d['before_action_candidate'],'raw_selector_selected_replacement':True,
            'original_detail_guard_exercised':downgrade,'original_projection_accepts_downgrade':accepted,
            'test_scope':'exact early detail guard on frozen content only; no full live ingestion replay',
            'live_canonical_projection_inspected':False,'historical_live_projection_reconstructed':False,'forecast_admission':False})
    challenges=[]
    for name,observation,previous,incoming,expected in [
        ('enriched_to_listing',{'eligible':True},{'detail_enriched':True},{'detail_enriched':False},False),
        ('enriched_to_missing_flag',{'eligible':True},{'detail_enriched':True},{},False),
        ('new_eligible_article',{'eligible':True},{},{'detail_enriched':False},True),
        ('unproven_observation',{'eligible':False},{},{'detail_enriched':True},False),
        ('listing_to_enriched_legacy',{'eligible':True},{'detail_enriched':False},{'detail_enriched':True},True)]:
        actual=ledger.select_observation(observation,previous,incoming);challenges.append({'case':name,'actual':actual,'expected':expected,'passed':actual==expected})
    gaps={'root_cause_scope':'offline latest raw-version selector conflates raw observation retention with canonical detail representation',
        'evidence':'original early detail guard rejects the frozen downgrades while raw ledger correctly retains them',
        'not_proven':['live canonical projection at historical times','full ingestion path behavior under all source contracts','publisher correction/retraction semantics'],
        'next':'macro_detail_representation_selector_repair_v2',
        'repair_requirements':['reuse original detail-preservation intent in a separately versioned offline representation selector',
            'retain all raw versions and latest listing diagnostics, never delete or rewrite receipts',
            'only select detail versions visible at cutoff; same-clock conflicting detail abstains',
            'never ignore a verified correction/retraction; unresolved replacement semantics remain explicit',
            'separate retained-detail context from issuer facts and historical feature readiness',
            'replay whole frozen cohort and prior perturbation tests before a new checkpoint; no model retuning']}
    report={'schema':'macro_collector_representation_storage_audit.v2','retained_versions':len(versions),'replacement_pairs':len(comparisons),
        'enriched_to_non_enriched_pairs':sum(c['original_detail_guard_exercised'] for c in comparisons),
        'original_guard_rejected_downgrades':sum(c['original_projection_accepts_downgrade'] is False for c in comparisons),
        'action_loss_pairs_rejected_by_original_guard':sum(c['before_action_candidate'] is not None and c['original_projection_accepts_downgrade'] is False for c in comparisons),
        'original_guard_challenges_passed':sum(c['passed'] for c in challenges),'base_models_fitted':0,'forecast_features_admitted':0,'forecast_improvement_proven':False,
        'live_collector_executed':False,'live_canonical_projection_inspected':False,'source_database_writes':False,
        'finding':'offline raw-version selection misses existing canonical detail protection; active live data loss not demonstrated'}
    return {'source_order_evidence.json':order,'frozen_projection_guard_comparison.json':comparisons,'original_guard_challenges.json':challenges,
            'representation_repair_requirements.json':gaps,'storage_audit_report.json':report}
