"""Validate retained source receipts and enumerate an outcome-blind population.

This module never opens a live database, fetches content or assigns FX direction.
Collector attestation is not independent publisher/host-clock authentication.
"""
import collections
import datetime as dt
import gzip
import hashlib
import io
import json
import sqlite3
import types

MAX_JSON_BYTES = 32 * 1024 * 1024
MAX_EXPANDED_BYTES = 64 * 1024 * 1024
JSON_NAMES = ('versions', 'observations', 'observation_groups', 'legacy_matches', 'unresolved_clocks')


def digest(b):
    return hashlib.sha256(b).hexdigest()


def require(condition, reason):
    if not condition:
        raise ValueError(reason)


def load_compressed(raw, expected):
    """Bound expansion before parsing; retain original JSON byte identity."""
    with gzip.GzipFile(fileobj=io.BytesIO(raw), mode='rb') as f:
        expanded = f.read(MAX_JSON_BYTES + 1)
    require(len(expanded) <= MAX_JSON_BYTES, 'expanded_json_member_limit')
    require(len(expanded) == expected['bytes'] and digest(expanded) == expected['sha256'], 'expanded_json_identity_mismatch')
    return json.loads(expanded), len(expanded)


def load_inputs(blobs):
    receipt = json.loads(blobs['CAPTURE_RECEIPT.json'])
    values = {}
    total = 0
    for name in JSON_NAMES:
        values[name], size = load_compressed(blobs[name+'.json.gz'], receipt['inputs'][name+'.json'])
        total += size
    require(total <= MAX_EXPANDED_BYTES, 'expanded_json_total_limit')
    for name in ('COHORT_PLAN', 'CAPTURE_RECEIPT', 'PRODUCER_PROVENANCE', 'PDF_LINKS', 'PDF_ARCHIVE_INVENTORY', 'baseline', 'universe'):
        values[name] = json.loads(blobs[name+'.json'])
    ledger = types.ModuleType('retained_source_ledger')
    exec(compile(blobs['source_ledger.py'], '<pinned-retained-source-ledger>', 'exec'), ledger.__dict__)
    return values, ledger


def validate_versions(versions, ledger):
    result = {}
    for v in versions:
        vid = v['version_id']
        require(vid not in result, 'duplicate_version_id')
        require(v['contract_id'] == ledger.CONTRACT, 'version_contract_mismatch')
        c = json.loads(v['content_json'])
        require(ledger.encoded(c) == v['content_json'], 'noncanonical_content')
        require(not (set(c) & ledger.ENVELOPE_FIELDS), 'content_envelope_overlap')
        require(ledger.digest(v['content_json']) == v['content_sha256'], 'content_hash_mismatch')
        identity = ledger.encoded({k:c.get(k) for k in ledger.SOURCE_FIELDS})
        require(identity == v['source_identity_json'] and ledger.digest(identity) == v['source_identity_sha256'], 'source_identity_mismatch')
        expected = ledger.digest(ledger.encoded([ledger.CONTRACT, v['canonical_event_id'], ledger.digest(identity), v['content_sha256']]))
        require(expected == vid, 'version_id_mismatch')
        require(c.get('source_verified') is True, 'outside_verified_source_flag_cohort')
        result[vid] = (v, c)
    return result


def validate_observations(versions, observations, ledger, provenance, plan, high_water):
    """Reuse the original reconstruction consumer against in-memory copied rows."""
    c = sqlite3.connect(':memory:')
    ledger.initialize(c)  # Only the isolated in-memory database is mutated.
    result = {}
    sequences = set()
    start, end, cutoff = (ledger.aware(plan[k]) for k in ('release_interval_start','release_interval_end_exclusive','observation_cutoff'))
    require(all(x is not None for x in (start,end,cutoff)) and start < end <= cutoff, 'invalid_cohort_interval')
    try:
        for v, _ in versions.values():
            c.execute('INSERT INTO article_source_versions_v1 VALUES(?,?,?,?,?,?,?)', tuple(v[k] for k in ('version_id','contract_id','canonical_event_id','source_identity_json','source_identity_sha256','content_json','content_sha256')))
        fields = ('observation_seq','observation_id','contract_id','canonical_event_id','incoming_event_id','version_id','envelope_json','incoming_payload_sha256','incoming_payload_bytes','supplied_now_json','source_observed_json','known_epoch','known_utc','clock_status','clock_reasons_json')
        for o in observations:
            oid, seq = o['observation_id'], o['observation_seq']
            require(oid not in result and seq not in sequences, 'duplicate_observation_identity')
            require(type(seq) is int and 0 < seq <= high_water, 'observation_high_water_violation')
            require(o['version_id'] in versions, 'orphan_observation')
            v, _ = versions[o['version_id']]
            require(o['contract_id'] == ledger.CONTRACT and o['canonical_event_id'] == v['canonical_event_id'], 'observation_lineage_mismatch')
            env = json.loads(o['envelope_json'])
            require(set(env) <= ledger.ENVELOPE_FIELDS and ledger.encoded(env) == o['envelope_json'], 'observation_envelope_invalid')
            c.execute('INSERT INTO article_source_observations_v1 VALUES('+','.join('?' for _ in fields)+')',tuple(o[k] for k in fields))
            incoming_text = ledger.reconstruct_observation(c, oid)
            incoming = json.loads(incoming_text)
            require(str(incoming['event_id']) == o['incoming_event_id'], 'incoming_event_id_mismatch')
            require(ledger.encoded(incoming.get('first_seen_utc')) == o['source_observed_json'], 'source_observed_clock_mismatch')
            now = json.loads(o['supplied_now_json'])
            require(ledger.encoded(now) == o['supplied_now_json'], 'noncanonical_supplied_clock')
            expected_id = ledger.digest(ledger.encoded([ledger.CONTRACT,o['canonical_event_id'],ledger.digest(incoming_text),o['supplied_now_json']]))
            require(oid == expected_id, 'observation_id_mismatch')
            status, reasons, epoch, utc = ledger._clock_evidence(incoming, now, provenance)
            require((status,reasons,epoch,utc) == (o['clock_status'],json.loads(o['clock_reasons_json']),o['known_epoch'],o['known_utc']), 'recomputed_clock_evidence_mismatch')
            # The SQL acquisition can admit parseable naive clocks. Preserve them
            # as unresolved instead of silently normalizing them into UTC.
            published, observed = ledger.aware(incoming.get('published_utc')), ledger.aware(now)
            in_cohort = published is not None and observed is not None and start <= published < end and observed < cutoff
            require(in_cohort or status == 'unproven_observation', 'attested_receipt_outside_cohort')
            result[oid] = {'row':o,'incoming':incoming,'in_cohort':in_cohort,
                           'collector_attested':status == 'valid_attested_observation' and in_cohort}
            sequences.add(seq)
    finally:
        c.close()
    return result


def validate_groups(groups, observations, versions, capture):
    byseq = {x['row']['observation_seq']:x['row'] for x in observations.values()}
    keys, represented = set(), set()
    for g in groups:
        key = (g['version_id'], g['clock_status'])
        require(key not in keys and key[0] in versions, 'invalid_observation_group')
        keys.add(key)
        count, first, last = g['observations'], g['first_seq'], g['last_seq']
        require(type(count) is int and count > 0 and type(first) is int and type(last) is int and first <= last, 'invalid_group_counts')
        require((first == last) == (count == 1) and count <= last-first+1, 'inconsistent_group_span')
        for seq in {first,last}:
            require(seq in byseq and (byseq[seq]['version_id'],byseq[seq]['clock_status']) == key, 'missing_or_wrong_boundary_receipt')
            represented.add(seq)
    require(represented == set(byseq), 'unaccounted_boundary_receipts')
    require({g['version_id'] for g in groups} == set(versions), 'unaccounted_version')
    require(sum(g['observations'] for g in groups) == capture['original_observation_count'], 'capture_repeat_count_mismatch')
    require(len(versions) == capture['versions'] and len(observations) == capture['observations'], 'capture_population_mismatch')


def document_kind(content):
    role = str(content.get('source_role','')).lower()
    source = str(content.get('source_id','')).lower()
    if 'calendar' in role or 'calendar' in source:
        return 'calendar_or_schedule_listing'
    if content.get('detail_enriched') is True and content.get('detail_quality_state') == 'valid':
        return 'retained_parsed_detail'
    return 'headline_or_listing_only'


def available_at(observation, ledger):
    if not observation['collector_attested']:
        return None
    o, incoming = observation['row'], observation['incoming']
    values = [o['known_epoch']]
    for k in ('published_utc','causal_known_utc','numeric_causal_known_utc','detail_available_utc','detail_attachment_available_utc','detail_publisher_resolution_known_utc','publication_clock_known_utc'):
        if incoming.get(k) not in (None,''):
            parsed = ledger.aware(incoming[k])
            if parsed is None:
                return None
            values.append(parsed.timestamp())
    for attachment in incoming.get('detail_attachments',[]):
        parsed = ledger.aware(attachment.get('available_utc'))
        if parsed is None:
            return None
        values.append(parsed.timestamp())
    return max(values)


def visible(documents, cutoff):
    """Visibility only; no collapse of conflicting versions or invented stance."""
    return [d for d in documents if d['available_epoch'] is not None and d['available_epoch'] <= cutoff]


def build(values, ledger, blobs):
    plan, capture = values['COHORT_PLAN'], values['CAPTURE_RECEIPT']
    require(plan == capture['started_plan'] and plan['outcome_data_read'] is False, 'capture_plan_mismatch')
    versions = validate_versions(values['versions'],ledger)
    observations = validate_observations(versions,values['observations'],ledger,values['PRODUCER_PROVENANCE']['expected'],plan,capture['observation_seq_high_water'])
    validate_groups(values['observation_groups'],observations,versions,capture)
    archive = {a['sha256']:a for a in values['PDF_ARCHIVE_INVENTORY']}
    pdfs = {}
    for a in values['PDF_LINKS']:
        require(a['sha256'] not in pdfs and a['sha256'] in archive, 'duplicate_or_unknown_pdf')
        name = a['sha256']+'.pdf'
        require(a['input_name'] == name and name in blobs, 'invalid_pdf_name')
        b = blobs[name]
        require(b.startswith(b'%PDF-') and digest(b) == a['sha256'] and len(b) == a['bytes'], 'pdf_bytes_mismatch')
        pdfs[a['sha256']] = a
    require({n for n in blobs if n.endswith('.pdf')} == {a['input_name'] for a in pdfs.values()}, 'unexpected_pdf_input')
    byversion = collections.defaultdict(list)
    for x in observations.values():
        byversion[x['row']['version_id']].append(x)
    documents = []
    for vid,(v,c) in sorted(versions.items()):
        obs = sorted(byversion[vid],key=lambda x:x['row']['observation_seq'])
        clocks = [available_at(o,ledger) for o in obs]
        clocks = [x for x in clocks if x is not None]
        refs = {c.get(k) for k in ('detail_content_sha256','detail_attachment_content_sha256') if c.get(k)}
        refs.update(a['content_sha256'] for a in c.get('detail_attachments',[]) if a.get('content_sha256'))
        currencies = c.get('currencies',[])
        require(isinstance(currencies,list) and all(isinstance(x,str) and len(x)==3 and x.isupper() for x in currencies), 'malformed_currency_mapping')
        documents.append({'version_id':vid,'canonical_event_id':v['canonical_event_id'],'source_id':c['source_id'],
            'source_role':c.get('source_role'),'source_url':c.get('source_url'),'source_verified_basis':'retained_collector_flag_not_independent_verification',
            'currencies':sorted(set(currencies)), 'kind':document_kind(c),'listing_bootstrap':c.get('source_listing_bootstrap') is True,
            'published_time_inferred':c.get('published_time_inferred'), 'timing_precision':c.get('timing_precision'),
            'publication_clocks':sorted(set(str(o['incoming'].get('published_utc')) for o in obs)),
            'available_epoch':min(clocks) if clocks else None,'availability_basis':'conservative_max_of_exact_captured_receipt_and_supplied_eligibility_clocks',
            'collector_attested_boundary_receipts':sum(o['collector_attested'] for o in obs),
            'observation_ids':[o['row']['observation_id'] for o in obs],
            'content_sha256':v['content_sha256'],'headline':c.get('headline',''),'summary':c.get('summary',''),
            'matched_pdf_sha256':sorted(refs & set(pdfs)),'unrecovered_raw_body_sha256':sorted(refs-set(pdfs)),
            'original_http_receipt_proven':False,'external_release_calendar_complete':False,
            'forecast_admission':False,'movement_label':None,'direction':None})
    legacy = []
    baseline = {b['event_id']:b['raw_payload_sha256'] for b in values['baseline']['baselines']}
    for row in values['legacy_matches']:
        require(row['event_id'] in baseline and row['expected_original_payload_sha256'] == baseline[row['event_id']], 'legacy_reference_mismatch')
        origin = row['origin_record']
        if origin:
            require(origin['canonical_event_id'] == row['event_id'] and ledger.digest(origin['original_row_json']) == origin['original_row_sha256'], 'legacy_origin_integrity_failure')
        require(not row['exact_observation_matches'], 'new_legacy_match_requires_full_receipt_validation')
        legacy.append({'event_id':row['event_id'],'exact_original_receipt_recovered':False,'origin_snapshot_present':origin is not None,
                       'status':'later_origin_snapshot_not_original_receipt' if origin else 'original_receipt_unrecovered'})
    require(len(legacy) == len(baseline) and len({x['event_id'] for x in legacy}) == len(baseline), 'legacy_population_mismatch')
    universe = values['universe']
    require(len(universe)==68 and sorted(set(universe))==universe, 'exact_sorted_68_universe_required')
    pair_coverage=[]
    for pair in universe:
        base, quote = pair.split('_')
        docs=[d for d in documents if set(d['currencies']) & {base,quote}]
        pair_coverage.append({'pair':pair,'mapped_versions':len(docs),'canonical_events':len({d['canonical_event_id'] for d in docs}),
            'base_versions':sum(base in d['currencies'] for d in docs),'quote_versions':sum(quote in d['currencies'] for d in docs),
            'parsed_detail_versions':sum(d['kind']=='retained_parsed_detail' for d in docs),
            'missing_reason':None if docs else 'no_retained_currency_mapping_in_selected_cohort',
            'outcomes_available':False,'independent_event_count_not_pair_expanded':True})
    statuses=collections.Counter(o['row']['clock_status'] for o in observations.values())
    report={'schema':'macro_receipt_population.v2','versions':len(documents),'canonical_events':len({d['canonical_event_id'] for d in documents}),
        'boundary_receipts':len(observations),'original_observation_count':capture['original_observation_count'],
        'intermediate_observation_bodies_restored':False,'boundary_consumer':'original_pinned_reconstruct_observation_and_clock_evidence',
        'clock_status_counts':dict(sorted(statuses.items())), 'kind_counts':dict(sorted(collections.Counter(d['kind'] for d in documents).items())),
        'listing_bootstrap_versions':sum(d['listing_bootstrap'] for d in documents),
        'available_by_cutoff_versions':len(visible(documents,ledger.aware(plan['observation_cutoff']).timestamp())),
        'matched_pdf_files':len(pdfs),'pdf_linked_versions':sum(bool(d['matched_pdf_sha256']) for d in documents),
        'legacy_exact_original_receipts_recovered':0,'universe_count':68,'source_count':len({d['source_id'] for d in documents}),
        'source_counts':dict(sorted(collections.Counter(d['source_id'] for d in documents).items())),
        'population_scope':'captured_verified_source_flag_publications_in_frozen_week',
        'complete_external_all_release_calendar':False,'unresolved_clock_population_scanned':capture['unresolved_clock_population_scanned'],
        'selection_has_no_market_outcome_filter':True,'outcome_data_read':False,'no_move_population_proven':False,
        'base_models_fitted':0,'forecast_improvement_proven':False,'independent_review':False,
        'limitations':['collector clock attestation is retained metadata, not independently authenticated',
                       'group counts are capture assertions; intermediate repeated bodies are not restored',
                       'SQL-NULL/unparseable publication clocks were outside this bounded acquisition',
                       'source currency tags are retained mappings, not independently verified economic exposure',
                       'PDF bytes do not restore HTTP headers or the original transport receipt',
                       'no aligned price outcomes, no no-move labels and no forecast admission']}
    clock_audit=[{'observation_id':oid,'version_id':x['row']['version_id'],'clock_status':x['row']['clock_status'],
                  'reasons':json.loads(x['row']['clock_reasons_json']),'in_cohort':x['in_cohort'],'available_epoch':available_at(x,ledger)} for oid,x in sorted(observations.items())]
    return {'documents.json':documents,'clock_audit.json':clock_audit,'pair_coverage.json':pair_coverage,
            'legacy_receipts.json':legacy,'population_report.json':report}
