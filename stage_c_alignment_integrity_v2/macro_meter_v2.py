"""Offline Design14.5 evidence meter. Source text is data, never executable UI content."""
import datetime as dt,hashlib,json,types
from collections import defaultdict
from contracts import fingerprint
from macro_component_state_v2 import validate_parent

RULE='unique_text_concept_presence.v1'
def require(value,reason):
    if not value:raise ValueError(reason)

def validate_span(e,text):
    a,b=e.get('start'),e.get('end')
    require(type(a) is int and type(b) is int and 0<=a<b<=len(text),'meter_span_bounds')
    require(text[a:b]==e['quote'],'meter_span_quote_mismatch')
    require(0<=e['context_start']<=a<b<=e['context_end']<=len(text),'meter_context_bounds')

def document(ref,texts,scopes,versions,cutoff,normalize):
    key=ref['cache_key'];require(key in texts and key in scopes,'meter_cache_missing')
    t=texts[key];s=scopes[key];text=t['retained_text'];digest=hashlib.sha256(text.encode()).hexdigest()
    require(digest==ref['text_sha256']==t['text_sha256']==s['text_sha256'],'meter_text_binding_mismatch')
    require(fingerprint(s)==ref['scoped_evidence_sha256'],'meter_scope_binding_mismatch')
    require(s['forecast_admission'] is False and s['policy_fact_admission'] is False and s['issuer_identity_verified'] is False,'meter_premature_admission')
    require(type(ref['published_epoch']) in (int,float) and ref['published_epoch']<=cutoff,'meter_future_publication')
    require(ref['version_ids'] and len(set(ref['version_ids']))==len(ref['version_ids']),'meter_unique_versions_required')
    for vid in ref['version_ids']:
        require(vid in versions,'meter_version_missing');v=versions[vid]
        require(v['cache_key']==key and v['event_id']==ref['event_id'] and v['source_id']==ref['source_id'],'meter_document_identity_mismatch')
        require(any(o['clock_status']=='valid_attested_observation' and o['available_epoch'] is not None and o['available_epoch']<=cutoff for o in v['observations']),'meter_version_not_available')
    evidence=[];concepts=set()
    for namespace,entries in [('action',s['action_evidence']),('claim',s['claim_evidence'])]:
        for raw in entries:
            # Unresolved spanless diagnostic evidence cannot become a counted concept.
            if raw.get('start') is None:
                require(raw.get('guard_reasons'),'meter_supported_span_missing');continue
            validate_span(raw,text);label=raw['action'] if namespace=='action' else raw['dimension'];name=namespace+':'+normalize(label)
            supported=not raw['guard_reasons'] and raw['assertion_status']=='asserted_linguistic_candidate'
            if supported:concepts.add(name)
            evidence.append({**raw,'concept':name,'included_in_meter':supported})
    d={'event_id':ref['event_id'],'version_ids':sorted(ref['version_ids']),'source_id':ref['source_id'],'cache_key':key,
       'text_sha256':digest,'scoped_evidence_sha256':ref['scoped_evidence_sha256'],'retained_text':text,
       'published_epoch':ref['published_epoch'],'age_seconds':cutoff-ref['published_epoch'],
       'concepts':sorted(concepts),'evidence':evidence,'action_candidate':s['action_candidate'],
       'issuer_verified':False,'language_scope':s['language_scope'],'historical_extraction_ready_epoch':None,'forecast_admission':False}
    d['document_reference_sha256']=fingerprint(d);return d

def meter_state(parent,docs,expected_sources):
    require(len({d['event_id'] for d in docs})==len(docs),'meter_duplicate_event')
    bytext={}
    for d in docs:
        sig=fingerprint(d['concepts'])
        if d['text_sha256'] in bytext:require(bytext[d['text_sha256']][0]==sig,'meter_same_text_conflicting_extraction')
        bytext[d['text_sha256']]=(sig,d)
    counts=defaultdict(list)
    for textsha,(_,d) in sorted(bytext.items()):
        for concept in d['concepts']:counts[concept].append(textsha)
    denominator=len(bytext);concepts=[]
    for concept,hashes in sorted(counts.items()):
        refs=sorted(d['document_reference_sha256'] for d in docs if d['text_sha256'] in hashes)
        concepts.append({'concept':concept,'unique_text_count':len(hashes),'normalization_denominator':denominator,
                         'normalized_presence':len(hashes)/denominator,'document_references':refs})
    supported={c['concept'] for c in concepts};actions={c for c in supported if c.startswith('action:')}
    coverage='no_usable_source' if not docs else 'supported_hold_language' if actions=={'action:hold'} else 'supported_concept_language' if supported else 'usable_source_no_supported_concept'
    sources=sorted({d['source_id'] for d in docs});refs=sorted(d['document_reference_sha256'] for d in docs)
    state={'currency':parent['currency'],'cutoff_epoch':parent['cutoff_epoch'],'parent_state_sha256':parent['state_sha256'],
        'document_version_count':len({v for d in docs for v in d['version_ids']}),'unique_event_count':len(docs),'unique_text_count':denominator,
        'repeated_text_event_count':len(docs)-denominator,'concepts':concepts,'coverage_status':coverage,
        'raw_visible_event_count':parent['raw_visible_events'],'excluded_detail_reasons':parent['exclusions'],
        'source_ages':[{'source_id':s,'youngest_seconds':min(d['age_seconds'] for d in docs if d['source_id']==s),
                        'oldest_seconds':max(d['age_seconds'] for d in docs if d['source_id']==s)} for s in sources],
        'configured_source_ids':sorted(expected_sources),'observed_eligible_source_ids':sources,
        'configured_sources_without_eligible_context':sorted(set(expected_sources)-set(sources)),
        'registry_scope':'current_frozen_config_not_historical_expectation','historical_expected_coverage_fraction':None,
        'linguistic_action_candidate':parent['linguistic_action_candidate'],'categorical_transitions':parent['categorical_transitions'],
        'policy_stance':None,'stance_change':None,'extraction_uncertainty':'issuer_language_and_historical_readiness_unverified',
        'uncertainty_probability':None,'document_references':refs,'document_set_sha256':fingerprint(refs),
        'normalization_rule':RULE,'nlp_comparison_status':'not_run','gpt_advisor_comparisons':'deferred',
        'forecast_admission':False,'can_place_orders':False}
    state['state_sha256']=fingerprint(state);return state

def render_html(states,documents):
    data=json.dumps({'states':states,'documents':documents},ensure_ascii=True,separators=(',',':')).replace('<','\\u003c').replace('>','\\u003e').replace('&','\\u0026')
    return '''<!doctype html><html lang="en"><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>Forex currency evidence</title>
<style>body{font:16px system-ui;background:#0c1625;color:#e3ecf8;margin:30px;max-width:1100px}select,button{font:inherit;padding:9px;margin:5px;background:#203650;color:#fff;border:1px solid #54728d;border-radius:5px}button{cursor:pointer}section{padding:18px;border:1px solid #36506d;margin:18px 0;border-radius:8px}pre{white-space:pre-wrap;overflow-wrap:anywhere;font:14px system-ui;line-height:1.5}mark{background:#f1d16b;color:#152135}small{color:#bdcedf}.bars{display:flex;flex-wrap:wrap}h1{margin-bottom:8px}</style>
<h1>Currency evidence</h1><p>Retained source context • offline research view</p><small>Counts describe language, not trading confidence. Issuer identity and historical extraction readiness are unverified. NLP comparison not run; GPT/advisor comparisons deferred.</small>
<p><label>Snapshot <select id="cutoff"></select></label><label>Currency <select id="currency"></select></label></p>
<section id="summary"></section><section><h2>Concepts</h2><div class="bars" id="concepts"></div><p id="rule"></p></section>
<section><h2>Sources and coverage</h2><pre id="coverage"></pre></section><section><h2>Contributing documents</h2><div id="documents"></div></section><section id="detail"><h2>Document evidence</h2><p>Select a document or concept to inspect the exact evidence.</p></section>
<script id="meter-data" type="application/json">'''+data+'''</script><script>
'use strict';const data=JSON.parse(document.getElementById('meter-data').textContent),el=id=>document.getElementById(id);
const docIndex=Object.fromEntries(data.documents.map(d=>[d.document_reference_sha256,d]));
function option(id,values){values.forEach(v=>{const o=document.createElement('option');o.value=v;o.textContent=v;el(id).append(o)})}
option('cutoff',[...new Set(data.states.map(s=>s.cutoff_epoch))].map(x=>String(x)));Array.from(el('cutoff').options).forEach(o=>o.textContent=new Date(Number(o.value)*1000).toISOString());
option('currency',[...new Set(data.states.map(s=>s.currency))].sort());
function text(tag,value,parent){const n=document.createElement(tag);n.textContent=value;parent.append(n);return n}
function showDoc(id){const d=docIndex[id],box=el('detail');box.replaceChildren();text('h2',d.source_id,box);text('small','Event '+d.event_id+' | Text SHA256 '+d.text_sha256,box);
text('p','Source age: '+(d.age_seconds/3600).toFixed(1)+' hours. Original versions: '+d.version_ids.join(', '),box);
d.evidence.forEach(e=>{text('h3',e.concept+' — '+(e.included_in_meter?'counted language':'excluded: '+e.guard_reasons.join(', ')),box);const p=document.createElement('pre');p.append(document.createTextNode(d.retained_text.slice(e.context_start,e.start)));const m=document.createElement('mark');m.textContent=d.retained_text.slice(e.start,e.end);p.append(m,document.createTextNode(d.retained_text.slice(e.end,e.context_end)));box.append(p);text('small','Offsets '+e.start+'–'+e.end+' in retained text',box)});
const full=document.createElement('details');text('summary','Full retained document',full);text('pre',d.retained_text,full);box.append(full)}
function showDocs(refs){el('documents').replaceChildren();refs.forEach(id=>{const d=docIndex[id],b=text('button',d.source_id+' · '+d.event_id,el('documents'));b.onclick=()=>showDoc(id)})}
function render(){const s=data.states.find(s=>s.currency===el('currency').value&&String(s.cutoff_epoch)===el('cutoff').value);el('summary').replaceChildren();text('h2',s.currency+' — '+s.coverage_status.replaceAll('_',' '),el('summary'));text('p',s.document_version_count+' document versions · '+s.unique_event_count+' unique events · '+s.unique_text_count+' unique texts',el('summary'));
text('p','Policy stance: unqualified. Change: unqualified. A hold-language candidate is not verified neutral policy.',el('summary'));el('concepts').replaceChildren();s.concepts.forEach(c=>{const b=text('button',c.concept+' '+(100*c.normalized_presence).toFixed(1)+'% ('+c.unique_text_count+'/'+c.normalization_denominator+')',el('concepts'));b.onclick=()=>showDocs(c.document_references)});
el('rule').textContent='One presence per concept per distinct text. Repeated spans and identical documents are downweighted; paraphrases are not detected. Missing concepts are not neutral observations.';
el('coverage').textContent=JSON.stringify({source_ages:s.source_ages,configured_sources_without_eligible_context:s.configured_sources_without_eligible_context,excluded_detail_reasons:s.excluded_detail_reasons,registry_scope:s.registry_scope},null,2);showDocs(s.document_references);el('detail').replaceChildren();text('h2','Document evidence',el('detail'));text('p','Select a document to inspect exact spans.',el('detail'))}
el('currency').onchange=render;el('cutoff').onchange=render;render();</script></html>'''

def build(blobs):
    read=lambda n:json.loads(blobs[n]);plan=read('METER_INPUT_PLAN.json')
    require(set(plan['payloads'])==set(blobs)-{'METER_INPUT_PLAN.json'},'meter_inventory_mismatch')
    require(all(hashlib.sha256(blobs[n]).hexdigest()==h for n,h in plan['payloads'].items()),'meter_input_pin_mismatch')
    require(plan['rule_id']==RULE and plan['adapter_activation_receipt'] is None,'meter_rule_or_activation_mismatch')
    parent=validate_parent(blobs,'detail_parent_manifest.json',['detail_currency_states.json'])
    native=types.ModuleType('frozen_meter_labels');exec(compile(blobs['legacy_normalized_term.py'],'frozen_meter_labels','exec'),native.__dict__)
    def index(name,key):
        rows=read(name);out={r[key]:r for r in rows};require(len(out)==len(rows),'meter_duplicate_identity');return out
    texts=index('extraction_cache.json','cache_key');scopes=index('scoped_text_cache.json','cache_key');versions=index('version_bindings.json','version_id')
    universe=read('universe.json');require(len(universe)==68 and sorted(set(universe))==universe,'meter_all68_required');currencies={c for p in universe for c in p.split('_')}
    expected=defaultdict(set)
    for source in read('news_sources_v1.json')['sources']:
        for currency in source.get('currencies',[]):expected[currency].add(source['source_id'])
    states=[];docs={};pairs=[]
    for snap in read('detail_currency_states.json'):
        cutoff=dt.datetime.fromisoformat(snap['cutoff'].replace('Z','+00:00')).timestamp();bycurrency={}
        require(len(snap['states'])==len(currencies) and {s['currency'] for s in snap['states']}==currencies,'meter_currency_population_mismatch')
        for s in snap['states']:
            require(s['cutoff_epoch']==cutoff and fingerprint({k:v for k,v in s.items() if k!='state_sha256'})==s['state_sha256'],'meter_parent_state_mismatch')
            ds=[document(r,texts,scopes,versions,cutoff,native._normalized_term) for r in s['evidence_references']]
            for d in ds:docs[d['document_reference_sha256']]=d
            row=meter_state(s,ds,expected[s['currency']]);states.append(row);bycurrency[row['currency']]=row
        for pair in universe:
            a,b=(bycurrency[c] for c in pair.split('_'));pairs.append({'pair':pair,'cutoff_epoch':cutoff,'base_state_sha256':a['state_sha256'],'quote_state_sha256':b['state_sha256'],
                'numeric_difference':None,'directional_difference':None,'forecast_admission':False,'shared_documents_are_independent_votes':False})
    doclist=[docs[k] for k in sorted(docs)]
    report={'currency_rows':len(states),'pair_rows':len(pairs),'document_evidence_rows':len(doclist),'concept_rows':sum(len(s['concepts']) for s in states),
        'supported_hold_rows':sum(s['coverage_status']=='supported_hold_language' for s in states),'no_usable_source_rows':sum(s['coverage_status']=='no_usable_source' for s in states),
        'usable_no_supported_concept_rows':sum(s['coverage_status']=='usable_source_no_supported_concept' for s in states),
        'rule_id':RULE,'parent_run_identity':parent,'numeric_surprises_computed':0,'base_models_fitted':0,'forecast_features_admitted':0,'forecast_improvement_proven':False,'independent_review':False}
    return {'meter_currency_states.json':states,'meter_document_evidence.json':doclist,'meter_pair_views.json':pairs,'meter_report.json':report,'meter_view.html':render_html(states,doclist)}
