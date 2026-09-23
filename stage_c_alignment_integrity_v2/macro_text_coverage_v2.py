"""Outcome-free coverage and adversarial reuse audit of retained text consumers."""
from collections import Counter,defaultdict
import ast
import hashlib
import json
import re
import types
from macro_text_layer_v2 import normalized
from macro_receipt_population_v2 import load_compressed,require

FUNCTIONS={'clean_text','explicit_policy_decision_action','policy_direction_text','semantic_claim_decomposition'}
CHALLENGES=(
 ('asserted_cut','The Committee decided to lower the policy rate.','cut'),
 ('asserted_hold','The Committee decided to maintain the policy rate at 4 percent.','hold'),
 ('conditional','If inflation falls, the Committee could lower the policy rate.',''),
 ('negation','The Committee did not lower the policy rate.',''),
 ('quotation','"The Committee decided to lower the policy rate," said an analyst.',''),
 ('historical_recap','Last year the Committee lowered the policy rate. Today the Committee decided to maintain the policy rate.','hold'),
 ('conflicting_actions','The Committee decided to raise the policy rate. The Committee decided to lower the policy rate.',''),
 ('decimal_condition','If inflation exceeds 2.5%, the Committee could raise the policy rate.',''),
 ('no_action','Policy rates around the world.',''),
 ('foreign_and_current','The Federal Reserve Committee lowered the policy rate. The Governing Council decided to maintain the policy rate.',''),
 ('hungarian_hold','A Monetáris Tanács az alapkamatot változatlanul hagyta.','hold'))


def full_normalized(text):
    chars=[];offsets=[]
    for match in re.finditer(r'\S+',text):
        if chars:chars.append(' ');offsets.append(match.start()-1)
        chars.extend(match.group());offsets.extend(range(match.start(),match.end()))
    return ''.join(chars),offsets


def raw_rule_spans(text,rules):
    value,offsets=full_normalized(text);spans=[]
    for rule in rules:
        pattern=rule['pattern'].replace('(?:become )?necessary','(?:(?:become|be) )?necessary')
        for m in re.finditer(pattern,value,re.I):
            start,end=offsets[m.start()],offsets[m.end()-1]+1
            spans.append({'axis':rule['axis'],'label':rule['label'],'start':start,'end':end,'quote':text[start:end],
                          'normalized_start':m.start(),'raw_match_only_not_assertion_or_forecast':True})
    return spans


def load_native(raw,provenance):
    require(hashlib.sha256(raw).hexdigest()==provenance['frozen_functions_sha256'],'recovered_function_pin_mismatch')
    text=raw.decode('utf-8');tree=ast.parse(text);lines=text.splitlines(keepends=True)
    funcs={n.name:n for n in tree.body if isinstance(n,ast.FunctionDef)}
    require(set(funcs)==FUNCTIONS and all(isinstance(n,(ast.Import,ast.ImportFrom,ast.FunctionDef)) for n in tree.body),'pure_function_module_required')
    for row in provenance['functions']:
        node=funcs[row['name']];fragment=''.join(lines[node.lineno-1:node.end_lineno])
        require(hashlib.sha256(fragment.encode()).hexdigest()==row['source_fragment_sha256'],'recovered_source_fragment_mismatch')
    module=types.ModuleType('frozen_original_policy_functions');exec(compile(raw,'<frozen-original-policy-functions>','exec'),module.__dict__)
    return module


def locate_evidence(text,evidence):
    words=evidence.split()
    if not words:return {'status':'no_native_evidence','start':None,'end':None,'quote':None}
    match=re.search(r'\s+'.join(re.escape(w) for w in words),text,re.I)
    if match:return {'status':'exact_raw_span','start':match.start(),'end':match.end(),'quote':text[match.start():match.end()]}
    return {'status':'native_cleaned_span_not_resolved_in_raw_text','start':None,'end':None,'quote':None}


def challenges(native):
    result=[]
    for name,text,expected in CHALLENGES:
        actual=native.explicit_policy_decision_action(text)
        result.append({'case':name,'text':text,'expected_action_or_abstention':expected,'native_action':actual,'oracle_passed':actual==expected,
                       'scope':'synthetic_semantic_safety_challenge_not_market_evidence'})
    text='It is false that inflation increased.';actual=native.semantic_claim_decomposition(text,['USD'])
    result.append({'case':'negated_semantic_claim','text':text,'expected_dimensions':[],
                   'native_dimensions':[a['dimension'] for a in actual],'oracle_passed':not actual,
                   'scope':'native_diagnostic_match_is_not_assertion_validation'})
    return result


def build(blobs,rules):
    read=lambda n:json.loads(blobs[n]);provenance=read('RECOVERED_FUNCTIONS.json');native=load_native(blobs['recovered_policy_functions.py'],provenance)
    cache=read('extraction_cache.json');bindings=read('version_bindings.json');capture=read('CAPTURE_RECEIPT.json')
    versions,_=load_compressed(blobs['versions.json.gz'],capture['inputs']['versions.json'])
    fields=read('COVERAGE_AUDIT_PLAN.json')['source_fields'];native_tests=challenges(native)
    bycache=defaultdict(list)
    for b in bindings:bycache[b['cache_key']].append(b)
    rows=[]
    for c in cache:
        text=c['retained_text'];require(hashlib.sha256(text.encode()).hexdigest()==c['text_sha256'],'text_cache_identity_mismatch')
        full,offsets=full_normalized(text);bounded,_=normalized(text);spans=raw_rule_spans(text,rules)
        claims=native.semantic_claim_decomposition(text,sorted({cur for b in bycache[c['cache_key']] for cur in b['currencies']}))
        claims=[{'native_diagnostic':a,'raw_evidence':locate_evidence(text,a['evidence_text']),
                 'assertion_validated':False,'fx_direction_admitted':False} for a in claims]
        nonascii=sum(ord(x)>127 for x in text if x.isalpha());letters=sum(x.isalpha() for x in text)
        rows.append({'cache_key':c['cache_key'],'versions':len(bycache[c['cache_key']]),'retained_characters':len(text),
            'full_normalized_characters':len(full),'inherited_examined_characters':len(bounded),
            'truncated_by_inherited_scope':len(bounded)<len(full),'empty':not text.strip(),
            'non_ascii_letter_fraction':nonascii/letters if letters else None,'language_identification':'unperformed_non_ascii_ratio_is_not_language',
            'inherited_evidence_spans':c['extraction']['evidence'],'whole_text_same_rule_raw_matches':spans,
            'native_policy_action_diagnostic':native.explicit_policy_decision_action(text),
            'native_semantic_claim_diagnostics':claims,'native_output_admitted':False})
    inventory=[];presence=Counter();numeric=Counter();source_classes=Counter();native_field_claims=0
    for v in versions:
        require(hashlib.sha256(v['content_json'].encode()).hexdigest()==v['content_sha256'],'version_content_hash_mismatch')
        c=json.loads(v['content_json']);selected={k:c.get(k) for k in fields}
        for k,value in selected.items():
            if value not in (None,'',[]):presence[k]+=1
            if type(value) in (int,float):numeric[k]+=1
        source_classes[c.get('policy_document_type','') or 'unclassified']+=1
        native_field_claims+=bool(c.get('semantic_claims'))
        inventory.append({'version_id':v['version_id'],'event_id':v['canonical_event_id'],'source_id':c['source_id'],
            'fields':selected,'fields_are_original_retained_metadata':True,'surprise_computed':False,'forecast_admission':False})
    structured={'versions':len(versions),'present_counts':dict(sorted(presence.items())),'numeric_scalar_counts':dict(sorted(numeric.items())),
        'policy_document_classes':dict(sorted(source_classes.items())),'versions_with_retained_native_claims':native_field_claims,
        'expectations_vintage_validated':False,'numeric_surprise_computed':False,'original_field_presence_is_not_semantic_validation':True}
    failures=[x['case'] for x in native_tests if not x['oracle_passed']]
    report={'schema':'macro_text_coverage_audit.v2','unique_texts':len(rows),'versions':len(versions),
        'texts_over4000_normalized':sum(r['full_normalized_characters']>4000 for r in rows),
        'texts_truncated_by_inherited_scope':sum(r['truncated_by_inherited_scope'] for r in rows),
        'empty_texts':sum(r['empty'] for r in rows),'inherited_supported_spans':sum(len(r['inherited_evidence_spans']) for r in rows),
        'whole_retained_text_same_rule_matches':sum(len(r['whole_text_same_rule_raw_matches']) for r in rows),
        'native_action_texts':sum(bool(r['native_policy_action_diagnostic']) for r in rows),
        'native_claim_texts':sum(bool(r['native_semantic_claim_diagnostics']) for r in rows),
        'native_claim_spans':sum(len(r['native_semantic_claim_diagnostics']) for r in rows),
        'native_challenges':len(native_tests),'native_challenge_failures':failures,'native_semantic_admission':False,
        'universe_count':len(read('universe.json')),'base_models_fitted':0,'forecast_improvement_proven':False,
        'conclusion':'inherited_rule_coverage_remains_zero_on_whole_text; existing_native_rules_offer_diagnostics_but_fail_assertion_controls',
        'next_item':'repair_recovered_policy_action_scoping_v2',
        'limitations':['no price or forecast outcomes consumed; no phrase/weight tuning',
            'full text same-rule coverage is a recall diagnostic, not semantic validation',
            'non-ASCII ratio does not identify language or translation equivalence',
            'native rate_currency_sign is preserved only as unvalidated diagnostic, never FX direction',
            'original policy/numeric field presence does not prove correct issuer, assertion, units or expectations vintage',
            'native challenge failures block semantic feature admission pending a separate tested repair']}
    return {'text_coverage.json':rows,'native_function_challenges.json':native_tests,'version_field_inventory.json':inventory,
            'structured_field_report.json':structured,'coverage_report.json':report}
