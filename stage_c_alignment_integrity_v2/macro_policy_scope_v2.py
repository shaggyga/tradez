"""Conservative assertion guard around exact recovered native action/claim rules."""
from collections import Counter
import ast
import hashlib
import json
import re
from contracts import fingerprint
from macro_text_coverage_v2 import full_normalized,load_native,locate_evidence
from macro_receipt_population_v2 import require


def recovered_patterns(raw):
    tree=ast.parse(raw.decode('utf-8'));fn=next(n for n in tree.body if isinstance(n,ast.FunctionDef) and n.name=='explicit_policy_decision_action')
    names={'subject','rate','prefixes','actions'};assignments=[]
    allowed=(ast.Assign,ast.Name,ast.Store,ast.Load,ast.Constant,ast.Tuple,ast.JoinedStr,ast.FormattedValue)
    for n in fn.body:
        if isinstance(n,ast.Assign) and len(n.targets)==1 and isinstance(n.targets[0],ast.Name) and n.targets[0].id in names:
            require(all(isinstance(x,allowed) for x in ast.walk(n)),'native_pattern_assignment_shape')
            require(all(x.id in names for x in ast.walk(n) if isinstance(x,ast.Name)),'native_pattern_assignment_name')
            assignments.append(n)
    require({n.targets[0].id for n in assignments}==names,'native_pattern_components_missing')
    namespace={'__builtins__':{}}
    exec(compile(ast.Module(body=assignments,type_ignores=[]),'<pinned-native-pattern-assignments>','exec'),namespace)
    patterns=[{'action':action,'pattern':prefix+rf"\b{verb}\b.{{0,56}}\b{namespace['rate']}\b",'language':'inherited_English_rule'}
              for action,verb in namespace['actions'] for prefix in namespace['prefixes']]
    for n in fn.body:
        if isinstance(n,ast.If) and isinstance(n.test,ast.Call) and isinstance(n.test.func,ast.Attribute) and n.test.func.attr=='search':
            if len(n.body)==1 and isinstance(n.body[0],ast.Return) and isinstance(n.body[0].value,ast.Constant):
                action=n.body[0].value.value
                if action in {'cut','hike','hold'}:
                    patterns.append({'action':action,'pattern':ast.literal_eval(n.test.args[0]),'language':'inherited_Hungarian_rule'})
    require(len(patterns)==9,'native_pattern_inventory_changed')
    return patterns


def sentence_ranges(text):
    """Linear scan preserves decimals/known abbreviations and semicolon scope."""
    protected=set()
    for m in re.finditer(r'\b(?:e\.g\.|i\.e\.|U\.S\.|U\.K\.|Mr\.|Ms\.|Dr\.|Prof\.|vs\.|etc\.)',text,re.I):
        protected.update(range(m.start(),m.end()))
    start=0
    for i,ch in enumerate(text):
        if ch not in '.!?':continue
        if ch=='.' and (i in protected or (0<i<len(text)-1 and text[i-1].isdigit() and text[i+1].isdigit())):continue
        left=start
        while left<i and text[left].isspace():left+=1
        if left<i:yield left,i,ch
        start=i+1
    while start<len(text) and text[start].isspace():start+=1
    if start<len(text):yield start,len(text),''


def guard_reasons(clause,separator,action=None,language=None):
    reasons=[]
    if re.search(r'\b(?:not|never|no longer|false|denies?|denied|rejects?|rejected|ruled out)\b',clause,re.I):reasons.append('negated_or_denied')
    if re.search(r"\b(?:didn|doesn|isn|wasn|won|wouldn|couldn|hasn|haven)['’]t\b|\bno\s+(?:decision|intention|plan)\b",clause,re.I):reasons.append('negated_or_denied')
    if re.search(r'\b(?:if|unless|may|might|could|would|should|will|conditional|contingent|expected|expects?|forecast|predicted|proposed|proposal|wanted|whether|rumou?r|allegedly|reportedly|hypothetical)\b',clause,re.I):reasons.append('conditional_future_or_proposed')
    if any(q in clause for q in ('"','“','”')):reasons.append('quotation_unresolved')
    if re.search(r"(?<!\w)'[^']+'(?!\w)|‘[^’]+’",clause):reasons.append('quotation_unresolved')
    if re.search(r'\baccording to\b|\banalysts?\s+(?:said|say|expect)|\breport(?:ed|s)\s+that|\bclaims?\s+that',clause,re.I):reasons.append('attributed_claim_unresolved')
    if separator=='?':reasons.append('question_not_assertion')
    if re.search(r'\b(?:last year|last month|previous(?:ly)?|earlier|formerly|historical|had|in\s+20\d{2})\b',clause,re.I):reasons.append('historical_scope_unresolved')
    if re.search(r'\blast\s+(?:week|meeting|Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday)\b|\bin\s+(?:January|February|March|April|May|June|July|August|September|October|November|December)\b',clause,re.I):reasons.append('historical_scope_unresolved')
    if language=='inherited_Hungarian_rule' and re.search(r'\bnem\b',clause,re.I):
        if not (action=='hold' and re.search(r'\bnem\s+változtatta\b',clause,re.I)):reasons.append('Hungarian_negation_unresolved')
    return reasons


def issuer_markers(text):
    patterns={'federal_reserve':r'\bfederal reserve\b|\bfomc\b','governing_council_or_ecb':r'\bgoverning council\b|\beuropean central bank\b|\becb\b',
              'bank_of_england':r'\bbank of england\b','bank_of_japan':r'\bbank of japan\b',
              'hungarian_monetary_council':r'\bmonetáris tanács\b'}
    return sorted(k for k,p in patterns.items() if re.search(p,text,re.I))


def scoped(text,native,patterns):
    value,offsets=full_normalized(text);actions=[];claims=[];seen=set()
    for left,right,separator in sentence_ranges(value):
        clause=value[left:right]
        context_start,context_end=offsets[left],offsets[right-1]+1
        for rule in patterns:
            for m in re.finditer(rule['pattern'],clause,re.I):
                start,end=offsets[left+m.start()],offsets[left+m.end()-1]+1
                key=(rule['action'],start,end)
                if key in seen:continue
                seen.add(key);reasons=guard_reasons(clause,separator,rule['action'],rule['language'])
                actions.append({'action':rule['action'],'start':start,'end':end,'quote':text[start:end],
                    'context_start':context_start,'context_end':context_end,'guard_reasons':reasons,
                    'assertion_status':'rejected_or_unresolved' if reasons else 'asserted_linguistic_candidate','rule_language':rule['language']})
        for c in native.semantic_claim_decomposition(clause,[]):
            located=locate_evidence(clause,c['evidence_text']);reasons=guard_reasons(clause,separator)
            if located['start'] is None:
                start=end=None;quote=None;reasons.append('raw_span_unresolved')
            else:
                start,end=offsets[left+located['start']],offsets[left+located['end']-1]+1;quote=text[start:end]
            claims.append({'dimension':c['dimension'],'start':start,'end':end,'quote':quote,
                'context_start':context_start,'context_end':context_end,'guard_reasons':reasons,
                'assertion_status':'rejected_or_unresolved' if reasons else 'asserted_linguistic_candidate'})
    supported={a['action'] for a in actions if not a['guard_reasons']};issuers=issuer_markers(value)
    if len(issuers)>1:status='multiple_issuer_context_unresolved';action=None
    elif len(supported)>1:status='conflicting_asserted_actions';action=None
    elif supported:status='unambiguous_scoped_action_candidate';action=next(iter(supported))
    else:status='no_supported_asserted_action';action=None
    return {'action_candidate':action,'status':status,'action_evidence':actions,'claim_evidence':claims,
        'issuer_markers':issuers,'issuer_identity_verified':False,'policy_fact_admission':False,
        'forecast_admission':False,'market_expectation':None,'language_scope':'inherited_English_Hungarian_patterns_not_validated_multilingual_NLP'}


def build(blobs):
    read=lambda n:json.loads(blobs[n]);raw=blobs['recovered_policy_functions.py'];native=load_native(raw,read('RECOVERED_FUNCTIONS.json'));patterns=recovered_patterns(raw)
    cache=read('extraction_cache.json');bindings=read('version_bindings.json');results=[];index={}
    for c in cache:
        require(hashlib.sha256(c['retained_text'].encode()).hexdigest()==c['text_sha256'],'scoping_text_identity_mismatch')
        result={'cache_key':c['cache_key'],'text_sha256':c['text_sha256'],'native_action_diagnostic':native.explicit_policy_decision_action(c['retained_text']),
                **scoped(c['retained_text'],native,patterns)}
        results.append(result);index[c['cache_key']]=result
    regression=[]
    for old in read('original_challenges.json'):
        new=scoped(old['text'],native,patterns)
        if 'expected_action_or_abstention' in old:
            actual=new['action_candidate'] or '';passed=actual==old['expected_action_or_abstention']
        else:
            actual=[c['dimension'] for c in new['claim_evidence'] if not c['guard_reasons']];passed=actual==old['expected_dimensions']
        regression.append({'case':old['case'],'original_oracle_passed':old['oracle_passed'],'guarded_oracle_passed':passed,'guarded_actual':actual,'guarded_result':new})
    inventory=[]
    for b in bindings:
        r=index[b['cache_key']];context=b['kind']=='retained_parsed_detail' and not b['listing_bootstrap']
        inventory.append({'version_id':b['version_id'],'event_id':b['event_id'],'cache_key':b['cache_key'],'source_id':b['source_id'],
            'detail_context_eligible':context,'action_candidate':r['action_candidate'] if context else None,
            'asserted_claim_dimensions':sorted({c['dimension'] for c in r['claim_evidence'] if not c['guard_reasons']}) if context else [],
            'source_issuer_qualified':False,'original_extraction_ready_epoch':None,'forecast_admission':False})
    report={'schema':'recovered_policy_scope_guard.v2','unique_texts':len(results),'versions':len(bindings),'universe_count':len(read('universe.json')),
        'native_action_texts':sum(bool(r['native_action_diagnostic']) for r in results),
        'guarded_action_candidate_texts':sum(r['action_candidate'] is not None for r in results),
        'asserted_claim_spans':sum(not c['guard_reasons'] for r in results for c in r['claim_evidence']),
        'action_guard_rejections':dict(sorted(Counter(reason for r in results for a in r['action_evidence'] for reason in a['guard_reasons']).items())),
        'original_challenges':len(regression),'guarded_challenges_passed':sum(r['guarded_oracle_passed'] for r in regression),
        'original_failed_cases_repaired':sum(not r['original_oracle_passed'] and r['guarded_oracle_passed'] for r in regression),
        'native_pattern_sha256':fingerprint(patterns),'base_models_fitted':0,'forecast_improvement_proven':False,
        'policy_fact_admission':False,'forecast_admission':False,
        'limitations':['scoped linguistic candidates are not verified issuer policy facts or market expectations',
            'conservative sentence/modal/quote/history/issuer guards can reduce recall; generic third-party attribution may remain unresolved',
            'fixed inherited English/Hungarian patterns only; no broad NLP or translation validation',
            'native rate_currency_sign deliberately excluded from guarded claim outputs',
            'source-asof integration and original extraction readiness remain separate gates; no new model or live collector mutation']}
    return {'scoped_text_cache.json':results,'guard_regression.json':regression,'version_scope_inventory.json':inventory,
            'recovered_patterns.json':patterns,'scoping_report.json':report}
