"""Offline lexical policy context; source evidence is never an instruction or FX vote."""
from collections import Counter, defaultdict
import copy
from datetime import datetime, timezone
import hashlib
import re
from contracts import fingerprint

AXES = ('guidance', 'inflation', 'activity')


def epoch(value):
    parsed = datetime.fromisoformat(value.replace('Z', '+00:00'))
    if parsed.tzinfo is None:
        raise ValueError('aware_document_clock_required')
    return parsed.timestamp()


def normalized(text):
    """Preserve offsets into retained text while reproducing predecessor whitespace."""
    chars, offsets = [], []
    for token in re.finditer(r'\S+', text):
        if chars:
            chars.append(' ')
            offsets.append(token.start() - 1)
        chars.extend(token.group())
        offsets.extend(range(token.start(), token.end()))
    value = ''.join(chars)
    old = re.search(r'\bAt the monetary policy meeting in [A-Z][a-z]+\b', value, re.I)
    end = min(4000, old.start() if old and old.start() >= 50 else len(value))
    return value[:end], offsets[:end]


def clause_bounds(text, start, end):
    """Do not mistake decimal or common abbreviation dots for sentence ends.

    Protecting a known abbreviation can conservatively merge two sentences;
    it must never discard an earlier condition merely because of punctuation.
    """
    protected = set()
    for match in re.finditer(r'\b(?:e\.g\.|i\.e\.|U\.S\.|U\.K\.|Mr\.|Ms\.|Dr\.|Prof\.|vs\.|etc\.)', text, re.I):
        protected.update(range(match.start(), match.end()))
    boundaries = []
    for i, char in enumerate(text):
        if char not in '.;!?':
            continue
        if char == '.' and (i in protected or (0 < i < len(text)-1 and text[i-1].isdigit() and text[i+1].isdigit())):
            continue
        boundaries.append(i)
    left = max((i+1 for i in boundaries if i < start), default=0)
    right = min((i for i in boundaries if i >= end), default=len(text))
    return left, right


def extract(text, rules):
    if not isinstance(text, str) or not text.strip():
        return {'status': 'missing_text', 'axes': {a: {'value': None, 'status': 'missing_text'} for a in AXES}, 'evidence': []}
    value, offsets = normalized(text)
    evidence = []
    for rule in rules:
        # Preserve the recovered rules and explicitly repair their omitted "be".
        pattern = rule['pattern'].replace('(?:become )?necessary', '(?:(?:become|be) )?necessary')
        for match in re.finditer(pattern, value, re.I):
            left, right = clause_bounds(value, match.start(), match.end())
            clause = value[left:right]
            prefix = value[max(left, match.start()-100):match.start()]
            negated = bool(re.search(r'\b(?:not|never|no longer|unlikely|denies?|denied|rejects?|rejected)\b', prefix, re.I))
            conditional = bool(re.search(r'\b(?:if|unless|may|might|could|conditional|contingent)\b', clause, re.I))
            quoted = '"' in clause or '“' in clause or '”' in clause
            # A lexical baseline cannot safely resolve quoted or negated claims.
            state = 'negated_unresolved' if negated else 'quoted_unresolved' if quoted else 'conditional' if conditional else 'asserted_phrase'
            start, end = offsets[match.start()], offsets[match.end()-1]+1
            weight = rule['weight']
            if conditional:
                weight = max(-.35, min(.35, weight))
            evidence.append({'axis': rule['axis'], 'label': rule['label'], 'start': start, 'end': end,
                             'quote': text[start:end], 'state': state, 'weight': weight,
                             'context_start': offsets[left] if left < len(offsets) else start,
                             'context_end': offsets[right-1]+1 if right else end})
    axes = {}
    for axis in AXES:
        matched = [e for e in evidence if e['axis'] == axis]
        usable = [e for e in matched if e['state'] in ('asserted_phrase', 'conditional')]
        signs = {1 if e['weight'] > 0 else -1 for e in usable}
        status = 'conflicting_phrases' if len(signs) > 1 else 'matched_lexical_proxy' if usable else 'unresolved_phrases' if matched else 'no_supported_phrase'
        axes[axis] = {'value': max(usable, key=lambda e: abs(e['weight']))['weight'] if usable and len(signs) == 1 else None,
                      'status': status, 'conditional_only': bool(usable) and all(e['state'] == 'conditional' for e in usable)}
    return {'status': 'retained_text_extracted', 'axes': axes, 'evidence': evidence,
            'retained_text_sha256': hashlib.sha256(text.encode()).hexdigest(),
            'characters_examined': len(value), 'scope': 'English_phrase_proxy_not_policy_fact_or_market_surprise',
            'no_match_means_neutral': False,
            'grammar_extension': 'conditional_necessary_allows_be_or_become.v1'}


def documents(baseline, rules):
    created = epoch(baseline['created_utc'])
    if baseline.get('research_only') is not True or baseline.get('can_execute') is not False:
        raise ValueError('research_only_retained_baseline_required')
    result = []
    seen = set()
    for row in baseline['baselines']:
        if row.get('use_policy') != 'prior_context_only_not_outcome_evidence' or row.get('execution_eligible') is not False:
            raise ValueError('retained_context_use_boundary_required')
        text = row['summary']
        item = {'event_id': row['event_id'], 'currency': row['currency'], 'source_id': row['source_id'],
                'source_url': row['source_url'], 'document_class': row['document_class'],
                'published_epoch': epoch(row['published_utc']),
                'available_epoch': max(created, epoch(row['published_utc']), epoch(row['known_utc']), epoch(row['detail_available_utc'])),
                'original_record_sha256': fingerprint(row), 'retained_text': text,
                'original_raw_payload_sha256_reference': row['raw_payload_sha256'],
                'original_raw_payload_recovered': False,
                'extraction': extract(text, rules), 'actual_extraction_ready_epoch': None,
                'evidence_tier': 'retrospective_retained_context_reprocessing_not_historical_issuance'}
        item['document_id'] = fingerprint(item)
        if item['document_id'] not in seen:
            result.append(item)
            seen.add(item['document_id'])
    return sorted(result, key=lambda x: (x['available_epoch'], x['document_id']))


def asof(docs, cutoff, currencies, max_age_days=35):
    """Select only visible versions before deriving comparable-document changes."""
    visible = [d for d in docs if d['available_epoch'] <= cutoff and d['published_epoch'] <= cutoff]
    by_event = defaultdict(list)
    for d in visible:
        by_event[d['currency'], d['event_id'], d['document_class']].append(d)
    unique = []
    for key, variants in sorted(by_event.items()):
        latest_time = max(d['available_epoch'] for d in variants)
        latest = [d for d in variants if d['available_epoch'] == latest_time]
        material = {fingerprint([d['retained_text'], d['published_epoch'], d['source_id']]) for d in latest}
        references = sorted(d['document_id'] for d in latest)
        if len(material) == 1:
            # Hash order selects a reference among identical material, never a stance.
            item = copy.deepcopy(min(latest, key=lambda d: d['document_id']))
            item['version_status'] = 'identical_mirrors' if len(latest) > 1 else 'unambiguous_version'
            item['contributing_document_ids'] = references
            unique.append(item)
        else:
            # Block every affected source/class lane, including prior change values.
            # Use max publication only for conservative recency/staleness; no text wins.
            for source in sorted({d['source_id'] for d in latest}):
                item = {'currency':key[0], 'event_id':key[1], 'document_class':key[2], 'source_id':source,
                        'published_epoch':max(d['published_epoch'] for d in latest), 'available_epoch':latest_time,
                        'document_id':None, 'version_status':'ambiguous_same_clock_versions',
                        'contributing_document_ids':references,
                        'extraction':{'axes':{a:{'value':None,'status':'ambiguous_same_clock_versions','conditional_only':False} for a in AXES}}}
                unique.append(item)
    states = []
    for currency in currencies:
        matching = [d for d in unique if d['currency'] == currency]
        fresh = [d for d in matching if cutoff-d['published_epoch'] <= max_age_days*86400]
        selected = []
        for source, kind in sorted({(d['source_id'], d['document_class']) for d in fresh}):
            candidates = sorted([d for d in fresh if (d['source_id'],d['document_class']) == (source,kind)], key=lambda d: (d['published_epoch'],d['available_epoch'],d['document_id'] or ''))
            current = candidates[-1]
            prior = [d for d in candidates if d['published_epoch'] < current['published_epoch'] and d['event_id'] != current['event_id']]
            previous = prior[-1] if prior else None
            change = {}
            for axis in AXES:
                now = current['extraction']['axes'][axis]['value']
                before = previous['extraction']['axes'][axis]['value'] if previous else None
                change[axis] = {'value': now-before if now is not None and before is not None else None,
                                'status': 'comparable_lexical_change' if now is not None and before is not None else 'no_comparable_supported_prior'}
            selected.append({'document_id': current['document_id'], 'prior_document_id': previous['document_id'] if previous else None,
                             'version_status':current['version_status'], 'contributing_document_ids':current['contributing_document_ids'],
                             'prior_contributing_document_ids':previous['contributing_document_ids'] if previous else [],
                             'source_id': source, 'document_class': kind, 'age_seconds': cutoff-current['published_epoch'],
                             'axes': copy.deepcopy(current['extraction']['axes']), 'change': change})
        states.append({'currency': currency, 'status': 'retained_context' if selected else 'stale_context' if matching else 'no_available_context',
                       'documents': selected, 'observed_zero_is_missing': False,
                       'expectation': None, 'numeric_surprise': None, 'observed_reaction': None})
    return states


def pair_view(states, universe, cutoff):
    index = {s['currency']: s for s in states}
    rows = []
    for pair in universe:
        base, quote = pair.split('_')
        rows.append({'instrument': pair, 'cutoff_epoch': cutoff, 'base': index[base], 'quote': index[quote],
                     'directional_forecast': None, 'forecast_eligible': False, 'can_place_orders': False,
                     'outcomes_included': False, 'shared_events_are_independent_votes': False})
    return rows


def build(baseline, universe, rules, cutoffs):
    docs = documents(baseline, rules)
    currencies = sorted({c for p in universe for c in p.split('_')})
    snapshots, pairs = [], []
    for value in cutoffs:
        cutoff = epoch(value)
        states = asof(docs, cutoff, currencies)
        snapshots.append({'cutoff': value, 'currency_states': states})
        pairs.extend(pair_view(states, universe, cutoff))
    counts = Counter(e['state'] for d in docs for e in d['extraction']['evidence'])
    report = {'status': 'retained_text_layer_completed', 'documents': len(docs), 'pair_rows': len(pairs),
              'universe_count': len(universe), 'currency_count': len(currencies), 'evidence_span_states': dict(counts),
              'comparable_change_values': sum(v['value'] is not None for s in snapshots for c in s['currency_states'] for d in c['documents'] for v in d['change'].values()),
              'base_models_fitted': 0, 'network_calls': 0, 'engineering_ready': False,
              'forecast_evidence_status': 'retrospective_lexical_context_only_not_forecast_improvement',
              'policy_evidence_status': 'not_evaluated', 'demo_authorization_status': 'not_granted', 'independent_review': False,
              'limitations': ['selected_prior_context_not_all_release_population', 'retained_summaries_may_be_truncated',
                             'original_transport_receipts_not_recovered', 'English_lexical_baseline_not_validated_NLP',
                             'actual_extraction_latency_not_historically_observed', 'no_2024_admission',
                             'no_numeric_expectations_or_surprise', 'no_outcome_study_or_confirmation']}
    return {'text_documents.json': docs, 'currency_states.json': snapshots, 'pair_context.json': pairs, 'run_report.json': report}
