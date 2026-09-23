"""Independent arithmetic/provenance review of a frozen JSON extract only.

No runtime modules, databases or original scoring helpers are imported.
Writes are exclusive new files in this script's own review directory.
"""
from collections import Counter, defaultdict
from datetime import datetime, timezone
from decimal import Decimal
import hashlib
import itertools
import json
import math
from pathlib import Path
import statistics


OUT = Path(__file__).resolve().parent
PROJECT = OUT.parent.parent / 'trad'
SOURCE = PROJECT / 'docs' / 'validation' / 'fixed_evaluation_20260906'
RAW = SOURCE / 'selected_candidate_rows.json'
PRIOR = PROJECT / 'FOREX_FIXED_EVALUATION_RESULTS_20260906.json'
FAMILIES = ('cross_pair_graph_transfer', 'modern_tabular_probabilistic_repaired',
            'probabilistic_state_space', 'ridge_return_repaired')
PIP = Decimal('0.0001')


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def sign(value):
    return int(value > 0)-int(value < 0)


def epoch(value):
    return datetime.fromisoformat(value.replace('Z', '+00:00')).timestamp()


def price(value):
    return Decimal(str(value))


def mid(bid, ask):
    return (price(bid)+price(ask))/2


def mean(values):
    return statistics.fmean(values) if values else None


def corr(xs, ys):
    dx = [x-mean(xs) for x in xs]
    dy = [y-mean(ys) for y in ys]
    denominator = math.sqrt(sum(x*x for x in dx)*sum(y*y for y in dy))
    return sum(x*y for x,y in zip(dx,dy))/denominator if denominator else None


def main():
    records = json.loads(RAW.read_text(encoding='utf-8-sig'))
    prior = json.loads(PRIOR.read_text(encoding='utf-8-sig'))
    groups = defaultdict(dict)
    provenance_errors = []
    integrity_events = []
    metadata = {family: {'all_forecasts':0, 'missing_outcomes':0,
                        'side_probability_disagreements_all':0} for family in FAMILIES}
    coalesced_fields = ('family','direction','entry_bid','entry_ask','entry_time','instrument',
                        'input_timeframe','model_version','feature_version','pip','probability_up')
    comparison_fields = ('event_id','family','direction','entry_bid','entry_ask','entry_time',
                         'instrument','input_timeframe','pip')
    for record in records:
        event = record['event_id']
        encoded = record['forecast_json'].encode('utf-8')
        payload = json.loads(encoded)
        if hashlib.sha256(encoded).hexdigest() != record['payload_sha256']:
            provenance_errors.append({'event_id':event,'error':'payload_hash_mismatch'})
        if payload['id'] != event:
            provenance_errors.append({'event_id':event,'error':'payload_id_mismatch'})
        for field in coalesced_fields:
            if field in record and record.get(field) != payload.get(field):
                provenance_errors.append({'event_id':event,'error':'column_payload_mismatch:'+field})
        family = payload['family']
        assert family in FAMILIES
        assert payload['instrument'] == 'EUR_USD' and payload['input_timeframe'] == 'M1'
        assert payload['remaining_horizons'] == [3600] and price(payload['pip']) == PIP
        assert 0 <= payload['probability_up'] <= 1
        assert payload['direction'] in ('buy','sell')
        assert price(payload['entry_ask']) >= price(payload['entry_bid']) > 0
        reference = payload['entry_time']
        assert family not in groups[reference], (reference,family)
        direction = 1 if payload['direction'] == 'buy' else -1
        expected_side = 1 if payload['expected_signed_pips'] >= 0 else -1
        assert direction == expected_side, event
        assert payload['account_eligible'] is False and payload['can_place_orders'] is False
        metadata[family]['all_forecasts'] += 1
        metadata[family]['side_probability_disagreements_all'] += direction != sign(payload['probability_up']-.5)
        outcome = record.get('outcome')
        if outcome is not None:
            for field in comparison_fields:
                expected_value = event if field == 'event_id' else payload.get(field)
                if outcome.get(field) != expected_value:
                    provenance_errors.append({'event_id':event,'error':'outcome_forecast_mismatch:'+field})
            assert outcome['horizon_sec'] == 3600
            assert price(outcome['exit_ask']) >= price(outcome['exit_bid']) > 0
        else:
            metadata[family]['missing_outcomes'] += 1
        integrity_events.extend(record.get('integrity_events', []))
        groups[reference][family] = {'raw':record,'payload':payload,'side':direction,'outcome':outcome}
    assert not provenance_errors, provenance_errors[:5]
    excluded = []
    paired = []
    samples = {family: [] for family in FAMILIES}
    outcome_class = Counter()
    flat_float_disagreement = 0
    producer_counts = Counter()
    per_day = Counter()
    stored_net_errors = []
    reference_recording_lags = []
    probability_direction_all_pairs = {}
    for reference, group in sorted(groups.items(), key=lambda item:epoch(item[0])):
        assert set(group) == set(FAMILIES)
        entry_quotes = {(record['payload']['entry_bid'],record['payload']['entry_ask']) for record in group.values()}
        assert len(entry_quotes) == 1
        for record in group.values():
            reference_recording_lags.append(epoch(record['raw']['recorded_utc'])-epoch(reference))
        missing = [family for family,record in group.items() if record['outcome'] is None]
        if missing:
            excluded.append({'reference':reference,'reason':'missing_one_or_more_outcomes','missing_families':missing})
            continue
        endpoints = {(record['outcome']['exit_time'],record['outcome']['exit_bid'],record['outcome']['exit_ask'])
                     for record in group.values()}
        if len(endpoints) != 1:
            excluded.append({'reference':reference,'reason':'different_endpoint_clock_or_quote'})
            continue
        exemplar = group[FAMILIES[0]]
        entry_bid, entry_ask = next(iter(entry_quotes))
        exit_time, exit_bid, exit_ask = next(iter(endpoints))
        move = (mid(exit_bid,exit_ask)-mid(entry_bid,entry_ask))/PIP
        actual_side = sign(move)
        outcome_class['up' if move > 0 else 'down' if move < 0 else 'flat'] += 1
        float_move = ((exit_bid+exit_ask)/2-(entry_bid+entry_ask)/2)/.0001
        flat_float_disagreement += sign(float_move) != actual_side
        diagnostics = json.loads(exemplar['outcome']['diagnostics_json'])
        producer = diagnostics.get('maturity_worker','legacy_recovered_provider_quote_clock')
        producer_counts[producer] += 1
        day = reference[:10]
        per_day[day] += 1
        pair = {'reference':reference,'exit_time':exit_time,'actual_signed_move_pips':float(move),
                'outcome_class':actual_side,'entry_bid':entry_bid,'entry_ask':entry_ask,
                'exit_bid':exit_bid,'exit_ask':exit_ask,'producer':producer,'families':{}}
        spread_drag = ((price(entry_ask)-price(entry_bid))+(price(exit_ask)-price(exit_bid)))/(2*PIP)
        for family, record in group.items():
            payload, direction, outcome = record['payload'], record['side'], record['outcome']
            p = payload['probability_up']
            up_label = int(move>0)
            net = ((price(exit_bid)-price(entry_ask)) if direction>0 else (price(entry_bid)-price(exit_ask)))/PIP
            gross = Decimal(direction)*move
            assert gross-net == spread_drag
            if abs(float(net)-outcome['theoretical_pips']) > .00000051:
                stored_net_errors.append({'event_id':payload['id'],'computed_net':float(net),
                                          'stored_net':outcome['theoretical_pips']})
            item = {
                'event_id':payload['id'],'side':direction,'probability_up':p,
                'expected_signed_pips':payload['expected_signed_pips'],
                'correct':direction==actual_side if actual_side else False,
                'net_pips':float(net),'positive_net':net>0,'gross_pips':float(gross),
                'brier':(p-up_label)**2,
                'absolute_error_pips':float(abs(price(payload['expected_signed_pips'])-move)),
                'actual_up':up_label,'actual_side':actual_side,
                'probability_direction_correct':sign(p-.5)==actual_side if actual_side else False,
                'probability_tie':p==.5,'expected_magnitude_zero':payload['expected_signed_pips']==0,
                'side_probability_disagreement':direction != sign(p-.5),
                'spread_drag_pips':float(spread_drag),
            }
            samples[family].append(item)
            pair['families'][family] = item
        paired.append(pair)
    totals = len(paired)
    metrics = {}
    prior_matches = {}
    for family, rows in samples.items():
        correct = sum(x['correct'] for x in rows)
        long = [x for x in rows if x['side']==1]
        short = [x for x in rows if x['side']==-1]
        up = [x for x in rows if x['actual_side']==1]
        down = [x for x in rows if x['actual_side']==-1]
        metrics[family] = metadata[family] | {
            'paired_count':totals,'correct_direction_count':correct,
            'incorrect_including_flat':totals-correct,
            'direction_accuracy':correct/totals,
            'direction_accuracy_excluding_one_flat':correct/(totals-outcome_class['flat']),
            'predicted_buy_count':len(long),'predicted_sell_count':len(short),
            'correct_buy_count':sum(x['correct'] for x in long),
            'correct_sell_count':sum(x['correct'] for x in short),
            'up_recall':sum(x['correct'] for x in up)/len(up),
            'down_recall':sum(x['correct'] for x in down)/len(down),
            'balanced_accuracy_nonflat':(sum(x['correct'] for x in up)/len(up)+sum(x['correct'] for x in down)/len(down))/2,
            'positive_net_count':sum(x['positive_net'] for x in rows),
            'positive_net_rate':mean([x['positive_net'] for x in rows]),
            'mean_net_bid_ask_pips':mean([x['net_pips'] for x in rows]),
            'mean_gross_pips':mean([x['gross_pips'] for x in rows]),
            'mean_spread_drag_pips':mean([x['spread_drag_pips'] for x in rows]),
            'mean_net_minus_0_25_pip':mean([x['net_pips']-.25 for x in rows]),
            'brier_up_vs_nonup':mean([x['brier'] for x in rows]),
            'signed_move_mae_pips':mean([x['absolute_error_pips'] for x in rows]),
            'probability_direction_correct_count':sum(x['probability_direction_correct'] for x in rows),
            'probability_direction_accuracy':mean([x['probability_direction_correct'] for x in rows]),
            'side_probability_disagreements_paired':sum(x['side_probability_disagreement'] for x in rows),
            'probability_ties':sum(x['probability_tie'] for x in rows),
            'expected_zero_forecasts':sum(x['expected_magnitude_zero'] for x in rows),
        }
        comparator = prior['archive_family_metrics'][family]
        mappings = {
            'direction_accuracy':'gross_direction_accuracy_flats_miss',
            'positive_net_rate':'positive_net_bid_ask_rate',
            'mean_net_bid_ask_pips':'mean_net_bid_ask_pips',
            'mean_spread_drag_pips':'mean_spread_drag_pips',
            'mean_net_minus_0_25_pip':'mean_net_bid_ask_minus_additional_0_25pip_slippage',
            'brier_up_vs_nonup':'brier_up_vs_nonup',
            'signed_move_mae_pips':'signed_move_mae_pips',
        }
        prior_matches[family] = {key:{'recomputed':metrics[family][key],'prior':comparator[prior_key],
                                      'absolute_difference':abs(metrics[family][key]-comparator[prior_key]),
                                      'matches_absolute_1e_9':abs(metrics[family][key]-comparator[prior_key])<=1e-9}
                                for key,prior_key in mappings.items()}
    pair_comovement = []
    for first,second in itertools.combinations(FAMILIES,2):
        a,b = samples[first],samples[second]
        pair_comovement.append({'first':first,'second':second,
            'direction_agreement_count':sum(x['side']==y['side'] for x,y in zip(a,b)),
            'direction_agreement_rate':mean([x['side']==y['side'] for x,y in zip(a,b)]),
            'expected_signed_pips_pearson':corr([x['expected_signed_pips'] for x in a],[x['expected_signed_pips'] for x in b]),
            'probability_up_pearson':corr([x['probability_up'] for x in a],[x['probability_up'] for x in b]),
            'net_pips_pearson':corr([x['net_pips'] for x in a],[x['net_pips'] for x in b]),
            'both_correct_count':sum(x['correct'] and y['correct'] for x,y in zip(a,b)),
            'both_wrong_count':sum(not x['correct'] and not y['correct'] for x,y in zip(a,b)),
        })
    correctness_histogram = Counter(sum(record['correct'] for record in pair['families'].values()) for pair in paired)
    agreement_histogram = Counter(sum(record['side']==1 for record in pair['families'].values()) for pair in paired)
    exclusion_counts = Counter(record['reason'] for record in excluded)
    baseline = {
        'always_sell_correct':outcome_class['down'],'always_sell_accuracy':outcome_class['down']/totals,
        'always_buy_correct':outcome_class['up'],'always_buy_accuracy':outcome_class['up']/totals,
        'constant_0_5_brier':.25,
        'zero_move_mae_pips':mean([abs(pair['actual_signed_move_pips']) for pair in paired]),
        'no_trade_net_pips':0.0,
        'note':'Constant direction baselines are descriptive comparisons on this inspected sample, not a strategy selected for deployment.',
    }
    assert totals == prior['archive_population']['complete_matched_endpoint_epochs']
    assert dict(outcome_class) == prior['archive_class_balance']
    assert dict(exclusion_counts) == prior['archive_population']['exclusion_epoch_counts']
    assert all(result['matches_absolute_1e_9'] for family in prior_matches.values() for result in family.values())
    result = {
        'schema_version':'independent_saved_four_family_sanity_v1',
        'generated_utc':datetime.now(timezone.utc).isoformat(),
        'method':'Raw frozen forecast_json bytes and joined outcome rows; independent decimal price arithmetic; standard library only; no original scoring helper imported.',
        'source_bindings':{str(path.relative_to(PROJECT)):sha(path) for path in (RAW,PRIOR)},
        'script_sha256':sha(Path(__file__)),
        'raw_forecasts':len(records),'raw_unique_references':len(groups),
        'matched_four_family_references':totals,'raw_missing_outcomes':sum(x['missing_outcomes'] for x in metadata.values()),
        'paired_exclusions':dict(exclusion_counts),'paired_class_balance':dict(outcome_class),
        'utc_day_counts':dict(per_day),'producer_counts':dict(producer_counts),
        'all_raw_payload_hashes_verified':True,'column_payload_outcome_mismatches':provenance_errors,
        'stored_theoretical_net_disagreements_above_0_00000051_pip':stored_net_errors,
        'decimal_vs_float_label_disagreements':flat_float_disagreement,
        'retained_integrity_events':integrity_events,
        'all_reference_quotes_precede_recording':min(reference_recording_lags)>0,
        'reference_to_recording_lag_sec':{'minimum':min(reference_recording_lags),'median':statistics.median(reference_recording_lags),'maximum':max(reference_recording_lags)},
        'family_metrics':metrics,'baseline_comparisons':baseline,
        'prior_metrics_comparison':prior_matches,
        'pairwise_comovement_descriptive_only':pair_comovement,
        'correct_model_count_per_epoch':dict(sorted(correctness_histogram.items())),
        'buy_model_count_per_epoch':dict(sorted(agreement_histogram.items())),
        'interpretation':[
            'Reported 42.39%-47.83% refers to preserved emitted buy/sell direction on 368 matched archived EURUSD one-hour endpoints, not every model and not all market regimes.',
            'Those four direction percentages reproduce exactly. Each model correctly predicted 156-176 of these 368 outcomes; none accurate is too absolute.',
            'All four emitted-direction accuracies fall below 50% on this sample; the down-majority constant-direction comparator is 195/368.',
            'Probability-up is scored independently of emitted direction, which follows expected_signed_pips. The tabular mean-return direction and probability-majority direction can disagree without proving a sign error.',
            'One flat endpoint counts as a direction miss and a non-up Brier label. Removing it changes percentages only slightly.',
            'Stored historical quotes predate forecast recording and retained target clocks differ by producer. These diagnostics do not establish realizable trading returns or causal out-of-sample accuracy.',
            'Forecast horizons overlap and cover six UTC days; independent-trial confidence or significance claims are not justified by the raw row count.',
            'Comovement calculations describe this already inspected matched subset; no ensemble, signal inversion or threshold optimization was performed.',
        ],
        'proof_eligible':False,'account_eligible':False,'runtime_started':False,
    }
    for name,payload in [('independent_four_family_results.json',result),
                         ('independently_paired_endpoints.json',paired),
                         ('independently_excluded_references.json',excluded)]:
        with (OUT/name).open('x',encoding='utf-8') as handle:
            json.dump(payload,handle,indent=2,allow_nan=False)
            handle.write('\n')
    print(json.dumps({key:result[key] for key in ('raw_forecasts','raw_unique_references','matched_four_family_references',
                     'raw_missing_outcomes','paired_exclusions','paired_class_balance','decimal_vs_float_label_disagreements')}))
    print(json.dumps(metrics,indent=2))
    print(json.dumps({'baseline':baseline,'comovement':pair_comovement,'correctness':dict(correctness_histogram)},indent=2))


if __name__=='__main__':
    main()
