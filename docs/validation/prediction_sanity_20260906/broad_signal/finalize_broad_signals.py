"""Finalize the frozen broad-signal extraction; no SQLite or project imports."""
import os
import sys
import json
import hashlib
import statistics
from collections import Counter, defaultdict
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

OUT = Path(__file__).resolve().parent
ROOT = OUT.parents[1] / 'trad'
sys.dont_write_bytecode = True


def guard(event, args):
    if event in {'sqlite3.connect', 'socket.connect', 'socket.connect_ex', 'socket.bind',
                 'subprocess.Popen', 'os.system', 'os.startfile'}:
        raise RuntimeError('Offline finalization forbids DB, network and process actions')
    if event == 'open' and isinstance(args[0], (str, bytes, os.PathLike)):
        path, mode, flags = args
        writing = ((isinstance(mode, str) and any(c in mode for c in 'wax+')) or
                   (isinstance(flags, int) and flags & (os.O_WRONLY | os.O_RDWR | os.O_CREAT | os.O_TRUNC | os.O_APPEND)))
        if writing and not Path(os.fsdecode(path)).resolve().is_relative_to(OUT):
            raise RuntimeError('Writes restricted to this evidence directory')


sys.addaudithook(guard)


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def stats(values):
    v = sorted(values)
    return {'n': len(v), 'min': v[0], 'median': statistics.median(v),
            'p95': v[min(len(v)-1, int(.95*len(v)))], 'max': v[-1],
            'mean': statistics.fmean(v)} if v else {'n': 0}


def epoch(value):
    return datetime.fromisoformat(value.replace('Z', '+00:00')).timestamp()


preliminary = json.loads((OUT / 'broad_signal_raw_verification.json').read_bytes())
rows = [json.loads(line) for line in (OUT / 'selected_rows.jsonl').read_text(encoding='utf-8').splitlines()]
assert sha(OUT / 'selected_rows.jsonl') == preliminary['selected_rows_sha256']
assert len(rows) == 8414
pip_by_instrument = defaultdict(list)
mismatches = defaultdict(list)
transitions = []
float_up = float_down = decimal_up = decimal_down = decimal_hits = decimal_wins = 0
entry_spreads, exit_spreads, spread_drag, gross_bps, net_bps = [], [], [], [], []
for r in rows:
    def check(label, invalid):
        if invalid:
            mismatches[label].append(r['id'])
    sign = 1 if r['direction'] == 'buy' else -1
    check('invalid_direction', r['direction'] not in ('buy', 'sell'))
    check('invalid_prices', not (0 < r['entry_bid'] < r['entry_ask'] and 0 < r['exit_bid'] <= r['exit_ask']))
    assert r['entry_spread_pips'] > 0
    # The legacy ledger omits pip_size. Infer its retained convention; this is
    # an internal consistency test, not broker-instrument-metadata validation.
    pip = (r['entry_ask'] - r['entry_bid']) / r['entry_spread_pips']
    pip_by_instrument[r['instrument']].append(pip)
    entry = (r['entry_bid'] + r['entry_ask']) / 2
    target = (r['exit_bid'] + r['exit_ask']) / 2
    move = target - entry
    raw_net = r['exit_bid'] - r['entry_ask'] if sign == 1 else r['entry_bid'] - r['exit_ask']
    gross, net = sign * move / pip, raw_net / pip
    entry_spread = (r['entry_ask'] - r['entry_bid']) / pip
    exit_spread = (r['exit_ask'] - r['exit_bid']) / pip
    check('entry_mid', abs(entry - r['entry_mid']) > 1e-12)
    check('exit_mid', abs(target - r['exit_mid']) > 1e-12)
    check('gross_pips', abs(gross - r['gross_pips']) > 1e-7)
    check('net_pips', abs(net - r['net_pips']) > 1e-7)
    check('win', int(net > 0) != r['win'])
    check('spread_identity', abs(net - (gross - (entry_spread + exit_spread)/2)) > 1e-7)
    check('target_equals_entry_plus_horizon', abs(r['target_epoch']-r['opened_epoch']-r['horizon_sec']) > 1e-6)
    check('outcome_provider_quote_target_window', not 0 <= r['outcome_quote_epoch']-r['target_epoch'] <= 60)
    check('closed_equals_provider_quote', r['closed_epoch'] != r['outcome_quote_epoch'])
    check('stored_target_distance', abs(r['target_quote_distance_sec']-(r['outcome_quote_epoch']-r['target_epoch'])) > 1e-6)
    entry_spreads.append(entry_spread)
    exit_spreads.append(exit_spread)
    spread_drag.append((entry_spread+exit_spread)/2)
    gross_bps.append(10000*sign*move/entry)
    net_bps.append(10000*raw_net/entry)
    float_up += move > 0
    float_down += move < 0
    d = lambda key: Decimal(str(r[key]))
    decimal_move = (d('exit_bid')+d('exit_ask')-d('entry_bid')-d('entry_ask'))/2
    decimal_net = d('exit_bid')-d('entry_ask') if sign == 1 else d('entry_bid')-d('exit_ask')
    decimal_up += decimal_move > 0
    decimal_down += decimal_move < 0
    decimal_hits += sign*decimal_move > 0
    decimal_wins += decimal_net > 0
    if move != 0 and decimal_move == 0:
        transitions.append({key: r[key] for key in ('id', 'instrument', 'direction', 'entry_bid', 'entry_ask',
                             'exit_bid', 'exit_ask', 'gross_pips', 'net_pips')} |
                           {'float_midpoint_move': move, 'decimal_midpoint_move': str(decimal_move),
                            'spurious_hit': r['gross_pips'] > 0})

assert not mismatches, dict(mismatches)
pip_map = {}
exotic = {}
for instrument, values in sorted(pip_by_instrument.items()):
    inferred = statistics.median(values)
    assert max(values)-min(values) < 1e-12
    pip_map[instrument] = {'rows': len(values), 'inferred_pip': inferred}
    naive = .01 if instrument.endswith('_JPY') else .0001
    if abs(inferred-naive) > 1e-10:
        exotic[instrument] = pip_map[instrument] | {'invalid_suffix_assumption': naive}

n = len(rows)
float_hits = sum(r['gross_pips'] > 0 for r in rows)
float_wins = sum(r['win'] == 1 for r in rows)


def class_summary(up, down, hits, wins):
    flat = n-up-down
    return {'rows': n, 'up': up, 'down': down, 'flat': flat, 'direction_hits': hits,
            'direction_accuracy': hits/n, 'direction_accuracy_pct': 100*hits/n,
            'nonflat_rows': n-flat, 'nonflat_direction_accuracy': hits/(n-flat),
            'nonflat_direction_accuracy_pct': 100*hits/(n-flat),
            'net_positive_rows': wins, 'net_positive_rate': wins/n, 'net_positive_pct': 100*wins/n,
            'fair_random_sign_flats_count_as_misses_expected_accuracy': (up+down)/(2*n),
            'fair_random_sign_flats_count_as_misses_expected_accuracy_pct': 100*(up+down)/(2*n),
            'same_row_always_buy_accuracy': up/n, 'same_row_always_sell_accuracy': down/n,
            'same_row_hindsight_majority_accuracy': max(up, down)/n,
            'hits_that_do_not_clear_spread': hits-wins,
            'fraction_of_direction_hits_not_clearing_spread': (hits-wins)/hits}


late = [r for r in rows if epoch(r['observed_at']) >= r['target_epoch']]
late_outcome = [r for r in rows if epoch(r['observed_at']) >= r['outcome_quote_epoch']]
float_summary = class_summary(float_up, float_down, float_hits, float_wins)
decimal_summary = class_summary(decimal_up, decimal_down, decimal_hits, decimal_wins)
assert (n, float_hits, float_wins) == (8414, 4156, 716)
assert (decimal_hits, decimal_summary['flat'], decimal_wins, len(transitions)) == (4148, 296, 716, 18)
spurious = [r['id'] for r in transitions if r['spurious_hit']]
assert len(spurious) == 8
report = {
    'generated_utc': datetime.now(timezone.utc).isoformat(),
    'scope': 'Final offline arithmetic and timing sanity check of the exact frozen 8,414-row v5 matured, maturity_valid=1 extraction; no executable edge claim.',
    'supersedes': {'filename': 'broad_signal_raw_verification.json',
        'sha256': sha(OUT/'broad_signal_raw_verification.json'),
        'reason': 'The preliminary suffix-based pip assumption was invalid for 247 rows across four instruments. Its recomputed mixed-pip means, spread-drag means and arithmetic-mismatch claims are invalid. This report supersedes the entire preliminary analysis; original extraction, row counts, prices, clocks, bps, hashes and duplicate counts were valid and are retained.'},
    'raw_extraction': {k: preliminary[k] for k in ('database', 'query', 'parameters', 'query_plan', 'query_seconds',
        'query_row_limit', 'database_and_wal_before', 'database_and_wal_after', 'unchanged', 'shm_caveat',
        'ledger_revision', 'version_status_counts', 'selected_rows_sha256')},
    'raw_stored_float_metrics': float_summary,
    'raw_count_match_saved_report': True,
    'decimal_retained_price_sensitivity': decimal_summary | {
        'method': 'Decimal(str(stored bid/ask)) midpoint comparison. These are exact decimal forms of retained float prices, not unavailable original vendor strings. Flat outcomes count as misses.',
        'changes_to_production': False,
        'percentage_point_direction_change': 100*(decimal_hits-float_hits)/n,
        'nonzero_float_to_decimal_flat_count': len(transitions),
        'spurious_positive_hit_ids': sorted(spurious),
        'all_nonzero_float_to_flat_transitions': sorted(transitions, key=lambda r: r['id']),
        'negative_nonzero_float_to_flat_count': sum(not r['spurious_hit'] for r in transitions)},
    'internal_arithmetic_check': {'mismatch_count': 0, 'tolerance_pips': 1e-7,
        'pip_method': '(entry_ask-entry_bid)/stored_entry_spread_pips; positive, constant per instrument within 1e-12; no stored pip_size or original broker metadata exists to independently validate that convention.',
        'pip_map': pip_map, 'nonstandard_relative_to_suffix_assumption': exotic,
        'nonstandard_row_count': sum(r['rows'] for r in exotic.values()),
        'stored_mean_gross_pips_mixed_instrument_diagnostic': statistics.fmean(r['gross_pips'] for r in rows),
        'stored_mean_net_pips_mixed_instrument_diagnostic': statistics.fmean(r['net_pips'] for r in rows),
        'entry_spread_pips': stats(entry_spreads), 'exit_spread_pips': stats(exit_spreads),
        'roundtrip_spread_drag_pips': stats(spread_drag),
        'mean_gross_bps_equal_row_descriptive': statistics.fmean(gross_bps),
        'mean_net_bps_equal_row_descriptive': statistics.fmean(net_bps),
        'bps_method': 'Computed directly from retained prices and entry midpoint, without pip assumptions. Equal-row descriptive means are not actual portfolio returns.'},
    'selection_and_dependence': {k: preliminary[k] for k in ('duplicates', 'family_counts', 'direction_source_counts',
        'eligibility_counts', 'horizon_1_to_3min_weight', 'population_caveats')},
    'horizon_stored_counts': {str(h): {'rows': len(g), 'direction_hits': sum(r['gross_pips'] > 0 for r in g),
        'net_positive': sum(r['win'] == 1 for r in g)} for h in sorted({r['horizon_sec'] for r in rows})
        for g in [[r for r in rows if r['horizon_sec'] == h]]},
    'policy_stored_counts': {p: {'rows': len(g), 'direction_hits': sum(r['gross_pips'] > 0 for r in g),
        'net_positive': sum(r['win'] == 1 for r in g)} for p in sorted({r['policy_state'] for r in rows})
        for g in [[r for r in rows if r['policy_state'] == p]]},
    'timing': {k: preliminary[k] for k in ('target_quote_delay_sec', 'signal_snapshot_time_minus_entry_quote_sec',
        'entry_quotes_before_signal_snapshot_time', 'signal_snapshot_more_than_90sec_old_at_entry_quote',
        'missing_availability_fields')} | {
        'signal_snapshot_at_or_after_original_target': len(late),
        'snapshot_at_or_after_target_direction_hits': sum(r['gross_pips'] > 0 for r in late),
        'snapshot_at_or_after_target_net_positive': sum(r['win'] == 1 for r in late),
        'snapshot_at_or_after_outcome_provider_quote': len(late_outcome),
        'snapshot_at_or_after_target_rows': [{k: r[k] for k in ('id', 'instrument', 'horizon_sec', 'observed_at',
            'opened_at', 'target_at', 'outcome_quote_at')} for r in sorted(late, key=lambda r: r['id'])],
        'interpretation': 'observed_at is the signal snapshot updated_at/generated_utc, not a local read clock. These 27 selected snapshots are timestamped at or after a retrospectively assigned target. Underlying original forecast issue/publication and actual quote local receipt are not retained, so causal availability cannot be established.'},
    'current_registered_scorer_precision_followup': {
        'files_unmodified': True,
        'references': ['oanda_top_signal_position_ledger.py:707-712 midpoint subtraction; :926 strict >0 count',
            'oanda_causal_forecast_ledger.py:254 float reference midpoint',
            'oanda_fixed_forecast_evaluation.py:297-300 float midpoint subtraction',
            'oanda_fixed_forecast_evaluation.py:303 actual>0 label; :190-194 strict sign metrics',
            'oanda_causal_forecast_study.py:22 and :126 imports/calls this scorer'],
        'finding': 'The current registered scorer has the same float midpoint classification sensitivity. This read-only review demonstrates code susceptibility; it does not establish any affected prospective scored row.'},
    'remaining_limits': ['No original quote decimal strings or stored pip size to validate broker precision metadata independently.',
        'No local quote receipt, original forecast committed availability, consumer first observation or per-quote tradeable flags in this historical ledger.',
        'Repeated signals and overlapping horizons mean 8,414 rows are not 8,414 independent prediction trials; confidence intervals and executable edge are unestablished.',
        'This broad 49-family selected diagnostic population is distinct from the four-family repaired prospective collection.',
        'Class baselines are descriptive of this same observed sample, not pre-registered deployable baselines; the sub-50% headline alone cannot establish worse-than-random prediction quality.'],
    'source_bindings': preliminary['source_bindings'] | {name: sha(ROOT/name) for name in
        ('oanda_causal_forecast_ledger.py', 'oanda_fixed_forecast_evaluation.py', 'oanda_causal_forecast_study.py')},
    'offline_finalizer_sha256': sha(Path(__file__)),
    'original_extractor_sha256': sha(OUT/'recompute_broad_signals.py'),
}
with (OUT/'broad_signal_final_verification.json').open('x', encoding='utf-8') as handle:
    json.dump(report, handle, indent=2, allow_nan=False)
print(json.dumps({'final_report': str(OUT/'broad_signal_final_verification.json'),
    'sha256': sha(OUT/'broad_signal_final_verification.json'), 'raw': float_summary, 'decimal': decimal_summary,
    'arithmetic_mismatches': 0, 'pip_exceptions': exotic,
    'mean_gross_pips': statistics.fmean(r['gross_pips'] for r in rows),
    'mean_net_pips': statistics.fmean(r['net_pips'] for r in rows),
    'mean_spread_drag_pips': statistics.fmean(spread_drag),
    'snapshot_at_or_after_target_rows': len(late)}, indent=2))
