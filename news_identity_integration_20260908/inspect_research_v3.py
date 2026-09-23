"""Bounded read-only inspection of the newly activated v3 cohort; no scoring.

Each database has its own coherent query-only transaction, not a globally atomic
snapshot. No ledger constructor, publication recovery, fit or broker call occurs.
"""
from __future__ import annotations
import argparse
from collections import Counter
from contextlib import closing
from datetime import datetime, timezone
from decimal import Decimal
import hashlib
import json
from pathlib import Path
import sqlite3
import sys
import time
from types import SimpleNamespace
import zlib

sys.dont_write_bytecode = True
HERE = Path(__file__).resolve().parent
ROOT = HERE.parent / 'trad'
sys.path.insert(0, str(ROOT))
import oanda_joint_price_news_forecast_study_v3 as worker

MAX_FORECASTS = 256
MAX_DATABASE_BYTES = 512 * 1024 * 1024
MAX_INPUT_BYTES = 8 * 1024 * 1024
MAX_TOTAL_INPUT_BYTES = 64 * 1024 * 1024
TABLES = ('quotes','attempts','diagnostics','inputs','forecasts','publication','consumption','entries','outcomes','exclusions')


def require(condition, reason):
    if not condition:
        raise ValueError(reason)


def sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def unpack_input(raw):
    require(len(raw) <= MAX_INPUT_BYTES, 'compressed_input_byte_bound')
    decoder = zlib.decompressobj()
    decoded = decoder.decompress(raw, MAX_INPUT_BYTES + 1)
    require(len(decoded) <= MAX_INPUT_BYTES and decoder.eof and not decoder.unused_data and not decoder.unconsumed_tail,
            'decoded_input_bound_or_geometry')
    return json.loads(decoded), len(decoded)


def inspect_ledger(path, contract, activated_epoch, *, clock=time.time):
    """Inspect one exact registered path; only the caller opens output evidence."""
    path = Path(path).resolve()
    started = clock()
    require(path.is_file() and path.stat().st_size <= MAX_DATABASE_BYTES, 'missing_or_oversized_database')
    pip = Decimal(str(contract['pip_size']))
    family, instrument = contract['family'], contract['instrument']
    require(f'.{worker.COHORT_SCOPE}.' in contract['cohorts'][family], 'old_cohort_not_new_v3')
    contract_hash = worker.digest(contract)
    with closing(sqlite3.connect(path.as_uri() + '?mode=ro', uri=True, timeout=2)) as db:
        db.row_factory = sqlite3.Row
        db.execute('PRAGMA query_only=ON')
        deadline = time.monotonic() + 8
        db.set_progress_handler(lambda: int(time.monotonic() > deadline), 1000)
        db.execute('BEGIN')
        registered = db.execute('SELECT sha,payload FROM contract WHERE id=1').fetchone()
        require(registered is not None and registered['sha'] == contract_hash and
                registered['payload'] == worker.encoded(contract).decode(), 'immutable_contract_mismatch')
        active = db.execute('SELECT epoch,contract_sha FROM activation WHERE id=1').fetchone()
        require(active is not None and active['contract_sha'] == contract_hash and
                active['epoch'] == activated_epoch and 0 < active['epoch'] <= started, 'activation_receipt_mismatch')
        counts = {table: db.execute(f'SELECT count(*) FROM {table}').fetchone()[0] for table in TABLES}
        require(counts['forecasts'] <= MAX_FORECASTS, 'forecast_inspection_bound')
        forecasts = db.execute('SELECT * FROM forecasts ORDER BY bucket,id').fetchall()
        observed = clock()
        require(observed >= started, 'inspector_clock_regressed')
        quotes = {}
        def read_quote(identity):
            if identity in quotes:
                return quotes[identity]
            row = db.execute('SELECT * FROM quotes WHERE id=?', (identity,)).fetchone()
            require(row is not None, 'referenced_quote_missing')
            value = json.loads(row['payload'])
            market, available = value['market_epoch'], value['available_epoch']
            bid, ask = Decimal(str(value['bid'])), Decimal(str(value['ask']))
            canonical = lambda price: format(price, 'f').rstrip('0').rstrip('.') if '.' in format(price, 'f') else format(price, 'f')
            identity_fields = {'instrument': instrument, 'pip_size': canonical(pip), 'market_epoch': market,
                               'bid': canonical(bid), 'ask': canonical(ask), 'tradeable': True}
            require(value['instrument'] == instrument and Decimal(str(value['pip_size'])) == pip and
                    value.get('tradeable') is True and value['quote_id'] == identity == worker.digest(identity_fields), 'quote_identity_mismatch')
            require(bid.is_finite() and ask.is_finite() and 0 < bid <= ask and
                    market == row['market'] and available == row['available'] and
                    activated_epoch < available <= observed and market <= value['source_read_epoch'] <= available and
                    0 <= available - market <= 60, 'quote_exact_price_or_clock_mismatch')
            quotes[identity] = value
            return value
        stages = Counter()
        examples = []
        checked_inputs = {}
        input_bytes = 0
        for forecast in forecasts:
            value = json.loads(forecast['payload'])
            identity = forecast['id']
            require(worker.digest(value) == forecast['sha'] and value['decision_id'] == identity and
                    identity == worker.digest({'contract': contract_hash, 'bucket': forecast['bucket']}), 'forecast_hash_or_identity')
            require(value['instrument'] == instrument and value['family'] == family and Decimal(str(value['pip_size'])) == pip,
                    'forecast_pair_or_family')
            attempt = db.execute('SELECT * FROM attempts WHERE id=?', (forecast['attempt_id'],)).fetchone()
            require(attempt is not None and value['attempt_id'] == attempt['id'] and value['attempt_epoch'] == attempt['epoch'] and
                    attempt['bucket'] == forecast['bucket'] == int(attempt['epoch'] // 900), 'attempt_binding')
            reference = read_quote(attempt['reference_id'])
            require(value['reference_quote_id'] == reference['quote_id'] and
                    forecast['reference'] == value['reference_epoch'] == reference['market_epoch'] and
                    forecast['target'] == value['target_epoch'] == reference['market_epoch'] + 3600, 'original_reference_target_mismatch')
            require(len(value['forecasts']) == 1, 'single_family_required')
            arm = value['forecasts'][0]
            require(arm['cohort_id'] == contract['cohorts'][family] and arm['family'] == family and
                    arm['forecast_id'] == identity + ':' + family, 'arm_cohort_identity')
            require(all(value.get(k) is False and arm.get(k) is False for k in
                        ('can_place_orders','can_promote','can_authorize','account_eligible','proof_eligible')) and
                    value.get('research_only') is True and arm.get('research_only') is True, 'forecast_inert_flags')
            input_id = value['input_capture_sha256']
            if input_id not in checked_inputs:
                source = db.execute('SELECT payload FROM inputs WHERE id=?', (input_id,)).fetchone()
                require(source is not None, 'retained_input_missing')
                capture, size = unpack_input(source[0]); input_bytes += size
                require(input_bytes <= MAX_TOTAL_INPUT_BYTES and worker.digest(capture) == input_id, 'retained_input_hash_or_total_bound')
                require(capture['source_capture_sha256'] == worker.digest({k:v for k,v in capture.items() if k != 'source_capture_sha256'}),
                        'original_input_seal')
                checked_inputs[input_id] = capture
            capture = checked_inputs[input_id]
            require(capture['news_capture_sha256'] == value['news_capture_sha256'] == arm['news_capture_sha256'], 'input_news_binding')
            require(all(capture[key] == arm[key] for key in ('news_evidence_epoch','news_generated_epoch','news_first_observed_epoch','news_available_epoch','news_expires_epoch')),
                    'input_news_clock_binding')
            publication = db.execute('SELECT * FROM publication WHERE id=?', (identity,)).fetchone()
            consumption = db.execute('SELECT * FROM consumption WHERE id=?', (identity,)).fetchone()
            require(publication is not None or consumption is None, 'consumption_without_publication')
            committed = publication['epoch'] if publication is not None else arm['issued_epoch']
            if publication is not None:
                require(publication['forecast_sha'] == forecast['sha'] and arm['issued_epoch'] <= committed <= observed,
                        'publication_hash_or_clock')
            if consumption is not None:
                require(consumption['forecast_sha'] == forecast['sha'] and consumption['publication_sha'] == worker.digest(
                    {'epoch': publication['epoch'], 'forecast_sha': forecast['sha']}) and
                    publication['epoch'] <= consumption['epoch'] <= observed and consumption['epoch'] < value['target_epoch'],
                    'consumer_hash_or_clock')
                committed = consumption['epoch']
            protocol = {**contract['evaluation_protocol'], 'historical_start_utc': worker.utc(activated_epoch)}
            require(not worker.forecast_errors({**arm, 'committed_available_epoch': committed}, value, protocol),
                    'frozen_forecast_contract_validation')
            entry = db.execute('SELECT * FROM entries WHERE id=?', (identity,)).fetchone()
            outcome = db.execute('SELECT * FROM outcomes WHERE id=?', (identity,)).fetchone()
            exclusion = db.execute('SELECT * FROM exclusions WHERE id=?', (identity,)).fetchone()
            require(outcome is None or exclusion is None, 'outcome_and_exclusion_overlap')
            if entry is not None:
                require(consumption is not None, 'entry_without_independent_consumer')
                quote = read_quote(entry['quote_id'])
                require(consumption['epoch'] < quote['market_epoch'] <= quote['available_epoch'] < value['target_epoch'] and
                        quote['available_epoch'] <= consumption['epoch'] + 60 and quote['available_epoch'] <= entry['epoch'] <= observed,
                        'later_entry_clock')
                earliest = db.execute('SELECT id FROM quotes WHERE market>? AND available>? AND available<? AND available<=? ORDER BY available,market,id LIMIT 1',
                    (consumption['epoch'], consumption['epoch'], value['target_epoch'], consumption['epoch'] + 60)).fetchone()
                require(earliest is not None and earliest[0] == entry['quote_id'], 'entry_not_original_earliest')
            if outcome is not None:
                require(entry is not None, 'outcome_without_entry')
                quote = read_quote(outcome['quote_id'])
                require(value['target_epoch'] <= quote['market_epoch'] <= quote['available_epoch'] <= value['target_epoch'] + 60 and
                        quote['available_epoch'] <= outcome['epoch'] <= observed, 'target_outcome_clock')
                earliest = db.execute('SELECT id FROM quotes WHERE market>=? AND available<=? ORDER BY available,market,id LIMIT 1',
                                     (value['target_epoch'], value['target_epoch'] + 60)).fetchone()
                require(earliest is not None and earliest[0] == outcome['quote_id'], 'outcome_not_original_earliest')
            if exclusion is not None:
                require(consumption is not None and exclusion['epoch'] <= observed, 'exclusion_without_consumption_or_future')
                reason = exclusion['reason']
                require((reason == 'missing_later_entry_before_deadline' and entry is None and
                         exclusion['epoch'] > min(value['target_epoch'], consumption['epoch'] + 60)) or
                        (reason == 'missing_quote_at_original_target' and entry is not None and exclusion['epoch'] > value['target_epoch'] + 60),
                        'exclusion_reason_or_deadline')
            stage = 'outcome' if outcome else 'excluded' if exclusion else 'awaiting_target' if entry else 'awaiting_entry' if consumption else 'awaiting_consumer' if publication else 'awaiting_publication'
            stages[stage] += 1
            if len(examples) < 2:
                examples.append({'decision_id': identity, 'forecast_sha256': forecast['sha'], 'instrument': instrument,
                    'cohort_id': arm['cohort_id'], 'stage': stage, 'attempt_epoch': attempt['epoch'],
                    'reference_epoch': value['reference_epoch'], 'issued_epoch': arm['issued_epoch'],
                    'publication_epoch': publication['epoch'] if publication else None,
                    'consumption_epoch': consumption['epoch'] if consumption else None,
                    'entry_quote_id': entry['quote_id'] if entry else None, 'target_epoch': value['target_epoch'],
                    'input_capture_sha256': input_id, 'news_capture_sha256': arm['news_capture_sha256'],
                    'news_clocks': {k: arm[k] for k in ('news_evidence_epoch','news_generated_epoch','news_first_observed_epoch','news_available_epoch','news_expires_epoch')},
                    'side': arm['side'], 'predicted_return_bps': arm['predicted_return_bps'],
                    'comparisons': {k: arm['diagnostics'].get(k) for k in ('matched_price_only_expected_pips','neutral_news_ablation_expected_pips','news_ablation_difference_pips','training_rows','nonzero_news_context_training_rows','vetted_news_training_rows')}})
        # Secondary unchanged worker receipt verification on this same RO snapshot.
        facade = SimpleNamespace(contract=contract, activated_epoch=activated_epoch, clock=lambda: observed,
                                 _read=lambda query, params=(): list(db.execute(query, params)))
        secondary = worker.verified_publication(facade)
        reasons = dict(db.execute('SELECT reason,count(*) FROM exclusions GROUP BY reason').fetchall())
        completed = clock()
    return {'status':'passed','path':str(path),'instrument':instrument,'contract_sha256':contract_hash,
            'activated_epoch':activated_epoch,'read_started_epoch':started,'observed_epoch':observed,'read_completed_epoch':completed,
            'counts':counts,'stages':dict(stages),'exclusion_reasons':reasons,'checked_input_count':len(checked_inputs),
            'checked_input_decoded_bytes':input_bytes,'referenced_quotes_verified':len(quotes),
            'latest_consumed_publication_secondary_verified':secondary is not None,'examples':examples}


def inspect_runtime(*, config=worker.DEFAULT_CONFIG, study=worker.STUDY,
                    activation_path=HERE/'DEPLOYMENT_ACTIVATION_20260908.json'):
    started = time.time()
    registry = worker.load_registry(Path(config))
    deployment = json.loads(Path(activation_path).read_bytes())
    receipt = deployment['activation']
    require(receipt['status'] == 'activated_empty' and receipt['registry_sha256'] == worker.digest(registry) and
            receipt['registry_file_sha256'] == sha(config), 'deployment_registry_binding')
    require(len(registry['pairs']) == 68 and len(receipt['ledgers']) == 68, 'canonical68_scope_required')
    active = {Path(row['path']).resolve(): row for row in receipt['ledgers']}
    require(len(active) == 68, 'duplicate_activation_paths')
    base = Path(study).resolve()
    results = []
    for instrument, entry in sorted(registry['pairs'].items()):
        family = 'ridge_price_news_v1'
        path = (base/'pairs'/instrument/family/'study.sqlite').resolve()
        require(path.is_relative_to(base) and path in active, 'registered_activation_path_mismatch')
        contract = entry['families'][family]['contract']
        require(active[path]['contract_sha256'] == worker.digest(contract) and not any(active[path]['counts'].values()),
                'initial_activation_not_empty_or_wrong_contract')
        try:
            results.append(inspect_ledger(path, contract, active[path]['activated_epoch']))
        except Exception as exc:
            results.append({'status':'failed','instrument':instrument,'path':str(path),'error':type(exc).__name__+':'+str(exc)[:300]})
    require(worker.digest(worker.load_registry(Path(config))) == worker.digest(registry), 'registry_or_sources_changed_during_inspection')
    totals, exclusions, stages = Counter(), Counter(), Counter()
    for row in results:
        totals.update(row.get('counts',{}));exclusions.update(row.get('exclusion_reasons',{}));stages.update(row.get('stages',{}))
    examples = [example for row in results for example in row.get('examples',[])][:2]
    return {'schema_version':'joint_news_v3_readonly_runtime_inspection_20260908',
        'status':'passed' if all(row['status']=='passed' for row in results) else 'failed',
        'started_epoch':started,'completed_epoch':time.time(),'registry_sha256':worker.digest(registry),
        'registry_file_sha256':sha(config),'activation_receipt_sha256':sha(activation_path),
        'source_bindings':registry['source_bindings'],'inspector_sha256':sha(__file__),
        'registered_ledgers':68,'verified_ledgers':sum(row['status']=='passed' for row in results),
        'counts':dict(totals),'stages':dict(stages),'exclusion_reasons':dict(exclusions),'examples':examples,'ledgers':results,
        'scope':'Per-database coherent read-only observations; no globally atomic claim. Completed outcomes are counted, not rescored. Future H1 targets are not performance results.',
        'limits':{'forecasts_per_ledger':MAX_FORECASTS,'database_bytes':MAX_DATABASE_BYTES,'single_input_decoded_bytes':MAX_INPUT_BYTES,'total_input_decoded_bytes_per_ledger':MAX_TOTAL_INPUT_BYTES,'query_seconds_per_ledger':8},
        'production_writes':False,'process_actions':False,'model_fits':0,'can_place_orders':False}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output',type=Path,required=True)
    args = parser.parse_args(argv)
    destination = args.output.resolve()
    require(destination.is_relative_to(HERE) and not destination.exists(), 'exclusive_evidence_output_under_integration_required')
    result = inspect_runtime()
    destination.parent.mkdir(parents=True,exist_ok=True)
    with destination.open('xb') as file:
        file.write(worker.encoded(result))
    print(json.dumps({key:result[key] for key in ('status','verified_ledgers','counts','stages','exclusion_reasons')}))
    return 0 if result['status']=='passed' else 1


if __name__=='__main__':
    raise SystemExit(main())
