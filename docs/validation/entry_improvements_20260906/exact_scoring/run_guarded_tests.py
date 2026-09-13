"""Guarded, offline exact-price tests and frozen-history replay. New receipts only."""
import sys
sys.dont_write_bytecode = True
import os
import json
import hashlib
from pathlib import Path
from datetime import datetime, timezone
import unittest
import io

OUT = Path(__file__).resolve().parent
FOREX = OUT.parents[1]
ROOT = FOREX/'trad'
denied = []


def guard(event, args):
    if event in {'sqlite3.connect', 'socket.__new__', 'socket.connect', 'socket.connect_ex',
                 'socket.bind', 'subprocess.Popen', 'os.system', 'os.startfile'}:
        denied.append(event)
        raise RuntimeError('Tests prohibit DB, network, and process actions')
    if event == 'open' and isinstance(args[0], (str, bytes, os.PathLike)):
        path, mode, flags = args
        writing = ((isinstance(mode, str) and any(c in mode for c in 'wax+')) or
                   (isinstance(flags, int) and flags & (os.O_WRONLY|os.O_RDWR|os.O_CREAT|os.O_TRUNC|os.O_APPEND)))
        if writing and not Path(os.fsdecode(path)).resolve().is_relative_to(OUT):
            denied.append('write:'+str(path))
            raise RuntimeError('Test writes restricted to evidence directory')


sys.addaudithook(guard)
sys.path.insert(0, str(ROOT))
import test_oanda_exact_price_scoring as core_tests
import test_oanda_fixed_forecast_evaluation_exact as integration_tests
import oanda_exact_price_scoring as scoring


class CountedResult(unittest.TextTestResult):
    subtests = 0
    def addSubTest(self, test, subtest, err):
        self.subtests += 1
        super().addSubTest(test, subtest, err)


suite = unittest.TestSuite(unittest.defaultTestLoader.loadTestsFromModule(m)
                           for m in (core_tests, integration_tests))
stream = io.StringIO()
result = unittest.TextTestRunner(stream=stream, verbosity=2, resultclass=CountedResult).run(suite)
source = FOREX/'prediction_sanity_20260906/broad_signal/selected_rows.jsonl'
replay = {'rows': 0, 'direction_hits': 0, 'flat_rows': 0, 'net_positive_rows': 0}
if result.wasSuccessful():
    for line in source.read_text(encoding='utf-8').splitlines():
        row = json.loads(line)
        score = scoring.score_prediction({'bid':row['entry_bid'], 'ask':row['entry_ask']},
            {'bid':row['exit_bid'], 'ask':row['exit_ask']}, direction=1 if row['direction']=='buy' else -1,
            probability_up='.5')
        replay['rows'] += 1
        replay['direction_hits'] += score['direction_correct']
        replay['flat_rows'] += score['outcome_class'] == 'flat'
        replay['net_positive_rows'] += score['positive_after_spread']
    assert replay == {'rows':8414, 'direction_hits':4148, 'flat_rows':296, 'net_positive_rows':716}, replay
else:
    replay = {'skipped': 'unit or integration tests failed'}
receipt = {'generated_utc':datetime.now(timezone.utc).isoformat(), 'tests':result.testsRun,
    'subtests':result.subtests, 'failures':len(result.failures), 'errors':len(result.errors),
    'successful':result.wasSuccessful(), 'guarded_database_network_process_attempts':denied,
    'frozen_broad_history_replay':replay,
    'frozen_broad_history_sha256':hashlib.sha256(source.read_bytes()).hexdigest(),
    'source_hashes':{name:hashlib.sha256((ROOT/name).read_bytes()).hexdigest() for name in
        ('oanda_exact_price_scoring.py','test_oanda_exact_price_scoring.py',
         'oanda_fixed_forecast_evaluation_exact.py','test_oanda_fixed_forecast_evaluation_exact.py')},
    'active_sources_imported':[name for name in sys.modules if name in
        ('oanda_fixed_forecast_evaluation','oanda_causal_forecast_study','oanda_causal_forecast_ledger')],
    'runtime_started':False, 'database_connections':0}
stamp = datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S%fZ')
receipt_path = OUT/('validation_'+stamp+'.json')
with receipt_path.open('x', encoding='utf-8') as handle:
    json.dump(receipt, handle, indent=2, allow_nan=False)
with (OUT/('tests_'+stamp+'.txt')).open('x', encoding='utf-8') as handle:
    handle.write(stream.getvalue())
print(stream.getvalue())
print(json.dumps(receipt | {'receipt_path':str(receipt_path)}, indent=2))
raise SystemExit(0 if result.wasSuccessful() else 1)
