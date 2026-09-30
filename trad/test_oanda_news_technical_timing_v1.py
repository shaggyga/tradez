import json
import sqlite3
import unittest
import zlib
import tempfile
from pathlib import Path
from unittest import mock

import oanda_news_technical_timing_v1 as timing
from oanda_rolling_technical_worker_v3 import WakeGate
import oanda_rolling_technical_worker_v3 as worker


class TimingTests(unittest.TestCase):
    def setUp(self):
        self.db = sqlite3.connect(':memory:')
        self.db.row_factory = sqlite3.Row
        self.db.executescript('''CREATE TABLE bars(pair TEXT,t INTEGER,body BLOB,first_observed REAL);
          CREATE TABLE observations(pair TEXT,t INTEGER,values_blob BLOB,published REAL,feature_hash TEXT);
          CREATE TABLE metadata(key TEXT,value BLOB);''')
        self.db.execute('INSERT INTO metadata VALUES(?,?)', ('contract',json.dumps({'contract':{'feature_names':['momentum']}}).encode()))

    def tearDown(self):
        self.db.close()

    def bar(self,t,price=1.1,observed=None):
        self.db.execute('INSERT INTO bars VALUES(?,?,?,?)',('EUR_USD',t,json.dumps({
            'close':price,'bid_close':price-.0001,'ask_close':price+.0001}).encode(),t+61 if observed is None else observed))

    def feature(self,t,published,value):
        self.db.execute('INSERT INTO observations VALUES(?,?,?,?,?)',('EUR_USD',t,zlib.compress(json.dumps([value]).encode()),published,str(t)))

    def test_uses_previous_published_snapshot_not_newer_unavailable_one(self):
        self.bar(0);self.feature(0,65,-1)
        self.bar(60);self.feature(60,150,99)
        r=timing.join(self.db,'EUR_USD',news_known=120,decision=130)
        self.assertEqual(r['features']['momentum'],-1)
        self.assertEqual(r['age_seconds'],70)

    def test_backfill_cannot_rewrite_prior_knowledge(self):
        self.bar(0,observed=10000);self.feature(0,10001,1)
        self.assertEqual(timing.select_asof(self.db,'EUR_USD',100)['status'],'unavailable')
        self.assertEqual(timing.select_asof(self.db,'EUR_USD',10002)['status'],'unavailable')

    def test_future_news_stale_features_and_unpublished_refused(self):
        self.bar(0);self.feature(0,0,1)
        self.assertEqual(timing.select_asof(self.db,'EUR_USD',100)['status'],'unavailable')
        self.assertEqual(timing.join(self.db,'EUR_USD',news_known=101,decision=100)['reason'],'news_not_known_at_decision')
        self.db.execute('UPDATE observations SET published=65')
        self.assertEqual(timing.select_asof(self.db,'EUR_USD',300)['status'],'unavailable')

    def test_outcome_entry_is_after_decision_and_costs_are_asymmetric(self):
        for t,price in [(0,1.0),(60,1.1),(120,1.1005)]:self.bar(t,price)
        r=timing.evaluate(self.db,'EUR_USD',75,horizons=(1,))
        self.assertEqual(r['entry_bar_close_epoch'],120)
        self.assertAlmostEqual(r['horizons']['1']['return_bps'],.0005/1.1*10000)
        self.assertAlmostEqual(r['horizons']['1']['long_net_bps'],.0003/1.1*10000)
        self.assertLess(r['horizons']['1']['short_net_bps'],0)

    def test_outcomes_refuse_gaps_and_excess_entry_delay(self):
        self.bar(60);self.bar(180)
        self.assertEqual(timing.evaluate(self.db,'EUR_USD',70,horizons=(2,))['horizons']['2']['status'],'missing_path')
        self.assertEqual(timing.evaluate(self.db,'EUR_USD',0)['status'],'unavailable')

    def test_frequency_deduplicates_overlapping_windows(self):
        for i in range(11):self.bar(i*60,1+i*.001)
        r=timing.move_frequency(self.db,'EUR_USD',pip=.0001,horizon=5,threshold_pips=10)
        day=next(iter(r['days_utc'].values()))
        self.assertEqual(day['eligible_windows'],6)
        self.assertEqual(day['nonoverlapping_moves'],2)

    def test_change_trigger_does_not_wait_for_minute_or_drop_midcycle_update(self):
        g=WakeGate()
        self.assertEqual(g.reason(('old',),1),'startup')
        self.assertIsNone(g.reason(('old',),2))
        self.assertEqual(g.reason(('new',),3),'source_changed')
        self.assertEqual(g.reason(('changed_during_calculation',),4),'source_changed')
        self.assertEqual(g.reason(('changed_during_calculation',),60),'minute_boundary')

    def test_revised_source_is_not_silently_reused(self):
        self.bar(0);self.feature(0,65,1)
        self.db.execute('CREATE TABLE revisions(pair TEXT)')
        self.db.execute("INSERT INTO revisions VALUES('EUR_USD')")
        self.assertEqual(timing.select_asof(self.db,'EUR_USD',70)['reason'],'unresolved_original_input_revision')
        self.assertEqual(timing.evaluate(self.db,'EUR_USD',70)['status'],'unavailable')

    def test_nonfinite_clock_is_rejected(self):
        with self.assertRaises(ValueError):timing.evaluate(self.db,'EUR_USD',float('nan'))
        with self.assertRaises(ValueError):timing.select_asof(self.db,'EUR_USD',float('inf'))

    def test_successor_entrypoint_runs_real_calculator_store_and_latency_output(self):
        from test_oanda_rolling_technical_worker_v2 import candles, BASE
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory);output=root/'output';output.mkdir();sources=root/'candles';sources.mkdir()
            config=root/'operations.json';config.write_text('{}')
            (sources/'EUR_USD_M1.csv').write_text('synthetic input fixture')
            base={'pairs':{'EUR_USD':.0001},'output_root':str(output),'candle_root':str(sources),
                  'tail_rows':2048,'bootstrap_rows':3,'minimum_free_bytes':0,
                  'maximum_dataset_bytes':512*1024**2,'maximum_bar_age_seconds':180}
            ops={'maximum_tail_rows':8192,'maximum_candidate_minutes':8,'source_bindings':{}}
            data=candles(702);now=float(BASE+702*60+10)
            contract={'feature_names':[r['name'] for r in worker.previous.original.kernel.feature_registry()],
                      'pairs':base['pairs']}
            def reader(*args):return data,{'observed_epoch':now-2,'range_truncated':False,'instrument':'EUR_USD','retained_rows':702}
            with mock.patch.object(worker.previous,'read_config',return_value=(ops,base)), \
                 mock.patch.object(worker.previous.original,'contract',return_value=contract), \
                 mock.patch.object(worker.previous.original,'read_pair',side_effect=reader), \
                 mock.patch('time.time',return_value=now):
                worker.main(['--config',str(config),'--once'])
            latency=json.loads((output/'latency_current.json').read_text())
            self.assertEqual(latency['trigger'],'startup')
            self.assertEqual(latency['rows'][0]['read_to_publication_seconds'],2)
            self.assertTrue((output/'latest_features.json').exists())
            self.assertFalse(latency['can_place_orders'])


if __name__ == '__main__':
    unittest.main()
