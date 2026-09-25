"""Offline contract checks for the EUR/USD recorder; no real credentials/network."""
from contextlib import ExitStack, redirect_stdout
import csv
from datetime import datetime, timedelta, timezone
import io
import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'trad'))
import oanda_eurusd_record as record
import oanda_eurusd_stream as stream


def price():
    return {'type': 'PRICE', 'instrument': 'EUR_USD',
            'time': '2026-09-25T00:00:00.123456789Z', 'status': 'tradeable',
            'bids': [{'price': '1.123450'}], 'asks': [{'price': '1.123570'}]}


class ParseQuoteTests(unittest.TestCase):
    def parse(self, payload):
        return stream.parse_quote(payload, 1790294401.25, 7, True)

    def test_exact_source_timestamp_and_prices(self):
        source = price()
        quote = self.parse(source)
        self.assertEqual(quote['broker_time'], source['time'])
        self.assertEqual((quote['bid'], quote['ask']), (1.12345, 1.12357))
        self.assertAlmostEqual(quote['mid'], 1.12351)
        self.assertAlmostEqual(quote['spread_pips'], 1.2)
        self.assertEqual(quote['generation'], 7)
        self.assertTrue(quote['initial_snapshot'])
        self.assertTrue(quote['tradeable'])

    def test_malformed_and_naive_timestamps_rejected(self):
        for stamp in [None, 123, {}, [], '', 'broken', '2026-09-25T00:00:00']:
            with self.subTest(stamp=stamp):
                payload = price(); payload['time'] = stamp
                self.assertIsNone(self.parse(payload))

    def test_invalid_price_shapes_and_nonfinite_values_rejected(self):
        for value in ['NaN', 'Infinity', '-1', '0', None, {}, '1.2']:
            with self.subTest(bid=value):
                payload = price(); payload['bids'][0]['price'] = value
                self.assertIsNone(self.parse(payload))
        for bids in [[], None, {}, [{}]]:
            with self.subTest(bids=bids):
                payload = price(); payload['bids'] = bids
                self.assertIsNone(self.parse(payload))
        self.assertIsNone(stream.parse_quote(price(), float('nan'), 1, False))

    def test_heartbeat_other_instrument_and_closed_status(self):
        self.assertIsNone(self.parse({'type': 'HEARTBEAT'}))
        payload = price(); payload['instrument'] = 'GBP_USD'
        self.assertIsNone(self.parse(payload))
        payload = price(); payload['status'] = 'non-tradeable'
        self.assertFalse(self.parse(payload)['tradeable'])


class RecorderContractTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix='eurusd-offline-test-')
        self.addCleanup(self.temp.cleanup)
        self.out = Path(self.temp.name)
        self.end = datetime.now(timezone.utc) + timedelta(hours=1)

    def invoke(self, *, end=None, response=None):
        end = end or self.end
        with ExitStack() as stack:
            stack.enter_context(patch.object(sys, 'argv', ['recorder', '--output',
                str(self.out), '--end-utc', end.isoformat()]))
            creds = stack.enter_context(patch.object(record, 'credentials',
                return_value=('synthetic-test-token', 'synthetic-test-account')))
            network = stack.enter_context(patch.object(record.urllib.request, 'urlopen'))
            if response is None:
                network.side_effect = AssertionError('Network is forbidden in this test')
            else:
                network.return_value = response
            if record.os.name == 'nt':
                stack.enter_context(patch.object(record.ctypes.windll.kernel32,
                    'SetThreadExecutionState', return_value=0))
            stack.enter_context(redirect_stdout(io.StringIO()))
            result = record.main()
            return result, creds, network

    def contract(self, **overrides):
        value = {'recording_id': 'offline-test', 'instrument': 'EUR_USD',
                 'end_utc': self.end.isoformat(), 'user_stopped': False}
        value.update(overrides)
        (self.out / 'recording.json').write_text(json.dumps(value), encoding='utf-8')

    def test_expired_deadline_never_reads_credentials_or_opens_network(self):
        result, creds, network = self.invoke(end=datetime.now(timezone.utc)-timedelta(seconds=1))
        self.assertEqual(result, 0)
        creds.assert_not_called(); network.assert_not_called()
        self.assertEqual(json.loads((self.out/'status.json').read_text())['state'], 'cutoff_reached')
        self.assertEqual(list(self.out.glob('quotes-*.csv')), [])

    def test_changed_existing_deadline_is_refused(self):
        self.contract(end_utc=(self.end+timedelta(seconds=1)).isoformat())
        with self.assertRaisesRegex(ValueError, 'contract does not match'):
            self.invoke()
        self.assertEqual(list(self.out.glob('quotes-*.csv')), [])

    def test_persistent_user_stop_never_reads_credentials_or_opens_network(self):
        self.contract(user_stopped=True)
        result, creds, network = self.invoke()
        self.assertEqual(result, 0)
        creds.assert_not_called(); network.assert_not_called()

    def test_second_lock_owner_cannot_create_contract_or_connect(self):
        with (self.out/'writer.lock').open('a+b') as owner:
            owner.write(b'1'); owner.flush(); owner.seek(0)
            if record.os.name == 'nt':
                import msvcrt
                msvcrt.locking(owner.fileno(), msvcrt.LK_NBLCK, 1)
            else:
                import fcntl
                fcntl.flock(owner, fcntl.LOCK_EX | fcntl.LOCK_NB)
            try:
                # Expiry also bounds this test if an OS ever admits the second lock.
                result, creds, network = self.invoke(
                    end=datetime.now(timezone.utc)-timedelta(seconds=1))
                self.assertEqual(result, 0)
                creds.assert_not_called(); network.assert_not_called()
                self.assertFalse((self.out/'recording.json').exists())
            finally:
                owner.seek(0)
                if record.os.name == 'nt':
                    msvcrt.locking(owner.fileno(), msvcrt.LK_UNLCK, 1)
                else:
                    fcntl.flock(owner, fcntl.LOCK_UN)

    def test_stop_file_prevents_connection_and_becomes_persistent(self):
        (self.out/'STOP').write_text('stop', encoding='utf-8')
        result, _, network = self.invoke()
        self.assertEqual(result, 0); network.assert_not_called()
        self.assertTrue(json.loads((self.out/'recording.json').read_text())['user_stopped'])
        self.assertEqual(json.loads((self.out/'status.json').read_text())['state'], 'user_stopped')

    def test_repeated_quotes_preserved_with_exact_decimals_and_snapshot_flag(self):
        out = self.out
        class FakeResponse:
            count = 0
            def __enter__(self): return self
            def __exit__(self, *_): return False
            def readline(self, limit):
                self.count += 1
                if self.count == 2:
                    (out/'STOP').write_text('synthetic test done', encoding='utf-8')
                if self.count > 2:
                    raise AssertionError('unexpected extra read')
                return (json.dumps(price())+'\n').encode()
        _, _, network = self.invoke(response=FakeResponse())
        network.assert_called_once()
        files = list(self.out.glob('quotes-*.csv'))
        self.assertEqual(len(files), 1)
        with files[0].open(newline='', encoding='utf-8') as handle:
            rows = list(csv.DictReader(handle))
        self.assertEqual(len(rows), 2)
        self.assertEqual([row['sequence'] for row in rows], ['1', '2'])
        self.assertEqual([row['initial_snapshot'] for row in rows], ['True', 'False'])
        for row in rows:
            self.assertEqual(row['bid'], '1.123450')
            self.assertEqual(row['ask'], '1.123570')
            self.assertEqual(row['broker_time'], price()['time'])
        state = json.loads((self.out/'status.json').read_text())
        self.assertEqual(state['segment_rows'], 2)
        self.assertEqual(state['connections'], 1)
        self.assertIsNone(state['pid'])
        journal = [json.loads(line) for line in (self.out/'events.jsonl').read_text().splitlines()]
        self.assertEqual(journal[-1]['event'], 'segment_closed')


if __name__ == '__main__':
    unittest.main()
