"""Optional raw pricing, liquidity and S5 price-count volume capture."""
import csv
import json
import threading
import time
import urllib.parse
import urllib.request
from datetime import datetime, timezone


class Extras:
    def __init__(self, out, segment, token, account, deadline):
        self.out, self.deadline = out, deadline
        self.raw = (out / ('messages-' + segment + '.jsonl')).open('x', encoding='utf-8')
        self.depth = (out / ('liquidity-' + segment + '.csv')).open('x', newline='', encoding='utf-8')
        self.writer = csv.writer(self.depth)
        self.writer.writerow(['connection_id', 'broker_time', 'received_time', 'best_bid_liquidity',
                             'best_ask_liquidity', 'bids_json', 'asks_json', 'closeoutBid', 'closeoutAsk', 'tradeable'])
        self.stop = threading.Event()
        self.thread = threading.Thread(target=self.candles, args=(token, account, segment), daemon=True)
        self.thread.start()

    def message(self, payload, received, connection):
        stamp = datetime.fromtimestamp(received, timezone.utc).isoformat()
        self.raw.write(json.dumps(dict(received_time=stamp, connection_id=connection, payload=payload)) + '\n')
        self.raw.flush()
        if isinstance(payload, dict) and payload.get('type') == 'PRICE':
            bids, asks = payload.get('bids', []), payload.get('asks', [])
            self.writer.writerow([connection, payload.get('time'), stamp,
                                  bids[0].get('liquidity') if bids else None,
                                  asks[0].get('liquidity') if asks else None,
                                  json.dumps(bids), json.dumps(asks), payload.get('closeoutBid'),
                                  payload.get('closeoutAsk'), payload.get('tradeable', payload.get('status') == 'tradeable')])
            self.depth.flush()

    def candles(self, token, account, segment):
        cursor = time.time() // 5 * 5
        with (self.out / ('volume-s5-' + segment + '.jsonl')).open('x', encoding='utf-8') as stream:
            while time.time() < self.deadline and not self.stop.is_set():
                if (self.out / 'STOP').exists():
                    break
                try:
                    query = urllib.parse.urlencode(dict(granularity='S5', price='MBA', count=5000,
                        **{'from': datetime.fromtimestamp(cursor, timezone.utc).isoformat()}))
                    url = 'https://api-fxpractice.oanda.com/v3/accounts/' + urllib.parse.quote(account, safe='') + '/instruments/EUR_USD/candles?' + query
                    req = urllib.request.Request(url, headers={'Authorization': 'Bearer ' + token})
                    with urllib.request.urlopen(req, timeout=12) as response:
                        candles = json.load(response).get('candles', [])
                    for candle in candles:
                        epoch = datetime.fromisoformat(candle['time'].replace('Z', '+00:00')).timestamp()
                        if candle.get('complete') and cursor <= epoch and epoch + 5 <= self.deadline:
                            stream.write(json.dumps(dict(received_time=datetime.now(timezone.utc).isoformat(),
                                volume_definition='OANDA number of prices, not traded units', candle=candle)) + '\n')
                            cursor = epoch + 5
                    stream.flush()
                    (self.out / 'volume_status.json').write_text(json.dumps(dict(updated_utc=datetime.now(timezone.utc).isoformat(),
                        state='polling', next_candle_epoch=cursor)), encoding='utf-8')
                except Exception as exc:
                    (self.out / 'volume_status.json').write_text(json.dumps(dict(updated_utc=datetime.now(timezone.utc).isoformat(),
                        state='retrying', error=type(exc).__name__)), encoding='utf-8')
                self.stop.wait(60)

    def close(self):
        self.stop.set()
        self.thread.join(timeout=15)
        self.raw.close()
        self.depth.close()
