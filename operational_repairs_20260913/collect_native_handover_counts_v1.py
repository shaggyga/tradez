"""Read-only local counts for the source-generation stop plan; no broker use."""
from contextlib import closing
import datetime as dt
import json
from pathlib import Path
import sqlite3
import time

AREA = Path(__file__).resolve().parent
DATA = AREA.parent / 'trad/data/oanda_training_manager'
study = DATA / 'operational_repair_20260913_v3/joint_price_news_study_v7'
trial = DATA / 'practice007_native_v7_20260913_v1'
started = time.monotonic()
paths = sorted(study.glob('pairs/*/*/study.sqlite'))
assert len(paths) == 68
rows = []
for path in paths:
    assert time.monotonic() - started < 15
    with closing(sqlite3.connect(path.resolve().as_uri() + '?mode=ro', uri=True, timeout=1)) as db:
        db.execute('PRAGMA query_only=ON')
        rows.append({'instrument': path.parent.parent.name,
                     **{name: db.execute('SELECT count(*) FROM ' + name).fetchone()[0]
                        for name in ('forecasts', 'outcomes', 'attempts')}})
with closing(sqlite3.connect((trial / 'trial.sqlite').resolve().as_uri() + '?mode=ro', uri=True, timeout=1)) as db:
    db.execute('PRAGMA query_only=ON')
    intents = db.execute('SELECT count(*) FROM intents').fetchone()[0]
status_path = trial / 'status.json'
status = json.loads(status_path.read_bytes())
result = {'observed_utc': dt.datetime.now(dt.timezone.utc).isoformat(),
          'scope': 'read-only individual database snapshots; no global transaction or broker reconciliation claimed',
          'native_study': str(study), 'native_database_count': len(rows),
          'native_totals': {name: sum(row[name] for row in rows) for name in ('forecasts', 'outcomes', 'attempts')},
          'native_pair_counts': rows, 'practice_intents': intents,
          'practice_status_path': str(status_path),
          'practice_status': {key: status.get(key) for key in (
              'observed_epoch', 'state', 'enabled', 'stop_requested', 'entry_halt_latched', 'loss_stop_latched', 'stop_epoch')},
          'broker_calls': 0, 'live_writes': False, 'elapsed_seconds': time.monotonic() - started}
destination = AREA / 'native_compact_handover_counts_20260914.json'
destination.write_text(json.dumps(result, indent=2, sort_keys=True))
print(json.dumps({key: result[key] for key in ('native_totals', 'practice_intents', 'practice_status', 'elapsed_seconds')}))
