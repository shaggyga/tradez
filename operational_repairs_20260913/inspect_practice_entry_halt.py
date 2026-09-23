"""Read-only, redacted diagnosis of the retained practice entry halt."""
from contextlib import closing
from datetime import datetime, timezone
from pathlib import Path
import json, sqlite3, sys

root=Path(__file__).resolve().parent.parent/'trad'
trial=root/'data/oanda_training_manager/practice007_native_v7_20260913_v1'
status=json.loads((trial/'status.json').read_text(encoding='utf-8'))
with closing(sqlite3.connect((trial/'trial.sqlite').as_uri()+'?mode=ro',uri=True)) as db:
    db.execute('PRAGMA query_only=ON')
    rows=db.execute("SELECT seq,epoch,kind,body FROM events WHERE kind='cycle_error' ORDER BY seq DESC LIMIT 12").fetchall()
    diagnostic=[]
    for seq,epoch,kind,raw in rows:
        body=json.loads(raw)
        marker='activation_or_process_changed' in raw
        diagnostic.append({'seq':seq,'utc':datetime.fromtimestamp(epoch,timezone.utc).isoformat(),'kind':kind,
            'activation_or_process_changed':marker,'code':body.get('code'),'type':body.get('type'),
            'diagnostic_keys':sorted(body.get('diagnostic',{})),
            'traceback_frames':body.get('diagnostic',{}).get('traceback_frames')})
    count=db.execute('SELECT count(*),coalesce(sum(attempted),0) FROM intents').fetchone()
sys.path.insert(0,str(root))
import oanda_practice_trial_runner_v2 as runner
report={'captured_utc':datetime.now(timezone.utc).isoformat(),'status':{k:status.get(k) for k in
    ('state','enabled','entry_halt_latched','foreign_trade_count','loss_stop_latched','stop_requested','unresolved_intents')},
    'all_intent_count':count[0],'durable_attempt_count':count[1],'recent_cycle_errors':diagnostic,
    'current_process_preflight':runner.process_preflight(),
    'scope':'Retained status/ledger and current process inspection only. No broker request, latch reset or configuration change.'}
path=Path(__file__).parent/'PRACTICE_ENTRY_HALT_20260914.json'
path.write_text(json.dumps(report,indent=2),encoding='utf-8')
print(json.dumps(report,indent=2))
