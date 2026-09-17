import json, pathlib, time, datetime, collections, hashlib
ROOT=pathlib.Path.cwd()
SRC=ROOT/pathlib.Path(r"data\oanda_training_manager\trace_mapping_20260917_morning\trace_mapping_monitor_until_0945_20260917_fixed.jsonl")
OUT=ROOT/pathlib.Path(r"data\oanda_training_manager\trace_mapping_20260917_morning\trace_goal_watch_until_0945_20260917.jsonl")
EVENTS=ROOT/pathlib.Path(r"data\oanda_training_manager\trace_mapping_20260917_morning\trace_goal_watch_events_until_0945_20260917.jsonl")
END=datetime.datetime.combine(datetime.date.today(), datetime.time(9,45))
if datetime.datetime.now()>END:
    END=datetime.datetime.now()+datetime.timedelta(hours=4)
last_key=None
last_src_size=-1

def load_last():
    rows=[]
    if not SRC.exists(): return None
    with SRC.open(encoding='utf-8', errors='replace') as f:
        for line in f:
            try:
                r=json.loads(line)
            except Exception:
                continue
            if r.get('event'): continue
            try:
                t=json.loads(r.get('trace') or '{}')
            except Exception:
                continue
            rows.append((r,t))
    if not rows: return None
    return rows[-1], rows

def summarize(last, rows):
    r,t=last
    top=t.get('top_moves') or []
    selected=t.get('selected') or []
    cov=t.get('technical_coverage') or {}
    acct=r.get('account') or {}
    simple_top=[]
    for m in top[:6]:
        simple_top.append({k:m.get(k) for k in ('instrument','move5_pips','move15_pips','move60_pips','spread_bps','tech_finite')})
    return {
        'watch_time':datetime.datetime.now().isoformat(timespec='seconds'),
        'source_time':r.get('time'),
        'regime':t.get('regime'),
        'selected':selected,
        'top_moves':simple_top,
        'coverage':cov,
        'account':acct,
        'status':t.get('status'),
        'fresh':t.get('fresh'),
        'source_error_count':len(t.get('source_errors') or []),
    }

def key(summary):
    top=[m.get('instrument') for m in summary.get('top_moves',[])[:4]]
    return json.dumps({'regime':summary.get('regime'),'selected':summary.get('selected'),'top':top,'coverage_status':(summary.get('coverage') or {}).get('status_counts')}, sort_keys=True)

def write(path,obj):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open('a',encoding='utf-8') as f:
        f.write(json.dumps(obj,separators=(',',':'))+'\n')

write(EVENTS, {'event':'watch_started','time':datetime.datetime.now().isoformat(timespec='seconds'),'source':str(SRC),'end':END.isoformat(timespec='seconds')})
while datetime.datetime.now()<END:
    try:
        loaded=load_last()
        if loaded:
            last, rows=loaded
            s=summarize(last, rows)
            write(OUT, s)
            k=key(s)
            if k!=last_key:
                write(EVENTS, {'event':'state_change','time':datetime.datetime.now().isoformat(timespec='seconds'),'key':hashlib.sha1(k.encode()).hexdigest()[:12],'summary':s})
                last_key=k
    except Exception as e:
        write(EVENTS, {'event':'watch_error','time':datetime.datetime.now().isoformat(timespec='seconds'),'error':repr(e)})
    time.sleep(60)
write(EVENTS, {'event':'watch_finished','time':datetime.datetime.now().isoformat(timespec='seconds')})
