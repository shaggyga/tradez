"""Read only dated canonical executor logs; never imports runtime modules."""
from collections import Counter,defaultdict
from datetime import datetime,timezone
import hashlib
import json
from pathlib import Path
import re

OUT=Path(__file__).resolve().parent
ROOT=OUT.parent.parent/'trad'
LOG=ROOT/'data/oanda_training_manager/logs'
START='2026-08-30T21:00:00+00:00'
END='2026-09-04T21:00:00+00:00'
parse=lambda s:datetime.fromisoformat(s.replace('Z','+00:00')).timestamp()
lo,hi=parse(START),parse(END)
files=[]
for path in LOG.glob('practice_top_signal_executor_*.jsonl*'):
    match=re.search(r'_(2026\d{4})_\d{6}',path.name)
    if match and '20260829'<=match.group(1)<='20260905':files.append(path)
assert sum(p.stat().st_size for p in files)<250_000_000
events=Counter();reasons=Counter();authorization_reasons=Counter();samples={};file_records=[];day_events=defaultdict(Counter)
all_fields=defaultdict(Counter);numeric_ranges=defaultdict(dict);bad_lines=[];week_rows=[]
for path in sorted(files):
    before=path.stat();hasher=hashlib.sha256();count=0;inside=0;first=None;last=None;inside_first=None;inside_last=None
    with path.open('rb') as handle:
        for line_no,line in enumerate(handle,1):
            hasher.update(line)
            if not line.strip():continue
            try:
                row=json.loads(line);at=parse(row['time'])
            except (ValueError,KeyError,TypeError) as exc:
                bad_lines.append({'file':path.name,'line':line_no,'error':type(exc).__name__});continue
            count+=1;first=first or row['time'];last=row['time']
            if not lo<=at<hi:continue
            inside+=1;inside_first=inside_first or row['time'];inside_last=row['time']
            event=row.get('event','');events[event]+=1;day_events[row['time'][:10]][event]+=1
            all_fields[event].update(row.keys())
            if row.get('reason'):reasons[event+'|'+str(row['reason'])]+=1
            if row.get('authorization_reason'):authorization_reasons[str(row['authorization_reason'])]+=1
            safe={k:v for k,v in row.items() if k not in ('account_id','account','token','credentials','response','order','request','trade')}
            if event not in samples:samples[event]=safe
            for key,value in row.items():
                if isinstance(value,(int,float)) and not isinstance(value,bool):
                    bounds=numeric_ranges[event].setdefault(key,{'min':value,'max':value,'sum':0,'observations':0})
                    bounds['min']=min(bounds['min'],value);bounds['max']=max(bounds['max'],value)
                    bounds['sum']+=value;bounds['observations']+=1
            week_rows.append({'file':path.name,'line':line_no,'time':row['time'],'event':event,
                              **{k:row[k] for k in ('reason','authorization_reason','cycles','fills','feed_candidates','qualified_candidates','candidate_count','signal_count','eligible_count','count','status') if k in row}})
    after=path.stat();assert(before.st_size,before.st_mtime_ns)==(after.st_size,after.st_mtime_ns)
    file_records.append({'file':path.name,'bytes':before.st_size,'sha256':hasher.hexdigest(),'rows':count,'first_time':first,'last_time':last,
                         'week_rows':inside,'week_first':inside_first,'week_last':inside_last})
result={'start_inclusive_utc':START,'end_exclusive_utc':END,'files':file_records,'events':dict(events),
        'reason_counts':dict(reasons),'authorization_reason_counts':dict(authorization_reasons),
        'day_events':{k:dict(v) for k,v in day_events.items()},'field_counts':{k:dict(v) for k,v in all_fields.items()},
        'numeric_ranges':dict(numeric_ranges),'first_event_examples':samples,'unparsed_lines':bad_lines}
for filename,value in [('executor_log_scan.json',result),('executor_week_event_index.json',week_rows)]:
    with(OUT/filename).open('x',encoding='utf-8')as handle:json.dump(value,handle,indent=2);handle.write('\n')
print(json.dumps({'events':events,'reasons':reasons,'authorization_reasons':authorization_reasons,'numeric_ranges':numeric_ranges,
                   'files':file_records,'first_event_examples':samples,'unparsed':bad_lines[:3]},indent=2)[:45000])
