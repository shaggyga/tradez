"""Apply only four continuation documents after completed evidence is supplied."""
from pathlib import Path
import datetime as dt
import difflib
import hashlib
import json

BASE=Path(__file__).resolve().parent
PROJECT=BASE.parent/'trad'
DIR=BASE/'continuation_documentation_v1'

def digest(raw):return hashlib.sha256(raw).hexdigest()

def main():
    prior=json.loads((DIR/'draft_002/CONTINUATION_DOCUMENTATION_DRAFT_20260909.json').read_bytes())
    before={Path(r['path']):Path(r['path']).read_bytes() for r in prior['records']}
    for r in prior['records']:assert digest(before[Path(r['path'])])==r['sha256']
    for p,h in prior['immutable_early_close_files'].items():assert digest(Path(p).read_bytes())==h
    fields=json.loads((DIR/'final_sections.json').read_bytes());assert set(fields)=={'PILOT_PARAGRAPH','RISK_PARAGRAPH','NEWS_PARAGRAPH','OPS_PARAGRAPH'}
    assert all(type(s)is str and len(s)>100 and '{{' not in s for s in fields.values())
    now=dt.datetime.now(dt.timezone.utc);stamp=now.strftime('%Y-%m-%d %H:%M:%S UTC')
    text=(DIR/'final_template.md').read_text(encoding='utf-8')
    for k,v in fields.items():assert text.count('{{'+k+'}}')==1;text=text.replace('{{'+k+'}}',v)
    text=text.replace('{{FINAL_WRITE_UTC}}',stamp);assert '{{' not in text
    new=PROJECT/'docs/FOREX_CONTINUATION_TO_0900_20260909.md';updates={new:text.encode()}
    p=PROJECT/'README.md';s=before[p].decode();a='**Resumed through 09:00 Eastern:**';start=s.index(a);end=s.index('\n',start)
    s=s[:start]+'**September 9 continuation through 09:00 Eastern:** [the continuation record](docs/FOREX_CONTINUATION_TO_0900_20260909.md) adds the completed 2,907-outcome joint reassessment, later original-curve/risk results, passive news-clock observation and final operations at their own cutoffs. Its separate validation identifies added evidence; the published early-close report below remains dated history. Orders and promotion remain disabled.'+s[end:];updates[p]=s.encode()
    p=PROJECT/'FOREX_PENDING_IMPROVEMENTS.md';s=before[p].decode();cut=s.index('## Early-close queue history - retained verbatim') if '## Early-close queue history - retained verbatim' in s else s.index('## Early-close queue history');history=s[cut:]
    current=f'''# Forex pending improvements\n\n## Current - September 9 continuation handoff\n\nUpdated {stamp}. Read the [continuation record](docs/FOREX_CONTINUATION_TO_0900_20260909.md) for completed new assessments and their exact cutoffs. The earlier report, published 629-member validation and canceled-run evidence remain historical. Main-model and active paper-management after-cost acceptance is still not established.\n\n1. Use the separate continuation validation/source publication receipts to verify the added evidence and final source inventory. Later-maturing targets or missed provider observations remain pending/missing; any later evaluation must retain its own cutoff.\n2. Register a shared curve/risk issue grid and fresh decision-time updates before a new matched management comparison. Current S5/M1 references and full-horizon estimates cannot be silently made compatible.\n3. Compare entry/continuation and existing hold/exit/reduce/rotation controls against fixed hold/no-trade under identical executable costs and independent sessions. Inactive reconciliation/channel/bridge repairs are not account execution permission.\n4. Extend causal richer inputs only with original observation clocks and prospective comparisons; preserve actual failure/refusal evidence and monitor bounded news/history capacity.\n\nNo source, model, scoring convention, registered cohort or runtime routing changed in this continuation documentation. Orders and promotion remain disabled.\n\n''';updates[p]=(current+history).encode()
    p=PROJECT/'FOREX_PROJECT_LOG.md';updates[p]=before[p]+f'''\n\n## {stamp} - continuation handoff through 09:00 Eastern\n\nThe resumed assessment added725 outcomes to the exact unchanged prior2182, for2907 total:47.51% direction,-4.6798 mean net bps after spread, and Brier0.2530481972 versus0.25. This is cumulative unchanged-model evidence, not an effect of observer or inactive-manager repairs. Later curve/risk and passive news-clock results are recorded at their own original cutoffs in docs/FOREX_CONTINUATION_TO_0900_20260909.md, including pending/missing observations.\n\nThe continuation has a separate validation/evidence index; the published early-close report/validation629-member index remain byte-identical. The final source pointer/manifest identifies the full resulting inventory. README and current pending work now link the continuation, with all earlier dated history preserved. Source/record publication remains established only by its own receipt. No broker action, runtime change, learned-number edit, scorer change or execution authorization occurred in this documentation pass.\n'''.encode()
    out=DIR/'final_001';out.mkdir(exist_ok=False);records=[]
    for p,raw in updates.items():
        old=before[p];(out/(p.name+'.before')).write_bytes(old);p.write_bytes(raw);assert p.read_bytes()==raw;(out/p.name).write_bytes(raw)
        (out/(p.name+'.diff.txt')).write_text(''.join(difflib.unified_diff(old.decode().splitlines(True),raw.decode().splitlines(True),fromfile=p.name+' draft',tofile=p.name+' final')),encoding='utf-8')
        records.append(dict(path=str(p),sha256=digest(raw),bytes=len(raw),before_sha256=digest(old)))
    for p,h in prior['immutable_early_close_files'].items():assert digest(Path(p).read_bytes())==h
    receipt=dict(status='continuation_documentation_frozen',created_utc=now.isoformat(),records=records,immutable_early_close_files=prior['immutable_early_close_files'],runtime_changes=False,publication_claim='Separate actual publication receipt required.',source_inventory='New continuation evidence index plus prior published629-member index; final source pointer carries full inventory.')
    path=out/'CONTINUATION_DOCUMENTATION_FINAL_20260909.json';path.write_text(json.dumps(receipt,indent=2)+'\n',encoding='utf-8')
    entry=dict(original_source=dict(path=str(path),sha256=digest(path.read_bytes()),bytes=path.stat().st_size),intended_member='docs/validation/overnight_curve_buildout_20260909/'+path.relative_to(BASE).as_posix(),kind='retained_evidence',group='continuation_documentation',artifact_status='Final documentation source bindings; prior published report/validation immutable.')
    selection=BASE/'CURATED_CONTINUATION_DOCUMENTATION_SELECTION_20260909.json';selection.write_text(json.dumps(dict(entries=[entry],file_count=1,selected_bytes=entry['original_source']['bytes'],canonical_source_dependencies=[],runtime_writes=False),indent=2)+'\n',encoding='utf-8')
    print(json.dumps(dict(receipt=str(path),receipt_sha256=digest(path.read_bytes()),selection=str(selection),selection_sha256=digest(selection.read_bytes()),records=records)))

if __name__=='__main__':main()