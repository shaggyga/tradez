"""Publish completed archive evidence and integrated source repairs, preserving001."""
from datetime import datetime,timezone
import hashlib,json,re
from pathlib import Path
ROOT=Path(__file__).resolve().parent
TRAD=ROOT.parent/'trad'
VAULT=Path('C:/Users/zmoor/OneDrive/thevault/projects/forex')
OUT=ROOT/'publication_checkpoint_002'
def sha(raw):return hashlib.sha256(raw).hexdigest()
def main():
    OUT.mkdir(exist_ok=False);backup=OUT/'before';backup.mkdir()
    records=[]
    def update(path,transform):
        before=path.read_bytes();new=transform(before.decode('utf-8')).encode('utf-8')
        saved=backup/(sha(before)[:12]+'_'+path.name);saved.write_bytes(before)
        assert path.read_bytes()==before,'concurrent_document_change'
        path.write_bytes(new);assert path.read_bytes()==new
        records.append({'path':str(path),'before_sha256':sha(before),'after_sha256':sha(new),'backup':str(saved)})
    now=datetime.now(timezone.utc).isoformat()
    newdoc=TRAD/'docs/FOREX_ARCHIVE_DIRECTION_COMPARISON_20260912.md';assert not newdoc.exists()
    raw=(ROOT/newdoc.name).read_bytes();newdoc.write_bytes(raw)
    records.append({'path':str(newdoc),'before_sha256':None,'after_sha256':sha(raw)})
    def audit(text):
        old='The next archive comparison preserves the fully specified24/99 technical/peer schemas; strict60-minute continuity makes17 technical coverage fields constant, and each training fold will report the number that actually varies.'
        new='The completed archive comparison preserved the fully specified24/99 technical/peer schemas; strict60-minute continuity made17 technical coverage fields constant. The separate richer-input audit maps631 close/clock formulas within the795-input engine; a new strictly timed source-derived adapter remains under review.'
        assert old in text;text=text.replace(old,new)
        marker='## Correctness repairs integrated during this run'
        summary='''## Completed longer-archive comparison

All36predeclared Ridge/HGB fits completed across68pairs, three periods and15/60/240-minute horizons. The selected-position cost results comprise32negative,2positive and2no-position cells, with no setup positive in every period. Sign accuracy spans49.42%–52.45%; adding75peer fields worsened MAE in all18matched comparisons. This does not test every broader historical feature engine. All5,425,308 saved learned estimates/actions were reproduced exactly from36saved artifacts, and all81report rows were independently recomputed. The observations overlap; they are not executed account profit. [Protocol, full results and recreation](FOREX_ARCHIVE_DIRECTION_COMPARISON_20260912.md).

'''
        assert text.count(marker)==1;text=text.replace(marker,summary+marker)
        row='| Fuzzy source freshness |'
        line=next(x for x in text.splitlines() if x.startswith(row))
        text=text.replace(line,line+'\n| Producer publication v4 |A frozen summary must be persisted and read back before its generation can be acknowledged; heartbeat follows those exact bytes. New inactive version, same34numerical inputs and retained scheduler.|[Publication repair and tests](PRODUCER_PUBLICATION_REPAIR_20260912.md)|\n| News replay/reaction |Exact four-quote bps, original scoring cutoffs, reconstructed fanout/episodes, actual maturity purge and report-only final block; immutable run outputs and truthful metric labels.|[99tests plus44subtests and16permanent checks, overlapping](NEWS_REACTION_CONTRACT_REPAIR_20260912.md)|')
        old=next(x for x in text.splitlines() if x.startswith('1. Finish the fixed longer-archive'))
        text=text.replace(old,'1. The fixed longer-archive comparison and saved-model recreation are complete. Preserve all negative and sparse-positive cells; do not tune the already viewed periods into fresh confirmation. Any richer-context experiment requires its own frozen feature/timing/resource difference and uses the existing comparison as prior evidence.')
        old=next(x for x in text.splitlines() if x.startswith('2. Complete the staged news reaction'))
        text=text.replace(old,'2. The news reaction repair is integrated and tested. The separate FXG-027 source/derived-observation and SQLite-transaction repair remains staged; reclassification availability must gate admission without resetting story age. New historical news replay remains unperformed and requires actual provenance.')
        return text
    update(TRAD/'docs/FOREX_MODEL_SPACE_AUDIT_20260912.md',audit)
    note='**September12 completed-archive checkpoint:** [Longer-archive results and exact saved-model recreation](FOREX_ARCHIVE_DIRECTION_COMPARISON_20260912.md). Four fixed setups across36cells produced32negative,2positive and2no-position selected-position results; no consistent trading edge was established. All36saved models reproduced5.43million estimates exactly. Producer-publication and news-reaction repairs are now integrated research source; source/derived news lineage remains staged. No service or account was activated. Earlier checkpoint paragraphs retain their dates.\n\n'
    def prefix(text):
        first,rest=text.split('\n',1);return first+'\n\n'+note+rest
    update(TRAD/'docs/FOREX_MODEL_REUSE_REGISTER_20260911.md',prefix)
    def changes(text):
        text=prefix(text)
        return text+'''

## September12 completed archive and seventh source repair checkpoint

FXG-009/010/012/016: the declared full-primary-archive comparison is complete and all36saved fitted artifacts reproduce their estimates exactly in the current runtime. Retain its negative/sparse-positive outcomes and explicit compact24/peer75 scope. This does not close exact older-v4 source/weight recreation or exhaust795/MA643 inputs. [Results and recreation](FOREX_ARCHIVE_DIRECTION_COMPARISON_20260912.md).

FXG-006/020: separate inactive producer v4 now acknowledges only exact persisted/read-back summary bytes. Numeric model fields and the retained fair scheduler are unchanged; package/native/input/process and operational closure remain distinct. [Repair](PRODUCER_PUBLICATION_REPAIR_20260912.md).

FXG-010/016/018/020: the exact-endpoint news reaction repair is now canonical with independent/current-dependency verification and retained failures. It does not prove historical reclassification was originally available, causal numeric surprise, or successful reaction forecasts. The separate FXG-027 source/derived lineage repair remains staged, including transaction ownership, stale writer and story-age/availability separation. [Repair](NEWS_REACTION_CONTRACT_REPAIR_20260912.md).

FXG-011/012/016: [richer own-price recreation audit](../../revamp_8h_20260912/models/richer_price_feature_recreation_001/RICHER_FEATURE_RECREATION.md) accounts for795ordered matrix inputs and631close/clock formulas. A new adapter can reuse those formulas with explicit history/availability; old complete-row eligibility and exactv4 source parity are not inherited. No new gap ID or best-model claim is created by this checkpoint.
'''
    update(TRAD/'docs/FOREX_CHANGE_REGISTER_20260911.md',changes)
    destination=VAULT/'REVAMP_8H_20260912/CHECKPOINT_002';destination.mkdir(exist_ok=False)
    originals=destination/'EXACT_SOURCE_DOCUMENT_BYTES';originals.mkdir()
    names=['FOREX_MODEL_SPACE_AUDIT_20260912.md','FOREX_ARCHIVE_DIRECTION_COMPARISON_20260912.md',
        'FOREX_CHANGE_REGISTER_20260911.md','FOREX_MODEL_REUSE_REGISTER_20260911.md',
        'PRODUCER_PUBLICATION_REPAIR_20260912.md','NEWS_REACTION_CONTRACT_REPAIR_20260912.md']
    sources=[TRAD/'docs'/name for name in names]
    sources.append(ROOT/'models/richer_price_feature_recreation_001/RICHER_FEATURE_RECREATION.md')
    by_path={p.resolve():p.name for p in sources};copies=[]
    for source in sources:
        raw=source.read_bytes();(originals/source.name).write_bytes(raw)
        def relocate(match):
            target=match.group(1).strip('<>')
            if target.startswith(('http:','https:','#')) or re.match(r'^[A-Za-z]:',target):return match.group(0)
            part,mark,anchor=target.partition('#');resolved=(source.parent/part).resolve()
            changed=by_path.get(resolved,resolved.as_posix())+('#'+anchor if mark else '')
            return ']('+changed+')'
        readable=re.sub(r'\]\(([^)]+)\)',relocate,raw.decode('utf-8')).encode('utf-8')
        (destination/source.name).write_bytes(readable)
        copies.append({'source':str(source),'destination':str(destination/source.name),
            'source_sha256':sha(raw),'readable_sha256':sha(readable),'exact_source_copy':str(originals/source.name)})
    readme=f'''# Forex revamp checkpoint002

Published {now}. The original eight-hour run remains active until September13 at01:42UTC / September12 at9:42pm Eastern.

Read [current model-space status](FOREX_MODEL_SPACE_AUDIT_20260912.md), [completed archive results and recreation](FOREX_ARCHIVE_DIRECTION_COMPARISON_20260912.md), and [richer input reuse](RICHER_FEATURE_RECREATION.md). The [change register](FOREX_CHANGE_REGISTER_20260911.md) preserves existing IDs and prior work. The older checkpoint001 remains in the parent folder with its original date.

These seven readable documents have relocated links and separately retained exact source bytes. Sources, model/data artifacts and working receipts linked outside this folder remain on the project computer. This is an evidence/navigation checkpoint, not a fully portable source/database release. Raw chat messages and private credentials are excluded. No live service, account, trading trial or schedule was activated.
'''
    (destination/'README.md').write_text(readme,encoding='utf-8')
    (destination/'COPIED_DOCUMENTS.json').write_text(json.dumps({'published_utc':now,'files':copies},indent=2)+'\n',encoding='utf-8')
    def nav(text):
        first,rest=text.split('\n',1)
        return first+'\n\n**September12 completed-archive checkpoint:** [Current audit, exact recreation and integrated source repairs](REVAMP_8H_20260912/CHECKPOINT_002/README.md). The eight-hour run continues; no consistent trading edge or live-operation claim is made.\n\n'+rest
    update(VAULT/'README.md',nav)
    receipt={'schema':'audit_navigation_checkpoint_v2','published_utc':now,'updated':records,'copied_documents':copies,
        'raw_chat_exported':False,'database_exported':False,'full_portable_source_release':False}
    (OUT/'PUBLICATION_RECEIPT.json').write_text(json.dumps(receipt,indent=2)+'\n',encoding='utf-8')
    print(json.dumps({'updated':len(records),'copied':len(copies),'receipt':str(OUT/'PUBLICATION_RECEIPT.json')}))
if __name__=='__main__':main()
