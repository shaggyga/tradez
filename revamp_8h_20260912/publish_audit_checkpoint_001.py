"""Dated navigation/checkpoint only; preserve earlier documents and receipts."""
from pathlib import Path
import datetime,hashlib,json,re

ROOT=Path(__file__).resolve().parent
PROJECT=ROOT.parent/'trad'
VAULT=Path('C:/Users/zmoor/OneDrive/thevault/projects/forex')
OUT=ROOT/'publication_checkpoint_001';OUT.mkdir(exist_ok=False)
BEFORE=OUT/'before';BEFORE.mkdir()
sha=lambda raw:hashlib.sha256(raw).hexdigest()
records=[]

def guarded_update(path,data):
    path=Path(path);before=path.read_bytes();old=sha(before)
    snapshot=BEFORE/(old[:12]+'_'+path.name);snapshot.write_bytes(before)
    new=data(before.decode('utf-8')).encode('utf-8')
    assert sha(path.read_bytes())==old,'Concurrent document edit: '+str(path)
    path.write_bytes(new)
    records.append({'path':str(path),'before_sha256':old,'before_snapshot':str(snapshot),'after_sha256':sha(new)})

update='''**September 12, 20:16 UTC — defined source audit and repairs:** [Model-space coverage, actual feature counts, prior results and remaining work](FOREX_MODEL_SPACE_AUDIT_20260912.md). All300 frozen source versions are dispositioned, the older catalogue is reconciled, and the primary archive contains53,512,475 M1rows across68pairs/28months. Five correctness/reader repairs are integrated with exact before/after receipts. This is not a claim that every model was rerun, missing artifacts were recovered, or profitable trading was established. The fixed longer-archive comparison is preparing; no service or trial was activated. Earlier status paragraphs retain their dates.

'''

def add_after_title(text):
    first,rest=text.split('\n',1)
    return first+'\n\n'+update+rest

reuse=PROJECT/'docs'/'FOREX_MODEL_REUSE_REGISTER_20260911.md'
guarded_update(reuse,add_after_title)

def update_register(text):
    text=add_after_title(text)
    old=next(line for line in text.splitlines() if line.startswith('| FXG-011 '))
    new='| FXG-011 — DEFINED CENSUS DISPOSITION COMPLETE / RECREATION GAPS OPEN | The frozen C census now covers all300 source-byte versions in five disjoint partitions, including later predictors, handwritten formulas, wrappers and explicit companions. All older29366 run records/10072 variants are structurally reconciled. | September12: actual roles, feature/target definitions, material differences, evaluation defects and recovery limits are dispositioned. The19:51 check accounts for all1165 C aliases; later repairs have separate integration receipts. This is complete coverage of the declared census, not independent re-execution of every historical model. Exact missing sources, ordered fitted columns, checkpoints and original-period memberships remain listed. [Coverage and limits](FOREX_MODEL_SPACE_AUDIT_20260912.md), [scope](FOREX_REVAMP_SCOPE_20260912.md), [model reuse register][models] |'
    text=text.replace(old,new)
    old=next(line for line in text.splitlines() if line.startswith('| FXG-009 '))
    extra=' September12: the sealed primary union reconciles53,512,475 rows/68pairs/28months, with explicit exact-target, future-gap, NY17-session and sampling counts. This does not fill the earlier1989 gaps or establish original live receipt times. [Archive population](../../revamp_8h_20260912/data/POPULATION_GUIDE.md).'
    text=text.replace(old,old[:-1]+extra+' |')
    old=next(line for line in text.splitlines() if line.startswith('| FXG-012 '))
    extra=' September12:137/146 old source versions and174/224 selected serialized references have exact hash matches in the bounded recovery review; the curve history explicitly preserves missing ordered family inputs/source bodies. D reads stopped after one timeout. [Expanded audit](FOREX_MODEL_SPACE_AUDIT_20260912.md).'
    text=text.replace(old,old[:-1]+extra+' |')
    text+='''

## September12 canonical correctness checkpoint

The profit-factor, M5 maturity/completeness, pip provenance, fuzzy selection/maturity and fuzzy temporal-reader fixes are integrated research-source improvements. Their scopes and overlapping test suites are listed in [the model-space audit](FOREX_MODEL_SPACE_AUDIT_20260912.md). They support FXG-010/011/012/016/020, but do not close prospective performance, deployment or all historical evaluator issues. No new gap ID is assigned to an already listed validation/recreation requirement. The passive v3 outcome caller and exact-endpoint news reaction repairs remain staged pending their specific integration/closure work.
'''
    return text

guarded_update(PROJECT/'docs'/'FOREX_CHANGE_REGISTER_20260911.md',update_register)

vaultdir=VAULT/'REVAMP_8H_20260912';vaultdir.mkdir(exist_ok=False)
rawdir=vaultdir/'CANONICAL_DOCUMENT_BYTES';rawdir.mkdir()
docs=['FOREX_MODEL_SPACE_AUDIT_20260912.md','FOREX_CHANGE_REGISTER_20260911.md','FOREX_MODEL_REUSE_REGISTER_20260911.md',
    'EVALUATOR_CONTRACT_REPAIR_20260912.md','M5_SUPERVISED_TIMING_REPAIR_20260912.md','PIP_METADATA_REPAIR_20260912.md',
    'FUZZY_SELECTION_MATURITY_REPAIR_20260912.md','FUZZY_READER_FRESHNESS_REPAIR_20260912.md']
copies=[]
for name in docs:
    source=PROJECT/'docs'/name;raw=source.read_bytes();destination=vaultdir/name
    (rawdir/name).write_bytes(raw)
    def relocate(match):
        target=match.group(1).strip('<>')
        if target.startswith(('http:','https:','#')) or re.match(r'^[A-Za-z]:',target):return match.group(0)
        path_text,mark,fragment=target.partition('#')
        resolved=(source.parent/path_text).resolve()
        if resolved.parent==PROJECT/'docs' and resolved.name in docs:new_target=resolved.name
        else:new_target=resolved.as_posix()
        if mark:new_target+='#'+fragment
        return ']('+new_target+')'
    relocated=re.sub(r'\]\(([^)]+)\)',relocate,raw.decode('utf-8')).encode('utf-8')
    destination.write_bytes(relocated)
    copies.append({'source':str(source),'destination':str(destination),'source_sha256':sha(raw),
        'destination_sha256':sha(relocated),'canonical_bytes_snapshot':str(rawdir/name),'bytes':len(raw),
        'transformation':'inline relative links relocated; original bytes retained separately'})
readme='''# Eight-hour Forex revamp — audit checkpoint

The run began September12 at17:42UTC and remains active until September13 at01:42UTC. This is the20:16UTC checkpoint, not the final run report or a live-operation claim.

Read the [model-space audit](FOREX_MODEL_SPACE_AUDIT_20260912.md) for scope, actual feature counts, prior performance, integrated repairs and remaining work. The [canonical version](C:/Users/zmoor/Documents/forex/trad/docs/FOREX_MODEL_SPACE_AUDIT_20260912.md) remains in the project. Readable copies here relocate their inline links; exact original bytes are separately retained in `CANONICAL_DOCUMENT_BYTES`, with both hashes in `COPIED_DOCUMENTS.json`. This small checkpoint is navigation/evidence, not a new complete portable source/database release. Links to excluded local evidence still require the project computer.

The [source-union guide](C:/Users/zmoor/Documents/forex/revamp_8h_20260912/models/source_union_review_001/SOURCE_AUDIT_GUIDE.md) covers all300 frozen source versions. The [archive population guide](C:/Users/zmoor/Documents/forex/revamp_8h_20260912/data/POPULATION_GUIDE.md) binds53,512,475 primary M1rows. The [curve-history review](C:/Users/zmoor/Documents/forex/revamp_8h_20260912/models/curve_history/CURVE_FEATURE_HISTORY.md) preserves the weak positive cross-currency result alongside negative, invalidated and unrecovered experiments.

Five Forex chat histories were retrieved/indexed locally. Raw chat messages, private credentials and databases are not copied here. Existing vault source ZIPs and validation seals retain their original dates. The old finite practice trial remains ended; no service/trading setting was activated in this run.

Continue from the [active work log](C:/Users/zmoor/Documents/forex/revamp_8h_20260912/WORK_LOG.md) and canonical [change register](C:/Users/zmoor/Documents/forex/trad/docs/FOREX_CHANGE_REGISTER_20260911.md). Source repair tests do not establish positive trading performance. Longer-archive direction/co-movement results are still pending this checkpoint.
'''
(vaultdir/'README.md').write_text(readme,encoding='utf-8',newline='\n')
(vaultdir/'COPIED_DOCUMENTS.json').write_text(json.dumps({'checkpoint':'2026-09-12T20:16:00Z','files':copies,'relative_link_base':str(PROJECT/'docs'),'full_source_release':False},indent=2),encoding='utf-8')

def vault_navigation(text):
    first,rest=text.split('\n',1)
    note='**September12, 20:16UTC — eight-hour revamp checkpoint:** [Defined model-space audit, canonical repairs and active work](REVAMP_8H_20260912/README.md). The frozen300-source census and primary53.5-million-row archive are reconciled; exact recreation gaps and weak/negative prior performance remain explicit. This supersedes the earlier unfinished-census description for that defined scope only. The longer-archive comparison is preparing; no service or trading trial was activated.\n\n'
    return first+'\n\n'+note+rest
guarded_update(VAULT/'README.md',vault_navigation)
receipt={'schema':'audit_navigation_checkpoint_v1','published_utc':datetime.datetime.now(datetime.timezone.utc).isoformat(),
    'updated':records,'copied_documents':copies,'raw_chat_exported':False,'database_exported':False,'source_release_composed':False}
(OUT/'PUBLICATION_RECEIPT.json').write_text(json.dumps(receipt,indent=2),encoding='utf-8')
print(json.dumps({'receipt':str(OUT/'PUBLICATION_RECEIPT.json'),'updated_documents':len(records),'copied_documents':len(copies),'vault':str(vaultdir)},indent=2))
