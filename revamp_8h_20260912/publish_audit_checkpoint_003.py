"""Publish dated news repairs and the frozen richer-context study, preserving prior checkpoints."""
from pathlib import Path
from datetime import datetime,timezone
import hashlib,json,re
ROOT=Path(__file__).resolve().parent
TRAD=ROOT.parent/'trad'
VAULT=Path('C:/Users/zmoor/OneDrive/thevault/projects/forex')
OUT=ROOT/'publication_checkpoint_003'
def sha(raw):return hashlib.sha256(raw).hexdigest()
def main():
    OUT.mkdir(exist_ok=False);backup=OUT/'before';backup.mkdir();changes=[]
    def update(path,transform):
        before=path.read_bytes();after=transform(before.decode('utf-8')).encode('utf-8')
        saved=backup/(sha(before)[:12]+'_'+path.name);saved.write_bytes(before)
        if path.read_bytes()!=before:raise ValueError('concurrent_document_change')
        path.write_bytes(after)
        if path.read_bytes()!=after:raise ValueError('document_readback_failed')
        changes.append({'path':str(path),'before_sha256':sha(before),'after_sha256':sha(after),'backup':str(saved)})
    now=datetime.now(timezone.utc).isoformat()
    freeze=ROOT/'direction_richer_archive_001/PROTOCOL_FREEZE_001.json'
    if sha(freeze.read_bytes())!='61712b03ac9f816c8af9b594d0a90a1fd70b3ee0fe39b32d7ef78ab709b8993d':raise ValueError('new_protocol_changed')
    note='**September 12, news and richer-history checkpoint:** Source/derived news lineage and three separate v2 consumers are integrated and tested. The new 655-input HGB comparison has a frozen 21-source protocol and is preparing the full primary archive; no richer-model result is available at this checkpoint. [Current audit](FOREX_MODEL_SPACE_AUDIT_20260912.md), [news lineage](NEWS_SOURCE_LINEAGE_REPAIR_20260912.md), [new consumers](NEWS_CONSUMER_V2_REPAIR_20260912.md), [richer protocol](../../revamp_8h_20260912/direction_richer_archive_001/PROTOCOL_001.md). Earlier paragraphs retain their dates. No service or trading activation occurred.\n\n'
    def prefix(text):
        first,rest=text.split('\n',1);return first+'\n\n'+note+rest
    def audit(text):
        text=prefix(text)
        old='The separate richer-input audit maps631 close/clock formulas within the795-input engine; a new strictly timed source-derived adapter remains under review.'
        new='The separate richer-input audit maps 631 close/clock formulas within the 795-input engine. Its reviewed adapter is now frozen in a new same-row HGB comparison, adding those fields to the original compact 24. Entirely missing training columns use a saved training-only support projection; rows and full missingness diagnostics remain intact. This is not old 795-engine weight parity.'
        if old not in text:raise ValueError('audit_anchor_changed')
        text=text.replace(old,new)
        line=next(x for x in text.splitlines() if x.startswith('| News replay/reaction |'))
        text=text.replace(line,line+'\n| News source and derived clocks |Immutable source/version observations, separate post-computation classification availability, preserved story age/expiry and caller-owned transactions. Current and historical provenance stay distinct.|[Core lineage repair](NEWS_SOURCE_LINEAGE_REPAIR_20260912.md)|\n| Separate news consumers v2 |Exact table/payload source identity, explicit new guard cohort, original story-age context and later classification admission; old consumers/outputs preserved.|[197 staged and canonical checks, overlapping](NEWS_CONSUMER_V2_REPAIR_20260912.md)|')
        old=next(x for x in text.splitlines() if x.startswith('2. The news reaction repair'))
        text=text.replace(old,'2. News reaction, source/derived lineage and separate v2 consumers are integrated and tested. Twenty-five old collector assertions require explicit current-contract fixture migration; old worker/study cohorts remain unchanged. Original-known historical news replay, source authentication and prospective operation remain open.')
        return text
    update(TRAD/'docs/FOREX_MODEL_SPACE_AUDIT_20260912.md',audit)
    update(TRAD/'docs/FOREX_MODEL_REUSE_REGISTER_20260911.md',prefix)
    def register(text):
        text=prefix(text)
        old='## FXG-027 — OPEN: source identity across deduplicated news versions'
        new='## FXG-027 — SOURCE REPAIR INTEGRATED / PROSPECTIVE EVIDENCE OPEN: source identity across deduplicated news versions'
        if old not in text:raise ValueError('gap_anchor_changed')
        text=text.replace(old,new)
        return text+'\n\n## September 12: source/derived news lineage and richer-history freeze\n\nFXG-027/018/020: the immutable source/derived observation repair and separate v2 news consumers are canonical. Root and independent reviews preserve first source identity, source/body age, later classification eligibility and caller transaction boundaries. The joint reaction006/collector008 check passes 99 tests plus 44 subtests; the full new consumer suite passes 197 cases. These counts overlap. No real database migration, worker repointing or prospective capture was performed. The original 299 mismatched historical rows remain evidence, not rewritten history. [Lineage](NEWS_SOURCE_LINEAGE_REPAIR_20260912.md), [consumer migration](NEWS_CONSUMER_V2_REPAIR_20260912.md).\n\nFXG-016/012: the richer close-history hypothesis is frozen before raw preparation and new fits. It uses 631 additional source-derived own-price fields, compact 24 and fixed 68 pair identities; H60/H240 and the same three periods, rows and executable-cost rules as the completed comparison. The installed HGB all-missing-column defect is handled by an explicit saved training-only support projection, preserving every row. Exactly six new fits are planned and old compact HGB predictions are reused. No result is available in this checkpoint. [Protocol](../../revamp_8h_20260912/direction_richer_archive_001/PROTOCOL_001.md).\n\nFXG-010/016/020: ensemble calibration/direction caller repairs remain staged. Eight original source counterexamples and subsequent metadata/score consistency findings are retained; no legacy threshold transfer, model promotion or profitability claim is made. The shared-feed confidence/side-profit-to-direction-probability semantics remain a separate open caller issue.\n'
    update(TRAD/'docs/FOREX_CHANGE_REGISTER_20260911.md',register)
    destination=VAULT/'REVAMP_8H_20260912/CHECKPOINT_003';destination.mkdir(exist_ok=False)
    exact=destination/'EXACT_SOURCE_DOCUMENT_BYTES';exact.mkdir()
    sources=[TRAD/'docs'/n for n in ('FOREX_MODEL_SPACE_AUDIT_20260912.md','FOREX_ARCHIVE_DIRECTION_COMPARISON_20260912.md','FOREX_CHANGE_REGISTER_20260911.md','FOREX_MODEL_REUSE_REGISTER_20260911.md','NEWS_SOURCE_LINEAGE_REPAIR_20260912.md','NEWS_CONSUMER_V2_REPAIR_20260912.md','NEWS_REACTION_CONTRACT_REPAIR_20260912.md')]
    sources += [ROOT/'direction_richer_archive_001/PROTOCOL_001.md',ROOT/'direction_richer_archive_001/PREPARATION_CONTRACT_002.md',ROOT/'runtime/news_collector_legacy_compatibility_001/COMPATIBILITY_ACCOUNTING_001.md']
    by_path={p.resolve():p.name for p in sources};copies=[]
    for source in sources:
        raw=source.read_bytes();(exact/source.name).write_bytes(raw)
        def relocate(match):
            target=match.group(1).strip('<>')
            if target.startswith(('http:','https:','#')) or re.match(r'^[A-Za-z]:',target):return match.group(0)
            part,mark,anchor=target.partition('#');resolved=(source.parent/part).resolve()
            return ']('+by_path.get(resolved,resolved.as_posix())+('#'+anchor if mark else '')+')'
        readable=re.sub(r'\]\(([^)]+)\)',relocate,raw.decode('utf-8')).encode('utf-8')
        (destination/source.name).write_bytes(readable)
        copies.append({'source':str(source),'destination':str(destination/source.name),'source_sha256':sha(raw),'readable_sha256':sha(readable),'exact_source_copy':str(exact/source.name)})
    readme=f'''# Forex revamp checkpoint 003

Published {now}. The original run continues until September 13 at 01:42 UTC / September 12 at 9:42 pm Eastern.

Read the [current audit](FOREX_MODEL_SPACE_AUDIT_20260912.md), [completed compact/peer archive results](FOREX_ARCHIVE_DIRECTION_COMPARISON_20260912.md), [integrated news lineage](NEWS_SOURCE_LINEAGE_REPAIR_20260912.md), [new consumers](NEWS_CONSUMER_V2_REPAIR_20260912.md), and [frozen richer-history protocol](PROTOCOL_001.md). The richer comparison is preparing; this checkpoint contains no new richer-model score.

Ten documents have relocated links and exact original byte copies. Prior checkpoints are preserved. Referenced source/data/model artifacts remain on the project computer; this is navigation and audit evidence, not a fully portable private-state release. Raw chats, credentials and databases are excluded. No service, account, trading trial or schedule was activated.
'''
    (destination/'README.md').write_text(readme,encoding='utf-8')
    (destination/'COPIED_DOCUMENTS.json').write_text(json.dumps({'published_utc':now,'files':copies},indent=2)+'\n',encoding='utf-8')
    def nav(text):
        first,rest=text.split('\n',1)
        return first+'\n\n**September 12 news/richer-history checkpoint:** [Integrated news repairs and frozen archive study](REVAMP_8H_20260912/CHECKPOINT_003/README.md). The run continues; richer-model results and live-operation evidence are not yet available.\n\n'+rest
    update(VAULT/'README.md',nav)
    result={'schema':'audit_navigation_checkpoint_v3','published_utc':now,'updated':changes,'copied_documents':copies,'raw_chat_exported':False,'database_exported':False,'full_portable_source_release':False,'script_sha256':sha(Path(__file__).read_bytes())}
    (OUT/'PUBLICATION_RECEIPT.json').write_text(json.dumps(result,indent=2)+'\n',encoding='utf-8')
    print(json.dumps({'updated':len(changes),'copied':len(copies),'receipt':str(OUT/'PUBLICATION_RECEIPT.json')}))
if __name__=='__main__':main()
