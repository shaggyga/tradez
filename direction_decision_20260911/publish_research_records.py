"""Append a dated, scoped project/vault record while preserving previous bytes."""
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import shutil

ROOT=Path(__file__).resolve().parent
PROJECT=ROOT.parent/'trad'
VAULT=Path('C:/Users/zmoor/OneDrive/thevault/projects/forex')


def digest(data):return hashlib.sha256(data).hexdigest()


def main():
    completion=json.loads((ROOT/'COMPLETION_RECEIPT.json').read_text())
    assert completion['verified'] is True
    count=completion['integrated_tests_passed']
    before=ROOT/'canonical_docs_before_signed_cost'
    before.mkdir(exist_ok=False)
    changes=[]
    def update(path,new_bytes,scope,expected):
        original=path.read_bytes()
        if original!=expected:raise RuntimeError('concurrent_document_change:'+str(path))
        copy=before/scope/path.name;copy.parent.mkdir(parents=True,exist_ok=True);copy.write_bytes(original)
        if path.read_bytes()!=original:raise RuntimeError('concurrent_document_change:'+str(path))
        path.write_bytes(new_bytes)
        assert path.read_bytes()==new_bytes
        changes.append({'path':str(path),'before_sha256':digest(original),'after_sha256':digest(new_bytes),'before_copy':str(copy)})
    def insert_after_title(path,text,scope):
        original=path.read_bytes();line,separator,rest=original.partition(b'\n')
        update(path,line+separator+b'\n'+text.encode()+b'\n\n'+rest,scope,original)
    note=(f'**September 11 late evening / September 12 UTC — signed-cost research completed:** '
          f'[Implementation, results and remaining gaps](docs/FOREX_DIRECTION_DECISION_20260912.md). '
          f'{count} integrated tests passed; eight saved model bundles and two news captures recreated. '
          'Probability calibration improved, but dependable after-cost direction remains unestablished. '
          'A four-horizon cost consumer and fresh currency-meter capture path are implemented in the isolated research package; '
          'continuous meter collection and broker-manager integration remain open. Earlier operational statements retain their own dates.')
    for name in ('README.md','FOREX_PENDING_IMPROVEMENTS.md'):
        insert_after_title(PROJECT/name,note,'project')
    logfile=PROJECT/'FOREX_PROJECT_LOG.md'
    addition=('\n\n## '+datetime.now(timezone.utc).isoformat()+' — signed mean/cost study and currency-news retention\n\n'+note+'\n\n'
              'Reused the existing boosting/opportunity and signed-regression lineages, exact prior panel/snapshot and separate chronological calibration. '
              '24 learned estimate/group/horizon policies across 39,893 rows: 17 negative, four positive, three with no actions. Positive support is tiny or unstable; '
              'signed-return optimism dominates endpoint exit-cost error. No selected replacement. The four 5/15/30/60-minute heads use original target identity, '
              'known asymmetric entry prices and predicted exit costs. New current currency capture has its own cohort and actual postcommit visibility; '
              'the old V12 ends September 5 and was not inserted into the September 7–11 study. Two captures retained 5,357 raw rows each; no numeric consensus and zero current directional scores.\n\n'
              'Exact code, sources, models, test and recreation proofs: '+str(ROOT)+'. The independent extracted restoration passed its recorded checks. '
              'Canonical additions are documentation only; no active numerical source, service, order, trial cap/cutoff, broker account or original outcome changed. '
              'A scoped dated vault addendum includes source and evidence metadata; the local restoration ZIP retains private input snapshots separately.\n')
    original_log=logfile.read_bytes()
    update(logfile,original_log+addition.encode(),'project',original_log)
    register=PROJECT/'docs/FOREX_CHANGE_REGISTER_20260911.md'
    register_note=('**September 12 UTC update:** [Signed-cost follow-up](FOREX_DIRECTION_DECISION_20260912.md) adds measured calibration and signed-mean/cost comparisons. '
                  '**FXG-016:** another fixed comparison completed; edge remains unestablished. '
                  '**FXG-017:** strict meter reader, current capture and 66-field pair projection implemented/tested; persistent retention and model consumption still open. '
                  '**FXG-018:** current actual/previous fields located; consensus/revisions/rate surprises and longer original-known history remain gaps. '
                  '**FXG-015/021:** four-head research economics consumer and exit-cost estimates tested, with broker/management comparison and live cost evidence still open. '
                  '**FXG-020:** new scoped extracted restoration verified; full private runtime recovery is not claimed. The earlier table descriptions remain dated problem statements.')
    insert_after_title(register,register_note,'project_docs')
    reuse=PROJECT/'docs/FOREX_MODEL_REUSE_REGISTER_20260911.md'
    reuse_add=('\n\n## September 12 UTC — signed-cost and meter reuse follow-up\n\n'
               'The older executable-opportunity rankers already used gradient boosting, absolute magnitude, conditional direction, Platt calibration and cost thresholds. '
               'Direct signed regressors also existed. The new comparison corrects the interpretation of the clearance-probability × median-magnitude proxy by estimating '
               'unconditional signed means, or positive/nonpositive conditional means with matching population weights, on the new shared technical/peer/news panel. '
               'It is not a new invention of boosting, calibration or magnitude modelling.\n\n'
               'Eight retained bundles, four horizons, 24 learned policies and 39,893 reused-history assessment rows: calibration scores improved in eight of eight aggregates, '
               'but 17 policies were negative, four positive with limited/inconsistent support, and three inactive. Do not nominate the small positive cells as proven winners or repeat '
               'the same inspected dates as confirmation. Older V12 currency-meter history ends September 5; its implemented 66-field projection was not backfilled into this study. '
               'Current capture restarts retention under a separate source-bound cohort; continuous service remains uninstalled. '
               '[All outcomes, predecessor inventory and recreation](FOREX_DIRECTION_DECISION_20260912.md).\n')
    original_reuse=reuse.read_bytes()
    update(reuse,original_reuse+reuse_add.encode(),'project_docs',original_reuse)
    canonical=PROJECT/'docs/FOREX_DIRECTION_DECISION_20260912.md'
    text=(f'# Signed direction and cost research — September 12 UTC\n\n'
          f'[Completed build and measured results](../../direction_decision_20260911/DIRECTION_DECISION_REPORT.md). '
          f'{count} integrated tests passed. Eight model bundles reproduced 718,074 numbers exactly; 384 replay curves / 1,536 heads matched; '
          '720 independent metric checks reconciled. Two exact currency-state captures also recreated.\n\n'
          'Separate-day probability calibration improves scores across every tested horizon, but signed-return optimism remains the main observed failure. '
          'Of 24 learned policies, 17 lose after endpoint costs, four are positive with small or unstable support, and three select nothing. '
          'These reused-history results do not qualify a trading replacement.\n\n'
          'Implemented: direct/conditional signed means, asymmetric expected entry/exit costs, fixed-margin wait decisions, a strict four-head research curve consumer, '
          'and source-pinned current currency-state capture with real postcommit clocks. The old meter stopped September 5; present raw snapshots contain no numeric consensus. '
          'Continuous capture, surprise/rate inputs, later directional validation and actual manager integration remain open.\n\n'
          '[Completion and restoration index](../../direction_decision_20260911/COMPLETION_RECEIPT.json), '
          '[full numerical interpretation](../../direction_decision_20260911/review/INDEPENDENT_RESULT_INTERPRETATION.md), '
          '[news input audit](../../direction_decision_20260911/event_bridge/EVENT_BRIDGE_REPORT.md). '
          'No active trading configuration or broker state was changed; earlier running/flat claims retain their original cutoffs.\n')
    with canonical.open('x',encoding='utf-8') as out:out.write(text)
    validation=PROJECT/'FOREX_DIRECTION_DECISION_VALIDATION_20260912.json'
    with validation.open('x',encoding='utf-8') as out:json.dump(completion,out,indent=2,sort_keys=True)
    # Add a separately named dated vault record, never republish all historical state.
    drop=VAULT/'SIGNED_COST_RESEARCH_20260912'
    drop.mkdir(exist_ok=False)
    study=drop/ROOT.name
    for name in ('src','tests','review','reuse_review'):
        for path in sorted((ROOT/name).rglob('*')):
            if path.is_file() and '__pycache__' not in path.parts and path.suffix in {'.py','.md','.json','.xml'}:
                dest=study/path.relative_to(ROOT);dest.parent.mkdir(parents=True,exist_ok=True);shutil.copy2(path,dest)
    for name in ('DIRECTION_DECISION_REPORT.md','STUDY_SPEC.json','COMPLETION_RECEIPT.json','verify_saved_cost_study.py','build_local_restoration.py','publish_research_records.py'):
        shutil.copy2(ROOT/name,study/name)
    for name in ('evaluation_001','event_bridge'):
        for path in (ROOT/name).iterdir():
            if path.is_file() and path.suffix in {'.py','.md','.json','.xml'}:
                dest=study/name/path.name;dest.parent.mkdir(parents=True,exist_ok=True);shutil.copy2(path,dest)
    for name in ('FOREX_WEEKEND_SETUP_20260911.md','FOREX_DIRECTION_RESEARCH_20260911.md','FOREX_DIRECTION_DECISION_20260912.md','FOREX_CHANGE_REGISTER_20260911.md','FOREX_MODEL_REUSE_REGISTER_20260911.md'):
        shutil.copy2(PROJECT/'docs'/name,drop/name)
    (drop/'README.md').write_text('# Dated research addendum — September 12 UTC\n\n'
        '[Main result and implemented code](direction_decision_20260911/DIRECTION_DECISION_REPORT.md) records improved probability calibration but no qualified profitable replacement. '
        '[Current needed changes](FOREX_CHANGE_REGISTER_20260911.md) and [reuse/performance history](FOREX_MODEL_REUSE_REGISTER_20260911.md) avoid repeating prior models.\n\n'
        'This addendum contains exact new source/tests and readable result/proof metadata. It does not replace the older vault source snapshot or claim their manifests cover these additions. '
        'Some copied canonical navigation links retain their original project-layout paths; use the canonical project for those.\n\n'
        'The full local inactive restoration ZIP, including eight model bundles, predecessor snapshot/panel and two source-row captures, is indexed in '
        '[COMPLETION_RECEIPT.json](direction_decision_20260911/COMPLETION_RECEIPT.json). It remains on this computer; private input databases, artifacts and credentials are not implicitly supplied by this vault addendum. '
        'For exact recreation use that local ZIP and its README/environment/manifest, not the metadata-only vault tree. No live service was activated.\n',encoding='utf-8')
    vault_note=('**September 12 UTC — latest research/setup addendum:** [Signed-cost results, code, recreation and pending gaps](SIGNED_COST_RESEARCH_20260912/README.md). '
                'Calibration improved but dependable profitable direction remains unestablished. Includes the later September 11 setup/direction records and current reuse/change registers. '
                'The old currency meter stopped September 5; a new one-shot capture is implemented. This is dated research evidence, not a fresh broker/runtime check. '
                'Earlier source snapshots and operational claims keep their original cutoffs.')
    insert_after_title(VAULT/'README.md',vault_note,'vault')
    manifest={p.relative_to(drop).as_posix():digest(p.read_bytes()) for p in sorted(drop.rglob('*')) if p.is_file()}
    (drop/'MANIFEST.json').write_text(json.dumps(manifest,indent=2,sort_keys=True)+'\n',encoding='utf-8')
    receipt={'published_utc':datetime.now(timezone.utc).isoformat(),'changes':changes,'new_canonical_report':str(canonical),
             'new_validation':str(validation),'vault_addendum':str(drop),'vault_files':len(manifest),'vault_manifest_sha256':digest((drop/'MANIFEST.json').read_bytes()),
             'completion_sha256':digest((ROOT/'COMPLETION_RECEIPT.json').read_bytes()),'prior_document_bytes_preserved':True,'live_source_modified':False}
    (ROOT/'PUBLICATION_RECEIPT.json').write_text(json.dumps(receipt,indent=2,sort_keys=True)+'\n',encoding='utf-8')
    print(json.dumps({'published':True,'vault_files':len(manifest),'canonical_report':str(canonical)},indent=2))


if __name__=='__main__':main()
