"""Publish a dated implementation record without changing older evidence."""
from datetime import datetime,timezone
import hashlib
import json
from pathlib import Path
import shutil

ROOT=Path(__file__).resolve().parent
PROJECT=ROOT.parent/'trad'
VAULT=Path('C:/Users/zmoor/OneDrive/thevault/projects/forex')
sha=lambda p:hashlib.sha256(p.read_bytes()).hexdigest()


def main():
    done=json.loads((ROOT/'COMPLETION_RECEIPT.json').read_text(encoding='utf-8'));assert done['implementation_verified']
    before=ROOT/'canonical_docs_before';before.mkdir(exist_ok=False);changes=[]
    def update(path,make,group):
        old=path.read_bytes();target=before/group/path.name;target.parent.mkdir(parents=True,exist_ok=True);target.write_bytes(old)
        new=make(old)
        if path.read_bytes()!=old:raise RuntimeError('concurrent_document_change')
        path.write_bytes(new)
        changes.append({'path':str(path),'before_copy':str(target),'before_sha256':hashlib.sha256(old).hexdigest(),'after_sha256':sha(path)})
    def prepend_after_title(path,note,group):
        def insert(old):
            title,sep,rest=old.partition(b'\n');return title+sep+b'\n'+note.encode()+b'\n\n'+rest
        update(path,insert,group)
    note=('**September 12 — continuous currency-news capture implemented and started:** '
          '[Verified build and current blocker](docs/FOREX_CONTINUOUS_METER_20260912.md). '
          '133 tests passed in both original and restored copies. Exact row deduplication, source-ID/lineage binding, bounded memory/time/storage and process recovery are implemented. '
          'The new service is running, but its first two attempts were withheld because upstream clock verification is stale. '
          'Automatic approval review blocked starting that dependency; no fresh captures or prediction gains are claimed.')
    for name in ('README.md','FOREX_PENDING_IMPROVEMENTS.md'):prepend_after_title(PROJECT/name,note,'project')
    log=PROJECT/'FOREX_PROJECT_LOG.md'
    addition=('\n\n## '+datetime.now(timezone.utc).isoformat()+' — continuous meter and source identity guard\n\n'+note+'\n\n'
        'Separate release: '+str(ROOT)+'. The original news supervisor was resumed at 03:41 UTC, but the collector returns blocked_clock_integrity. '
        'The new passive supervisor/worker started at 04:06 UTC. At 04:11:48 UTC it had two refused attempts, no successful captures, and no heartbeat/diagnostic/clock errors. '
        'Existing clock-monitor startup was rejected by automatic approval review with only "blocked by policy"; no retry/bypass or clock-threshold change occurred. '
        'No quote stream, broker worker, trial, model or promotion changed. Old signed-cost archive hash verified unchanged.\n\n'
        'Storage retains exact versions and ordered manifests; a second capture of the same 5,357 retained rows adds 8 KiB of database storage. The prior full compressed snapshot was about 3.45 MB. '
        'Storage peak commit reduced from about 736 MB to 546 MB. New source admission excludes 299 ID/lineage mismatches beyond 59 untrusted-clock rows in the inspected snapshot; '
        'the upstream upsert defect remains to repair with versioned provenance. All 133 tests passed in the restored copy; the vault ZIP includes no article database or credentials. '
        'Clock dependency, successful future collection, release-surprise inputs, sign-in recovery and later predictive validation remain open.\n')
    update(log,lambda old:old+addition.encode(),'project')
    register=PROJECT/'docs/FOREX_CHANGE_REGISTER_20260911.md'
    prefix=('**September 12 continuous-meter update:** [Implementation and current operation](FOREX_CONTINUOUS_METER_20260912.md). '
            'FXG-017 now has a started passive collector with deduplicated storage, exact source binding and tested process recovery. '
            'Live evidence is still blocked by the upstream clock dependency. FXG-018 retains the missing consensus/revision/surprise inputs. '
            'New FXG-027 below records the observed upstream source-ID/lineage defect; older states remain dated evidence.')
    prepend_after_title(register,prefix,'project_docs')
    tail=('\n\n## FXG-027 — OPEN: source identity across deduplicated news versions\n\n'
          '299 inspected article rows retained one table source ID while their replacement payload contained another configured source lineage. '
          'Cross-query document deduplication is intentional, but the update omits table source identity fields and no per-source observation chain was found. '
          'The new meter excludes these mismatches and retains exact raw versions. Close the upstream gap with explicit source/version provenance and availability clocks, '
          'regression tests for cross-query updates, and prospective observations. Do not rewrite old first-seen times or declare all historical news studies invalid. '
          '[Exact trace and retained example](../../currency_meter_continuous_20260912/feed_audit/SOURCE_ID_LINEAGE_REVIEW.json).\n')
    def update_register_rows(old):
        rows = old.decode('utf-8-sig').splitlines(keepends=True)
        replaced = set()
        for index, row in enumerate(rows):
            if row.startswith('| FXG-017 '):
                rows[index] = ('| FXG-017 — IMPLEMENTED / LIVE EVIDENCE BLOCKED | Assess the existing continuous currency narrative meter and source-strength/rank outputs as additional inputs with preserved observation clocks. | '
                    'September 12: reused the pure formulas in a separate passive five-minute capture service with exact source-ID/lineage binding, deduplicated retention, actual publication clocks and tested process recovery. '
                    '133 tests passed in original and restored copies. Service started; its first two attempts were withheld because upstream clock attestation is stale. '
                    'Automatic approval review blocked clock-monitor startup. Successful prospective captures and incremental predictive evidence remain open; no active model feature schema changed. '
                    '[Implementation and blocker](FOREX_CONTINUOUS_METER_20260912.md), [feature/model inventory][inventory] |\n')
                replaced.add('FXG-017')
            elif row.startswith('| FXG-026 '):
                rows[index] = ('| FXG-026 — SCOPED CAPTURE LIMITS VERIFIED / LONG-RUN GROWTH OPEN | Measure logical capture/history capacity pressure separately from disk free-space observations. | '
                    'September 12: the new passive capture store caps physical database and unique logical content, reserves disk headroom and refuses capacity overflow without deleting retained evidence. '
                    'A second capture of the same 5,357 retained rows added 8,192 database bytes. Limits, rollback and lossless replay passed tests. '
                    'These are offline storage observations; whole-project growth and prospective article churn remain unmeasured. '
                    '[Capture build](FOREX_CONTINUOUS_METER_20260912.md), [earlier feed watch][feeds] |\n')
                replaced.add('FXG-026')
        assert replaced == {'FXG-017', 'FXG-026'}
        return ''.join(rows).encode('utf-8') + tail.encode('utf-8')
    update(register,update_register_rows,'project_docs_after_prefix')
    reuse=PROJECT/'docs/FOREX_MODEL_REUSE_REGISTER_20260911.md'
    update(reuse,lambda old:old+('\n\n## September 12 — continuous currency-meter retention\n\n'
        'Reused the original V12-era pure formulas and prior one-shot capture. Added a separate continuous cohort, content-addressed version/manifest storage, '
        'actual publication receipts, exact source-ID-to-lineage admission, clock/cadence guards and passive process supervision. '
        '133 integrated and restored tests passed; this is collection infrastructure, not another fitted model or evidence of profitability. '
        'The earlier two raw captures were unchanged across 5,357 rows and served only the lossless storage benchmark. '
        'New production capture is withheld until upstream clock verification resumes. [Build, limits and restoration](FOREX_CONTINUOUS_METER_20260912.md).\n').encode(),'project_docs')
    canonical=PROJECT/'docs/FOREX_CONTINUOUS_METER_20260912.md'
    content=('# Continuous currency-news capture — September 12\n\n'
        '[Implemented service, source audit and recreation](../../currency_meter_continuous_20260912/README.md). '
        '**The service has started; fresh collection remains blocked by upstream clock verification.** '
        'Automatic approval review rejected the clock-monitor startup with "blocked by policy" and no further reason. No bypass occurred.\n\n'
        '133 tests passed in both original and independent restored copies. New storage preserves exact article versions and ordering; the unchanged second benchmark capture adds 8 KiB. '
        'The worker enforces source-ID/lineage consistency, real publication clocks, a 300-second feature-age limit, a 75-second capture timeout and a 768 MiB child-memory cap. '
        'Process supervision handles crashes and duplicate launches, with bounded restart attempts.\n\n'
        'The audit found 299 source identity mismatches beyond 59 old clock exclusions. The upstream dedup/upsert needs a versioned provenance repair; it was not silently rewritten. '
        'Numeric consensus and surprise inputs remain absent in the retained snapshot.\n\n'
        '[Completion](../../currency_meter_continuous_20260912/COMPLETION_RECEIPT.json) and '
        '[dated operational observation](../../currency_meter_continuous_20260912/review/OPERATIONAL_OBSERVATION.json). '
        'At 04:11:48 UTC, two capture attempts were refused, zero production captures published, and heartbeat/diagnostic/clock-error counts were zero. '
        'No broker worker, numerical model, trading policy or old trial was changed. Successful live collection, clock-monitor restoration, official surprise inputs and separate sign-in recovery remain open.\n')
    with canonical.open('x',encoding='utf-8') as out:out.write(content)
    drop=VAULT/'CONTINUOUS_METER_20260912';drop.mkdir(exist_ok=False)
    for name in ('README.md','COMPLETION_RECEIPT.json','RELEASE_MANIFEST.json','SOURCE_REUSE.json','STORAGE_DESIGN_RECEIPT.json','currency_meter_continuous_20260912.zip'):
        shutil.copy2(ROOT/name,drop/name)
    for folder in ('feed_audit','review'):
        for path in sorted((ROOT/folder).rglob('*')):
            if path.is_file() and path.suffix in {'.json','.md','.xml'}:
                target=drop/path.relative_to(ROOT);target.parent.mkdir(parents=True,exist_ok=True);shutil.copy2(path,target)
    for name in ('FOREX_CONTINUOUS_METER_20260912.md','FOREX_CHANGE_REGISTER_20260911.md','FOREX_MODEL_REUSE_REGISTER_20260911.md'):
        shutil.copy2(PROJECT/'docs'/name,drop/name)
    vault_note=('**September 12 — continuous meter service:** [Build, source fixes, recreation and current blocker](CONTINUOUS_METER_20260912/README.md). '
                'Includes the exact inactive source/test ZIP; 133 original/restored tests passed. The service is started but refuses capture while upstream clock verification is stale. '
                'Automatic approval review blocked restoring that dependency. No fresh forecasting or trading success is claimed; older source/runtime records keep their dates.')
    prepend_after_title(VAULT/'README.md',vault_note,'vault')
    files={p.relative_to(drop).as_posix():sha(p) for p in sorted(drop.rglob('*')) if p.is_file()}
    (drop/'MANIFEST.json').write_text(json.dumps(files,indent=2,sort_keys=True),encoding='utf-8')
    receipt={'published_utc':datetime.now(timezone.utc).isoformat(),'changes':changes,'new_canonical_report':str(canonical),
             'new_canonical_report_sha256':sha(canonical),'vault_addendum':str(drop),'vault_files':len(files),'vault_manifest_sha256':sha(drop/'MANIFEST.json'),
             'completion_sha256':sha(ROOT/'COMPLETION_RECEIPT.json'),'older_document_bytes_preserved':True}
    with (ROOT/'PUBLICATION_RECEIPT.json').open('x',encoding='utf-8') as out:json.dump(receipt,out,indent=2,sort_keys=True)
    print(json.dumps({'published':True,'vault_files':len(files),'canonical':str(canonical)}))


if __name__=='__main__':main()
