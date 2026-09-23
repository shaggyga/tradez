"""Retain five compact external historical sources under the portable root."""
from copy import deepcopy
import hashlib
import json
import os
from pathlib import Path
import time

BASE=Path(__file__).resolve().parent
PRIOR_SHA='4b6ed38fc87f8a8101d390b34b7ef43f0fd9306e737937650968f62cd77c38b3'


def record(path):
    raw=path.read_bytes()
    return dict(path=str(path.resolve()),sha256=hashlib.sha256(raw).hexdigest(),bytes=len(raw))


def write_new(path,raw):
    with path.open('xb') as stream:
        stream.write(raw);stream.flush();os.fsync(stream.fileno())
    return record(path)


def main():
    prior=BASE/'CURATED_NEWS_CAPACITY_REFRESH_SELECTION_20260909.json'
    raw=prior.read_bytes();assert hashlib.sha256(raw).hexdigest()==PRIOR_SHA
    value=deepcopy(json.loads(raw));selected=value['entries'];external=[]
    for item in selected:
        source=Path(item['original_source']['path']).resolve()
        assert record(source)==item['original_source']
        if not source.is_relative_to(BASE):external.append(item)
    assert len(external)==5
    root=BASE/'news_capacity/prior_history_validation'
    assert root.resolve().is_relative_to(BASE) and not root.exists()
    root.mkdir()
    transitions=[]
    for item in external:
        old=deepcopy(item['original_source']);source=Path(old['path'])
        target=root/source.name
        raw=source.read_bytes();assert hashlib.sha256(raw).hexdigest()==old['sha256']
        retained=write_new(target,raw)
        assert retained['sha256']==old['sha256'] and retained['bytes']==old['bytes']
        item['original_source']=retained
        item['retained_historical_origin']=old
        item['copy_performed']=False
        item['portable_retention_performed']=True
        transitions.append(dict(original_source=old,retained_source=retained,exact_same_bytes=True,
                                intended_member=item['intended_member']))
    receipt=dict(schema_version='news_capacity_portable_source_retention_v2_20260909',
        status='five_exact_compact_sources_retained_under_portable_root',retained_epoch=time.time(),
        previous_selection=record(prior),source_helper=record(Path(__file__)),transitions=transitions,
        original_selection_and_sources_unchanged=True,project_or_vault_copy=False,
        scope='Four dated September8 compact receipts/XML and one existing test source, not datasets or raw news.')
    receipt_path=root/'PORTABLE_HISTORY_SOURCE_RETENTION_20260909.json'
    proof=write_new(receipt_path,(json.dumps(receipt,sort_keys=True,indent=2)+'\n').encode())
    selected.append(dict(original_source=proof,
        intended_member='docs/validation/overnight_curve_buildout_20260909/news_capacity/prior_history_validation/'+receipt_path.name,
        artifact_status='exact_portable_retention_provenance',copy_performed=False))
    assert all(Path(item['original_source']['path']).resolve().is_relative_to(BASE) for item in selected)
    assert len({x['intended_member'].lower() for x in selected})==len(selected)
    value.update(schema_version='curated_news_capacity_refresh_selection_v2_20260909',
        status='selection_only_all_members_inside_portable_root',created_epoch=time.time(),
        source_helper=record(Path(__file__)),previous_selection=record(prior),portable_source_transition=proof,
        counts=dict(files=len(selected),bytes=sum(x['original_source']['bytes'] for x in selected)))
    path=BASE/'CURATED_NEWS_CAPACITY_REFRESH_SELECTION_V2_20260909.json'
    result=write_new(path,(json.dumps(value,sort_keys=True,indent=2)+'\n').encode())
    note=BASE/'CURATED_NEWS_CAPACITY_REFRESH_SELECTION_V2_20260909.md'
    text=f'''# News and capacity portable evidence selection V2

All {len(selected)} selected members now originate inside the declared portable evidence root. Five compact historical sources were copied as exact bytes into `news_capacity/prior_history_validation`: four dated receipts/XML and one test source. The transition receipt binds each original and retained path, SHA and byte count. Original evidence and selection V1 remain unchanged.

This resolves source containment only. It does not rerun historical tests or news/database audits and makes no project or vault copy. The initial history test result remains eight passed/two failed; the final frozen suite remains231 passed. Raw data and private captures remain excluded. New current-news/capacity findings and inactive successor scope are unchanged.

Selection SHA-256: `{result['sha256']}`. Transition SHA-256: `{proof['sha256']}`.
'''
    note_result=write_new(note,text.encode())
    print(json.dumps(dict(selection=result,note=note_result,transition=proof,counts=value['counts'])))


if __name__=='__main__':main()
