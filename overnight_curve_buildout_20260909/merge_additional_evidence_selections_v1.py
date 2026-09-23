"""Merge explicitly pinned small selections without copying or changing evidence."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import time

BASE=Path(__file__).resolve().parent


def sha(raw):return hashlib.sha256(raw).hexdigest()


def main():
    parser=argparse.ArgumentParser()
    parser.add_argument('--selection',nargs=2,action='append',required=True)
    parser.add_argument('--output',type=Path,required=True)
    args=parser.parse_args();output=args.output.absolute()
    if not 1<=len(args.selection)<=16 or output.parent!=BASE or output.exists() or output.is_symlink():
        raise ValueError('bounded_fresh_merge_required')
    entries={};dependencies={};inputs=[]
    for path,expected in args.selection:
        path=Path(path).absolute()
        if path.parent!=BASE or path.is_symlink() or path.stat().st_size>4*1024*1024:
            raise ValueError('bounded_original_selection_required')
        raw=path.read_bytes()
        if sha(raw)!=expected:raise ValueError('original_selection_changed')
        value=json.loads(raw)
        if not isinstance(value.get('entries'),list):raise ValueError('explicit_entries_required')
        inputs.append(dict(path=str(path),sha256=expected,bytes=len(raw)))
        for row in value['entries']:
            name=row['intended_member']
            if not isinstance(name,str) or not name.startswith('docs/validation/overnight_curve_buildout_20260909/'):
                raise ValueError('unexpected_evidence_member')
            key=name.casefold()
            if key in entries:
                prior=entries[key]
                if prior['intended_member']!=name or prior['original_source']!=row['original_source']:
                    raise ValueError('conflicting_evidence_member')
            else:entries[key]=row
        for row in value.get('canonical_source_dependencies',[]):
            record=row.get('original_source',row)
            source={k:record[k] for k in ('path','sha256','bytes')};key=source['path'].casefold()
            if key in dependencies and dependencies[key]!=source:raise ValueError('conflicting_canonical_dependency')
            dependencies[key]=source
    if not 0<len(entries)<=1200:raise ValueError('merged_file_count_bound')
    for binding in inputs:
        if sha(Path(binding['path']).read_bytes())!=binding['sha256']:raise ValueError('selection_changed_during_merge')
    result=dict(schema_version='merged_additional_evidence_selection_v1_20260909',prepared_epoch=time.time(),
        helper_sha256=sha(Path(__file__).read_bytes()),original_selections=inputs,
        entries=sorted(entries.values(),key=lambda r:r['intended_member']),
        canonical_source_dependencies=sorted(dependencies.values(),key=lambda r:r['path']),
        file_count=len(entries),total_bytes=sum(r['original_source']['bytes'] for r in entries.values()),
        project_or_vault_copies=False,original_selections_or_evidence_changed=False,
        scope='Exact metadata merge only; the separately reviewed curator must verify every payload, source binding, credential rule and destination before copying.')
    raw=(json.dumps(result,indent=2,sort_keys=True,allow_nan=False)+'\n').encode()
    with output.open('xb') as h:h.write(raw);h.flush();os.fsync(h.fileno())
    print(json.dumps(dict(path=str(output),sha256=sha(raw),files=len(entries),bytes=result['total_bytes'])))


if __name__=='__main__':main()
