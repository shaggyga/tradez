"""Export a reviewable Forex knowledge snapshot; never execute copied instructions.

This is a documentation mirror, not a runnable artifact capsule or live work claim.
Existing destinations are never overwritten. Credentials, raw outputs and embedded
source checkouts are excluded. The manifest reports every omission explicitly.
"""
from __future__ import annotations

import argparse
from collections import Counter
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import re

TEXT = {'.md', '.json', '.txt', '.sha256'}
EXCLUDED = {'source', 'sources', 'source_snapshot', 'source_snapshots', 'staged_project',
            'before_source', 'after_source', 'environment', '.git', '__pycache__',
            'credentials', 'secrets', '.venv', 'node_modules', 'evidence', 'data'}
SECRET = re.compile(rb'-----BEGIN (?:RSA |EC |OPENSSH )?PRIVATE KEY-----|'
                    rb'\bgh[pousr]_[A-Za-z0-9]{30,}\b|\bgithub_pat_[A-Za-z0-9_]{30,}\b|'
                    rb'\bsk-(?:proj-)?[A-Za-z0-9_-]{32,}\b|'
                    rb'\b[0-9a-f]{32}-[0-9a-f]{32}\b')
ASSIGNMENT = re.compile(rb'(?i)["\']?(?:password|api[_-]?key|access[_-]?token|secret[_-]?key)["\']?\s*[:=]\s*["\']([^"\'\r\n]{12,})["\']')
PLACEHOLDERS = (b'example', b'placeholder', b'your_', b'os.environ', b'getenv', b'redact', b'dummy', b'env:', b'<', b'***')

def sha(raw):
    return hashlib.sha256(raw).hexdigest()

def json_bytes(value):
    return (json.dumps(value, indent=2, ensure_ascii=True)+'\n').encode()

def unsafe_link(path):
    return path.is_symlink() or (hasattr(path, 'is_junction') and path.is_junction())

def exclusion(relative, size):
    parts = [p.lower() for p in relative.parts]
    name = parts[-1]
    if any(p in EXCLUDED for p in parts[:-1]):
        return 'embedded_source_or_raw_artifact_directory'
    if name.startswith('.env') or any(x in name for x in ('credentials', 'secrets', 'accounts_registry', '.private.', '.local.')) or name in {'creds','auth.json','token.json'}:
        return 'private_configuration'
    if relative.suffix.lower() not in TEXT:
        return 'non_document_artifact'
    if len(parts) > 2 and 'specification' not in parts[1:-1]:
        return 'nested_historical_material_retrieve_from_shared_vault'
    if size > 2 * 1024 * 1024:
        return 'large_document_retrieve_from_shared_vault'
    return None

def contains_secret(raw):
    if SECRET.search(raw):
        return True
    for match in ASSIGNMENT.finditer(raw):
        value = match[1].lower()
        if not any(part in value for part in PLACEHOLDERS):
            return True
    return False

def export(source: Path, destination: Path):
    source, destination = source.absolute(), destination.absolute()
    for path in (source, destination.parent):
        if any(unsafe_link(p) for p in (path, *path.parents)):
            raise ValueError('Symlink/junction roots are not permitted')
    if not source.is_dir() or destination.exists():
        raise ValueError('Source must exist and destination must be new')
    if destination.is_relative_to(source) or source.is_relative_to(destination):
        raise ValueError('Source and destination must be disjoint')
    destination.mkdir(parents=True)
    records, omitted = [], []
    started = datetime.now(timezone.utc).isoformat()
    for directory, folders, files in os.walk(source, followlinks=False):
        base = Path(directory)
        for folder in list(folders):
            path = base/folder
            if unsafe_link(path):
                omitted.append({'path':path.relative_to(source).as_posix(), 'reason':'link_directory'})
                folders.remove(folder)
        for filename in sorted(files):
            path=base/filename; relative=path.relative_to(source); rel=relative.as_posix()
            if unsafe_link(path):
                omitted.append({'path':rel,'reason':'link_file'});continue
            before=path.stat();reason=exclusion(relative,before.st_size)
            if getattr(before, 'st_file_attributes', 0) & (0x1000 | 0x40000 | 0x400000):
                reason='cloud_content_not_locally_available'
            if reason:
                omitted.append({'path':rel,'bytes':before.st_size,'reason':reason});continue
            raw=path.read_bytes();after=path.stat()
            if (before.st_size,before.st_mtime_ns)!=(after.st_size,after.st_mtime_ns):
                raise ValueError(f'Source changed during export: {rel}')
            try:raw.decode('utf-8-sig')
            except UnicodeDecodeError:
                omitted.append({'path':rel,'bytes':len(raw),'reason':'non_utf8_document'});continue
            if contains_secret(raw):
                omitted.append({'path':rel,'bytes':len(raw),'reason':'potential_credential_content_withheld'});continue
            target=destination/relative;target.parent.mkdir(parents=True,exist_ok=True)
            with target.open('xb') as stream:stream.write(raw)
            records.append({'path':rel,'bytes':len(raw),'sha256':sha(raw)})
    # Copied files must still describe one stable publication interval.
    for row in records:
        if sha((source/row['path']).read_bytes())!=row['sha256']:
            raise ValueError('Source changed before snapshot sealing: '+row['path'])
    manifest={'schema':'forex_vault_knowledge_snapshot.v1','started_utc':started,
              'completed_utc':datetime.now(timezone.utc).isoformat(),
              'source_scope':'thevault/projects/forex only', 'live_coordination':False,
              'artifact_capsule':False,'files':sorted(records,key=lambda r:r['path']),
              'excluded':sorted(omitted,key=lambda r:r['path']),
              'omission_counts':dict(Counter(r['reason'] for r in omitted))}
    (destination/'SNAPSHOT_MANIFEST.json').write_bytes(json_bytes(manifest))
    return verify(destination)

def verify(destination: Path):
    manifest=json.loads((destination/'SNAPSHOT_MANIFEST.json').read_text(encoding='utf-8'))
    expected={'SNAPSHOT_MANIFEST.json'}
    for row in manifest['files']:
        relative=Path(row['path'])
        if relative.is_absolute() or relative.drive or '..' in relative.parts:
            raise ValueError('Unsafe snapshot manifest path')
        path=destination/relative
        if any(unsafe_link(p) for p in (path,*path.parents)):
            raise ValueError('Snapshot redirects to another path')
        raw=path.read_bytes()
        if len(raw)!=row['bytes'] or sha(raw)!=row['sha256']:
            raise ValueError('Snapshot hash mismatch: '+row['path'])
        expected.add(relative.as_posix())
    actual={p.relative_to(destination).as_posix() for p in destination.rglob('*') if p.is_file()}
    if actual!=expected:
        raise ValueError('Unexpected or missing snapshot members')
    return {'status':'verified','files':len(manifest['files']),
            'bytes':sum(r['bytes'] for r in manifest['files']),
            'excluded':len(manifest['excluded']),'manifest_sha256':sha((destination/'SNAPSHOT_MANIFEST.json').read_bytes())}

if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('mode',choices=['export','verify'])
    parser.add_argument('--source',type=Path)
    parser.add_argument('--destination',type=Path,required=True)
    args=parser.parse_args()
    if args.mode=='export' and args.source is None:parser.error('--source is required for export')
    print(json.dumps(export(args.source,args.destination) if args.mode=='export' else verify(args.destination)))
