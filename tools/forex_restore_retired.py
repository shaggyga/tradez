"""Recover one retired duplicate by copying its authenticated retained bytes."""
import argparse
import hashlib
import json
from pathlib import Path
import shutil

ROOT = Path(__file__).resolve().parents[1]

def safe_path(relative):
    p = Path(relative)
    if p.is_absolute() or p.drive or '..' in p.parts or not p.parts:
        raise ValueError('Unsafe relative path')
    result = ROOT / p
    for part in (result, *result.parents):
        if part == ROOT:
            break
        if part.is_symlink() or part.is_junction():
            raise ValueError('Linked path is not permitted')
    if not result.resolve().is_relative_to(ROOT) or result == ROOT:
        raise ValueError('Path escaped project')
    return result

def digest(path):
    with path.open('rb') as stream:
        return hashlib.file_digest(stream, 'sha256').hexdigest()

def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--journal', type=Path, required=True)
    parser.add_argument('--path', required=True)
    parser.add_argument('--apply', action='store_true')
    args = parser.parse_args()
    records = [json.loads(line) for line in args.journal.read_text(encoding='utf-8').splitlines()]
    matches = [r for r in records if r['event'] == 'removed' and r['retired'] == args.path]
    if len(matches) != 1:
        raise ValueError('Expected one exact retired path in journal')
    r = matches[0]
    source, target = safe_path(r['retained']), safe_path(r['retired'])
    if source.stat().st_size != r['bytes'] or digest(source) != r['sha256']:
        raise ValueError('Retained bytes fail identity check')
    status = 'verified_preview'
    if target.exists():
        if digest(target) != r['sha256']:
            raise ValueError('Refusing to overwrite differing existing bytes')
        status = 'already_present_verified'
    elif args.apply:
        if shutil.disk_usage(ROOT).free < r['bytes'] + 8 * 1024**3:
            raise ValueError('Recovery would violate 8 GiB reserve')
        target.parent.mkdir(parents=True, exist_ok=True)
        with source.open('rb') as src, target.open('xb') as dst:
            shutil.copyfileobj(src, dst, 4 * 1024**2)
        if digest(target) != r['sha256']:
            raise ValueError('Recovered bytes fail identity check')
        status = 'restored_verified'
    print(json.dumps({'status': status, 'path': r['retired'], 'sha256': r['sha256'],
                      'bytes': r['bytes'], 'model_fits': 0, 'policy_replays': 0}))

if __name__ == '__main__':
    main()
