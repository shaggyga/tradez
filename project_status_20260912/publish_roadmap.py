"""Publish a dated project assessment, preserving previous navigation bytes."""
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import re

ROOT = Path(__file__).resolve().parent
PROJECT = ROOT.parent / 'trad'
VAULT = Path('C:/Users/zmoor/OneDrive/thevault/projects/forex')
REPORT = 'FOREX_STATUS_ROADMAP_20260912.md'


def sha(blob):
    return hashlib.sha256(blob).hexdigest()


def main():
    if (ROOT / 'PUBLICATION_RECEIPT.json').exists():
        raise RuntimeError('already_published')
    members = [p for p in ROOT.rglob('*') if p.is_file() and p.suffix in {'.md', '.json', '.py'}]
    links = 0
    for path in members:
        if path.suffix != '.md':
            continue
        for match in re.finditer(r'\]\(([^)]+)\)', path.read_text(encoding='utf-8-sig')):
            target = match.group(1).strip('<>')
            if target.startswith(('http:', 'https:', '#')):
                continue
            target = re.sub(r':\d+$', '', target).split('#', 1)[0]
            resolved = Path(target) if re.match(r'^[A-Za-z]:[/\\]', target) else path.parent / target
            assert resolved.exists(), str(resolved)
            links += 1
    changes = []
    before = ROOT / 'prior_navigation'
    before.mkdir(exist_ok=False)
    def update(path, group, transform):
        old = path.read_bytes()
        backup = before / group / path.name
        backup.parent.mkdir(parents=True, exist_ok=True)
        backup.write_bytes(old)
        new = transform(old)
        assert path.read_bytes() == old, 'concurrent_document_change'
        temporary = path.with_name(path.name + '.status_roadmap_pending')
        with temporary.open('xb') as out:
            out.write(new)
            out.flush()
            os.fsync(out.fileno())
        assert path.read_bytes() == old, 'concurrent_document_change'
        os.replace(temporary, path)
        changes.append({'path': str(path), 'before_copy': str(backup), 'before_sha256': sha(old), 'after_sha256': sha(path.read_bytes())})
    def prefix(note):
        def transform(old):
            title, sep, rest = old.partition(b'\n')
            return title + sep + b'\n' + note.encode('utf-8') + b'\n\n' + rest
        return transform
    notice = ('**September 12, 11:25–11:26 a.m. Eastern — current status and roadmap:** '
              '[Full strengths, weak points, runtime and priorities](docs/' + REPORT + '). '
              'No local Python/project PowerShell services or dashboard listener were observed. The new meter last recorded 135 blocked attempts and zero publications; clock verification remained stale. '
              'The original trial has a retained completed_flat receipt at Friday cutoff; current broker state was not queried. '
              'All 27 gap items and the older 29-action crosswalk are preserved. Earlier running statements below retain their original dates.')
    for name in ('README.md', 'FOREX_PENDING_IMPROVEMENTS.md'):
        update(PROJECT / name, 'project', prefix(notice))
    for name in ('FOREX_CHANGE_REGISTER_20260911.md', 'FOREX_MODEL_REUSE_REGISTER_20260911.md'):
        update(PROJECT / 'docs' / name, 'project_docs', prefix(notice.replace('(docs/', '(')))
    log = ('\n\n## September 12 — whole-project status and ordered roadmap\n\n' + notice + '\n\n'
           'Read-only runtime inspection and independent data/model and management/recreation reviews produced a consolidated roadmap. '
           'Full historical archive consumption remains incomplete; older price-only work can proceed independently of causal news gaps. '
           'The final report distinguishes observed runtime, retained local trial completion, older broker checks, staged code and unestablished profitability. '
           'Independent review confirmed all 27 FXG items and corrected the 299 source mismatches to an offline admission-audit result. '
           'No model, source code, trial policy, process or broker state was changed for this assessment.\n')
    update(PROJECT / 'FOREX_PROJECT_LOG.md', 'project', lambda old: old + log.encode('utf-8'))
    canonical = PROJECT / 'docs' / REPORT
    pointer = ('# Forex status and roadmap — September 12, 2026\n\n'
               '[Read the complete status, strengths, weaknesses and roadmap](../../project_status_20260912/' + REPORT + ').\n\n'
               'Fresh local runtime evidence is dated 11:25–11:26 a.m. Eastern: no Python/project PowerShell services or dashboard listener were observed. '
               'The last meter record has 135 blocked attempts and zero publications. The local trial receipt says completed_flat at Friday cutoff; today’s broker state was not queried.\n\n'
               'The report covers the historical archive, actual fitted windows, 200-plus feature families, official news, blurbs, prediction performance, horizon curves, position management, submission accounting, accounts, recovery and all 27 current change items. '
               'It preserves the earlier 29-action crosswalk. No runtime or trading changes were made for this roadmap.\n')
    with canonical.open('x', encoding='utf-8') as out:
        out.write(pointer)
    drop = VAULT / 'PROJECT_STATUS_20260912'
    drop.mkdir(exist_ok=False)
    copies = []
    for source in members:
        target = drop / source.relative_to(ROOT)
        target.parent.mkdir(parents=True, exist_ok=True)
        raw = source.read_bytes()
        output = raw
        if source.suffix == '.md':
            # Keep internal copied links portable; bind links outside the addendum
            # to their real project locations rather than a nonexistent vault tree.
            def relocate(match):
                ref = match.group(1).strip('<>')
                if not ref.startswith('../'):
                    return match.group(0)
                absolute = (source.parent / ref).resolve().as_posix()
                return '](' + absolute + ')'
            output = re.sub(r'\]\(([^)]+)\)', relocate, raw.decode('utf-8-sig')).encode('utf-8')
        target.write_bytes(output)
        assert target.read_bytes() == output
        copies.append({'source': str(source), 'member': target.relative_to(drop).as_posix(),
                       'source_sha256': sha(raw), 'copied_sha256': sha(output), 'navigation_relocated': output != raw})
    vault_readme = ('# Current Forex status — September 12\n\n'
                    '[Complete roadmap](' + REPORT + ') with current runtime, strengths, weak points and all 27 change items. '
                    'The addendum includes three independent scope reviews and local runtime evidence. '
                    'Links to older project records name their actual computer locations; those larger archives are not duplicated here.\n')
    (drop / 'README.md').write_text(vault_readme, encoding='utf-8')
    vault_note = ('**September 12, 11:25–11:26 a.m. Eastern — whole-project status:** '
                  '[Current runtime, strengths, weak points and roadmap](PROJECT_STATUS_20260912/README.md). '
                  'Local services and dashboard listener were absent; latest meter record shows 135 blocked attempts, zero captures. '
                  'The old trial has a dated local completed_flat receipt; current broker state was not queried. '
                  'This supersedes earlier running-status observations below without changing their historical evidence.')
    update(VAULT / 'README.md', 'vault', prefix(vault_note))
    file_hashes = {p.relative_to(drop).as_posix(): sha(p.read_bytes()) for p in sorted(drop.rglob('*')) if p.is_file()}
    manifest = {'schema_version': 'forex_status_roadmap_v1', 'created_utc': datetime.now(timezone.utc).isoformat(),
                'files': file_hashes, 'copies': copies, 'portable_private_runtime': False}
    (drop / 'MANIFEST.json').write_text(json.dumps(manifest, indent=2, sort_keys=True), encoding='utf-8')
    for item in changes:
        assert sha(Path(item['before_copy']).read_bytes()) == item['before_sha256']
        assert sha(Path(item['path']).read_bytes()) == item['after_sha256']
    for name, expected in file_hashes.items():
        assert sha((drop / name).read_bytes()) == expected
    receipt = {'published_utc': datetime.now(timezone.utc).isoformat(), 'verified': True,
               'report_sha256': sha((ROOT / REPORT).read_bytes()), 'source_links_checked': links,
               'canonical_pointer': str(canonical), 'canonical_pointer_sha256': sha(canonical.read_bytes()),
               'vault_directory': str(drop), 'vault_files_verified': len(file_hashes),
               'vault_manifest_sha256': sha((drop / 'MANIFEST.json').read_bytes()), 'navigation_changes': changes,
               'all_27_gap_ids_reviewed': True, 'runtime_or_broker_changed': False}
    with (ROOT / 'PUBLICATION_RECEIPT.json').open('x', encoding='utf-8') as out:
        json.dump(receipt, out, indent=2, sort_keys=True)
    print(json.dumps({'published': True, 'local_links_checked': links, 'vault_files_verified': len(file_hashes), 'report': str(ROOT / REPORT)}))


if __name__ == '__main__':
    main()
