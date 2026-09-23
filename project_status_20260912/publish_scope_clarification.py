"""Persist the user's scope correction without rewriting earlier sealed reports."""
from datetime import datetime, timezone
import hashlib, json, os, re
from pathlib import Path

ROOT = Path(__file__).resolve().parent
PROJECT = ROOT.parent / 'trad'
VAULT = Path('C:/Users/zmoor/OneDrive/thevault/projects/forex')
NAME = 'FOREX_REVAMP_SCOPE_20260912.md'


def main():
    evidence = ROOT / 'scope_clarification'
    evidence.mkdir(exist_ok=False)
    sha = lambda data: hashlib.sha256(data).hexdigest()
    changes = []
    def update(path, group, transform):
        original = path.read_bytes()
        backup = evidence / group / path.name
        backup.parent.mkdir(parents=True, exist_ok=True)
        backup.write_bytes(original)
        replacement = transform(original.decode('utf-8-sig')).encode('utf-8')
        assert path.read_bytes() == original, 'concurrent_document_change'
        pending = path.with_name(path.name + '.revamp_scope_pending')
        with pending.open('xb') as out:
            out.write(replacement); out.flush(); os.fsync(out.fileno())
        assert path.read_bytes() == original, 'concurrent_document_change'
        os.replace(pending, path)
        changes.append({'path': str(path), 'before_copy': str(backup), 'before_sha256': sha(original), 'after_sha256': sha(path.read_bytes())})
    def prefix(note):
        def run(text):
            title, sep, rest = text.partition('\n')
            return title + sep + '\n' + note + '\n\n' + rest
        return run
    note = ('**September 12 — revamp scope clarified:** [Full model-audit requirement and first work packages](' + NAME + '). '
            'The user confirms ample demo environments; account inventory is not a revamp prerequisite. '
            'The original every-model audit remains unfinished. Complete coverage must include later families, material variants and orphaned source/artifacts, with explicit dispositions and selective recomputation. '
            'This is a scope correction, not a claim that the audit or revamp has been completed.')
    register = PROJECT / 'docs/FOREX_CHANGE_REGISTER_20260911.md'
    def change_register(text):
        rows = text.splitlines(keepends=True); changed = set()
        for i, row in enumerate(rows):
            if row.startswith('| FXG-011 '):
                rows[i] = ('| FXG-011 — OPEN: COMPLETE MODEL AUDIT | Complete the original audit of every distinct model and material variant across the older catalogue, later studies, actual sources and artifacts. The 140-family index and selected validations do not fulfill this scope. | '
                           'Reconcile both catalogue-to-implementation and implementation-to-catalogue coverage, all retained run records and duplicate lineage. Record actual features, fitting data/caps, target/cost/selection history, original performance, evaluator verification, reconstruction and operational fitness. '
                           'Every model requires an evidence-backed disposition; recompute where needed to resolve a material gap, without automatically refitting all 29,366 records. Missing source links do not prove missing implementations. '
                           '[Scope and completion criteria](' + NAME + '), [model reuse register][models] |\n')
                changed.add('011')
            elif row.startswith('| FXG-019 '):
                rows[i] = ('| FXG-019 — DEMO CAPACITY AVAILABLE / SELECTED-ENVIRONMENT SETUP | The user confirms existing demo accounts are plentiful spare practice environments. A census of historical aliases or funding is not a research-revamp prerequisite. | '
                           'For an environment actually used, bind the correct endpoint and experiment, prevent competing submissions, record comparable risk settings and preserve original trial evidence. '
                           'Reuse existing managers and exposure diagnostics. Broader account history stays dated reference material and does not delay archive/model work. '
                           '[Scope clarification](' + NAME + '), [historical accounts][accounts] |\n')
                changed.add('019')
        assert changed == {'011', '019'}
        return prefix(note)(''.join(rows))
    update(register, 'project_docs', change_register)
    for name in ('FOREX_MODEL_REUSE_REGISTER_20260911.md', 'FOREX_STATUS_ROADMAP_20260912.md'):
        update(PROJECT / 'docs' / name, 'project_docs', prefix(note))
    for name in ('README.md', 'FOREX_PENDING_IMPROVEMENTS.md'):
        update(PROJECT / name, 'project', prefix(note.replace('](' + NAME + ')', '](docs/' + NAME + ')')))
    log = ('\n\n## ' + datetime.now(timezone.utc).isoformat() + ' — revamp scope correction\n\n'
           + note.replace('](' + NAME + ')', '](docs/' + NAME + ')') + '\n\n'
           'FXG-011 now carries the complete original model-audit requirement; FXG-019 makes demo capacity nonblocking. '
           'Added evaluator/selection-history checks, source/artifact reverse coverage and explicit per-model dispositions. '
           'No model run, runtime change, broker request or account change occurred. Prior sealed roadmap and performance records remain unchanged.\n')
    update(PROJECT / 'FOREX_PROJECT_LOG.md', 'project', lambda text: text + log)
    drop = VAULT / 'PROJECT_STATUS_20260912/REVAMP_SCOPE'
    drop.mkdir(exist_ok=False)
    source = PROJECT / 'docs' / NAME
    raw = source.read_bytes()
    def relocate(match):
        target = match.group(1).strip('<>')
        if re.match(r'^[A-Za-z]:[/\\]', target) or target.startswith(('http:', 'https:', '#')):
            return match.group(0)
        return '](' + (source.parent / target).resolve().as_posix() + ')'
    portable = re.sub(r'\]\(([^)]+)\)', relocate, raw.decode('utf-8')).encode('utf-8')
    (drop / NAME).write_bytes(portable)
    vault_note = ('**September 12 — revamp scope clarified:** [Every-model audit and first work packages](PROJECT_STATUS_20260912/REVAMP_SCOPE/' + NAME + '). '
                  'Demo accounts are available practice spaces and are not a revamp bottleneck. The original complete model audit remains unfinished; every distinct implementation/material variant needs an explicit evidence-backed disposition. Earlier roadmap and runtime observations remain dated.')
    update(VAULT / 'README.md', 'vault', prefix(vault_note))
    for change in changes:
        assert sha(Path(change['path']).read_bytes()) == change['after_sha256']
        assert sha(Path(change['before_copy']).read_bytes()) == change['before_sha256']
    assert (drop / NAME).read_bytes() == portable
    prior = json.loads((ROOT / 'PUBLICATION_RECEIPT.json').read_text())
    assert sha((ROOT / 'FOREX_STATUS_ROADMAP_20260912.md').read_bytes()) == prior['report_sha256']
    receipt = {'published_utc': datetime.now(timezone.utc).isoformat(), 'verified': True, 'changes': changes,
               'clarification_source': str(source), 'clarification_source_sha256': sha(raw),
               'vault_copy': str(drop / NAME), 'vault_copy_sha256': sha(portable),
               'prior_sealed_roadmap_unchanged': True, 'full_model_audit_completed': False,
               'demo_account_capacity_is_revamp_blocker': False, 'runtime_or_broker_changed': False}
    encoded = json.dumps(receipt, indent=2, sort_keys=True).encode('utf-8')
    (evidence / 'PUBLICATION_RECEIPT.json').write_bytes(encoded)
    (drop / 'PUBLICATION_RECEIPT.json').write_bytes(encoded)
    print(json.dumps({'published': True, 'navigation_files_verified': len(changes), 'prior_roadmap_preserved': True}))


if __name__ == '__main__':
    main()
