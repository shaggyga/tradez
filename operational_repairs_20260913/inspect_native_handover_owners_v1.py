"""Read-only, bounded process/import-owner inventory for root's later handover.

Writes only a sanitized receipt in OPS. No remote-process inspection, signals,
broker call, live configuration change, or credential values are performed.
"""
from collections import deque
import ast
import datetime as dt
import hashlib
import json
from pathlib import Path
import re
import time

import psutil

AREA = Path(__file__).resolve().parent
ROOT = AREA.parent / 'trad'
STAGE = AREA / 'compact_native_stage_007'
PROFILE = ROOT / 'config/operational_runtime_v7_20260914_features.json'


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def run():
    started = time.monotonic()
    stage = json.loads((STAGE / 'SOURCE_STAGE.json').read_bytes())
    changed = {name: {'old_sha256': sha(ROOT / name) if (ROOT / name).exists() else None,
                      'representative_new_sha256': value}
               for name, value in stage['source_bindings'].items()
               if not (ROOT / name).exists() or sha(ROOT / name) != value}
    modules = {p.stem: p for p in ROOT.glob('*.py')}
    cache = {}
    def imports(name):
        if name in cache:
            return cache[name]
        assert len(cache) < 800 and time.monotonic() - started < 30, 'inventory_bound'
        path = modules[name]
        if path.stat().st_size > 2 * 1024**2:
            cache[name] = {'error': 'source_over_2MiB', 'imports': [], 'literal_files': []}
            return cache[name]
        raw = path.read_bytes()
        tree = ast.parse(raw)
        edges = set()
        refs = set()
        dynamic = []
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                edges.update(alias.name.split('.')[0] for alias in node.names)
            elif isinstance(node, ast.ImportFrom) and node.module:
                edges.add(node.module.split('.')[0])
            elif isinstance(node, ast.Call) and node.args and isinstance(node.args[0], ast.Constant):
                value = node.args[0].value
                if (isinstance(node.func, ast.Attribute) and node.func.attr == 'import_module'
                        or isinstance(node.func, ast.Name) and node.func.id == '__import__'):
                    if type(value) is str:
                        edges.add(value.split('.')[0])
            elif isinstance(node, ast.Constant) and type(node.value) is str:
                if node.value in changed:
                    refs.add(node.value)
            if isinstance(node, ast.Call):
                function = node.func.attr if isinstance(node.func, ast.Attribute) else node.func.id if isinstance(node.func, ast.Name) else ''
                if function in ('import_module', '__import__', 'spec_from_file_location', 'SourceFileLoader', 'exec', 'eval', 'exec_module', 'load_module', 'run_module', 'run_path'):
                    dynamic.append({'function': function, 'line': node.lineno,
                                    'literal_first_argument': node.args[0].value if node.args and isinstance(node.args[0], ast.Constant) and type(node.args[0].value) is str else None})
        cache[name] = {'imports': sorted(edges.intersection(modules)), 'literal_files': sorted(refs), 'dynamic_calls': dynamic}
        return cache[name]

    def closure(name):
        pending = deque([(name, [name])])
        seen = set()
        affected = {}
        literal = {}
        failures = []
        dynamic = []
        while pending:
            current, chain = pending.popleft()
            if current in seen:
                continue
            seen.add(current)
            row = imports(current)
            if row.get('error'):
                failures.append({'module': current, 'error': row['error']})
            if current + '.py' in changed:
                affected[current + '.py'] = chain
            for target in row['literal_files']:
                literal.setdefault(target, chain)
            dynamic.extend({'module': current, 'chain': chain, **item} for item in row.get('dynamic_calls', []))
            pending.extend((edge, chain + [edge]) for edge in row['imports'] if edge not in seen)
        return {'changed_import_chains': affected, 'literal_changed_source_references': literal,
                'modules_examined': len(seen), 'unresolved': failures, 'dynamic_loader_calls': dynamic}

    flag_names = {'--config', '--study', '--news-io-config', '--model-study', '--archive-root',
                  '--heartbeat', '--registry', '-OperationalProfilePath', '-OperationalProfile',
                  '-ProfilePath', '-Root', '-ProjectRoot', '-ConfigPath'}
    processes = []
    for p in psutil.process_iter(['pid', 'ppid', 'name', 'create_time', 'cmdline']):
        argv = p.info.get('cmdline') or []
        scripts = []
        for arg in argv:
            candidate = Path(arg)
            if candidate.suffix.lower() in ('.py', '.ps1') and candidate.parent == ROOT:
                scripts.append(candidate.name)
        if not scripts:
            continue
        selected = [{'flag': arg, 'value': argv[i + 1]} for i, arg in enumerate(argv[:-1]) if arg in flag_names]
        rows = {name: closure(Path(name).stem) for name in scripts if Path(name).stem in modules}
        processes.append({'pid': p.info['pid'], 'parent_pid': p.info['ppid'],
                          'started_utc': dt.datetime.fromtimestamp(p.info['create_time'], dt.timezone.utc).isoformat(),
                          'image': p.info['name'], 'scripts': scripts, 'selected_path_arguments': selected,
                          'static_import_analysis': rows})
    profile = json.loads(PROFILE.read_bytes())
    services = []
    for item in profile['services']:
        arguments = item['arguments']
        services.append({'name': item['name'], 'script': item['script'],
                         'selected_paths': [{'flag': value, 'value': arguments[i + 1]}
                                            for i, value in enumerate(arguments[:-1]) if value in flag_names],
                         'heartbeat': item['heartbeat'], 'source_sha256': item['source_sha256']})
    result = {'observed_utc': dt.datetime.now(dt.timezone.utc).isoformat(),
              'scope': 'read-only process listing and bounded static local import/reference paths; not proof of another process sys.modules',
              'representative_stage': str(STAGE), 'source_stage_created_by_this_inventory': False,
              'deployment_acceptance_pending': True, 'changed_files': changed,
              'current_profile': str(PROFILE), 'current_profile_sha256': sha(PROFILE),
              'services': services, 'processes': processes, 'source_files_examined': len(cache),
              'elapsed_seconds': time.monotonic() - started, 'live_changes': False,
              'broker_calls': 0, 'raw_command_lines_or_credentials_retained': False}
    destination = AREA / 'native_compact_handover_owner_inventory_stage007_20260914.json'
    destination.write_text(json.dumps(result, indent=2, sort_keys=True))
    print(json.dumps({'receipt': str(destination), 'changed_file_count': len(changed),
                      'process_count': len(processes), 'source_files_examined': len(cache),
                      'elapsed_seconds': result['elapsed_seconds']}))


if __name__ == '__main__':
    run()
