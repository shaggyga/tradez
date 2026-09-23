"""Prepare a reviewed runtime handover beside a frozen kit; never start services."""
import argparse
import ast
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path

AREA = Path(__file__).resolve().parent
ROOT = AREA.parent / 'trad'


def sha(raw):
    return hashlib.sha256(raw).hexdigest()


def encoded(value):
    return json.dumps(value, sort_keys=True, indent=2, allow_nan=False).encode()


def prepare(stage, previous_profile=None, new_profile_name='operational_runtime_v5_20260914.json', output_name='runtime_handover'):
    stage = stage.resolve()
    if stage.parent != AREA.resolve():
        raise ValueError('owned_source_stage_required')
    source = json.loads((stage / 'SOURCE_STAGE.json').read_bytes())
    registration = json.loads((stage / 'PROSPECTIVE_REGISTRATION.json').read_bytes())
    if any(sha((stage / 'kit' / name).read_bytes()) != expected
           for name, expected in source['source_bindings'].items()):
        raise ValueError('staged_source_changed')
    if Path(output_name).name != output_name or output_name in ('.','..'):
        raise ValueError('owned_runtime_output_name_required')
    folder = stage / output_name
    folder.mkdir()
    original = Path(previous_profile).resolve() if previous_profile else ROOT / 'config/operational_runtime_v3_20260913.json'
    if original.parent != (ROOT / 'config').resolve() or Path(new_profile_name).name != new_profile_name:
        raise ValueError('owned_previous_and_new_profile_required')
    profile = json.loads(original.read_bytes())
    replacements = {'operational_repair_20260913_v3': 'operational_repair_20260913_v4',
                    '_operational_v3_20260913.json': '_operational_v4_20260913.json'}
    profile['prepared_utc'] = datetime.now(timezone.utc).isoformat()
    profile['notes'] = (
        'Native-news V4 uses a fresh compact publisher/consumer/archive and the unchanged numerical V7 model. '
        'Prior native generations and their failed evidence remain retained; no historical consumption is imported. '
        'Bounded non-authorizing bootstrap precedes a separately verified current capture. '
        'The feature observer retains its V7 publication budget/source schema and existing price study, '
        'while its native model-study argument selects V4. The unchanged eight-source forward ledger continues '
        'original pending targets and reads the current feature archive. M1 collection and retained price '
        'settlement repairs remain selected. Execution belongs to a separately sealed finite practice trial; '
        'this profile cannot place orders or establish profitable model acceptance.'
    )
    for service in profile['services']:
        service['arguments'] = [replace(arg, replacements) for arg in service['arguments']]
        service['heartbeat'] = replace(service['heartbeat'], replacements)
        name = service['script']
        if name in source['source_bindings']:
            service['source_sha256'] = source['source_bindings'][name]
        if service['name'] == 'revision_news_transport_v4':
            ix = service['arguments'].index('--config-sha256') + 1
            service['arguments'][ix] = registration['configurations']['transport']['sha256']
            declarations = ast.parse((stage / 'kit/revision_transport_v4.py').read_bytes()).body
            statuses = [ast.literal_eval(node.value) for node in declarations if isinstance(node, ast.Assign)
                        and any(isinstance(t, ast.Name) and t.id == 'STATUS' for t in node.targets)]
            if len(statuses) != 1:
                raise ValueError('exact_transport_heartbeat_schema_required')
            service['heartbeat_schema'] = statuses[0]
    if len(profile['services']) != 10 or profile['can_place_orders'] is not False:
        raise ValueError('research_profile_scope_changed')
    # Research data-generation changes must not reinstall an earlier supervisor
    # or undo independent operational repairs already installed on this host.
    files = {'config/' + new_profile_name: encoded(profile)}
    unchanged_runtime_sources = {name: sha((ROOT / name).read_bytes()) for name in
        ('oanda_always_on_supervisor.ps1','oanda_operational_recovery_contract.ps1',
         'oanda_retained_price_settlement_v1.py','oanda_all68_m1_cadence_v2.py')}
    for relative, raw in files.items():
        target = folder / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        with target.open('xb') as stream:
            stream.write(raw)
    receipt = dict(status='prepared_not_installed',
                   original_profile_path=str(original),
                   original_profile_sha256=sha(original.read_bytes()),
                   files={name: sha(raw) for name, raw in files.items()},
                   unchanged_runtime_sources=unchanged_runtime_sources,
                   research_only=True, can_place_orders=False,
                   other_processes_preserved=True)
    (folder / 'RUNTIME_HANDOVER.json').write_bytes(encoded(receipt))
    return receipt


def replace(value, replacements):
    for old, new in replacements.items():
        value = value.replace(old, new)
    return value


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--source-stage', type=Path, required=True)
    parser.add_argument('--previous-profile', type=Path)
    parser.add_argument('--new-profile-name', default='operational_runtime_v5_20260914.json')
    parser.add_argument('--output-name', default='runtime_handover')
    args = parser.parse_args()
    print(json.dumps(prepare(args.source_stage, args.previous_profile, args.new_profile_name, args.output_name)))
