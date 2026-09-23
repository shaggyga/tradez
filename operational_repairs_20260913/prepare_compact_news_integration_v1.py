"""Copy original integration fixtures and bind their inventory to an isolated kit."""
import argparse
import hashlib
import json
from pathlib import Path

AREA = Path(__file__).resolve().parent


def prepare(stage):
    stage = stage.resolve()
    if stage.parent != AREA.resolve():
        raise ValueError('owned_source_stage_required')
    target = stage / 'kit'
    fixtures = AREA / 'compact_native_stage_002/kit'
    inputs = {name: fixtures / name for name in (
        'native_fixture_contract_v1.py', 'test_core_operational_v2.py', 'test_full_operational_v2.py')}
    inputs['test_async_bootstrap.py'] = AREA / 'worker_bootstrap_validation_v1/test_async_bootstrap.py'
    inputs['test_news_completion_handoff.py'] = (
        AREA / 'worker_news_handoff_validation_v2/kit/test_news_completion_handoff.py')
    copied = {}
    for name, source in inputs.items():
        raw = source.read_bytes()
        with (target / name).open('xb') as stream:
            stream.write(raw)
        if source.read_bytes() != raw or (target / name).read_bytes() != raw:
            raise ValueError('integration_fixture_changed_during_copy')
        copied[name] = {'source': str(source), 'sha256': hashlib.sha256(raw).hexdigest()}
    report = json.loads((stage / 'SOURCE_STAGE.json').read_bytes())
    inventory = {'scope': 'Synthetic integration fixture rebound to isolated successor; no activation or live authority',
                 'files': [{'name': name, 'sha256': digest} for name, digest in sorted(report['source_bindings'].items())]}
    (target / 'SOURCE_KIT_INVENTORY_003.json').write_text(json.dumps(inventory, sort_keys=True), encoding='utf-8')
    (stage / 'INTEGRATION_FIXTURES.json').write_text(json.dumps({
        'scope': 'Original integration fixtures plus four bootstrap and seven handoff tests; not a test execution result',
        'source_stage_sha256': hashlib.sha256((stage / 'SOURCE_STAGE.json').read_bytes()).hexdigest(),
        'fixtures': copied,
    }, indent=2, sort_keys=True), encoding='utf-8')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--source-stage', type=Path, required=True)
    prepare(parser.parse_args().source_stage)
