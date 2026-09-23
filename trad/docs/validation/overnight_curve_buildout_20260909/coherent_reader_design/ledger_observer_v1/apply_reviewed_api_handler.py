"""Apply only reviewed handler blocks while preserving untouched file bytes."""
import ast
import hashlib
import json
import os
from pathlib import Path
import time

BASE = Path(__file__).resolve().parent
TARGET = BASE.parents[2] / 'trad/oanda_practice_live_dashboard.py'
BEFORE = 'cc0f6698a30c59cb291dada08d73283f0484996e6873b8ce27894e0168614f6f'
CANDIDATE = BASE / 'api_runtime_preparation_001/dashboard_handler_pinned.py.txt'
CANDIDATE_SHA = '42c8ccb40cb7ccc9fc9eb88caa095b1636a4b2f4025f3257696c84453838be1f'


def sha(value):
    return hashlib.sha256(value).hexdigest()


def main():
    started = time.time()
    original = TARGET.read_bytes()
    candidate = CANDIDATE.read_bytes()
    assert sha(original) == BEFORE and sha(candidate) == CANDIDATE_SHA
    marker = b'def select_primary_pair_forecasts('
    start = candidate.index(b'\n# Parent fills this reviewed immutable configuration hash at deployment.')
    end = candidate.index(marker)
    addition = candidate[start:end]
    assert original.count(marker) == 1
    result = original.replace(marker, addition + marker, 1)
    handler_start = b'        if parsed.path == "/api/main":'
    handler_end = b'        if parsed.path == "/api/full-state":'
    old_start, old_end = result.index(handler_start), result.index(handler_end)
    new_start = candidate.index(b'        if parsed.path == "/api/joint-v3-ledger-observation":')
    new_end = candidate.index(handler_end)
    handler = candidate[new_start:new_end].replace(b'\n', b'\r\n')
    result = result[:old_start] + handler + result[old_end:]
    assert result.replace(b'\r\n', b'\n') == candidate
    compile(result, str(TARGET), 'exec')
    before_tree, after_tree = ast.parse(original), ast.parse(result)
    preserved = {}
    for name in ('summarize_joint_price_news_forecasts', '_summarize_registered_family_forecasts',
                 'project_joint_collection_status', 'build_main_state'):
        old = next(n for n in before_tree.body if isinstance(n, ast.FunctionDef) and n.name == name)
        new = next(n for n in after_tree.body if isinstance(n, ast.FunctionDef) and n.name == name)
        assert ast.dump(old) == ast.dump(new)
        preserved[name] = sha(ast.dump(old).encode())
    output = BASE / 'handler_application_001'
    output.mkdir(exist_ok=False)
    (output / 'dashboard_before.py.txt').write_bytes(original)
    (output / 'dashboard_applied.py.txt').write_bytes(result)
    assert TARGET.read_bytes() == original
    temporary = TARGET.with_name(TARGET.name + '.reviewed-handler.tmp')
    with temporary.open('xb') as stream:
        stream.write(result)
        stream.flush()
        os.fsync(stream.fileno())
    assert TARGET.read_bytes() == original
    os.replace(temporary, TARGET)
    assert TARGET.read_bytes() == result
    receipt = dict(schema_version='reviewed_ledger_handler_application_v1_20260909',
        started_epoch=started, completed_epoch=time.time(), before_sha256=sha(original),
        reviewed_normalized_candidate_sha256=sha(candidate), applied_sha256=sha(result),
        normalized_candidate_exact_match=True, preserved_function_ast_sha256=preserved,
        untouched_line_endings_preserved=True, server_reloaded=False,
        model_sources_changed=False, orders_enabled=False)
    raw = (json.dumps(receipt, indent=2, sort_keys=True)+'\n').encode()
    (output / 'HANDLER_APPLICATION_20260909.json').write_bytes(raw)
    print(json.dumps({'applied_sha256':sha(result), 'receipt_sha256':sha(raw), 'server_reloaded':False}))


if __name__ == '__main__':
    main()
