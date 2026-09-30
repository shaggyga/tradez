"""Capacity-profile reader qualification; no production stores are written."""
import ast
import json
from pathlib import Path
import pytest
import oanda_all68_technical_availability_v1 as old
import oanda_all68_technical_availability_v2 as new
from test_oanda_all68_technical_availability_v1 import fixture_inputs


def test_all_reader_functions_preserve_original_validations():
    def functions(module):
        return {n.name: ast.dump(n) for n in ast.parse(Path(module.__file__).read_text()).body
                if isinstance(n, (ast.FunctionDef, ast.ClassDef))}
    assert functions(new) == functions(old)
    assert new.IMPORTED_SOURCE_BINDINGS == old.IMPORTED_SOURCE_BINDINGS
    assert new.DEFAULT_OUTPUT != old.DEFAULT_OUTPUT


def test_exact_capacity_config_and_original_numerical_sources_validate():
    config, reference = new.read_config(new.DEFAULT_CONFIG)
    operations, sources = new.verify_dependencies(config, reference)
    assert operations['observation_config_sha256'] == reference['sha256'] == new.ORIGINAL_CONFIG_SHA256
    assert sources['operations_config']['sha256'] == new.OPERATIONS_CONFIG_SHA256
    original = json.loads(old.DEFAULT_CONFIG.read_bytes())
    differences = {k for k in set(config)|set(original) if config.get(k) != original.get(k)}
    assert differences == {'maximum_dataset_bytes'}
    assert config['maximum_dataset_bytes'] > original['maximum_dataset_bytes']


def test_old_or_altered_runtime_cannot_be_relabeled(tmp_path):
    config, ref = new.read_config(new.DEFAULT_CONFIG)
    with pytest.raises(ValueError, match='operations_config_binding_changed'):
        new.verify_dependencies(config, ref, old.DEFAULT_OPERATIONS_CONFIG)
    p = tmp_path/'operations.json'; p.write_bytes(new.DEFAULT_OPERATIONS_CONFIG.read_bytes()+b' ')
    with pytest.raises(ValueError, match='operations_config_binding_changed'):
        new.verify_dependencies(config, ref, p)


def test_all68_support_and_unknowns_match_predecessor():
    parts = fixture_inputs()
    config, envelope, status, quotes, heartbeat, retained, now = parts
    import inspect
    # Both functions are byte-for-byte AST equivalent; exercise the same actual consumer.
    assert inspect.signature(new.build_report) == inspect.signature(old.build_report)
    a = old.build_report(config, envelope, status, quotes, heartbeat, retained, {}, now=now)
    b = new.build_report(config, envelope, status, quotes, heartbeat, retained, {}, now=now)
    a['schema_version'] = b['schema_version']
    assert a == b
