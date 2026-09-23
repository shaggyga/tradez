from __future__ import annotations

import copy
import ast
import hashlib
import json
from bisect import bisect_left, bisect_right
from pathlib import Path

import pytest

from tools import build_forex_feature_dictionary as subject


def example(root=None):
    raw = b"# reviewed source\nvalue = 1\n"
    if root:
        (root / "source.py").write_bytes(raw)
    bindings = {"source.py": hashlib.sha256(raw).hexdigest()}

    def feature(index, block="price"):
        return {
            "feature_id": f"feature_{index}", "name": f"feature_{index}", "block": block,
            "plain_language": f"Specific meaning {index}.", "role": "context",
            "intended_inputs": ["completed prices"], "intended_units": "pips",
            "intended_lookback": "5 completed minutes", "computation": "Design intent only; not verified code.",
            "intended_definition": "Intended signed price difference divided by pip size.",
            "implementation_status": "design_intent_only", "causal_warning": "No future values.",
            "source_refs": [{"path": "source.py", "line": 2, "evidence_kind": "design_reference"}],
            "gaps": ["Actual implementation not established."],
        }

    catalog = [feature(0), feature(1)]
    active = [feature(index, "joint") for index in range(34)]
    engines = [{"id": f"engine_{i}", "name": f"Historical schema {i}", "feature_count": 1,
                "horizons": [5, 60], "generation_recipe": "Observed source recipe, version-specific.",
                "availability": "historical only", "validation_limits": ["not active"],
                "source_refs": [{"path": "source.py", "line": 1, "evidence_kind": "historical_audit"}],
                "gaps": ["Weights not part of source snapshot."],
                "features": [{"id": "raw_price", "name": "Raw price", "meaning": "Observed source price",
                              "inputs": ["close"], "units": "quote currency", "lookback": "current completed bar",
                              "generation_recipe": "close", "availability": "historical", "validation_limits": ["no edge claim"],
                              "source_refs": [{"path": "source.py", "line": 1, "evidence_kind": "historical_audit"}],
                              "gaps": []}]} for i in range(6)]
    parts = {"catalog": {"features": catalog, "source_hashes": bindings},
             "active_joint": {"features": active, "source_hashes": bindings},
             "historical": {"engines": engines, "source_hashes": bindings}}
    spec = {"feature_blocks": {"price": {"features": ["feature_0", "feature_1"]}}}
    return parts, spec


def test_valid_complete_shape(tmp_path):
    parts, spec = example(tmp_path)
    report = subject.validate_parts(parts, spec, tmp_path)
    assert report["design_entries"] == 2
    assert report["current_joint_entries"] == 34
    assert report["inspected_source_bindings_checked"] == 1


@pytest.mark.parametrize("field", subject.REQUIRED)
def test_missing_definition_field_is_rejected(field):
    parts, spec = example()
    del parts["catalog"]["features"][0][field]
    with pytest.raises(ValueError):
        subject.validate_parts(parts, spec)


def test_duplicate_does_not_inflate_coverage():
    parts, spec = example()
    parts["catalog"]["features"][1] = copy.deepcopy(parts["catalog"]["features"][0])
    with pytest.raises(ValueError, match="membership|duplicate"):
        subject.validate_parts(parts, spec)


def test_missing_current_input_is_rejected():
    parts, spec = example()
    parts["active_joint"]["features"].pop()
    with pytest.raises(ValueError, match="34"):
        subject.validate_parts(parts, spec)


def test_design_catalog_cannot_be_silently_renamed():
    parts, spec = example()
    parts["catalog"]["features"][0]["name"] = "renamed"
    with pytest.raises(ValueError, match="membership"):
        subject.validate_parts(parts, spec)


@pytest.mark.parametrize("line", [0, -1, "2", 1000])
def test_bad_source_line_is_rejected(tmp_path, line):
    parts, spec = example(tmp_path)
    parts["catalog"]["features"][0]["source_refs"][0]["line"] = line
    with pytest.raises(ValueError, match="source line"):
        subject.validate_parts(parts, spec, tmp_path)


def test_source_drift_requires_definition_review(tmp_path):
    parts, spec = example(tmp_path)
    (tmp_path / "source.py").write_text("value = 2\n", encoding="utf-8")
    with pytest.raises(ValueError, match="inspected source changed"):
        subject.validate_parts(parts, spec, tmp_path)


def test_runtime_data_is_not_read_as_documentation_source(tmp_path):
    parts, spec = example(tmp_path)
    parts["catalog"]["source_hashes"] = {"data/private.sqlite": "0" * 64}
    with pytest.raises(ValueError, match="runtime/bulk"):
        subject.validate_parts(parts, spec, tmp_path)


def test_reference_cannot_escape_root(tmp_path):
    parts, spec = example(tmp_path)
    parts["catalog"]["features"][0]["source_refs"][0]["path"] = "../outside.py"
    with pytest.raises(ValueError, match="escapes"):
        subject.validate_parts(parts, spec, tmp_path)


def test_renderer_keeps_design_and_evidence_separate():
    parts, spec = example()
    coverage = subject.validate_parts(parts, spec)
    rendered = subject.render({"parts": parts, "coverage": coverage})
    assert "design_intent_only" in rendered
    assert "Design interpretation (not implementation proof)" in rendered
    assert "Specific meaning 0." in rendered
    assert "Actual implementation not established." in rendered
    assert "not be added together" in rendered
    assert "| `raw_price` | Observed source price<br>close |" in rendered


def test_historical_counts_and_duplicate_fields_are_checked():
    parts, spec = example()
    parts["historical"]["engines"][0]["features"] *= 2
    with pytest.raises(ValueError, match="inventory count mismatch|duplicate feature"):
        subject.validate_parts(parts, spec)


def test_historical_math_description_cannot_be_omitted():
    parts, spec = example()
    del parts["historical"]["engines"][0]["features"][0]["generation_recipe"]
    with pytest.raises(ValueError, match="missing generation_recipe"):
        subject.validate_parts(parts, spec)


def test_deterministic_roundtrip(tmp_path):
    parts, spec = example(tmp_path)
    for key, relative in subject.PARTS.items():
        target = tmp_path / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(subject.encode(parts[key]))
    target = tmp_path / "config/model_feature_space.json"
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_bytes(subject.encode(spec))
    first = subject.assemble(tmp_path)
    second = subject.assemble(tmp_path)
    assert first == second
    assert json.loads(first[0])["coverage"]["current_joint_entries"] == 34


def test_historical_schemas_remain_separate():
    parts, spec = example()
    parts["historical"]["engines"][1]["id"] = parts["historical"]["engines"][0]["id"]
    with pytest.raises(ValueError, match="six engine"):
        subject.validate_parts(parts, spec)


def actual_pure_feature_functions():
    """AST-isolate numerical functions; never import a worker or fit a model."""
    import numpy as np
    root = Path(__file__).resolve().parent
    price_tree = ast.parse((root / "oanda_pair_local_models_v2.py").read_text(encoding="utf-8"))
    parameters = next(node for node in price_tree.body if isinstance(node, ast.Assign)
                      and any(isinstance(target, ast.Name) and target.id == "PARAMETERS" for target in node.targets))
    parameter_values = ast.literal_eval(parameters.value.args[0])
    namespace = {"np": np, "bisect_left": bisect_left, "bisect_right": bisect_right,
                 "PARAMETERS": parameter_values}
    selected = [node for node in price_tree.body if isinstance(node, ast.FunctionDef)
                and node.name in {"_sessions", "_window", "_features"}]
    exec(compile(ast.Module(body=selected, type_ignores=[]), "isolated_price_features", "exec"), namespace)
    joint_tree = ast.parse((root / "oanda_joint_price_news_models_v1.py").read_text(encoding="utf-8"))
    joint = [node for node in joint_tree.body if isinstance(node, ast.FunctionDef) and node.name == "_joint_features"]
    exec(compile(ast.Module(body=joint, type_ignores=[]), "isolated_joint_features", "exec"), namespace)
    return namespace


def test_dense_synthetic_price_feature_units_and_windows():
    functions = actual_pure_feature_functions()
    rows = {minute * 60: 1.1 + minute * .0001 for minute in range(61)}
    epochs = list(rows)
    features = functions["_features"](rows, epochs, functions["_sessions"](epochs), epochs[-1], .0001)
    assert len(features) == 24
    for index in range(0, 20, 4):
        assert features[index:index + 4] == pytest.approx([1., 1., 0., 0.])
    assert features[20:] == pytest.approx([1., 1., 1., 1.])


def test_sparse_prices_keep_unavailability_separate_from_zero_rate():
    functions = actual_pure_feature_functions()
    rows = {minute * 60: 1.1 + minute * .0001 for minute in range(0, 61, 2)}
    epochs = list(rows)
    features = functions["_features"](rows, epochs, functions["_sessions"](epochs), epochs[-1], .0001)
    assert features[:4] == pytest.approx([0., 0., 1., 1.])
    assert features[4:8] == pytest.approx([1., .8, .6, 0.])
    assert features[21] == 2.
    assert features[22] == pytest.approx(31 / 61)


def test_current_joint_34_order_and_interactions():
    import numpy as np
    functions = actual_pure_feature_functions()
    technical = np.zeros(24)
    technical[0] = 2.
    news = np.asarray([.6, .7, .2, .1, -.3, .2, 0., .5])
    values = functions["_joint_features"](technical, news)
    assert len(values) == 34
    assert values[24:32] == pytest.approx(news)
    assert values[32:] == pytest.approx([1.2, -.6])


def test_current_joint_dictionary_vector_identity():
    target = Path(__file__).resolve().parent / subject.PARTS["active_joint"]
    if not target.exists():
        pytest.skip("authored dictionary not yet delivered")
    rows = json.loads(target.read_text(encoding="utf-8"))["features"]
    assert [row["vector_index"] for row in rows] == list(range(34))
    functions = actual_pure_feature_functions()
    expected = [f"{prefix}_{window}m" for window in functions["PARAMETERS"]["feature_windows_minutes"]
                for prefix in ("price_rate", "price_span_fraction", "price_missing_fraction", "price_unavailable_flag")]
    expected += ["price_rms_elapsed_change", "price_max_gap_minutes", "price_observation_fraction", "price_session_age_hours"]
    tree = ast.parse((target.parents[2] / "oanda_joint_price_news_models_v1.py").read_text(encoding="utf-8"))
    assignment = next(node for node in tree.body if isinstance(node, ast.Assign)
                      and any(isinstance(name, ast.Name) and name.id == "NEWS_FEATURES" for name in node.targets))
    expected += list(ast.literal_eval(assignment.value))
    expected += ["price_rate_x_context_balance", "price_rate_x_vetted_balance"]
    assert [row["feature_id"] for row in rows] == expected


def test_shared_and_historical_references_cannot_omit_hash_binding():
    parts, spec = example()
    parts["active_joint"]["shared_definitions"] = {
        "source_refs": [{"path": "unbound.py", "line": 1, "evidence_kind": "implementation"}]}
    with pytest.raises(ValueError, match="lacks inspected-source binding"):
        subject.validate_parts(parts, spec)
    del parts["active_joint"]["shared_definitions"]
    parts["historical"]["engines"][0]["source_refs"][0]["path"] = "unbound.py"
    with pytest.raises(ValueError, match="lacks inspected-source binding"):
        subject.validate_parts(parts, spec)
