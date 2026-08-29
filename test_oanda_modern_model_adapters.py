from __future__ import annotations

import json

import numpy as np
import pytest

import oanda_gpt_training_strategy_manager as manager
import oanda_model_space_agenda as agenda
import oanda_modern_model_dependency_probe as dependency_probe


def _engine() -> manager.ContinuousResearchEngine:
    return object.__new__(manager.ContinuousResearchEngine)


@pytest.mark.parametrize("model_type", ["catboost", "ngboost"])
def test_modern_tabular_model_types_normalize(model_type: str) -> None:
    raw = {
        "dataset_kind": "base_trade_quality",
        "model_type": model_type,
        "target": "would_profit_30m",
        "outcome": "net_vol_units_30",
        "feature_set": "base",
        "parameters": {},
    }

    normalized = manager.normalize_gpt_experiment_spec(raw)

    assert normalized is not None
    assert normalized["model_type"] == model_type


def test_catboost_adapter_preserves_identity_and_safe_runtime_options(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class FakeCatBoostClassifier:
        def __init__(self, **kwargs: object) -> None:
            self.kwargs = kwargs

    monkeypatch.setattr(manager, "CATBOOST_AVAILABLE", True)
    monkeypatch.setattr(manager, "CatBoostClassifier", FakeCatBoostClassifier)
    model = _engine().estimator({
        "model_type": "catboost",
        "parameters": {
            "iterations": 240,
            "depth": 6,
            "l2_leaf_reg": 3.0,
            "unsupported": "discarded",
        },
    })

    assert isinstance(model, FakeCatBoostClassifier)
    assert model.kwargs["iterations"] == 240
    assert model.kwargs["allow_writing_files"] is False
    assert model.kwargs["loss_function"] == "Logloss"
    assert "unsupported" not in model.kwargs


def test_ngboost_adapter_preserves_identity(monkeypatch: pytest.MonkeyPatch) -> None:
    class FakeNGBClassifier:
        def __init__(self, **kwargs: object) -> None:
            self.kwargs = kwargs

    monkeypatch.setattr(manager, "NGBOOST_AVAILABLE", True)
    monkeypatch.setattr(manager, "NGBClassifier", FakeNGBClassifier)
    model = _engine().estimator({
        "model_type": "ngboost",
        "parameters": {
            "n_estimators": 320,
            "minibatch_frac": 0.65,
            "col_sample": 0.8,
            "unsupported": "discarded",
        },
    })

    assert isinstance(model, FakeNGBClassifier)
    assert model.kwargs["n_estimators"] == 320
    assert model.kwargs["minibatch_frac"] == 0.65
    assert model.kwargs["verbose"] is False
    assert "unsupported" not in model.kwargs


@pytest.mark.parametrize(
    ("model_type", "flag_name", "class_name", "message"),
    [
        ("catboost", "CATBOOST_AVAILABLE", "CatBoostClassifier", "CatBoost"),
        ("ngboost", "NGBOOST_AVAILABLE", "NGBClassifier", "NGBoost"),
    ],
)
def test_missing_optional_estimator_fails_clearly(
    monkeypatch: pytest.MonkeyPatch,
    model_type: str,
    flag_name: str,
    class_name: str,
    message: str,
) -> None:
    monkeypatch.setattr(manager, flag_name, False)
    monkeypatch.setattr(manager, class_name, None)

    with pytest.raises(RuntimeError, match=message):
        _engine().estimator({"model_type": model_type, "parameters": {}})


def test_model_space_agenda_has_bounded_modern_tabular_sweeps() -> None:
    catboost = agenda.parameter_sets("catboost")
    ngboost = agenda.parameter_sets("ngboost")

    assert len(catboost) == 2
    assert len(ngboost) == 2
    assert all("iterations" in params for params in catboost)
    assert all("minibatch_frac" in params for params in ngboost)


def test_dependency_probe_writes_auditable_inventory(tmp_path) -> None:
    output = tmp_path / "dependencies.json"

    report = dependency_probe.build_report()
    dependency_probe.write_report(output, report)
    loaded = json.loads(output.read_text(encoding="utf-8"))

    assert loaded["schema_version"] == 1
    assert loaded["execution_policy"] == "shadow_only"
    assert "catboost" in loaded["packages"]
    assert "torch" in loaded["packages"]
    assert loaded["packages"]["catboost"]["required_for"] == [
        "catboost_tabular"
    ]


@pytest.mark.parametrize(
    ("model_type", "parameters", "available"),
    [
        (
            "catboost",
            {"iterations": 8, "depth": 2, "learning_rate": 0.1},
            manager.CATBOOST_AVAILABLE,
        ),
        (
            "ngboost",
            {
                "n_estimators": 8,
                "learning_rate": 0.05,
                "minibatch_frac": 1.0,
                "col_sample": 1.0,
            },
            manager.NGBOOST_AVAILABLE,
        ),
    ],
)
def test_installed_modern_adapter_fits_and_predicts_probabilities(
    model_type: str,
    parameters: dict[str, object],
    available: bool,
) -> None:
    if not available:
        pytest.skip(f"{model_type} is not installed")
    features = np.asarray(
        [
            [-1.2, -0.8, 0.1],
            [-1.0, -0.5, 0.2],
            [-0.8, -0.4, 0.3],
            [-0.6, -0.2, 0.4],
            [-0.4, -0.1, 0.5],
            [-0.2, 0.0, 0.6],
            [0.2, 0.1, 0.4],
            [0.4, 0.2, 0.5],
            [0.6, 0.4, 0.6],
            [0.8, 0.5, 0.7],
            [1.0, 0.7, 0.8],
            [1.2, 0.9, 0.9],
        ],
        dtype=float,
    )
    target = np.asarray([0, 0, 0, 0, 0, 0, 1, 1, 1, 1, 1, 1], dtype=int)

    model = _engine().estimator(
        {"model_type": model_type, "parameters": parameters}
    )
    model.fit(features, target)
    probability = np.asarray(model.predict_proba(features), dtype=float)

    assert probability.shape == (len(features), 2)
    assert np.isfinite(probability).all()
    assert np.allclose(probability.sum(axis=1), 1.0, atol=1e-6)
