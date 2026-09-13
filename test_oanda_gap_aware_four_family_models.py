"""Offline synthetic timestamp/gap tests; these do not establish trading edge."""
import ast
from copy import deepcopy
from datetime import datetime, timezone
import math
from pathlib import Path

import numpy as np
import pytest

import oanda_gap_aware_four_family_models as model


FRIDAY = int(datetime(2026, 9, 4, 10, tzinfo=timezone.utc).timestamp())
SUNDAY = int(datetime(2026, 9, 6, 21, tzinfo=timezone.utc).timestamp())


def make_rows(old_n=512, new_n=61, reopen=SUNDAY):
    rows = {}
    for pair_index, pair in enumerate(model.PAIRS):
        pip = .01 if pair.endswith("_JPY") else .0001
        base = 145. if pair.endswith("_JPY") else 1.1 + .1 * pair_index
        rows[pair] = {
            FRIDAY + i * 60: base + pip * (5 * math.sin(i / 8. + pair_index) + .01 * i)
            for i in range(old_n)}
        rows[pair].update({
            reopen + i * 60: base + pip * (30 + 2 * math.sin(i / 5. + pair_index) - .02 * i)
            for i in range(new_n)})
    return rows, reopen + (new_n - 1) * 60


def continuous_rows(n=512):
    rows, _ = make_rows(old_n=n, new_n=0)
    return rows, FRIDAY + (n - 1) * 60


def legacy_pure_functions():
    # Load just the old pure numerical definitions as a regression oracle.
    # Never import the legacy worker or execute its module-level I/O/imports.
    names = {"feature_vector", "supervised_rows", "ridge_fit_predict", "normal_probability_up",
        "ridge_predictions", "pooled_tabular_predictions", "currency_factor_matrix",
        "graph_predictions", "state_space_predictions"}
    path = Path(__file__).with_name("oanda_proof_shadow_predictors.py")
    source = ast.parse(path.read_text(encoding="utf-8"))
    selected = [node for node in source.body if isinstance(node, ast.FunctionDef) and node.name in names]
    assert {node.name for node in selected} == names
    tree = ast.Module(body=[ast.ImportFrom(module="__future__", names=[ast.alias(name="annotations")], level=0), *selected], type_ignores=[])
    namespace = {"np": np, "math": math, "FEATURE_WINDOWS": model.FEATURE_WINDOWS,
                 "HORIZON_STEPS": 60, "split_pair": lambda pair: tuple(pair.split("_"))}
    exec(compile(ast.fix_missing_locations(tree), "<legacy_pure_oracle>", "exec"), namespace)
    return namespace


def test_all_four_can_use_friday_training_after_only_sixty_new_intervals():
    rows, cutoff = make_rows()
    output = model.predict_all(rows, cutoff)
    assert set(output) == set(model.FAMILIES)
    for expected, probability, diagnostics in output.values():
        assert math.isfinite(expected) and 0 <= probability <= 1
        assert diagnostics["feature_cutoff_epoch"] == cutoff + 60
        assert diagnostics["research_only"] is True
        assert diagnostics["can_place_orders"] is False and diagnostics["proof_eligible"] is False
        if diagnostics["training_label_maturity_max_epoch"] is not None:
            assert diagnostics["training_label_maturity_max_epoch"] < SUNDAY
    pooled = output[model.FAMILIES[1]][2]
    assert pooled["pooled_training_rows"] >= 300
    assert all(len(times) >= 24 for times in pooled["training_row_start_epochs_by_pair"].values())
    assert output[model.FAMILIES[3]][2]["observations"] == 60


def test_contiguous_numerical_predictions_match_original_models():
    rows, cutoff = continuous_rows()
    old = legacy_pure_functions()
    prices = {pair: np.asarray(list(values.values())) for pair, values in rows.items()}
    pips = {pair: .01 if pair.endswith("_JPY") else .0001 for pair in model.PAIRS}
    actual = model.predict_all(rows, cutoff)
    for family, function in zip(model.FAMILIES, ("ridge_predictions", "pooled_tabular_predictions",
                                                "graph_predictions", "state_space_predictions")):
        expected = old[function](prices, pips)["EUR_USD"]
        assert actual[family][0] == pytest.approx(expected[0], rel=1e-9, abs=1e-9)
        assert actual[family][1] == pytest.approx(expected[1], rel=1e-9, abs=1e-9)


def test_training_samples_equal_independent_timestamp_oracle_with_interior_holes():
    rows, cutoff = make_rows(old_n=700, new_n=70)
    for pair_index, pair in enumerate(model.PAIRS):
        for i in (170 + pair_index, 371, 511 - pair_index):
            rows[pair].pop(FRIDAY + i * 60)
    data = model._Data(rows, cutoff)
    for pair in model.PAIRS:
        for stride in (3, 5):
            expected = [t for t in sorted(rows[pair]) if t // 60 % stride == 0
                and t + 3600 <= cutoff
                and all(t + lag * 60 in rows[pair] for lag in range(-60, 61))]
            assert data.training_epochs(pair, stride) == expected
            for t in expected:
                target = (rows[pair][t + 3600] - rows[pair][t]) / data.pips[pair]
                assert data.target(pair, t) == target


def test_missing_exact_horizon_target_is_not_replaced_by_next_array_row():
    rows, cutoff = continuous_rows()
    decision = FRIDAY + 120 * 60
    data = model._Data(rows, cutoff)
    assert decision in data.training_epochs("EUR_USD", 3)
    rows["EUR_USD"].pop(decision + 3600)
    assert decision not in model._Data(rows, cutoff).training_epochs("EUR_USD", 3)


@pytest.mark.parametrize("missing_offset", [-60, -17, 0, 23, 60])
def test_any_missing_feature_or_label_interval_excludes_affected_training_row(missing_offset):
    rows, cutoff = continuous_rows()
    decision = FRIDAY + 180 * 60
    rows["EUR_USD"].pop(decision + missing_offset * 60)
    assert decision not in model._Data(rows, cutoff).training_epochs("EUR_USD", 3)


def test_peer_hole_excludes_graph_feature_but_not_unrelated_own_price_rows():
    rows, cutoff = continuous_rows()
    decision = FRIDAY + 180 * 60
    rows["NZD_USD"].pop(decision - 15 * 60)
    data = model._Data(rows, cutoff)
    assert decision in data.training_epochs("EUR_USD", 3)
    assert data.common_runs.get(decision, 0) < 61
    output = model.predict_all(rows, cutoff)
    assert decision in output[model.FAMILIES[0]][2]["training_row_start_epochs_by_pair"]["EUR_USD"]
    assert decision not in output[model.FAMILIES[2]][2]["training_row_start_epochs_by_pair"]["EUR_USD"]


def test_state_ewma_never_treats_weekend_jump_as_one_minute_return():
    rows, cutoff = make_rows()
    first = model.predict_all(rows, cutoff)[model.FAMILIES[3]]
    for pair in model.PAIRS:
        rows[pair] = {t: price * 100 if t < SUNDAY else price for t, price in rows[pair].items()}
    second = model.predict_all(rows, cutoff)[model.FAMILIES[3]]
    assert first == second
    assert first[2]["state_segment_start_epoch"] == SUNDAY
    prices = np.asarray([rows["EUR_USD"][SUNDAY + i * 60] for i in range(61)])
    oracle = legacy_pure_functions()["state_space_predictions"]({"EUR_USD": prices}, {"EUR_USD": .0001})["EUR_USD"]
    assert first[0] == oracle[0] and first[1] == oracle[1]


def test_market_calendar_gap_size_does_not_allocate_or_compress_a_grid():
    rows, cutoff = make_rows(reopen=SUNDAY + 365 * 86400)
    output = model.predict_all(rows, cutoff)
    assert set(output) == set(model.FAMILIES)
    assert output[model.FAMILIES[3]][2]["observations"] == 60


def test_map_order_and_pair_order_do_not_change_fit_or_training_selection():
    rows, cutoff = make_rows()
    expected = model.predict_all(rows, cutoff)
    reversed_rows = {pair: dict(reversed(list(values.items()))) for pair, values in reversed(list(rows.items()))}
    assert model.predict_all(reversed_rows, cutoff) == expected


def test_fixed_utc_stride_does_not_shift_when_earliest_rows_removed():
    rows, cutoff = continuous_rows()
    old = model._Data(rows, cutoff).training_epochs("EUR_USD", 5)
    for i in range(13):
        rows["EUR_USD"].pop(FRIDAY + i * 60)
    new = model._Data(rows, cutoff).training_epochs("EUR_USD", 5)
    assert new == [t for t in old if t >= FRIDAY + 73 * 60]


@pytest.mark.parametrize("count", [1, 60])
def test_insufficient_current_shared_feature_history_abstains(count):
    rows, cutoff = make_rows(new_n=count)
    with pytest.raises(ValueError, match="61_closes"):
        model.predict_all(rows, cutoff)


@pytest.mark.parametrize("pair", model.PAIRS)
def test_each_pair_must_cover_every_current_feature_minute(pair):
    rows, cutoff = make_rows()
    rows[pair].pop(cutoff - 23 * 60)
    with pytest.raises(ValueError, match="61_closes"):
        model.predict_all(rows, cutoff)


def test_insufficient_historical_supervised_rows_do_not_lower_model_minima():
    rows, cutoff = make_rows(old_n=100)
    output = model.predict_all(rows, cutoff)
    assert set(output) == {model.FAMILIES[3]}


def test_single_class_labels_do_not_fake_an_hgb_probability():
    rows, cutoff = continuous_rows()
    for pair, values in rows.items():
        rows[pair] = {t: 1.1 for t in values}
    output = model.predict_all(rows, cutoff)
    assert model.FAMILIES[1] not in output
    assert set(output) == {model.FAMILIES[0], model.FAMILIES[2], model.FAMILIES[3]}


@pytest.mark.parametrize("value", [None, True, -1, 0, float("inf"), float("nan"), "1.1"])
def test_invalid_prices_rejected_before_fit(value):
    rows, cutoff = make_rows()
    rows["EUR_USD"][cutoff] = value
    with pytest.raises(ValueError, match="price_required"):
        model.predict_all(rows, cutoff)


@pytest.mark.parametrize("value", [None, True, -1, 0, float("inf"), float("nan"), str(SUNDAY), SUNDAY + 1, 10 ** 1000])
def test_invalid_cutoff_clock_rejected(value):
    rows, _ = make_rows()
    with pytest.raises(ValueError, match="START"):
        model.predict_all(rows, value)


def test_future_price_is_rejected_not_silently_used_or_discarded():
    rows, cutoff = make_rows()
    rows["EUR_USD"][cutoff + 60] = 10.
    with pytest.raises(ValueError, match="future_bar"):
        model.predict_all(rows, cutoff)


def test_nonminute_or_string_timestamp_rejected():
    rows, cutoff = make_rows()
    rows["EUR_USD"][str(FRIDAY)] = 1.1
    with pytest.raises(ValueError, match="START"):
        model.predict_all(rows, cutoff)


@pytest.mark.parametrize("mutation", ["missing_pair", "extra_pair", "empty_pair", "oversized_pair"])
def test_required_pair_identity_and_retained_real_row_bounds(mutation):
    rows, cutoff = make_rows()
    if mutation == "missing_pair":
        rows.pop("NZD_USD")
    elif mutation == "extra_pair":
        rows["EUR_GBP"] = dict(rows["EUR_USD"])
    elif mutation == "empty_pair":
        rows["EUR_USD"] = {}
    else:
        rows["EUR_USD"].update({FRIDAY - (i + 1) * 60: 1.1 for i in range(1025)})
    with pytest.raises(ValueError):
        model.predict_all(rows, cutoff)


def test_inputs_and_parameters_are_not_mutated():
    rows, cutoff = make_rows()
    original = deepcopy(rows)
    model.predict_all(rows, cutoff)
    assert rows == original
    with pytest.raises(TypeError):
        model.PARAMETERS["pooled_minimum_rows"] = 1
