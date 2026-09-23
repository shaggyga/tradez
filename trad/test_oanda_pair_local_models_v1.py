"""Synthetic EURUSD-only model tests; no broker, runtime or market performance."""
from copy import deepcopy
from datetime import datetime, timezone
import math

import numpy as np
import pytest

import oanda_pair_local_models_v1 as local
import oanda_eurusd_local_models_v1 as eur
import oanda_gap_aware_four_family_models as gap


def predict(rows, cutoff):
    return local.predict_all(rows, cutoff, instrument="EUR_USD", pip_size=.0001)


FRIDAY = int(datetime(2026, 9, 4, 10, tzinfo=timezone.utc).timestamp())
SUNDAY = int(datetime(2026, 9, 6, 21, tzinfo=timezone.utc).timestamp())


def fixture_rows(old_n=512, current_n=61, reopen=SUNDAY):
    rows = {FRIDAY + i * 60: 1.1 + .0001 * (5 * math.sin(i / 8.) + .01 * i)
            for i in range(old_n)}
    rows.update({reopen + i * 60: 1.1 + .0001 * (30 + 2 * math.sin(i / 5.) - .02 * i)
                 for i in range(current_n)})
    cutoff = reopen + (current_n - 1) * 60 if current_n else FRIDAY + (old_n - 1) * 60
    return rows, cutoff


def distinct_peer_fixture(eurusd):
    # Different synthetic peer histories are used only by the frozen oracle.
    # The local model never receives or constructs these maps.
    output = {"EUR_USD": dict(eurusd)}
    for index, pair in enumerate(gap.PAIRS[1:], start=1):
        pip = .01 if pair.endswith("_JPY") else .0001
        base = 145. if pair.endswith("_JPY") else 1.1 + index * .1
        output[pair] = {t: base + pip * (2 * math.sin(i / 7. + index) + .01 * i)
                        for i, t in enumerate(sorted(eurusd))}
    return output


@pytest.mark.parametrize("shape", ["continuous", "weekend", "historical_holes"])
def test_matches_bound_gap_v2_ridge_and_state_numerics(shape):
    rows, cutoff = fixture_rows(current_n=0 if shape == "continuous" else 61)
    if shape == "historical_holes":
        rows.pop(FRIDAY + 170 * 60)
        rows.pop(FRIDAY + 371 * 60)
    expected = gap.predict_all(distinct_peer_fixture(rows), cutoff)
    actual = predict(rows, cutoff)
    assert local.FAMILIES == ("ridge_return_repaired", "probabilistic_state_space")
    assert set(actual) == set(local.FAMILIES)
    for family in local.FAMILIES:
        assert actual[family][0] == expected[family][0]
        assert actual[family][1] == expected[family][1]
        for field in ("training_rows", "training_label_maturity_max_epoch", "feature_cutoff_epoch",
                      "training_row_start_epochs_by_pair"):
            assert actual[family][2][field] == expected[family][2][field]


def test_peer_holes_cannot_block_eurusd_only_forecasts():
    rows, cutoff = fixture_rows()
    expected = predict(rows, cutoff)
    peers = distinct_peer_fixture(rows)
    peers["AUD_USD"].pop(cutoff - 60)
    peers["NZD_USD"].pop(cutoff - 120)
    peers["USD_CHF"].pop(cutoff - 180)
    with pytest.raises(ValueError, match="current_common_feature_window"):
        gap.predict_all(peers, cutoff)
    assert predict(rows, cutoff) == expected
    assert set(expected) == set(local.FAMILIES)
    assert all(value[2]["input_scope"] == "single_pair_only_no_peer_inputs" for value in expected.values())


def test_friday_mature_labels_can_train_after_sixty_real_sunday_intervals():
    rows, cutoff = fixture_rows()
    result = predict(rows, cutoff)
    ridge = result[local.FAMILIES[0]][2]
    assert ridge["training_rows"] == 131
    assert ridge["training_label_maturity_max_epoch"] < SUNDAY
    assert ridge["feature_cutoff_epoch"] == cutoff + 60
    assert result[local.FAMILIES[1]][2]["observations"] == 60


@pytest.mark.parametrize("n,has_ridge", [(189, False), (190, True)])
def test_ridge_minimum_24_mature_rows_is_preserved(n, has_ridge):
    rows, cutoff = fixture_rows(old_n=n, current_n=0)
    output = predict(rows, cutoff)
    assert (local.FAMILIES[0] in output) is has_ridge
    assert local.FAMILIES[1] in output
    if has_ridge:
        assert output[local.FAMILIES[0]][2]["training_rows"] == 24


def test_exact_timestamp_window_selection_matches_naive_oracle():
    rows, cutoff = fixture_rows(old_n=800, current_n=70)
    for index in (130, 211, 470, 605, 729):
        rows.pop(FRIDAY + index * 60)
    expected = [t for t in sorted(rows) if t // 60 % 3 == 0 and t + 3600 <= cutoff
                and all(t + offset * 60 in rows for offset in range(-60, 61))]
    assert local._training_epochs(rows, cutoff) == expected


@pytest.mark.parametrize("offset", [-60, -1, 0, 17, 60])
def test_missing_feature_or_target_path_price_invalidates_sample(offset):
    rows, cutoff = fixture_rows(current_n=0)
    sample = FRIDAY + 180 * 60
    assert sample in local._training_epochs(rows, cutoff)
    rows.pop(sample + offset * 60)
    assert sample not in local._training_epochs(rows, cutoff)


def test_ridge_training_labels_use_exact_plus3600_endpoints(monkeypatch):
    rows, cutoff = fixture_rows()
    original_fit = local._ridge
    captured = {}
    def capture(x, y, current):
        captured.update(x=x.copy(), y=y.copy(), current=current.copy())
        return original_fit(x, y, current)
    monkeypatch.setattr(local, "_ridge", capture)
    output = predict(rows, cutoff)
    epochs = output[local.FAMILIES[0]][2]["training_row_start_epochs_by_pair"]["EUR_USD"]
    assert np.array_equal(captured["y"], np.asarray([(rows[t + 3600] - rows[t]) / .0001 for t in epochs]))
    assert all(t + 3660 <= cutoff + 60 for t in epochs)


def test_training_scaling_excludes_current_feature_outlier(monkeypatch):
    rows, cutoff = fixture_rows()
    # With only61 Sunday closes there are no Sunday mature training rows.
    # A current-only outlier may change the prediction, never training X/y.
    original_fit = local._ridge
    captured = []
    def capture(x, y, current):
        captured.append((x.copy(), y.copy(), current.copy()))
        return original_fit(x, y, current)
    monkeypatch.setattr(local, "_ridge", capture)
    predict(rows, cutoff)
    rows[cutoff] += .01
    predict(rows, cutoff)
    assert np.array_equal(captured[0][0], captured[1][0])
    assert np.array_equal(captured[0][1], captured[1][1])
    assert not np.array_equal(captured[0][2], captured[1][2])


def test_state_resets_at_eurusd_gap_and_excludes_weekend_jump():
    rows, cutoff = fixture_rows()
    expected = predict(rows, cutoff)[local.FAMILIES[1]]
    changed = {t: price * 100 if t < SUNDAY else price for t, price in rows.items()}
    assert predict(changed, cutoff)[local.FAMILIES[1]] == expected
    assert expected[2]["state_segment_start_epoch"] == SUNDAY
    assert expected[2]["observations"] == 60


def test_state_uses_at_most_256_consecutive_prices():
    rows, cutoff = fixture_rows(old_n=1024, current_n=0)
    state = predict(rows, cutoff)[local.FAMILIES[1]][2]
    assert state["observations"] == 255
    assert state["state_segment_start_epoch"] == cutoff - 255 * 60


@pytest.mark.parametrize("offset", [0, -1, -17, -60])
def test_any_missing_current_eurusd_close_abstains(offset):
    rows, cutoff = fixture_rows()
    rows.pop(cutoff + offset * 60)
    with pytest.raises(ValueError, match="61_closes"):
        predict(rows, cutoff)


@pytest.mark.parametrize("n", [1, 60])
def test_current_window_cannot_be_substituted_with_friday_prices(n):
    rows, cutoff = fixture_rows(current_n=n)
    with pytest.raises(ValueError, match="61_closes"):
        predict(rows, cutoff)


def test_pure_sixty_interval_window_can_return_state_but_not_invent_ridge_labels():
    rows, cutoff = fixture_rows(old_n=0)
    assert set(predict(rows, cutoff)) == {local.FAMILIES[1]}


def test_large_calendar_gap_does_not_change_training_count_or_materialize_missing_prices():
    rows, cutoff = fixture_rows(reopen=SUNDAY + 365 * 86400)
    original = deepcopy(rows)
    output = predict(rows, cutoff)
    assert len(rows) == 573 and rows == original
    assert output[local.FAMILIES[0]][2]["training_rows"] == 131
    assert output[local.FAMILIES[1]][2]["observations"] == 60


def test_chronological_input_disorder_is_canonical_and_nonmutating():
    rows, cutoff = fixture_rows()
    shuffled = dict(reversed(list(rows.items())))
    original = deepcopy(shuffled)
    assert predict(shuffled, cutoff) == predict(rows, cutoff)
    assert list(shuffled.items()) == list(original.items())


def test_fixed_utc_stride_does_not_shift_with_earliest_retained_row():
    rows, cutoff = fixture_rows(current_n=0)
    expected = local._training_epochs(rows, cutoff)
    for offset in range(13):
        rows.pop(FRIDAY + offset * 60)
    assert local._training_epochs(rows, cutoff) == [t for t in expected if t >= FRIDAY + 73 * 60]


@pytest.mark.parametrize("value", [None, True, 0, -1, float("nan"), float("inf"), "1.1", 10 ** 1000])
def test_invalid_price_abstains(value):
    rows, cutoff = fixture_rows()
    rows[cutoff] = value
    with pytest.raises(ValueError, match="price_required"):
        predict(rows, cutoff)


@pytest.mark.parametrize("value", [None, True, 0, -1, float("nan"), float("inf"), str(SUNDAY), SUNDAY + 1, 10 ** 1000])
def test_invalid_cutoff_abstains(value):
    rows, _ = fixture_rows()
    with pytest.raises(ValueError, match="START"):
        predict(rows, value)


@pytest.mark.parametrize("offset", [-60, 60])
def test_cutoff_must_be_latest_actual_row_without_silent_filtering(offset):
    rows, cutoff = fixture_rows()
    with pytest.raises(ValueError, match="future_bar" if offset < 0 else "61_closes"):
        predict(rows, cutoff + offset)


def test_nonminute_row_timestamp_abstains():
    rows, cutoff = fixture_rows()
    rows[FRIDAY + 1] = 1.1
    with pytest.raises(ValueError, match="START"):
        predict(rows, cutoff)


@pytest.mark.parametrize("shape", ["empty", "list", "nested_pair_map", "oversized"])
def test_direct_single_pair_api_and_bound(shape):
    rows, cutoff = fixture_rows()
    if shape == "empty": rows = {}
    elif shape == "list": rows = list(rows.items())
    elif shape == "nested_pair_map": rows = {"EUR_USD": rows}
    else: rows.update({FRIDAY - (i + 1) * 60: 1.1 for i in range(1025)})
    with pytest.raises(ValueError):
        predict(rows, cutoff)


def test_constant_history_remains_finite_without_numerical_noise_hits():
    rows, cutoff = fixture_rows(current_n=0)
    output = predict(dict.fromkeys(rows, 1.1), cutoff)
    assert set(output) == set(local.FAMILIES)
    assert all(expected == 0 and probability == .5 for expected, probability, _ in output.values())


def test_numerical_overflow_abstains_instead_of_emitting_nonfinite_probability():
    rows, cutoff = fixture_rows()
    rows[cutoff] = 1e308
    with pytest.raises(ValueError, match="numerical_failure"):
        predict(rows, cutoff)


def test_no_model_or_diagnostic_grants_authority():
    rows, cutoff = fixture_rows()
    for expected, probability, diagnostics in predict(rows, cutoff).values():
        assert math.isfinite(expected) and math.isfinite(probability)
        assert diagnostics["research_only"] is True
        assert all(diagnostics[field] is False for field in (
            "can_place_orders", "can_authorize", "can_promote", "account_eligible", "proof_eligible"))
        assert "available_epoch" not in diagnostics
    with pytest.raises(TypeError):
        local.PARAMETERS["ridge_minimum_rows"] = 1


@pytest.mark.parametrize("pair,pip", [("EUR_USD", .0001), ("USD_JPY", .01), ("HKD_JPY", .0001), ("USD_HUF", .01), ("EUR_HUF", .01), ("USD_THB", .01)])
def test_instrument_and_explicit_metadata_pip_are_preserved(pair, pip):
    rows, cutoff = fixture_rows()
    result = local.predict_all(rows, cutoff, instrument=pair, pip_size=pip)
    assert set(result) == set(local.FAMILIES)
    assert all(v[2]["instrument"] == pair and v[2]["pip_size"] == pip for v in result.values())
    assert set(result[local.FAMILIES[0]][2]["training_row_start_epochs_by_pair"]) == {pair}


@pytest.mark.parametrize("shape", ["continuous", "weekend", "historical_holes"])
def test_frozen_eurusd_numerics_are_exactly_equivalent(shape):
    rows, cutoff = fixture_rows(current_n=0 if shape == "continuous" else 61)
    if shape == "historical_holes":
        rows.pop(FRIDAY + 170 * 60)
    expected = eur.predict_all(rows, cutoff)
    actual = predict(rows, cutoff)
    for family in local.FAMILIES:
        assert actual[family][:2] == expected[family][:2]


def test_jpy_price_and_pip_scaling_keep_equivalent_pip_forecasts():
    rows, cutoff = fixture_rows()
    non_jpy = predict(rows, cutoff)
    jpy = local.predict_all({t: v * 100 for t, v in rows.items()}, cutoff, instrument="USD_JPY", pip_size=.01)
    for family in local.FAMILIES:
        assert jpy[family][0] == pytest.approx(non_jpy[family][0], rel=1e-8, abs=1e-8)
        assert jpy[family][1] == pytest.approx(non_jpy[family][1], rel=1e-8, abs=1e-8)


@pytest.mark.parametrize("instrument", ["../EUR_USD", "eur_usd", "EURUSD", "EUR_USD/..", "USD_USD", None, 1])
def test_invalid_instrument_is_rejected(instrument):
    rows, cutoff = fixture_rows()
    with pytest.raises(ValueError, match="currency_pair"):
        local.predict_all(rows, cutoff, instrument=instrument, pip_size=.0001)


@pytest.mark.parametrize("pip", [None, True, 0, -1, float("nan"), float("inf"), ".0001", .1, 1e-20])
def test_invalid_pip_size_is_rejected(pip):
    rows, cutoff = fixture_rows()
    with pytest.raises(ValueError, match="pip_size"):
        local.predict_all(rows, cutoff, instrument="EUR_USD", pip_size=pip)
