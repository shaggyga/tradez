from __future__ import annotations

import pytest

try:
    from forex_system.contracts.signed_currency_exposure import (
        INDEPENDENT,
        OPPOSITE_SIDE_CONFLICT,
        SAME_SIDE_OVERLAP,
        CurrencyExposure,
        ExposureContractError,
        InvalidDirectionError,
        InvalidFactorIdError,
        InvalidInstrumentError,
        canonical_factor_ids,
        compare_exposure_sets,
        compare_factor_ids,
        compare_pair_exposures,
        normalize_direction,
        pair_currency_exposures,
        parse_factor_id,
        split_currency_pair,
    )
except ModuleNotFoundError:
    from src.forex_system.contracts.signed_currency_exposure import (
        INDEPENDENT,
        OPPOSITE_SIDE_CONFLICT,
        SAME_SIDE_OVERLAP,
        CurrencyExposure,
        ExposureContractError,
        InvalidDirectionError,
        InvalidFactorIdError,
        InvalidInstrumentError,
        canonical_factor_ids,
        compare_exposure_sets,
        compare_factor_ids,
        compare_pair_exposures,
        normalize_direction,
        pair_currency_exposures,
        parse_factor_id,
        split_currency_pair,
    )


@pytest.mark.parametrize(
    ("raw", "expected"),
    (("buy", "long"), ("LONG", "long"), (" sell ", "short"), ("short", "short")),
)
def test_direction_aliases_normalize_only_to_long_or_short(raw, expected):
    assert normalize_direction(raw) == expected


@pytest.mark.parametrize("raw", (None, "", "hold", "neutral", "flat", 1, True))
def test_unknown_direction_fails_closed(raw):
    with pytest.raises(InvalidDirectionError):
        normalize_direction(raw)
    with pytest.raises(InvalidDirectionError):
        pair_currency_exposures("EUR_USD", raw)


def test_pair_maps_to_ordered_canonical_currency_exposures():
    long_exposures = pair_currency_exposures(" eur_usd ", "buy")
    short_exposures = pair_currency_exposures("USD_JPY", "sell")

    assert [row.payload() for row in long_exposures] == [
        {
            "currency": "EUR",
            "side": "long",
            "sign": 1,
            "factor_id": "currency:EUR:long",
        },
        {
            "currency": "USD",
            "side": "short",
            "sign": -1,
            "factor_id": "currency:USD:short",
        },
    ]
    assert canonical_factor_ids("USD_JPY", "short") == (
        "currency:USD:short",
        "currency:JPY:long",
    )
    assert [row.side for row in short_exposures] == ["short", "long"]


@pytest.mark.parametrize(
    "raw",
    (None, "", "EUR/USD", "EURUSD", "EUR_USD_EXTRA", "EUR_EUR", "EU_USD", 1),
)
def test_malformed_pair_identity_fails_closed(raw):
    with pytest.raises(InvalidInstrumentError):
        split_currency_pair(raw)


def test_inverse_pair_and_opposite_direction_have_identical_economic_factors():
    assert set(canonical_factor_ids("EUR_USD", "long")) == set(
        canonical_factor_ids("USD_EUR", "short")
    )
    comparison = compare_pair_exposures(
        "EUR_USD", "long", "USD_EUR", "short"
    )
    assert comparison.relation == SAME_SIDE_OVERLAP
    assert comparison.same_side_factor_ids == (
        "currency:EUR:long",
        "currency:USD:short",
    )
    assert comparison.has_conflict is False
    assert comparison.can_count_as_independent_confirmation is False


def test_same_signed_currency_propagation_is_correlated_not_independent():
    comparison = compare_pair_exposures(
        "EUR_USD", "long", "GBP_USD", "long"
    )
    assert comparison.relation == SAME_SIDE_OVERLAP
    assert comparison.same_side_factor_ids == ("currency:USD:short",)
    assert comparison.shared_currencies == ("USD",)
    assert comparison.has_conflict is False
    assert comparison.is_independent is False


def test_opposite_signed_currency_overlap_is_an_explicit_conflict():
    comparison = compare_pair_exposures(
        "EUR_USD", "long", "USD_JPY", "long"
    )
    assert comparison.relation == OPPOSITE_SIDE_CONFLICT
    assert comparison.same_side_factor_ids == ()
    assert comparison.shared_currencies == ("USD",)
    assert comparison.has_conflict is True
    assert comparison.can_count_as_independent_confirmation is False
    assert comparison.opposite_side_conflicts[0].payload() == {
        "currency": "USD",
        "left_factor_id": "currency:USD:short",
        "right_factor_id": "currency:USD:long",
    }


def test_exact_opposite_pair_direction_conflicts_on_both_currencies():
    comparison = compare_pair_exposures(
        "EUR_USD", "long", "EUR_USD", "short"
    )
    assert comparison.relation == OPPOSITE_SIDE_CONFLICT
    assert comparison.shared_currencies == ("EUR", "USD")
    assert {row.currency for row in comparison.opposite_side_conflicts} == {
        "EUR",
        "USD",
    }


def test_disjoint_pairs_are_independent_confirmation_candidates():
    comparison = compare_pair_exposures(
        "EUR_USD", "long", "AUD_NZD", "short"
    )
    assert comparison.relation == INDEPENDENT
    assert comparison.shared_currencies == ()
    assert comparison.has_conflict is False
    assert comparison.is_independent is True
    assert comparison.can_count_as_independent_confirmation is True


def test_factor_parser_accepts_only_exact_canonical_identity():
    exposure = parse_factor_id("currency:JPY:short")
    assert exposure == CurrencyExposure("JPY", "short")
    for invalid in (
        "JPY:short",
        "currency:jpy:short",
        "currency:JPY:sell",
        "Currency:JPY:short",
        "currency:JPY:short:extra",
        None,
    ):
        with pytest.raises((InvalidFactorIdError, InvalidDirectionError)):
            parse_factor_id(invalid)


def test_factor_comparison_deduplicates_repeated_same_side_identity():
    comparison = compare_factor_ids(
        ["currency:JPY:short", "currency:JPY:short"],
        ["currency:JPY:short"],
    )
    assert comparison.relation == SAME_SIDE_OVERLAP
    assert comparison.same_side_factor_ids == ("currency:JPY:short",)


def test_internally_conflicted_factor_set_is_rejected():
    with pytest.raises(ExposureContractError):
        compare_exposure_sets(
            [CurrencyExposure("USD", "long"), CurrencyExposure("USD", "short")],
            [CurrencyExposure("EUR", "long")],
        )


def test_comparison_is_order_invariant_except_for_conflict_provenance():
    forward = compare_pair_exposures("EUR_USD", "long", "USD_JPY", "long")
    reverse = compare_pair_exposures("USD_JPY", "long", "EUR_USD", "long")

    assert forward.relation == reverse.relation == OPPOSITE_SIDE_CONFLICT
    assert forward.shared_currencies == reverse.shared_currencies == ("USD",)
    assert forward.has_conflict == reverse.has_conflict is True
    assert (
        forward.opposite_side_conflicts[0].left_factor_id
        == reverse.opposite_side_conflicts[0].right_factor_id
    )
