from oanda_model_gap_horizon_extension import build_extension_specs, missing_extension_specs


def test_extension_builds_one_long_horizon_panel_per_timeframe() -> None:
    timeframes = ["S5", "M1", "H1", "H4"]
    horizons = [21600, 28800, 43200, 86400]

    specs = build_extension_specs(timeframes, horizons, cycles=500)

    assert len(specs) == 4
    assert all(spec.horizons_sec == tuple(horizons) for spec in specs)
    assert specs[0].source == "s5"
    assert specs[1].source == "m1"
    assert all(spec.role == "canonical_horizon_extension" for spec in specs)


def test_missing_specs_are_computed_per_timeframe() -> None:
    state = {
        "runs": [
            {
                "timeframe": "S5",
                "horizons_sec": [21600, 28800, 43200, 86400],
                "status": "completed",
            }
        ]
    }

    specs = missing_extension_specs(
        state,
        ["S5", "M1"],
        [21600, 28800, 43200, 86400],
        cycles=64,
    )

    assert [spec.timeframe for spec in specs] == ["M1"]
    assert specs[0].horizons_sec == (21600, 28800, 43200, 86400)
