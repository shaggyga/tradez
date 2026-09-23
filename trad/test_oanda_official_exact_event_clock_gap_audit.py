import datetime as dt

import oanda_official_exact_event_clock_gap_audit as audit


UTC = dt.timezone.utc


def _exact_row(currency: str, scheduled: str) -> dict:
    return {
        "external_id": f"{currency}:{scheduled}",
        "scheduled_utc": scheduled,
        "event_series_id": f"{currency.lower()}_cpi",
        "event_name": f"{currency} CPI",
        "timing_precision": "minute",
        "independent_domestic_event": True,
        "linked_policy_factor": False,
        "actual": None,
        "actual_value": None,
        "consensus": None,
        "consensus_value": None,
        "direction": None,
        "directional_research_only": True,
        "research_only": True,
        "execution_eligible": False,
        "can_place_orders": False,
    }


def _safe_link_registries():
    dependencies = {
        "contract_id": "dependencies",
        "dependencies": [
            {
                "dependent_currency": "DKK",
                "driver_currency": "EUR",
                "assign_direction": False,
                "timing_policy": "driver_clock_preflight_only",
            },
            {
                "dependent_currency": "HKD",
                "driver_currency": "USD",
                "assign_direction": False,
                "timing_policy": "driver_clock_preflight_only",
            },
        ],
    }
    linked = {
        "contract_id": "linked",
        "relationships": [
            {
                "currency": "DKK",
                "driver_currency": "EUR",
                "can_assign_direction": False,
                "can_confirm": False,
            },
            {
                "currency": "HKD",
                "driver_currency": "USD",
                "can_assign_direction": False,
                "can_confirm": False,
            },
        ],
    }
    return dependencies, linked


def test_exact_gap_audit_counts_domestic_clocks_not_linked_drivers():
    registry = {
        "contract_id": "registry",
        "guards": ["no direction"],
        "currencies": [
            {
                "currency": "DKK",
                "priced_pairs": ["EUR_DKK", "USD_DKK"],
                "domestic_statistics": {
                    "status": "exact_minute_available",
                    "source_id": "dkk_clock",
                },
                "domestic_policy": {
                    "status": "no_independent_preannounced_exact_clock_verified"
                },
                "linked_driver": {"driver_currency": "EUR"},
            },
            {
                "currency": "HKD",
                "priced_pairs": ["USD_HKD"],
                "domestic_statistics": {
                    "status": "exact_minute_available",
                    "source_id": "hkd_clock",
                },
                "domestic_policy": {
                    "status": "no_independent_preannounced_exact_clock_verified"
                },
                "linked_driver": {"driver_currency": "USD"},
            },
            {
                "currency": "HUF",
                "priced_pairs": ["EUR_HUF", "USD_HUF"],
                "domestic_statistics": {
                    "status": "exact_minute_available",
                    "source_id": "huf_cpi_clock",
                    "scope": "headline_cpi_only",
                },
                "domestic_policy": {
                    "status": "official_date_window_only",
                    "timing_precision": "date_window",
                },
                "linked_driver": None,
            },
        ],
    }
    observations = {
        "dkk_clock": {
            "http_status": 200,
            "parsed_rows": 1,
            "rows": [_exact_row("DKK", "2026-08-20T06:00:00Z")],
        },
        "hkd_clock": {
            "http_status": 200,
            "parsed_rows": 1,
            "rows": [_exact_row("HKD", "2026-08-20T08:30:00Z")],
        },
        "huf_cpi_clock": {
            "http_status": 200,
            "parsed_rows": 1,
            "rows": [_exact_row("HUF", "2026-09-08T06:30:00Z")],
        },
    }

    dependencies, linked = _safe_link_registries()
    report = audit.build_report(
        registry,
        observations,
        as_of=dt.datetime(2026, 8, 17, tzinfo=UTC),
        policy_dependency_registry=dependencies,
        linked_policy_registry=linked,
    )

    assert report["summary"]["live_exact_domestic_currency_count"] == 3
    assert report["summary"]["pairs_with_live_exact_domestic_clock"] == 5
    assert report["summary"]["target_pair_count"] == 5
    assert report["summary"]["huf_policy_exact_clock"] is False
    assert report["summary"]["huf_cpi_statistics_exact_clock"] is True
    assert report["summary"]["linked_driver_safety_pass"] is True
    assert report["summary"]["audit_guard_pass"] is True
    by_currency = {row["currency"]: row for row in report["currencies"]}
    assert by_currency["DKK"]["linked_driver_counted_as_domestic_or_independent"] is False
    assert by_currency["HKD"]["linked_driver_counted_as_domestic_or_independent"] is False
    assert by_currency["HUF"]["domestic_exact_statistics_clock_live_ready"] is True
    assert by_currency["HUF"]["domestic_statistics_scope"] == "headline_cpi_only"
    assert by_currency["HUF"]["domestic_policy_timing_precision"] == "date_window"


def test_exact_gap_audit_fails_closed_on_direction_or_execution_fields():
    registry = {
        "contract_id": "registry",
        "currencies": [
            {
                "currency": "DKK",
                "priced_pairs": ["EUR_DKK"],
                "domestic_statistics": {
                    "status": "exact_minute_available",
                    "source_id": "dkk_clock",
                },
                "domestic_policy": {},
                "linked_driver": None,
            }
        ],
    }
    unsafe = _exact_row("DKK", "2026-08-20T06:00:00Z")
    unsafe["direction"] = "LONG"
    observations = {
        "dkk_clock": {
            "http_status": 200,
            "parsed_rows": 1,
            "rows": [unsafe],
        }
    }

    report = audit.build_report(
        registry,
        observations,
        as_of=dt.datetime(2026, 8, 17, tzinfo=UTC),
    )

    assert report["summary"]["live_exact_domestic_currency_count"] == 0
    assert report["currencies"][0]["direction_or_execution_guard_violations"]
    assert report["execution_policy_changed"] is False


def test_exact_gap_audit_fails_when_linked_driver_can_confirm():
    registry = {
        "contract_id": "registry",
        "currencies": [
            {
                "currency": "HKD",
                "priced_pairs": ["USD_HKD"],
                "domestic_statistics": {
                    "status": "exact_minute_available",
                    "source_id": "hkd_clock",
                },
                "domestic_policy": {},
                "linked_driver": {"driver_currency": "USD"},
            }
        ],
    }
    observations = {
        "hkd_clock": {
            "http_status": 200,
            "parsed_rows": 1,
            "rows": [_exact_row("HKD", "2026-08-20T08:30:00Z")],
        }
    }
    dependencies, linked = _safe_link_registries()
    linked["relationships"] = [
        {
            "currency": "HKD",
            "driver_currency": "USD",
            "can_assign_direction": False,
            "can_confirm": True,
        }
    ]

    report = audit.build_report(
        registry,
        observations,
        as_of=dt.datetime(2026, 8, 17, tzinfo=UTC),
        policy_dependency_registry=dependencies,
        linked_policy_registry=linked,
    )

    assert report["summary"]["linked_driver_safety_pass"] is False
    assert report["summary"]["audit_guard_pass"] is False
    assert report["currencies"][0]["linked_driver_counted_as_domestic_or_independent"] is True
    assert "linked_policy_can_confirm" in report["currencies"][0][
        "linked_driver_safety_violations"
    ]
