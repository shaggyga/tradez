import oanda_four_hour_best_improvement_pass as watch


def test_integrity_failure_preempts_research() -> None:
    selected = watch.select_action({"integrity": {"failures": ["clock_explicitly_classified"]}})
    assert selected["branch"] == "operational_integrity"
    assert "Windows Time" in selected["action"]


def test_no_active_build_means_evidence_collection() -> None:
    selected = watch.select_action({"integrity": {"failures": []}, "controller": {"portfolio": {"active_build_branch": None}}})
    assert selected["branch"] == "governed_evidence_collection"


def test_compare_reports_material_forecast_change() -> None:
    previous = {"opportunity": {"totals": {"forecasts": 10, "matured": 2}}, "account": {}, "integrity": {}, "quotes": {}, "files": {}}
    current = {"opportunity": {"totals": {"forecasts": 11, "matured": 2}}, "account": {}, "integrity": {}, "quotes": {}, "files": {}}
    assert "opportunity.forecasts:10->11" in watch.compare(previous, current)


def test_compare_reports_gib_scale_drive_change() -> None:
    previous = {"opportunity": {}, "account": {}, "integrity": {}, "quotes": {}, "files": {}, "disk": {"free_bytes": 200 * 1024**3}}
    current = {"opportunity": {}, "account": {}, "integrity": {}, "quotes": {}, "files": {}, "disk": {"free_bytes": 198 * 1024**3}}
    assert any(change.startswith("drive_free_delta_bytes:") for change in watch.compare(previous, current))


def test_account_snapshot_preserves_unavailable_values_as_unknown() -> None:
    compact = watch.compact_account_snapshot(
        {
            "environment": "practice",
            "accounts": [
                {
                    "account_id": "101-001-37981792-007",
                    "ok": False,
                    "account_values_current": False,
                    "positions_current": False,
                    "orders_current": False,
                }
            ],
            "aggregate": {
                "snapshot_state": "unavailable",
                "nav": None,
                "balance": None,
                "openTradeCount": None,
                "pendingOrderCount": None,
            },
        }
    )

    assert compact["account_current"] is False
    assert compact["snapshot_state"] == "unavailable"
    assert compact["nav"] is None
    assert compact["balance"] is None
    assert compact["open_trades"] is None
    assert compact["pending_orders"] is None
