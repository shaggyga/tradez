import forex_migration_guard as guard


def test_current_registry_is_safe_and_complete():
    result = guard.validate()
    assert result["status"] == "ok"
    assert result["failures"] == []
    assert result["entry_count"] >= 10
    assert result["live_root_entrypoint_count"] >= 9
    assert result["policy"]["can_move_files"] is False
    assert result["policy"]["can_place_orders"] is False
