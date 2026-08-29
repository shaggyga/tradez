import json

try:
    import oanda_post_upgrade_audit as audit
except ModuleNotFoundError:
    from trad import oanda_post_upgrade_audit as audit


def _generate_with_account(tmp_path, monkeypatch, aggregate):
    state = tmp_path / "state"
    news = tmp_path / "news"
    state.mkdir()
    news.mkdir()
    (state / "account_007_dashboard_v1.json").write_text(
        json.dumps({"aggregate": aggregate}),
        encoding="utf-8",
    )
    monkeypatch.setattr(audit, "STATE", state)
    monkeypatch.setattr(audit, "NEWS", news)
    output = tmp_path / "audit.md"
    audit.generate(output)
    return output.read_text(encoding="utf-8")


def test_unavailable_account_renders_unknown_and_exposes_failure_state(tmp_path, monkeypatch):
    text = _generate_with_account(
        tmp_path,
        monkeypatch,
        {
            "snapshot_state": "unavailable",
            "account_values_current": False,
            "positions_current": False,
            "orders_current": False,
            "balance": None,
            "nav": None,
            "openTradeCount": None,
            "pendingOrderCount": None,
            "current_errors": [
                {
                    "account_id": "101-001-37981792-007",
                    "status_code": 503,
                    "error": "System under maintenance, please try again later.",
                }
            ],
        },
    )

    assert "- Account snapshot state: `unavailable`." in text
    assert "- Currentness — account values / positions / orders: `false` / `false` / `false`." in text
    assert '"status_code":503' in text
    assert "- Practice 007 balance/NAV: `unknown` / `unknown`." in text
    assert "- Open trades/orders: `unknown` / `unknown`." in text


def test_noncurrent_retained_values_are_not_presented_as_current(tmp_path, monkeypatch):
    text = _generate_with_account(
        tmp_path,
        monkeypatch,
        {
            "snapshot_state": "retained_stale_account_values",
            "account_values_current": False,
            "positions_current": False,
            "orders_current": False,
            "balance": 41.6042,
            "nav": 41.6042,
            "openTradeCount": 0,
            "pendingOrderCount": 0,
            "current_errors": [{"status_code": 503}],
        },
    )

    assert "- Practice 007 balance/NAV: `unknown` / `unknown`." in text
    assert "- Open trades/orders: `unknown` / `unknown`." in text


def test_current_zero_position_and_order_counts_remain_zero(tmp_path, monkeypatch):
    text = _generate_with_account(
        tmp_path,
        monkeypatch,
        {
            "snapshot_state": "current",
            "account_values_current": True,
            "positions_current": True,
            "orders_current": True,
            "balance": 41.6042,
            "nav": 41.6042,
            "openTradeCount": 0,
            "pendingOrderCount": 0,
            "current_errors": [],
        },
    )

    assert "- Currentness — account values / positions / orders: `true` / `true` / `true`." in text
    assert "- Current account errors: `none`." in text
    assert "- Practice 007 balance/NAV: `41.6042` / `41.6042`." in text
    assert "- Open trades/orders: `0` / `0`." in text
