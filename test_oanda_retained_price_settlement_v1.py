from types import SimpleNamespace
import pytest
import oanda_retained_price_settlement_v1 as drain


@pytest.mark.parametrize("version,method", [(1, "schedule_fit"), (2, "schedule_work")])
def test_settlement_keeps_quote_and_outcome_work_but_never_schedules_fit(version, method):
    calls = []
    class Original:
        def tick(self):
            calls.extend(["quote", "settle"])
            getattr(self, method)()
            calls.append("status")
        def schedule_fit(self):
            raise AssertionError("new fit")
        def schedule_work(self):
            raise AssertionError("new fit/capture")
    runner = drain.settlement_class(SimpleNamespace(PairRunner=Original), version)()
    runner.tick()
    assert calls == ["quote", "settle", "status"]


def test_all_registered_ledgers_are_required(tmp_path):
    registry = {"pairs": {"EUR_USD": {"families": {"ridge": {}, "state": {}}}}}
    assert drain.ledger_paths(registry, tmp_path, 2) == [
        tmp_path / "pairs/EUR_USD/ridge/study.sqlite", tmp_path / "pairs/EUR_USD/state/study.sqlite"]
    with pytest.raises(ValueError):
        drain.settlement_class(SimpleNamespace(), 3)
