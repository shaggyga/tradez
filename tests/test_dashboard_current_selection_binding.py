"""Catch configuration/source migrations omitted from the actual dashboard pointer."""
from pathlib import Path
import sys,time
ROOT=Path(__file__).resolve().parents[1]/'trad'
sys.path.insert(0,str(ROOT))
import oanda_operational_dashboard_selection_v1 as selection


def test_actual_checked_in_selection_matches_reviewed_sources():
    value=selection.read_selection(ROOT,time.time())
    assert value is not None
    assert set(value['source_bindings'])==selection.SOURCE_FILES
    assert value['can_place_orders'] is False and value['can_promote'] is False
