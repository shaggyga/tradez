import json
from pathlib import Path
import pytest
from currency_projection_policy_fixture_v2 import scenario
from currency_projection_policy_input_v2 import validate_frame_authority

ROOT = Path(__file__).resolve().parent

def contract(): return json.loads((ROOT / 'CURRENCY_PROJECTION_POLICY_CONTRACT_V2.json').read_text())

def test_matrix_and_dated_scenarios_are_frozen():
    c = contract(); assert len(c['methods']) == 6 and c['expected_run_count'] == 48
    for name in c['scenarios']:
        value = scenario(c, name); assert value['rollover_epoch'] == c['cohorts'][c['scenarios'][name]['cohort']]['rollover_epoch']

def test_projection_authority_refuses_replaced_packet_hash():
    frame = {'model_profile':'currency_projection_policy.v1','method':'ridge__direct','cohort':'later_monday','epoch':12,'target_epoch':99,
        'market_points':[], 'historical_packets':[], 'projection_input_authority':{'method':'ridge__direct','cohort':'later_monday','origin_epoch':10,'target_epoch':99,'market_panel_sha256':'x','packet_sha256':'x'}}
    with pytest.raises(ValueError, match='market_authority'): validate_frame_authority(frame, {})
