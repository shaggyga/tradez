import json
from pathlib import Path
from currency_projection_policy_attribution_v2 import build

ROOT=Path(__file__).resolve().parent
def test_contract_retains_all_variants_and_arms():
    c=json.loads((ROOT/'CURRENCY_PROJECTION_POLICY_ATTRIBUTION_CONTRACT_V2.json').read_text())
    assert len(c['methods'])==6 and c['parent_run_count']==48 and c['analysis_engine']=='reference_only_after_verified_engine_neutral_parity'
