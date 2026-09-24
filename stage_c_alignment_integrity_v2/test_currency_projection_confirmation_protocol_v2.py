import pytest
from currency_projection_confirmation_protocol_v2 import protocol,validate
def candidate(origins):
 p=protocol();return {'origins':origins,'methods':p['required']['all_methods'],'all68_market_quotes':True,'mature_labels_before_assessment':True}
def test_inspected_dates_refuse_and_untouched_candidate_passes():
 with pytest.raises(ValueError,match='reuses'):validate(candidate([1722535260+i for i in range(8)]))
 assert validate(candidate([1722535320+i for i in range(8)]))
