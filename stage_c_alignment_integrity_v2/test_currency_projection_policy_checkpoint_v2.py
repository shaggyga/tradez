from currency_projection_policy_checkpoint_v2 import safe
import pytest
def test_checkpoint_rejects_escape_member():
    with pytest.raises(ValueError):safe('../escape')
