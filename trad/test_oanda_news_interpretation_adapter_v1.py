import copy
import datetime as dt
from unittest.mock import patch

import pytest

import oanda_local_news_sentiment as collector
from oanda_news_interpretation_adapter_v1 import classify_article_with_interpretation
from oanda_news_interpretation_v1 import interpret_headline


@pytest.mark.parametrize('title,summary', [
    ('ECB Raises Rates to 2.5% as Energy Inflation Surges', ''),
    ('Fed may turn hawkish - Reuters', ''),
    ('EUR/USD rebounds after Fed hike', 'Already reported price response.'),
    ('Weaker oil gives politically-hit euro mild relief', ''),
    ('US inflation held steady at 3.4%', ''),
    ('A quiet morning', ''),
    ('', ''),
    (None, None),
])
def test_opt_in_preserves_every_original_field_and_input(title, summary):
    now = dt.datetime(2026, 9, 30, tzinfo=dt.timezone.utc)
    raw = {'title': title, 'summary': summary, 'source_id': 'test',
           'source_name': 'Reuters', 'url': 'https://example.com/a',
           'published_utc': now.isoformat(), 'source_currencies': ['EUR']}
    saved = copy.deepcopy(raw)
    original = collector.classify_article(raw, first_seen=now)
    enriched = classify_article_with_interpretation(raw, first_seen=now)
    interpretation = enriched.pop('headline_interpretation')
    assert enriched == original
    assert raw == saved
    assert 'headline_interpretation' not in original
    clean = collector.headline_content(original['headline'], publisher_name='Reuters')
    assert interpretation == interpret_headline(clean)
    assert interpretation['forecast_eligible'] is False
    assert enriched['execution_eligible'] is False
    assert enriched['can_place_orders'] is False


def test_wrapper_does_not_mutate_classifier_result_or_reclassify():
    record = {'headline': 'Fed hikes rates - Reuters', 'source_name': 'Reuters',
              'clock': {'first_seen': 'unchanged'}, 'execution_eligible': False}
    original = copy.deepcopy(record)
    now = dt.datetime(2026, 9, 30, tzinfo=dt.timezone.utc)
    with patch.object(collector, 'classify_article', return_value=record) as classify:
        enriched = classify_article_with_interpretation({}, first_seen=now)
    classify.assert_called_once_with({}, first_seen=now)
    assert record == original
    assert enriched is not record


def test_classifier_failure_is_not_hidden():
    now = dt.datetime(2026, 9, 30, tzinfo=dt.timezone.utc)
    with patch.object(collector, 'classify_article', side_effect=ValueError('invalid input')):
        with pytest.raises(ValueError, match='invalid input'):
            classify_article_with_interpretation({}, first_seen=now)
