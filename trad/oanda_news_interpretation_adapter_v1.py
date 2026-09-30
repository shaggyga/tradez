"""Opt-in research interpretation without changing the qualified collector.

The returned interpretation is newly computed text evidence, never a historical
prediction. No collector, registry, score, availability clock or permission is
updated by this wrapper. Existing producers continue using the original collector.
"""
import datetime as dt
from typing import Any, Mapping

import oanda_local_news_sentiment as collector
from oanda_news_interpretation_v1 import interpret_headline


def classify_article_with_interpretation(
    raw: Mapping[str, Any], *, first_seen: dt.datetime
) -> dict[str, Any]:
    """Classify once and append the separate interpretation to a new record."""
    article = collector.classify_article(raw, first_seen=first_seen)
    headline = collector.headline_content(
        article['headline'], publisher_name=article['source_name']
    )
    return {**article, 'headline_interpretation': interpret_headline(headline)}
