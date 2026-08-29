import datetime as dt

import oanda_local_news_sentiment as news


UTC = dt.timezone.utc


def _prepared(rows):
    output = []
    for raw in rows:
        article = dict(raw)
        if article.get("topic_clustered"):
            output.append(article)
            continue
        headline_key = news.normalized_headline(article.get("headline"))
        article["_headline_key"] = headline_key
        article["_cluster_signature"] = str(
            article.get("topic_signature") or f"headline|{headline_key}"
        )
        output.append(article)
    output.sort(
        key=lambda row: (
            news.parse_datetime(row.get("published_utc"))
            or news.parse_datetime(row.get("first_seen_utc"))
            or dt.datetime.min.replace(tzinfo=UTC),
            str(row.get("event_id") or ""),
        )
    )
    return output


def _legacy_groups(candidates):
    """Reference the pre-index reverse scan for exact equivalence testing."""

    groups = []
    for article in candidates:
        if article.get("topic_clustered"):
            groups.append([article])
            continue
        article_time = news.parse_datetime(
            article.get("published_utc")
        ) or news.parse_datetime(article.get("first_seen_utc"))
        matched = None
        for rows in reversed(groups):
            prior = rows[-1]
            if prior.get("topic_clustered"):
                continue
            article_scheduled = news.clean_text(article.get("scheduled_utc"))
            prior_scheduled = news.clean_text(prior.get("scheduled_utc"))
            if (
                bool(article.get("structured_event"))
                or bool(prior.get("structured_event"))
            ) and article_scheduled != prior_scheduled:
                continue
            exact_match = bool(
                article.get("_headline_key")
                and article.get("_headline_key") == prior.get("_headline_key")
            )
            semantic_match = bool(
                article.get("_cluster_signature")
                and (
                    article.get("_cluster_signature")
                    == prior.get("_cluster_signature")
                    or (
                        str(article.get("category") or "")
                        in news.CLUSTER_ENTITY_MATCH_CATEGORIES
                        and str(article.get("category") or "")
                        == str(prior.get("category") or "")
                        and str(article.get("topic_action") or "")
                        == str(prior.get("topic_action") or "")
                        and bool(
                            set(article.get("topic_entities") or ())
                            & set(prior.get("topic_entities") or ())
                        )
                    )
                )
                and news.headlines_support_same_claim(
                    article.get("headline"), prior.get("headline")
                )
            )
            if not (exact_match or semantic_match):
                continue
            prior_time = news.parse_datetime(
                prior.get("published_utc")
            ) or news.parse_datetime(prior.get("first_seen_utc"))
            if exact_match or (
                article_time is not None
                and prior_time is not None
                and abs((article_time - prior_time).total_seconds()) <= 18 * 3600
            ):
                matched = rows
                break
        if matched is None:
            groups.append([article])
        else:
            matched.append(article)
    return groups


def _members(groups):
    return [[row["event_id"] for row in group] for group in groups]


def test_indexed_clustering_preserves_legacy_group_selection_and_tail_updates():
    start = dt.datetime(2026, 8, 1, tzinfo=UTC)

    def row(identifier, hours, headline, signature, **extra):
        return {
            "event_id": identifier,
            "headline": headline,
            "topic_signature": signature,
            "published_utc": (start + dt.timedelta(hours=hours)).isoformat(),
            "first_seen_utc": (start + dt.timedelta(hours=hours)).isoformat(),
            **extra,
        }

    candidates = _prepared(
        [
            row("a", 0, "Central bank outlook alpha", "policy|one"),
            # Same signature updates group zero's tail identity.
            row("b", 1, "Central bank outlook beta", "policy|one"),
            # The old alpha headline must no longer point at group zero.
            row("c", 2, "Central bank outlook alpha", "policy|two"),
            # Same semantic signature after 18 hours must split.
            row("d", 25, "Central bank outlook gamma", "policy|one"),
            # Exact headline matches regardless of elapsed time.
            row("e", 50, "Central bank outlook gamma", "policy|three"),
            row(
                "f",
                51,
                "Rate decision calendar",
                "calendar|one",
                structured_event=True,
                scheduled_utc="2026-09-01T12:00:00+00:00",
            ),
            row(
                "g",
                52,
                "Rate decision calendar",
                "calendar|one",
                structured_event=True,
                scheduled_utc="2026-10-01T12:00:00+00:00",
            ),
            row(
                "h",
                53,
                "Oil supply shock escalates in Gulf",
                "commodity|one",
                category="commodity_shock",
                topic_action="oil_up",
                topic_entities=["oil", "gulf"],
            ),
            row(
                "i",
                54,
                "Oil supply shock deepens in Gulf",
                "commodity|two",
                category="commodity_shock",
                topic_action="oil_up",
                topic_entities=["gulf"],
            ),
            row(
                "j",
                55,
                "Already clustered immutable topic",
                "clustered|one",
                topic_clustered=True,
            ),
        ]
    )

    assert _members(news._cluster_candidate_groups(candidates)) == _members(
        _legacy_groups(candidates)
    )


def test_index_keys_cover_every_legacy_match_family():
    article = {
        "_headline_key": "oil supply shock",
        "_cluster_signature": "commodity|oil|up",
        "category": "commodity_shock",
        "topic_action": "oil_up",
        "topic_entities": ["oil", "gulf"],
    }

    assert news._cluster_candidate_index_keys(article) == {
        ("headline", "oil supply shock"),
        ("signature", "commodity|oil|up"),
        ("entity", "commodity_shock", "oil_up", "oil"),
        ("entity", "commodity_shock", "oil_up", "gulf"),
    }
