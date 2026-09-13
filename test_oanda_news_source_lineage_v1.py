"""Requirements, including red counterexamples, against the actual collector."""
import copy
import datetime as dt
import json
import sqlite3
import inspect

import pytest
import oanda_local_news_sentiment as news
import oanda_news_source_observation_ledger_v1 as ledger

UTC = dt.timezone.utc
BASE = dt.datetime(2026, 9, 12, 10, tzinfo=UTC)


def at(minute):
    return BASE + dt.timedelta(minutes=minute)


def article(source="A", minute=0, **changes):
    stamp = at(minute).isoformat()
    row = dict(event_id="shared_event", source_id=source, source_name="query_" + source,
        source_kind="google_news", source_contract_id="contract_" + source,
        source_cohort_id="cohort_" + source, source_quality=.8, source_verified=True,
        first_seen_utc=stamp, last_seen_utc=stamp, published_utc=BASE.isoformat(), causal_known_utc=stamp,
        headline="Federal Reserve raises rates", summary="The Federal Reserve raised its policy rate.",
        source_url="https://fixture.invalid/item", domain="fixture.invalid", relevant=True,
        category="policy", scope="currency", currencies=["USD"], currency_scores={"USD": .25},
        directional_bias={"USD": "up"}, generic_sentiment_score=.25, monetary_impulse=.5,
        risk_off_score=0., risk_on_score=0., directional_confidence=.7, severity=.6,
        movement_potential="medium", post_window_minutes=60, observation_clock_trusted=True,
        observation_clock_source="fixture_attested", detail_enriched=False,
        collector_contract_id=news.COLLECTOR_CONTRACT_ID, collector_cohort_id=news.COLLECTOR_COHORT_ID,
        observation_time_contract_id=news.OBSERVATION_TIME_CONTRACT_ID)
    row.update(changes)
    return row


def source_map(source="A"):
    return {source: {"source_id": source, "name": "query_" + source, "kind": "google_news",
        "source_contract_id": "contract_" + source, "source_cohort_id": "cohort_" + source,
        "currencies": ["USD"], "verified": True, "quality": .8}}


@pytest.fixture
def db(tmp_path):
    connection = news.open_database(tmp_path / "news.sqlite")
    yield connection
    connection.close()


def put(db, row, minute):
    return news.upsert_articles(db, [row], at(minute), **clock_kwargs(news.upsert_articles, at(minute)))


def attested(value):
    return value, dict(collector_contract_id=news.COLLECTOR_CONTRACT_ID,
        collector_cohort_id=news.COLLECTOR_COHORT_ID, observation_time_contract_id=news.OBSERVATION_TIME_CONTRACT_ID,
        observation_clock_trusted=True, observation_clock_source="fixture_postcompute")


def clock_kwargs(function, value):
    # Only provider plumbing adapts to005; original requirements/values remain.
    return {"classification_clock_provider": lambda: attested(value)} if "classification_clock_provider" in inspect.signature(function).parameters else {}


def reclassify(db, **kwargs):
    return news.reclassify_stored_articles(db, **kwargs, **clock_kwargs(news.reclassify_stored_articles, at(15)))


def payload(db, event="shared_event"):
    return json.loads(db.execute("SELECT payload_json FROM articles WHERE event_id=?", (event,)).fetchone()[0])


def table_dump(db):
    names = [row[0] for row in db.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name")]
    return {name: db.execute('SELECT * FROM "' + name + '" ORDER BY rowid').fetchall() for name in names}


def test_a_b_a_and_stale_source_observations_keep_current_and_exact_history(db):
    rows = [article(), article("B", 2, summary="new source content"), article("A", 3),
            article("A", 1, summary="old delivery that arrived late")]
    for row, minute in zip(rows, [0, 2, 3, 4]):
        put(db, row, minute)
    active = payload(db)
    assert active["source_id"] == "B"
    assert active["first_seen_utc"] == BASE.isoformat()
    assert active["source_version_first_known_utc"] == at(2).isoformat()
    observations = db.execute("SELECT observation_id FROM article_source_observations_v1 ORDER BY observation_seq").fetchall()
    assert [json.loads(ledger.reconstruct_observation(db, value[0])) for value in observations] == rows
    assert db.execute("SELECT COUNT(*) FROM article_source_versions_v1").fetchone()[0] == 3


def test_unchanged_repeat_does_not_artificially_refresh_causal_availability(db):
    put(db, article(), 0)
    before = payload(db)
    put(db, article(minute=4), 4)
    assert payload(db) == before


def test_original_legacy_ambiguous_source_is_retained_exactly_not_reconstructed(db):
    put(db, article(), 0)
    row = list(db.execute("SELECT * FROM articles").fetchone())
    columns = [col[1] for col in db.execute("PRAGMA table_info(articles)")]
    legacy = dict(zip(columns, row)); old = payload(db)
    for key in ledger.ACTIVE_FIELDS:
        old.pop(key, None)
    old["source_id"] = "unresolved_B"
    legacy["payload_json"] = json.dumps(old, sort_keys=True)
    # A distinct legacy row has no earlier ledger origin; create it using the
    # exact existing schema, retaining mismatched denormalized source identity.
    legacy["event_id"] = "legacy_event"; old["event_id"] = "legacy_event"
    legacy["payload_json"] = json.dumps(old, sort_keys=True)
    db.execute("INSERT INTO articles (" + ",".join(columns) + ") VALUES (" + ",".join("?" for _ in columns) + ")", [legacy[key] for key in columns])
    db.commit()
    put(db, article("C", 5, event_id="legacy_event", source_url="https://fixture.invalid/legacy"), 5)
    origin = db.execute("SELECT origin_status, original_row_json FROM article_source_origins_v1 WHERE canonical_event_id='legacy_event'").fetchone()
    assert origin[0] == "legacy_origin_query_history_unresolved"
    assert json.loads(origin[1]) == legacy


def test_update_projection_failure_rolls_back_source_and_projection(db):
    put(db, article(), 0)
    db.execute("CREATE TRIGGER injected_failure BEFORE INSERT ON article_source_projections_v1 BEGIN SELECT RAISE(ABORT,'fixture_failure'); END")
    before = table_dump(db)
    with pytest.raises(sqlite3.IntegrityError, match="fixture_failure"):
        put(db, article("B", 2), 2)
    assert table_dump(db) == before


def test_upsert_respects_outer_transaction(db):
    db.execute("CREATE TABLE caller_marker(value TEXT)")
    db.commit()
    db.execute("BEGIN")
    db.execute("INSERT INTO caller_marker VALUES('pending')")
    put(db, article(), 0)
    assert db.in_transaction
    db.rollback()
    assert db.execute("SELECT COUNT(*) FROM articles").fetchone()[0] == 0
    assert db.execute("SELECT COUNT(*) FROM caller_marker").fetchone()[0] == 0


def test_real_reclassifier_respects_outer_transaction(db):
    put(db, article(classification_version="old_rules"), 0)
    before = payload(db)
    db.execute("CREATE TABLE caller_marker(value TEXT)"); db.commit()
    db.execute("BEGIN"); db.execute("INSERT INTO caller_marker VALUES('pending')")
    assert reclassify(db, sources=source_map(), since=BASE) == 1
    assert db.in_transaction, "reclassification committed caller-owned transaction"
    db.rollback()
    assert payload(db) == before
    assert db.execute("SELECT COUNT(*) FROM caller_marker").fetchone()[0] == 0


def test_actual_reclassifier_new_derived_values_have_postcomputation_floor(db, monkeypatch):
    put(db, article(classification_version="old_rules", monetary_impulse=-.99), 0)
    monkeypatch.setattr(news, "utc_now", lambda: at(15))
    assert reclassify(db, sources=source_map(), since=BASE) == 1
    updated = payload(db)
    assert updated["monetary_impulse"] != -.99  # Actual deterministic classifier.
    assert news.causal_known_datetime(updated) >= at(15), "new derived value backdated to old source observation"


def test_recent_refresh_failure_keeps_article_and_topic_coherent(db, monkeypatch):
    put(db, article(classification_version="old_rules"), 0)
    before = table_dump(db)
    def fail(*args, **kwargs):
        raise RuntimeError("fixture_topic_computation_failure")
    monkeypatch.setattr(news, "cluster_articles", fail)
    with pytest.raises(RuntimeError, match="fixture_topic_computation_failure"):
        news.refresh_recent_topic_contract(db, sources=source_map(), since=BASE, as_of=at(15),
            **clock_kwargs(news.refresh_recent_topic_contract, at(15)))
    assert table_dump(db) == before, "article classification committed despite failed same-boundary topic refresh"


@pytest.mark.parametrize("action", ["UPDATE", "DELETE"])
def test_appendonly_enforcement_survives_reopen(db, tmp_path, action):
    put(db, article(), 0)
    path = db.execute("PRAGMA database_list").fetchone()[2]
    other = news.open_database(__import__("pathlib").Path(path))
    try:
        for table in ledger.TABLES:
            sql = f"DELETE FROM {table}" if action == "DELETE" else f"UPDATE {table} SET contract_id='changed'"
            with pytest.raises(sqlite3.IntegrityError, match="append_only_source_evidence"):
                other.execute(sql)
            other.rollback()
    finally:
        other.close()


def test_run_cycle_preserves_source_clock_and_postcompute_derived_clock(tmp_path, monkeypatch):
    config = tmp_path / "sources.json"
    source = {"source_id": "A", "name": "Source A", "kind": "rss", "url": "https://fixture.invalid/rss",
              "source_contract_id": "contract_A", "source_cohort_id": "cohort_A", "enabled": True,
              "runtime_supported": True, "verified": True, "direct": True, "currencies": ["USD"]}
    config.write_text(json.dumps({"policy": {"retention_days": 45}, "sources": [source]}))
    output = tmp_path / "outputs"
    clock = {"now": BASE}
    monkeypatch.setattr(news, "utc_now", lambda: clock["now"])
    monkeypatch.setattr(news, "normalized_observation_time", lambda value: (value, {
        "source": "fixture_postcompute", "normalized_utc": news.iso_utc(value), "status": "aligned",
        "contract_id": news.OBSERVATION_TIME_CONTRACT_ID, "trusted_for_prospective_evidence": True}))
    def fetch(source, source_state, **kwargs):
        clock["now"] = at(2)
        return [{"source_id": "A", "source_name": "Source A", "source_kind": "rss", "source_quality": 1.,
            "source_verified": True, "source_direct": True, "source_currencies": ["USD"],
            "source_contract_id": "contract_A", "source_cohort_id": "cohort_A",
            "title": "Federal Reserve raises interest rate", "summary": "The Federal Reserve announced an interest rate hike.",
            "url": "https://fixture.invalid/item", "published_utc": BASE.isoformat()}], {
                **source_state, "last_status": 200, "last_error": "", "parsed_items": 1}
    monkeypatch.setattr(news, "fetch_source", fetch)
    actual_classifier = news.classify_article
    def costly_classifier(*args, **kwargs):
        result = actual_classifier(*args, **kwargs)
        clock["now"] = at(15)
        return result
    monkeypatch.setattr(news, "classify_article", costly_classifier)
    monkeypatch.setattr(news.event_tagger, "discover_instruments", lambda: ["EUR_USD"])
    # The complete source loop, two reclassification callers, and topic input
    # plumbing execute; stop at the final publication boundary before any
    # canonical coverage output paths can be reached.
    class FixturePublicationBoundary(Exception):
        pass
    def finish(*args, **kwargs):
        raise FixturePublicationBoundary()
    monkeypatch.setattr(news, "refresh_pair_aggregation_clock", finish)
    path = output / "local_news_sentiment_v1.sqlite"
    try:
        with pytest.raises(FixturePublicationBoundary):
            news.run_cycle(config_path=config, output_root=output, ledger_path=tmp_path / "ledger.csv",
                event_root=tmp_path / "events", refresh_event_catalog=False)
        with sqlite3.connect(path) as reader:
            rows = reader.execute("SELECT payload_json FROM articles").fetchall()
            assert len(rows) == 1
            value = json.loads(rows[0][0])
            assert value["first_seen_utc"] == at(2).isoformat()
            assert news.causal_known_datetime(value) >= at(15), "source loop used pre-computation clock for derived availability"
    finally:
        key = str(path.resolve())
        for cache_key, connection in list(news._PROCESS_DATABASE_CONNECTIONS.items()):
            if str(__import__("pathlib").Path(cache_key).resolve()) == key:
                news.close_process_database(path, connection)


def test_reclassification_cannot_overwrite_newer_same_source_material(db, monkeypatch):
    put(db, article(classification_version="old_rules"), 0)
    database_path = __import__("pathlib").Path(db.execute("PRAGMA database_list").fetchone()[2])
    actual_classifier = news.classify_article
    expected = {}
    def concurrent_material(*args, **kwargs):
        result = actual_classifier(*args, **kwargs)
        other = news.open_database(database_path)
        try:
            put(other, article(minute=10, summary="new source material published while old row is computing",
                monetary_impulse=-.75, classification_version=news.CLASSIFICATION_VERSION), 10)
            expected.update(payload(other))
        finally:
            other.close()
        return result
    monkeypatch.setattr(news, "classify_article", concurrent_material)
    reclassify(db, sources=source_map(), since=BASE)
    assert payload(db) == expected, "stale reclassification overwrote a newer same-source version"


@pytest.mark.parametrize("table", ["article_source_versions_v1", "article_source_observations_v1",
    "article_source_origins_v1", "article_source_projections_v1", "article_classification_observations_v1"])
def test_appendonly_schema_rejects_replace_of_existing_evidence(db, table):
    put(db, article(), 0)
    if not db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,)).fetchone():
        pytest.skip("classification table belongs to005")
    columns = [row[1] for row in db.execute('PRAGMA table_info("' + table + '")')]
    row = list(db.execute('SELECT * FROM "' + table + '" LIMIT 1').fetchone())
    before = tuple(row)
    row[columns.index("contract_id")] = "altered_evidence_contract"
    sql = 'INSERT OR REPLACE INTO "' + table + '" (' + ','.join(columns) + ') VALUES (' + ','.join('?' for _ in row) + ')'
    with pytest.raises(sqlite3.IntegrityError, match="append_only|conflicting_immutable_evidence_identity"):
        db.execute(sql, row)
    assert db.execute('SELECT * FROM "' + table + '" LIMIT 1').fetchone() == before


@pytest.mark.parametrize("clock_value,provenance_change", [(at(-1), {}), (at(15).replace(tzinfo=None), {}),
    (None, {}), (at(15), {"observation_clock_trusted": "true"}),
    (at(15), {"observation_time_contract_id": "other_contract"})])
def test_invalid_classification_clock_is_retained_without_active_mutation(db, clock_value, provenance_change):
    if "classification_clock_provider" not in inspect.signature(news.reclassify_stored_articles).parameters:
        pytest.skip("separate classification clock belongs to005")
    put(db, article(classification_version="old_rules"), 0)
    before = payload(db)
    provenance = dict(attested(at(15))[1], **provenance_change)
    assert news.reclassify_stored_articles(db, sources=source_map(), since=BASE,
        classification_clock_provider=lambda: (clock_value, provenance)) == 0
    assert payload(db) == before
    rows = db.execute("SELECT clock_status, clock_reasons_json FROM article_classification_observations_v1 ORDER BY observation_seq").fetchall()
    assert len(rows) == 2
    assert rows[-1][0] == "unproven_classification" and json.loads(rows[-1][1])


def test_run_cycle_derived_clock_refusal_does_not_advance_poll_validators(tmp_path, monkeypatch):
    if "classification_clock_provider" not in inspect.signature(news.upsert_articles).parameters:
        pytest.skip("separate classification clock belongs to005")
    source = {"source_id": "A", "name": "Source A", "kind": "rss", "url": "https://fixture.invalid/rss",
        "source_contract_id": "contract_A", "source_cohort_id": "cohort_A", "enabled": True,
        "runtime_supported": True, "verified": True, "direct": True, "currencies": ["USD"]}
    config = tmp_path / "sources.json"
    config.write_text(json.dumps({"policy": {"retention_days": 45}, "sources": [source]}))
    output = tmp_path / "outputs"
    clock = {"now": BASE}
    monkeypatch.setattr(news, "utc_now", lambda: clock["now"])
    monkeypatch.setattr(news, "normalized_observation_time", lambda value: (value, {
        "source": "fixture_postcompute", "normalized_utc": news.iso_utc(value), "status": "aligned",
        "contract_id": news.OBSERVATION_TIME_CONTRACT_ID, "trusted_for_prospective_evidence": value < at(15)}))
    def fetch(source, source_state, **kwargs):
        clock["now"] = at(2)
        return [{"source_id": "A", "source_name": "Source A", "source_kind": "rss", "source_quality": 1.,
            "source_verified": True, "source_direct": True, "source_currencies": ["USD"],
            "source_contract_id": "contract_A", "source_cohort_id": "cohort_A",
            "title": "Federal Reserve raises interest rate", "summary": "The Federal Reserve announced an interest rate hike.",
            "url": "https://fixture.invalid/item", "published_utc": BASE.isoformat()}], {
                **source_state, "last_status": 200, "last_error": "", "parsed_items": 1,
                "etag": "rejected_derivation_etag", "last_success_utc": at(2).isoformat()}
    monkeypatch.setattr(news, "fetch_source", fetch)
    actual_classifier = news.classify_article
    def costly_classifier(*args, **kwargs):
        result = actual_classifier(*args, **kwargs)
        clock["now"] = at(15)
        return result
    monkeypatch.setattr(news, "classify_article", costly_classifier)
    result = news.run_cycle(config_path=config, output_root=output, ledger_path=tmp_path / "ledger.csv",
        event_root=tmp_path / "events", refresh_event_catalog=False)
    assert "blocked" in result["status"]
    with sqlite3.connect(output / "local_news_sentiment_v1.sqlite") as reader:
        assert reader.execute("SELECT COUNT(*) FROM articles").fetchone()[0] == 0
        assert reader.execute("SELECT COUNT(*) FROM article_classification_observations_v1 WHERE clock_status='unproven_classification'").fetchone()[0] == 1
    state = json.loads((output / "collector_state_v1.json").read_text())
    assert state["sources"]["A"].get("etag") != "rejected_derivation_etag", "poll validator advanced although the derived article was refused"


def test_trusted_retry_keeps_first_retained_source_clock_and_later_derivation(db):
    if "classification_clock_provider" not in inspect.signature(news.upsert_articles).parameters:
        pytest.skip("separate classification clock belongs to005")
    initial = article()
    assert news.upsert_articles(db, [initial], BASE, classification_clock_provider=None) == (0, 0)
    assert db.execute("SELECT COUNT(*) FROM articles").fetchone()[0] == 0
    assert put(db, article(minute=10), 10) == (1, 0)
    value = payload(db)
    assert value["first_seen_utc"] == initial["first_seen_utc"]
    assert value["published_utc"] == initial["published_utc"]
    assert value["observation_clock_source"] == initial["observation_clock_source"]
    assert value["source_version_first_known_utc"] == BASE.isoformat()
    assert value["classification_available_utc"] == at(10).isoformat()
    assert news.causal_known_datetime(value) == at(10)


def member():
    return {"event_id": "event_A", "source_id": "source_A", "source_name": "Publisher A",
        "publisher_url": "https://a.fixture.invalid", "source_url": "https://a.fixture.invalid/story",
        "headline": "Central bank announces policy rate increase following scheduled committee decision",
        "published_utc": BASE.isoformat(), "first_seen_utc": BASE.isoformat(), "source_verified": True,
        "source_direct": True, "source_quality": .8, "directional_confidence": .6, "currency_scores": {"USD": .5},
        "forward_signal_timely": True, "forward_timeliness_limit_minutes": 30, "estimated_reaction_horizon_minutes": 60,
        "reports_prior_market_move": False, "context_only": False, "classification_version": "fixture",
        "category": "monetary_policy", "scope": "currencies", "direct_currencies": ["USD"], "inferred_currencies": []}


def topic():
    return {"topic_id": "topic_A", "headline": "Original retained topic", "first_seen_utc": BASE.isoformat(),
        "causal_known_utc": BASE.isoformat(), "published_utc": BASE.isoformat(), "currency_scores": {"USD": .5},
        "post_window_minutes": 60, "directional_publish_eligible": True, "classification_version": "fixture"}


@pytest.mark.parametrize("minute,expected", [(10, "context"), (15, "directional"), (61, "context")])
def test_new_guard_uses_derived_availability_and_original_story_expiry(minute, expected):
    if not hasattr(news, "classification_publication_as_of"):
        pytest.skip("story-age guard belongs to006")
    import oanda_news_causal_aggregation_guard_v2 as guard
    import oanda_news_classification_observation_v1 as derived
    row = dict(member(), causal_known_utc=at(15).isoformat(), classification_observation_contract=derived.CONTRACT,
        classification_clock_status="valid_attested_classification", classification_first_known_utc=at(15).isoformat(),
        classification_available_utc=at(15).isoformat(), source_evidence_contract="retained_story_body_age_v1_20260912",
        source_evidence_content_sha256="fixture_body", source_evidence_available_utc=BASE.isoformat())
    result = guard.guard_topic(topic(), [row], as_of=at(minute))
    review = guard.validate_guarded_topic(result, as_of=at(minute))
    assert review["status"] == expected
    evidence = result["causal_aggregation_guard"]["member_evidence"][0]
    assert evidence["available_utc"] == at(15).isoformat()
    assert evidence["expires_utc"] == at(60).isoformat()


def test_new_guard_dispatches_old_guard_receipt_without_relabeling():
    if not hasattr(news, "classification_publication_as_of"):
        pytest.skip("story-age guard belongs to006")
    import oanda_news_causal_aggregation_guard_v1 as original
    import oanda_news_causal_aggregation_guard_v2 as new
    result = original.guard_topic(topic(), [member()], as_of=at(15))
    before = copy.deepcopy(result)
    assert new.validate_guarded_topic(result, as_of=at(15)) == original.validate_guarded_topic(result, as_of=at(15))
    assert result == before
