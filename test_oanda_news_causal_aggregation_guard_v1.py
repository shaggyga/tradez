import copy
import datetime as dt
import hashlib
import json

import pytest

import oanda_news_causal_aggregation_guard_v1 as guard

UTC=dt.timezone.utc
START=dt.datetime(2026,9,7,16,0,tzinfo=UTC)

def row(name="a", *, published=0, seen=1, verified=False, headline=None):
    return {"event_id":"event_"+name,"source_id":"feed_"+name,"source_name":"Publisher "+name,
            "publisher_url":"https://"+name+".example","source_url":"https://"+name+".example/story",
            "headline":headline or "Central bank announces policy rate increase following scheduled committee decision",
            "published_utc":(START+dt.timedelta(minutes=published)).isoformat(),
            "first_seen_utc":(START+dt.timedelta(minutes=seen)).isoformat(),
            "source_verified":verified,"source_direct":verified,"source_quality":0.8,"directional_confidence":0.6,
            "currency_scores":{"USD":0.5},"forward_signal_timely":True,"forward_timeliness_limit_minutes":30,
            "estimated_reaction_horizon_minutes":360,"reports_prior_market_move":False,"context_only":False,
            "classification_version":"original_test_version","category":"monetary_policy","scope":"currencies",
            "direct_currencies":["USD"],"inferred_currencies":[],"semantic_claims":[]}

def second(**kwargs):
    return row("b",headline="Central bank announces policy rate increase after scheduled committee decision",**kwargs)

def topic():
    return {"topic_id":"topic_test","headline":"Original retained topic","first_seen_utc":row()["first_seen_utc"],
            "causal_known_utc":row()["first_seen_utc"],"published_utc":row()["published_utc"],
            "currency_scores":{"USD":0.5},"post_window_minutes":360,"directional_publish_eligible":True,"classification_version":"original_test_version"}

NOW=START+dt.timedelta(minutes=30)

def test_two_current_independent_members_preserve_latest_availability_and_earliest_expiry():
    source=topic(); members=[row(),second(published=19,seen=20)]
    untouched=copy.deepcopy((source,members)); result=guard.guard_topic(source,members,as_of=NOW)
    assert (source,members)==untouched
    assert result["currency_scores"]=={"USD":0.5}
    assert result["first_seen_utc"]==source["first_seen_utc"]
    assert result["direction_available_utc"]==(START+dt.timedelta(minutes=20)).isoformat()
    assert result["direction_expires_utc"]==(START+dt.timedelta(minutes=361)).isoformat()
    assert result["forward_source_count"]==2
    assert guard.validate_guarded_topic(result,as_of=NOW)["status"]=="directional"
    assert guard.guard_topic(result,None,as_of=NOW)==result

def test_current_real_failure_late_corroborator_cannot_borrow_first_clock():
    first=row(headline="Blockade Pushes Iran Output Onto Domestic Market",published=32.65,seen=34.764866)
    late=row("b",headline="Trump's blockade pushes Iran toward military escalation",published=79.383333,seen=135.41739)
    late["forward_signal_timely"]=False
    result=guard.guard_topic(topic(),[first,late],as_of=START+dt.timedelta(minutes=325))
    assert result["currency_scores"]=={}
    assert result["forward_source_count"]==0
    assert result["context_reason"]=="member_independent_corroboration_missing"
    assert "member_arrived_late" in result["causal_aggregation_guard"]["member_evidence"][1]["reasons"]

def test_broad_category_cannot_join_different_claims_even_if_both_timely():
    first=row(headline="Blockade Pushes Iran Output Onto Domestic Market")
    other=row("b",headline="Trump's blockade pushes Iran toward military escalation")
    result=guard.guard_topic(topic(),[first,other],as_of=NOW)
    assert result["currency_scores"]=={}
    assert result["context_reason"]=="member_claims_not_coherent"

@pytest.mark.parametrize("field",["reports_prior_market_move","context_only","non_catalyst_context","secondary_analysis_context","source_listing_bootstrap","directional_research_only","detail_enrichment_research_only"])
def test_every_member_context_guard_excludes_own_score(field):
    a=row(verified=True); b=second();b[field]=True;b["currency_scores"]={"EUR":0.9}
    result=guard.guard_topic(topic(),[a,b],as_of=NOW)
    assert result["currency_scores"]=={"USD":0.5}
    assert result["forward_source_count"]==1
    assert result["source_names"]==["Publisher a"]
    assert result["direct_currencies"]==["USD"]

@pytest.mark.parametrize("field",["first_seen_utc","published_utc","detail_available_utc","numeric_causal_known_utc","publication_clock_known_utc","causal_known_utc","observed_available_utc"])
def test_future_member_clocks_do_not_publish(field):
    a=row(verified=True);a[field]=(NOW+dt.timedelta(seconds=1)).isoformat()
    result=guard.guard_topic(topic(),[a],as_of=NOW)
    assert result["currency_scores"]=={}

@pytest.mark.parametrize("value",[None,"", "2026-09-07T16:01:00", "not a clock", 12])
def test_first_seen_requires_exact_aware_clock(value):
    a=row(verified=True);a["first_seen_utc"]=value
    assert guard.guard_topic(topic(),[a],as_of=NOW)["currency_scores"]=={}

def test_later_mapping_availability_never_renews_original_expiry():
    a=row(verified=True);a["observed_available_utc"]=(START+dt.timedelta(minutes=500)).isoformat()
    result=guard.guard_topic(topic(),[a],as_of=START+dt.timedelta(minutes=501))
    evidence=result["causal_aggregation_guard"]["member_evidence"][0]
    assert evidence["expires_utc"]==(START+dt.timedelta(minutes=361)).isoformat()
    assert evidence["available_utc"]==a["observed_available_utc"]
    assert "member_reaction_window_expired" in evidence["reasons"]
    assert result["currency_scores"]=={}

def test_direction_expires_at_earliest_original_member_deadline():
    result=guard.guard_topic(topic(),[row(),second(published=19,seen=20)],as_of=NOW)
    expired=guard.validate_guarded_topic(result,as_of=START+dt.timedelta(minutes=362))
    assert expired["status"]=="context" and expired["currency_scores"]=={}

@pytest.mark.parametrize("mode",["same_publisher","exact_syndication","same_event_id"])
def test_duplicates_are_not_independent_support(mode):
    a=row();b=second()
    if mode=="same_publisher":b["publisher_url"]=a["publisher_url"]
    if mode=="exact_syndication":b["headline"]=a["headline"]
    if mode=="same_event_id":b["event_id"]=a["event_id"]
    result=guard.guard_topic(topic(),[a,b],as_of=NOW)
    assert result["currency_scores"]=={} and result["forward_source_count"]==0

def test_transitive_syndication_cannot_inflate_support():
    a=row(); bx=row("b");by=second();cy=row("c",headline=by["headline"])
    by["event_id"]="event_b_second_report"
    result=guard.guard_topic(topic(),[a,bx,by,cy],as_of=NOW)
    assert result["currency_scores"]=={}

def test_category_wrapper_is_not_independent_syndication():
    a=row();b=row("b",headline="Business News | "+a["headline"]+" - Publisher b")
    assert guard.guard_topic(topic(),[a,b],as_of=NOW)["currency_scores"]=={}

@pytest.mark.parametrize("headline",["Central bank denies policy rate increase following scheduled committee decision","Central bank announces policy rate increase to 4 percent following scheduled committee decision"])
def test_negated_or_different_numeric_claim_is_not_corroboration(headline):
    assert guard.guard_topic(topic(),[row(),row("b",headline=headline)],as_of=NOW)["currency_scores"]=={}

def test_matching_explicit_claims_allow_independent_paraphrases():
    a=row();b=row("b",headline="A separately worded verified description")
    a["semantic_claims"]=b["semantic_claims"]=[{"subject":"bank","action":"raises","value":"4"}]
    assert guard.guard_topic(topic(),[a,b],as_of=NOW)["currency_scores"]=={"USD":0.5}

def test_zero_aggregate_is_context_not_directional():
    a=row();b=second();b["currency_scores"]={"USD":-0.5}
    result=guard.guard_topic(topic(),[a,b],as_of=NOW)
    assert result["context_only"] and not result["directional_publish_eligible"]
    assert guard.validate_guarded_topic(result,as_of=NOW)["status"]=="context"

@pytest.mark.parametrize("field,value",[("source_quality",float("nan")),("directional_confidence",float("inf")),("currency_scores",{"USD":2}),("currency_scores",{"bad":0.5}),("currency_scores",{}),("forward_timeliness_limit_minutes",31),("estimated_reaction_horizon_minutes",0)])
def test_invalid_scores_and_numeric_metadata_withhold(field,value):
    a=row(verified=True);a[field]=value
    assert guard.guard_topic(topic(),[a],as_of=NOW)["currency_scores"]=={}

def test_no_legacy_topic_can_supply_unsealed_forward_scores():
    result=guard.guard_topic(topic(),None,as_of=NOW)
    assert result["currency_scores"]=={}
    with pytest.raises(ValueError,match="missing"):guard.validate_guarded_topic(result,as_of=NOW)

@pytest.mark.parametrize("identity",[[],{},None,12])
def test_malformed_member_identity_withholds_without_crashing(identity):
    malformed=row(verified=True);malformed["event_id"]=identity
    result=guard.guard_topic(topic(),[malformed],as_of=NOW)
    assert result["currency_scores"]=={}

@pytest.mark.parametrize("field,value",[("currency_scores",{"EUR":1}),("headline","changed"),("causal_known_utc","2026-09-07T16:00:00+00:00"),("first_seen_utc","2026-09-07T15:00:00+00:00"),("direction_expires_utc","2027-01-01T00:00:00Z")])
def test_public_fields_cannot_disagree_with_seal(field,value):
    result=guard.guard_topic(topic(),[row(verified=True)],as_of=NOW);result[field]=value
    with pytest.raises(ValueError,match="public_fields"):guard.validate_guarded_topic(result,as_of=NOW)

def test_forged_resealed_member_does_not_match_replay():
    result=guard.guard_topic(topic(),[row(verified=True)],as_of=NOW)
    envelope=result["causal_aggregation_guard"];envelope["members"][0]["currency_scores"]={"USD":-1}
    material={k:v for k,v in envelope.items() if k!="sha256"}
    envelope["sha256"]=hashlib.sha256(guard._canonical(material)).hexdigest()
    with pytest.raises(ValueError,match="replay_mismatch"):guard.validate_guarded_topic(result,as_of=NOW)

def test_bad_seal_and_future_publication_withhold():
    result=guard.guard_topic(topic(),[row(verified=True)],as_of=NOW)
    with pytest.raises(ValueError,match="future"):guard.validate_guarded_topic(result,as_of=NOW-dt.timedelta(seconds=1))
    result["causal_aggregation_guard"]["sha256"]="0"*64
    with pytest.raises(ValueError,match="hash_mismatch"):guard.validate_guarded_topic(result,as_of=NOW)

@pytest.mark.parametrize("members",[[],[{}]*129,[{"headline":"x"*(guard.MAX_EVIDENCE_BYTES+1)}]])
def test_hard_member_and_byte_bounds(members):
    assert guard.guard_topic(topic(),members,as_of=NOW)["currency_scores"]=={}

def test_awareness_required_and_orders_always_false():
    with pytest.raises(ValueError):guard.guard_topic(topic(),[row()],as_of=dt.datetime(2026,9,7))
    source=topic();source.update(can_place_orders=True,execution_eligible=True,research_only=False)
    result=guard.guard_topic(source,[row(verified=True)],as_of=NOW)
    assert result["can_place_orders"] is False and result["execution_eligible"] is False and result["research_only"] is True

def test_producer_binds_new_version_and_rechecks_preclustered_clocks():
    import oanda_local_news_sentiment as news
    from oanda_news_classification_contract import NEWS_CLASSIFICATION_VERSION,NEWS_CLASSIFICATION_VERSION_V164
    assert NEWS_CLASSIFICATION_VERSION==guard.CLASSIFICATION_VERSION
    assert NEWS_CLASSIFICATION_VERSION_V164.endswith("v164_conflict_duration_recap_guard")
    a=row(verified=True);a.update(topic_signature="monetary_policy|USD|hike",relevant=True)
    result=news.cluster_articles([a],as_of=NOW)[0]
    assert result["currency_scores"]=={"USD":0.5}
    assert guard.validate_guarded_topic(result,as_of=NOW)["status"]=="directional"
    later=news.cluster_articles([result],as_of=START+dt.timedelta(minutes=362))[0]
    assert later["currency_scores"]=={}

def test_new_cutoff_recomputes_expired_inputs_instead_of_retimestamping(monkeypatch):
    import oanda_local_news_sentiment as news
    a=row(verified=True);a["estimated_reaction_horizon_minutes"]=1
    a.update(topic_signature="monetary_policy|USD|hike",post_window_minutes=360,relevant=True)
    earlier=START+dt.timedelta(minutes=1,seconds=30)
    old=news.build_pair_scores([a],["EUR_USD"],as_of=earlier)
    assert old["pairs"]["EUR_USD"]["direction"]=="SHORT"
    later=START+dt.timedelta(minutes=3)
    monkeypatch.setattr(news,"utc_now",lambda:later)
    monkeypatch.setattr(news,"normalized_observation_time",lambda value:(value,{"trusted_test":True}))
    monkeypatch.setattr(news,"prospective_clock_attestation",lambda value:value.get("trusted_test") is True)
    cutoff,_=news.refresh_pair_aggregation_clock(earlier)
    fresh=news.build_pair_scores([a],["EUR_USD"],as_of=cutoff)
    assert fresh["pairs"]["EUR_USD"]["direction"]=="NEUTRAL"
    assert a["first_seen_utc"]==row()["first_seen_utc"]

@pytest.mark.parametrize("mode",["untrusted","backwards"])
def test_publication_cutoff_clock_failures_withhold(monkeypatch,mode):
    import oanda_local_news_sentiment as news
    sampled=NOW-dt.timedelta(seconds=1) if mode=="backwards" else NOW
    monkeypatch.setattr(news,"utc_now",lambda:sampled)
    monkeypatch.setattr(news,"normalized_observation_time",lambda value:(value,{}))
    monkeypatch.setattr(news,"prospective_clock_attestation",lambda value:mode!="untrusted")
    with pytest.raises(ValueError,match="aggregation_clock"):
        news.refresh_pair_aggregation_clock(NOW)

def test_current_snapshot_keeps_one_proof_and_distinguishes_no_evidence():
    proof=guard.guard_topic(topic(),[row(verified=True)],as_of=NOW)
    current=guard.build_current_news_snapshot([proof,copy.deepcopy(proof)],as_of=NOW)
    assert current["status"]=="current" and current["news_state"]=="directional"
    assert current["topic_count"]==1 and current["duplicate_topics_collapsed"]==1
    assert guard.validate_guarded_topic(current["topics"][0],as_of=NOW)["status"]=="directional"
    empty=guard.build_current_news_snapshot([],as_of=NOW)
    assert empty["status"]=="current" and empty["news_state"]=="no_current_evidence"

def test_current_snapshot_missing_proof_is_unavailable_not_neutral():
    result=guard.build_current_news_snapshot([topic()],as_of=NOW)
    assert result["status"]=="unavailable" and result["news_state"]=="unavailable" and not result["topics"]

def test_current_snapshot_conflicting_identity_withholds_every_topic():
    first=guard.guard_topic(topic(),[row(verified=True)],as_of=NOW)
    changed=row(verified=True);changed["currency_scores"]={"USD":-0.5}
    second=guard.guard_topic(topic(),[changed],as_of=NOW)
    result=guard.build_current_news_snapshot([first,second],as_of=NOW)
    assert result["status"]=="unavailable" and result["topics"]==[]
    assert result["errors"]==["conflicting_current_topic_identity"]

def test_current_snapshot_recomputes_context_and_omits_expired_display_windows():
    a=row(verified=True);a["estimated_reaction_horizon_minutes"]=1
    proof=guard.guard_topic(topic(),[a],as_of=START+dt.timedelta(minutes=1))
    context=guard.build_current_news_snapshot([proof],as_of=NOW)
    assert context["status"]=="current" and context["news_state"]=="context_only"
    assert context["topics"][0]["currency_scores"]=={}
    expired=guard.build_current_news_snapshot([proof],as_of=START+dt.timedelta(minutes=361))
    assert expired["news_state"]=="no_current_evidence" and not expired["topics"]

def test_current_snapshot_topic_and_serialized_byte_bounds(monkeypatch):
    proof=guard.guard_topic(topic(),[row(verified=True)],as_of=NOW)
    other_topic=topic();other_topic["topic_id"]="other_topic"
    other=guard.guard_topic(other_topic,[row(verified=True)],as_of=NOW)
    monkeypatch.setattr(guard,"MAX_CURRENT_TOPICS",1)
    assert guard.build_current_news_snapshot([proof,other],as_of=NOW)["status"]=="unavailable"
    monkeypatch.setattr(guard,"MAX_CURRENT_SNAPSHOT_BYTES",100)
    assert guard.build_current_news_snapshot([proof],as_of=NOW)["status"]=="unavailable"
