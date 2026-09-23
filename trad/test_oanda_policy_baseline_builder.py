import datetime as dt

import oanda_policy_baseline_builder as builder


UTC = dt.timezone.utc


def test_document_class_keeps_incompatible_policy_documents_separate():
    assert builder.policy_document_class("Summary of Opinions at the meeting") == "summary_of_opinions"
    assert builder.policy_document_class("Minutes of the Monetary Policy Committee") == "minutes_or_account"
    assert builder.policy_document_class("Monetary Policy Report") == "monetary_policy_report"
    assert builder.policy_document_class("Federal Reserve issues FOMC statement") == "decision_statement"


def test_freeze_latest_is_per_source_currency_and_document_class():
    common = {
        "source_id": "central_bank",
        "currency": "USD",
        "summary": "x" * 300,
        "published_utc": "2026-07-01T00:00:00+00:00",
        "source_url": "https://example.test",
    }
    rows = builder.freeze_latest(
        [
            {**common, "event_id": "old", "document_class": "decision_statement", "known_utc": "2026-07-01T00:01:00+00:00"},
            {**common, "event_id": "new", "document_class": "decision_statement", "known_utc": "2026-08-01T00:01:00+00:00"},
            {**common, "event_id": "minutes", "document_class": "minutes_or_account", "known_utc": "2026-07-15T00:01:00+00:00"},
        ]
    )
    assert {row["event_id"] for row in rows} == {"new", "minutes"}
    assert all(row["execution_eligible"] is False for row in rows)
