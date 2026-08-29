import oanda_macro_consensus_access_audit as audit


def test_finnhub_forbidden_is_explicit_not_misreported_as_missing_key():
    assert audit.classify_finnhub(403, None) == (
        "credential_present_but_economic_calendar_access_forbidden"
    )


def test_offline_audit_never_persists_credentials(tmp_path):
    output = tmp_path / "audit.json"
    report = tmp_path / "audit.md"
    secret = "sensitive-value"
    result = audit.run(
        output,
        report,
        environment={
            "FINNHUB_API_KEY": secret,
            "ALPHA_VANTAGE_API_KEY": secret,
            "FRED_API_KEY": secret,
        },
        probe_network=False,
    )
    assert result["secrets_persisted"] is False
    assert secret not in output.read_text()
    assert secret not in report.read_text()
    assert result["status"] == "blocked_no_accessible_consensus_provider"
