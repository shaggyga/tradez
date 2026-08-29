from trad.oanda_runtime_dependency_audit import classify_worker, parse_supervisor_workers


def test_supervisor_parser_groups_launcher_and_declared_artifacts() -> None:
    text = '''
        $managed += Start-ManagedProcess `
            -Name "worker_one" `
            -Needle "worker.py" `
            -Arguments @(
                (Join-Path $Trad "worker.py"),
                "--state", (Join-Path $State "worker_state.json")
            )
        $managed += Start-ManagedProcess `
            -Name "worker_two" `
            -Arguments @((Join-Path $Trad "worker_two.py"))
    '''
    result = parse_supervisor_workers(text)
    assert set(result) == {"worker_one", "worker_two"}
    assert result["worker_one"]["script"] == "worker.py"
    assert result["worker_one"]["declared_artifacts"] == ["worker_state.json"]


def test_classification_keeps_evidence_and_only_queues_retirement() -> None:
    assert classify_worker("canonical_outcome_worker", {})[0] == "frozen_outcome_maturation"
    category, action = classify_worker("practice_002_no_gpt_movement_ledger", {})
    assert category == "retire_candidate"
    assert "prove" in action


def test_unknown_freshness_failure_is_repair_not_retirement() -> None:
    runtime = {"freshness": {"reason": "stale_output"}}
    category, _ = classify_worker("new_source", runtime)
    assert category == "broken_needs_repair"
