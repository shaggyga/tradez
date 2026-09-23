import json

try:
    from oanda_historical_case_index import build_index, render_markdown
except ModuleNotFoundError:
    from trad.oanda_historical_case_index import build_index, render_markdown


def test_case_index_is_deduplicated_hashed_and_does_not_index_itself(tmp_path):
    project = tmp_path.parent
    first = tmp_path / "major_move_case_audits" / "case.md"
    second = tmp_path / "live_case_audits" / "case.json"
    generated = tmp_path / "HISTORICAL_CASE_INDEX_CURRENT.json"
    first.parent.mkdir(parents=True)
    second.parent.mkdir(parents=True)
    first.write_text("case one", encoding="utf-8")
    second.write_text(json.dumps({"case": 2}), encoding="utf-8")
    generated.write_text("{}", encoding="utf-8")

    # build_index paths are project-relative; temporarily provide a report tree
    # nested beneath the module's real project root is not required for the core
    # hash assertions, so use the public candidate contract via monkeypatched root.
    import oanda_historical_case_index as module
    original = module.PROJECT_ROOT
    module.PROJECT_ROOT = project
    try:
        payload = build_index(tmp_path)
    finally:
        module.PROJECT_ROOT = original

    assert payload["file_count"] == 2
    assert len(payload["entries_sha256"]) == 64
    assert all(len(row["sha256"]) == 64 for row in payload["entries"])
    assert "HISTORICAL_CASE_INDEX_CURRENT" not in json.dumps(payload)
    assert "content-hashed inventory" in render_markdown(payload)
