import hashlib
import json
from pathlib import Path

out = Path(__file__).parent
root = out.parent / "trad"
path = out / "RICH_MAPPER_VALIDATION.json"
proof = json.loads(path.read_text())
before = path.read_bytes()
saved = out / "RICH_MAPPER_VALIDATION_metadata_collision_original.json"
assert not saved.exists()
saved.write_bytes(before)
sources = [root / "oanda_feature_move_mapping_v1.py", root / "oanda_feature_observations_v1.py",
           out.parent / "git_publication_20260913/mapper_performance_review_001/source_001/oanda_feature_move_mapping_v1.py"]
proof["source_sha256"] = {str(p): hashlib.sha256(p.read_bytes()).hexdigest() for p in sources}
proof["metadata_correction"] = "Original receipt used filename keys and the predecessor mapper hash overwrote the current mapper hash. Full path keys disambiguate unchanged tested sources; results and measured time unchanged. Original receipt retained."
path.write_text(json.dumps(proof, indent=2) + "\n", encoding="utf-8")
receipts = [path,
    out.parent / "git_publication_20260913/producer_file_cycle_v3/PRODUCER_FILE_CYCLE_RECEIPT.json",
    out.parent / "git_publication_20260913/feature_forward_001/summary_parity_001/RECEIPT.json",
    out.parent / "git_publication_20260913/forward_independent_review_001/FORWARD_PEER_REVIEW_002.json"]
context = {
    "supervisor_syntax": "PowerShell AST parse passed; no execution",
    "peer_review_scope": "13 independently authored forward tests rerun in final integrated suite; earlier accepted peer receipt predates performance/source successors",
    "receipts": {str(p): hashlib.sha256(p.read_bytes()).hexdigest() for p in receipts},
    "rich_mapper_seconds": proof["new_shared_reader_seconds"],
    "rich_mapper_full_output_parity": True,
    "failed_attempts_retained": ["integration_tests_001.xml", "integration_tests_002.xml", "integrated_tests_attempt_001.xml"],
    "post_test_changes": "Supervisor argument indentation only and documentation; PowerShell AST checked after formatting",
    "activation": "Not attempted; prior automatic approval review rejected clock-monitor activation with 'blocked by policy'",
    "live_market_health_verified": False,
    "prediction_or_profit_improvement_established": False,
    "remaining": ["Live activation and fresh-input verification", "Continuous native M1 freshness", "Verified news adapter", "Independent predictive/after-cost evaluation"],
}
destination = out / "validation_context.json"
assert not destination.exists()
destination.write_text(json.dumps(context, indent=2) + "\n", encoding="utf-8")
print("Final validation context prepared")
