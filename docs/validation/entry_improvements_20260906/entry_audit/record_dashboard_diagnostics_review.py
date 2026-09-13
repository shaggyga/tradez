"""Record completed offline dashboard integration review without runtime reads."""
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path

OUT = Path(__file__).resolve().parent
PROJECT = OUT.parents[1] / "trad"
FILES = ("oanda_practice_live_dashboard.py", "test_oanda_executor_dashboard_diagnostics.py", "oanda_entry_diagnostics.py")
sources = {}
for name in FILES:
    raw = (PROJECT / name).read_bytes()
    sources[name] = {"bytes": len(raw), "sha256": hashlib.sha256(raw).hexdigest()}
result = {
    "reviewed_utc": datetime.now(timezone.utc).isoformat(),
    "canonical_project": str(PROJECT),
    "sources": sources,
    "finding_repaired": "Lab-only log discovery prevented fast practice-007 executor skips from reaching the diagnostic dashboard payload.",
    "implementation": [
        "A dedicated reader opens only the newest two matching dum4 executor log files, capped at the final 1048576 bytes each.",
        "Only complete JSONL lines with explicit timezone timestamps are accepted. Rotated parts retain their run identity.",
        "Freshness uses event clocks, not file mtime; files are selected using mtime. Stale, missing, future and previous-session observations remain historical or unknown.",
        "A newer selection, order observation or explicit session start prevents an earlier skip from being presented as current.",
        "Legacy nested candidate block counts are labeled shown_only_not_total; missing stage, full totals and truncation are unknown.",
        "Local accepted counts and shared feed observations remain separate. No cycle, distinct-signal, order, fill, authorization or runtime-health totals are inferred.",
        "build_state exposes the same entry_diagnostics object at top level and inside primary_signal_system; build_main_state exposes it at top level.",
        "Existing embedded practice monitor HTML has a timestamped executor-entry card; standalone main HTML remains unchanged.",
    ],
    "verification": {
        "independent_test_command": "python -B -m pytest -q -p no:cacheprovider test_oanda_executor_dashboard_diagnostics.py",
        "independent_tests_passed": 19,
        "independent_test_elapsed_sec": 0.82,
        "isolation": "Synthetic temporary JSONL only. Heavy loaders and external snapshot paths mocked in build_state/build_main_state tests; no workers, servers, broker, credentials, production database or live log reads.",
        "parent_reported_integration": "274 tests and 112 subtests passed; 5 unchanged unrelated thread fixtures deselected. This was reported by the parent, not independently rerun in this receipt.",
    },
    "limits": [
        "The bounded tail can omit earlier events. Missing receipts cannot prove absence of execution.",
        "Sampled selection and throttled skip receipts have no common cycle ID. The payload explicitly leaves their same-cycle association unknown.",
        "Recent observations do not prove the executor is currently running or a candidate is currently authorized.",
        "The standalone main dashboard HTML receives the new API field but does not render a new card. The existing monitor view renders it.",
        "No project runtime was started and no entry gate or authorization rule was loosened.",
    ],
}
target = OUT / "DASHBOARD_ENTRY_DIAGNOSTIC_REVIEW_20260906.json"
with target.open("x", encoding="utf-8", newline="\n") as handle:
    json.dump(result, handle, indent=2)
    handle.write("\n")
print(json.dumps({"receipt": str(target), "sha256": hashlib.sha256(target.read_bytes()).hexdigest(), "sources": sources}, indent=2))
