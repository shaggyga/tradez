"""Locate named master-prompt recovery anchors on authorized C-drive roots."""
import json
import subprocess
from datetime import datetime, timezone
from pathlib import Path

OUT=Path(__file__).resolve().parent
names=["INDEX.md","CHECKPOINT_LATEST.json","forex_model_checkpoint_current.zip","BACKTEST_AND_MODEL_REGISTRY.md",
"MODEL_GAP_CLOSURE_20260722.md","CHAT_LOG_RECAP_20260722.md","POST_GAP_EXECUTION_POLICY_20260720.md",
"CONTINUOUS_SIGNAL_LOOP_20260721.md","PAIR_FAMILY_SIGNAL_MATRIX_20260720.md","oanda_audit_package_20260708_124446.zip",
"chat_handoff_20260709_154238.zip","CHAT_HANDOFF.md","00_CHATGPT_HANDOFF.md","COMPLETE_SOURCE_AUDIT_20260806.md",
"NEWS_SOURCE_AUDIT_20260806.md","FOREX_ARCHIVE_FEATURE_MODEL_EVIDENCE.md","ARCHIVE_EVIDENCE_INDEX.json",
"METHOD_COVERAGE_CHECK_20260918.md","BOJ_INCIDENT_SEARCH_STATUS.md","FOREX_CODEX_PROMPT_ADDENDUM_SIMULATION_METHOD_COVERAGE.md",
"FOREX_PROMPT_ADDENDUM_SIMULATION_AND_METHOD_COVERAGE.md","FOREX_NEWS_BLURB_RESEARCH_RECOVERED.zip","README_RECOVERED_PROJECT.md","CODEX_HANDOFF_PROMPT.md"]
roots=[Path(r"C:\Users\zmoor\Documents\forex"),Path(r"C:\Users\zmoor\OneDrive\thevault"),
    Path(r"C:\Users\zmoor\Documents\vault_cold_archive\20260901_record_consolidation\FOREX_MODEL_LIBRARY"),
    Path(r"C:\Users\zmoor\Documents\vault_cold_archive\20260901_record_consolidation\UNIFIED_QUERY_VAULT"),
    Path(r"C:\Users\zmoor\Documents\vault_cold_archive\20260901_record_consolidation\fxgap26")]
cmd=["rg","--files","--hidden","--no-ignore"]
for glob in ["!**/.git/**","!**/.venv/**","!**/venv/**","!**/node_modules/**","!**/.pytest*/**","!**/__pycache__/**","!**/audit_20260921/**"]:
    cmd += ["-g",glob]
cmd += [str(p) for p in roots]
proc=subprocess.run(cmd,capture_output=True,text=True,encoding="utf-8",errors="replace",timeout=45)
paths=[Path(x) for x in proc.stdout.splitlines()]
downloads=Path(r"C:\Users\zmoor\Downloads")
paths += [x for x in downloads.iterdir() if x.is_file() and x.name.casefold() in {n.casefold() for n in names}]
by_name={n.casefold():[] for n in names}
for p in paths:
    if p.name.casefold() in by_name:
        by_name[p.name.casefold()].append(str(p))
rows=[{"anchor":n,"status":"located" if by_name[n.casefold()] else "not located in bounded C filename search",
       "paths":sorted(by_name[n.casefold()])} for n in names]
result={"observed_utc":datetime.now(timezone.utc).isoformat(),"recursive_roots":[str(p) for p in roots],
    "additional_scope":"Downloads immediate files matching exact requested anchor names only",
    "limitations":"D reads stopped for fresh disk errors; archive payload names not searched except separately verified source ZIP; no remote chats/workspaces searched. A miss is not project-wide absence.",
    "search_exit_code":proc.returncode,"search_errors":proc.stderr.splitlines(),"records":rows}
(OUT/"RECOVERY_ANCHOR_INDEX.json").write_text(json.dumps(result,indent=2),encoding="utf-8")
print(json.dumps({"located":[r["anchor"] for r in rows if r["paths"]],"not_located":[r["anchor"] for r in rows if not r["paths"]],"search_errors":result["search_errors"]},indent=2))
