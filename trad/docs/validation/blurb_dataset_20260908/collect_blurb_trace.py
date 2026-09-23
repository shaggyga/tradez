"""Bounded read-only source/report trace; writes only new evidence beside this helper."""
import collections
import datetime as dt
import hashlib
import json
from pathlib import Path
import sys
import zipfile

sys.dont_write_bytecode = True
ROOT = Path(r"C:\Users\zmoor\Documents\forex\trad")
OUT = Path(__file__).resolve().parent
REPORTS = "data/oanda_training_manager/reports/"
paths = [
    "config/pure_change_strategy_space_v1.json",
    "config/news_blurb_episode_schema_v1.json",
    "config/move_first_live_arm_alignment_v1_20260901.json",
    "config/news_blurb_direction_attribution_v1.json",
    "config/forex_source_gap_register_v1.json",
    "oanda_continuous_narrative_meter.py",
    "oanda_continuous_narrative_backtest.py",
    "oanda_news_blurb_direction_attribution.py",
    "oanda_causal_forecast_inputs_joint_news_v1.py",
    "oanda_joint_price_news_models_v1.py",
    "FOREX_PENDING_IMPROVEMENTS.md",
    REPORTS + "news_blurb_recovery_20260809/NEWS_BLURB_RECOVERY_AUDIT_20260809.md",
    REPORTS + "spike_blurb_factor_reconstruction/RECOVERED_NEWS_BLURB_CANONICALIZATION_AUDIT_V1.md",
    REPORTS + "spike_blurb_factor_reconstruction/SPIKE_BLURB_FACTOR_RESPONSE_ANALOGS_V2.md",
    REPORTS + "spike_blurb_factor_reconstruction/verified_factor_response_analogs_v2/VERIFIED_FACTOR_RESPONSE_ANALOGS_V2.md",
    REPORTS + "news_blurb_direction_attribution/NEWS_BLURB_DIRECTION_ATTRIBUTION_V1.json",
    REPORTS + "news_blurb_direction_attribution/NEWS_BLURB_DIRECTION_ATTRIBUTION_V1.md",
    REPORTS + "continuous_narrative/CONTINUOUS_NARRATIVE_BACKTEST_V10.json",
]
bindings = []
raw = {}
for rel in paths:
    p = ROOT / rel
    assert p.stat().st_size < 8 * 1024 * 1024, rel
    b = p.read_bytes()
    raw[rel] = b
    bindings.append({"path": str(p), "bytes": len(b), "sha256": hashlib.sha256(b).hexdigest()})

archive = ROOT / "data/oanda_training_manager/source_archives/recovered_news_blurb_project_20260809/FOREX_NEWS_BLURB_RESEARCH_RECOVERED.zip"
archive_bytes = archive.read_bytes()
assert len(archive_bytes) == 119040
archive_sha = hashlib.sha256(archive_bytes).hexdigest()
assert archive_sha == "f0b79f7f1f58084573541c428d68b929041d890b7e7f6bdf4db4527b64210903"
with zipfile.ZipFile(archive) as z:
    members = [{"name": i.filename, "bytes": i.file_size, "compressed_bytes": i.compress_size} for i in z.infolist()]

lineage = ROOT.parent / "feature_horizon_audit_20260908/FEATURE_IMPLEMENTATION_LINEAGE_AUDIT_20260908.json"
lineage_sha = hashlib.sha256(lineage.read_bytes()).hexdigest()
assert lineage_sha == "426d0a454991d6c141ed5c190e2c9319ea6506588180a86c06482086a7e6bb03"

narr = json.loads(raw[REPORTS + "continuous_narrative/CONTINUOUS_NARRATIVE_BACKTEST_V10.json"])
groups = collections.defaultdict(dict)
weighted = collections.defaultdict(lambda: [0, 0.0])
for row in narr["summaries"]:
    key = tuple(row[k] for k in ("model_id", "arm", "horizon_minutes", "sampling_cohort", "quality_slice"))
    groups[key][row["split"]] = row
    acc = weighted[(row["arm"], row["horizon_minutes"], row["split"])]
    acc[0] += row["raw_n"]
    acc[1] += row["raw_n"] * row["mean_net_bps"]
positive = []
for key, rows in groups.items():
    if set(rows) == {"train", "validation", "test"} and all(r["mean_net_bps"] > 0 for r in rows.values()):
        positive.append({"model_id": key[0], "arm": key[1], "horizon_minutes": key[2], "sampling_cohort": key[3], "quality_slice": key[4], "splits": rows})
arm_summary = [{"arm": k[0], "horizon_minutes": k[1], "split": k[2], "raw_rows": v[0], "mean_net_bps": v[1] / v[0]} for k, v in sorted(weighted.items())]
arm_groups = collections.defaultdict(dict)
for row in arm_summary:
    arm_groups[(row["arm"], row["horizon_minutes"])][row["split"]] = row
assert sum(set(v) == {"train", "validation", "test"} and all(r["mean_net_bps"] > 0 for r in v.values()) for v in arm_groups.values()) == 0
attrib = json.loads(raw[REPORTS + "news_blurb_direction_attribution/NEWS_BLURB_DIRECTION_ATTRIBUTION_V1.json"])
anchors = []
terms = {
    "oanda_continuous_narrative_meter.py": ["inactive_no_prequential_orientation_rows", 'model_scores["recovered_blurb_analog_v1"]'],
    "oanda_causal_forecast_inputs_joint_news_v1.py": ["news_fast_lane_mappings_v3", "news_source_governance_fast_lane_v3_committed_visibility_20260905", "joint_news_current_v1.json"],
    "config/pure_change_strategy_space_v1.json": ["historical_news_attribution", "historical_ex_post_attribution"],
    "config/move_first_live_arm_alignment_v1_20260901.json": ["recovered_blurb_analog_v1", "selection_conditioned_on_realized_executable_move", "predictive_backtest_eligible"],
    "FOREX_PENDING_IMPROVEMENTS.md": ["168,378", "91,933", "29 market"],
}
for rel, needles in terms.items():
    for n, line in enumerate(raw[rel].decode("utf-8-sig").splitlines(), 1):
        if any(t in line for t in needles):
            anchors.append({"path": str(ROOT / rel), "line": n, "text": line[:1800]})

result = {
    "schema": "blurb_dataset_model_use_trace_v1_20260908",
    "observed_utc": dt.datetime.now(dt.timezone.utc).isoformat(),
    "scope": "Bounded read-only source/report review; report aggregation only; no database opened, no replay, fitting, unpickling, orders or runtime changes.",
    "source_bindings": bindings,
    "recovered_archive": {"path": str(archive), "bytes": len(archive_bytes), "sha256": archive_sha, "members": members, "large_output_absence_scope": "The named missing CSV/raw JSONL/digests/mapper outputs are absent from this recovered ZIP; this does not establish absence of other canonical historical datasets."},
    "existing_wide_model_lineage": {"path": str(lineage), "sha256": lineage_sha, "conclusion": "Prior complete audit: 227/220 macro columns are constant placeholders in inspected retained matrices; 795 model has price/calendar inputs and no news columns. No recovered blurb dataset training join was found in their inspected producers."},
    "current_joint_H1": {"inputs": 34, "conclusion": "Consumes classified article history from post-commit-visible governance-v3 mappings plus current guarded news snapshot. It does use news, but does not join the recovered movement-first blurb/response-analog dataset."},
    "inactive_recovered_analog": {"status": "inactive_no_prequential_orientation_rows", "emitted_score": 0.0, "reason": "Requires prior matured same-factor response orientation; inspected retained analog reports have zero prequential predictions."},
    "direction_attribution": {k: v for k, v in attrib.items() if k != "arms"},
    "direction_attribution_metrics": {k: {"metrics": v["metrics"], "by_liquidity": v["by_liquidity"]} for k, v in attrib["arms"].items()},
    "continuous_news_and_technical": {
        "metadata": {k: v for k, v in narr.items() if k != "summaries"},
        "report_summary_rows": len(narr["summaries"]),
        "descriptive_arm_horizon_split_aggregates": arm_summary,
        "aggregate_denominator_warning": "These are sums of retained summary rows across named models, not independent trades; duplicated exact model streams and shared clocks remain disclosed. No new backtest was run.",
        "broad_arm_horizon_groups_positive_in_all_three_splits": 0,
        "positive_exact_subgroups_all_three_splits": positive,
        "positive_exact_subgroups_with_minimum_30_raw_rows_each_split": sum(all(r["raw_n"] >= 30 for r in p["splits"].values()) for p in positive),
        "interpretation": "Historical sentiment+technical combinations were actually tried. Positive small subgroups do not establish untouched forecast skill; the report labels all rows retrospective classifier-adaptive discovery with confirmation ineligible.",
    },
    "line_anchors": anchors,
}
dest = OUT / "BLURB_MODEL_USE_TRACE_20260908.json"
with dest.open("x", encoding="utf-8", newline="\n") as f:
    json.dump(result, f, indent=2, ensure_ascii=False)
    f.write("\n")
print(json.dumps({"path": str(dest), "sha256": hashlib.sha256(dest.read_bytes()).hexdigest(), "source_bindings": len(bindings), "archive_members": len(members), "source_unchanged": all(hashlib.sha256(Path(b["path"]).read_bytes()).hexdigest() == b["sha256"] for b in bindings)}))
