"""Source-bound extraction of the single fixed development run; no scoring."""
from collections import Counter
from datetime import datetime,timezone
import hashlib
import json
import os
from pathlib import Path
import xml.etree.ElementTree as ET

BASE=Path(__file__).resolve().parent
TRAD=BASE.parents[2]/'trad'
RUN=BASE/'actual_development_001'

def binding(path):
    raw=path.read_bytes();return dict(path=str(path),bytes=len(raw),sha256=hashlib.sha256(raw).hexdigest())
def load(path):return json.loads(path.read_bytes())
def new(path,value):
    with path.open('x',encoding='utf8',newline='\n') as f:
        f.write(value);f.flush();os.fsync(f.fileno())
    return binding(path)
def utc(t):return datetime.fromtimestamp(t,timezone.utc).isoformat()

report_path=RUN/'PATH_RISK_DEVELOPMENT_REPORT.json';report=load(report_path)
assert report['status']=='retrospective_development_completed'
assert report['frozen_protocol']==binding(Path(report['frozen_protocol']['path']))
assert report['pre_score_baselines']==binding(Path(report['pre_score_baselines']['path']))
fits=load(Path(report['pre_score_baselines']['path']))
assert fits['created_epoch']>=report['started_epoch']
for b in report['artifacts']:assert b==binding(Path(b['path']))
xml=BASE/'labels_comparison_protocol_final_tests.xml'
tree=ET.parse(xml);cases=tree.findall('.//testcase')
assert len(cases)==50 and not tree.findall('.//failure') and not tree.findall('.//error')
cells=[c for c in report['cells'] if c['partition']=='test']
valid=[c for c in cells if c['status']=='development_evaluated']
labels=report['policy']['quantile_targets']
summaries=[]
for label in labels:
    group=[c for c in valid if c['label']==label]
    improved=sum(c['methods']['training_empirical_volatility_scaled']['mean_pinball_across_quantiles']<c['methods']['training_empirical']['mean_pinball_across_quantiles'] for c in group)
    summaries.append(dict(label=label,evaluated_pair_horizon_cells=len(group),scaled_lower_pinball_cells=improved,
        per_cell_rows_min=min((c['methods']['training_empirical']['rows'] for c in group),default=None),
        per_cell_rows_max=max((c['methods']['training_empirical']['rows'] for c in group),default=None)))
primary=[c for c in cells if c['label']=='absolute_terminal_bps']
tail=[c for c in cells if c['label'] in ('long_envelope_MAE_bps','short_envelope_MAE_bps')]
binary=[c for c in report['opportunity_prevalence_reports'] if c['partition']=='test']
compact=dict(schema_version='path_risk_development_compact_assessment_v1_20260909',created_utc=datetime.now(timezone.utc).isoformat(),
    source_report=binding(report_path),evidence_class=report['policy']['evidence_class'],
    began_utc=utc(report['started_epoch']),completed_utc=utc(report['completed_epoch']),
    duration_sec=report['completed_epoch']-report['started_epoch'],source_inventories=report['source_inventories'],
    test_named_development_cells=len(cells),status_counts=dict(Counter(c['status'] for c in cells)),
    per_label_descriptive_cell_counts=summaries,primary_absolute_movement=primary,adverse_envelope=tail,
    opportunity_prevalence_baseline=binary,
    UTC_date_counts=sorted({c['dependence']['UTC_date_count'] for c in valid}),
    interval_unavailable_cells=sum(c['dependence']['scaled_pinball_improvement_interval95'] is None for c in valid),
    independent_sample_size=None,
    limitations=report['limitations']+['Counts of lower-loss cells are descriptive multiple comparisons, not a sign test or independent trial count.',
        'Terminal, magnitude, variation and side-specific path quantities are separate labels; their scores cannot be pooled as one forecast accuracy.',
        'Endpoint and path bid/ask diagnostics exclude slippage, financing, commissions, executable extreme prices and broker fills.'],
    research_only=True,can_place_orders=False,can_promote=False,account_eligible=False,execution_eligible=False,proof_eligible=False)
compact_binding=new(BASE/'PATH_RISK_DEVELOPMENT_COMPACT_ASSESSMENT_20260909.json',json.dumps(compact,indent=2,sort_keys=True,allow_nan=False)+'\n')
lines=['# Strict M1 path-risk development — 2026-09-09','',
    f"The single frozen comparison completed in {compact['duration_sec']:.2f} seconds. It uses the same immutable three-pair July–September history whose final period was already inspected in the MA experiment. Every result here is retrospective development under a new protocol, including the partition named `test`; no fresh confirmation, predictive edge or promotion is claimed.",'',
    'The protocol was frozen before scores. Both fixed methods use the same label-valid positive past-volatility training support: unconditional empirical quantiles, and empirical quantiles of the target divided by past 60-minute RMS volatility scaled to the original horizon. Neither method uses validation for refitting or selection. Zero past volatility is withheld explicitly without a floor. All fits were durably written before validation/test scoring.','',
    'Origins use 204 real warm rows and the same UTC five-minute grid, strict sessions and chronological 60/20/20 cutoffs as the earlier MA comparison. Targets are exact 15/30/60-minute completed closes. Future paths exclude the origin bar’s earlier high/low and require every subsequent real minute. Midpoint ingestion and original historical arrival remain unproven. Real bid/ask absence withholds the affected path/cost label; there is no spread inference or filling.','',
    '| Pair | Horizon m | Common N | Absolute movement pinball empirical / scaled | Coverage empirical / scaled | Width bps empirical / scaled |',
    '| --- | ---: | ---: | ---: | ---: | ---: |']
for c in primary:
    if c['status']!='development_evaluated':
        lines.append(f"| {c['instrument']} | {c['horizon_minutes']} | unavailable | — | — | — |");continue
    e=c['methods']['training_empirical'];s=c['methods']['training_empirical_volatility_scaled']
    lines.append(f"| {c['instrument']} | {c['horizon_minutes']} | {e['rows']} | {e['mean_pinball_across_quantiles']:.4f} / {s['mean_pinball_across_quantiles']:.4f} | {e['interval_coverage']:.1%} / {s['interval_coverage']:.1%} | {e['mean_interval_width_bps']:.3f} / {s['mean_interval_width_bps']:.3f} |")
lines+=['','Quantiles are 10/50/90%; lower pinball loss is better. Nominal central coverage is 80%, while the table gives actual development coverage. No calibrated-coverage guarantee follows from the nominal levels.','',
    '| Label | Evaluated pair/horizon cells | Scaled lower pinball cells |',
    '| --- | ---: | ---: |']
for s in summaries:lines.append(f"| {s['label']} | {s['evaluated_pair_horizon_cells']} | {s['scaled_lower_pinball_cells']} |")
lines+=['',f"The test-named development cells contain UTC-date counts {compact['UTC_date_counts']}. The minimum ten-date rule leaves {compact['interval_unavailable_cells']} cells without a descriptive interval. Overlapping horizons, repeated origins and correlated pairs are not independent trials; the cell counts above are not significance evidence.",'',
    'Both long and short close-path excursions use only observed closes plus the initial liquidation spread. Intrabar envelopes additionally use future bid/ask high/low and remain separately labeled. An envelope does not identify barrier order, an executable best price or a fill. Their full side/horizon coverage, upper-tail misses and pinball losses are retained in JSON.','',
    'The either-side endpoint-positive event is a separate direction-neutral opportunity label. Only its training-prevalence probability baseline is scored; no direction is inferred from the event. Bid/ask diagnostics contain no slippage, commissions, financing, USD account sizing or broker execution.','',
    'No method, threshold, manager rule or horizon was selected from these scores. Any later candidate using this development work requires a separately frozen protocol and future prospective confirmation. Existing paper management, the live joint model and the frozen pilot remain unchanged.','']
md_binding=new(BASE/'PATH_RISK_DEVELOPMENT_20260909.md','\n'.join(lines))
sources=[TRAD/'oanda_m1_path_risk_labels_v1.py',TRAD/'test_oanda_m1_path_risk_labels_v1.py',
    BASE/'path_risk_development_comparison_v1.py',BASE/'test_path_risk_development_comparison_v1.py',Path(__file__),xml,
    BASE/'labels_initial_tests.xml',BASE/'labels_comparison_initial_tests.xml',BASE/'labels_comparison_final_tests.xml',
    BASE/'FROZEN_PATH_RISK_DEVELOPMENT_PROTOCOL_20260909.json']
reviews=sorted(BASE.glob('*INDEPENDENT*REVIEW*.json'))
receipt=dict(schema_version='path_risk_development_validation_v1_20260909',status='completed',created_utc=datetime.now(timezone.utc).isoformat(),
    evidence_class=report['policy']['evidence_class'],test_cases_passed=50,label_test_cases=28,comparison_test_cases=22,
    test_failures=0,actual_development_runs=1,source_and_evidence_bindings=[binding(p) for p in sources+reviews]+[binding(report_path),report['pre_score_baselines'],compact_binding,md_binding],
    retained_label_artifacts=report['artifacts'],source_inventories=report['source_inventories'],
    acceptance='Source/label/protocol integrity and fixed retrospective development comparison completed. No predictive or management performance gate is asserted.',
    live_source_changes=False,new_model_activation=False,research_only=True,can_place_orders=False,can_promote=False,account_eligible=False,proof_eligible=False)
r=new(BASE/'PATH_RISK_DEVELOPMENT_VALIDATION_20260909.json',json.dumps(receipt,indent=2,sort_keys=True,allow_nan=False)+'\n')
print(json.dumps({'receipt':r,'compact_assessment':compact_binding,'report':md_binding,
    'evaluated_cells':len(valid),'label_counts':summaries,'UTC_date_counts':compact['UTC_date_counts']}))
