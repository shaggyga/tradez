"""Preserve the completed price repair's evidence without reclassifying old results."""
from pathlib import Path
import hashlib,json,time
from datetime import datetime,timezone
import xml.etree.ElementTree as ET

ROOT=Path(__file__).resolve().parents[1]
TRAD=ROOT/'trad'
OLD=ROOT/'pair_forecast_repair_v2_20260907'
def sha(path):return hashlib.sha256(path.read_bytes()).hexdigest()
def record(path):return {'path':str(path.resolve()),'sha256':sha(path),'bytes':path.stat().st_size}
evidence=[OLD/'ACTIVATION_RECEIPT_20260907.json',OLD/'PAIR_FAMILY_V2_RUNTIME_AUDIT_20260907.json',
          OLD/'RUNTIME_CONTINUITY_AND_RETIRED_BACKUPS_20260907.json',OLD/'dashboard_worker_final_tests.xml',
          OLD/'dashboard/LIVE_V2_DASHBOARD_FINAL_CHECK_VERIFICATION_20260907.json',
          OLD/'project_health/final/INTEGRITY_RUNTIME_REPAIR_VALIDATION_20260907.json',
          OLD/'project_health/STORAGE_GUARD_INDEPENDENT_REVIEW_20260907.json']
registry=TRAD/'config/pair_local_forecast_study_v2_20260907.json';registered=json.loads(registry.read_bytes())
bindings={name:{'expected':expected,'actual':sha(TRAD/name),'unchanged':expected==sha(TRAD/name)} for name,expected in registered['source_bindings'].items()}
assert all(item['unchanged'] for item in bindings.values())
value={'schema_version':'pair_family_repair_validation_v1_20260907','recorded_utc':datetime.now(timezone.utc).isoformat(),
       'scope':'Completed price-v2 repair and dated activation/runtime/UI receipts. Later joint news/price work has a separate receipt.',
       'registry':record(registry),'registered_source_bindings':bindings,'evidence':[record(p) for p in evidence],
       'historical_observation':{'utc':'2026-09-07T21:30:32Z','active_pairs':65,'registered_pairs':68,'active_family_forecasts':130,
         'publications':274,'outcomes':0,'worker_errors':0,'heartbeat_write_errors':0},
       'orders_enabled':False,'promotion_enabled':False,'predictive_improvement_demonstrated':False,
       'limitations':['Observation counts describe the dated receipt, not the time this summary is read.',
         'New availability does not correct poor earlier after-cost results.',
         'The price-v2 model excludes news; see the separate joint successor.',
         'Runtime health and dashboard sources may change for the joint successor; their earlier source-bound receipts remain dated.']}
target=TRAD/'FOREX_PAIR_FAMILY_REPAIR_VALIDATION_20260907.json'
if target.exists():raise RuntimeError('Refuse to overwrite completed validation')
target.write_text(json.dumps(value,indent=2)+'\n',encoding='utf-8')
report=TRAD/'docs/FOREX_PAIR_FAMILY_REPAIR_20260907.md'
report.write_text('''# Pair forecast coverage and runtime repair — September 7

The previous model requirement blocked most pairs despite usable real price history. The new registered price-v2 study uses actual UTC minute timestamps and elapsed time, exact one-hour historical endpoints and explicit missingness. State-space and Ridge now publish independently: one unavailable family cannot suppress the other.

At the independently verified **21:30:32 UTC** observation, **65 of 68 pairs had 130 active model forecasts**, with 274 publications and zero worker or heartbeat-write errors. No H1 outcomes had matured at that observation. Three TRY pairs lacked current usable inputs. These are dated coverage figures, not an accuracy result.

Each pair/family has its own immutable contract, attempts, forecasts, publication receipts, independent consumer observations, later quote-entry matches and original one-hour targets. Successful cadence buckets and market-reference times are unique. Failed attempts may retry with fresh inputs. No candle, prior forecast or outcome was backfilled.

The dashboard now shows current family availability and original forecast times, expected moves before costs, and uncertainty as an uncalibrated estimate. The API's primary collection status follows the selected pair study. Original EUR/USD/shared-gap companions were stopped and backed up read-only; their source and evidence remain intact. Pair-v1 continues separately.

The storage guard expanded from six legacy databases to 216 actual registered/core databases, retaining a fresh inventory baseline before projecting growth. The integrity audit distinguishes fresh active-worker observations from retained historical failures; inactive executor artifacts no longer imply a running executor. Source-bound tests, independent review and controlled reload evidence are linked in the validation receipt.

The completed one-hour watch before this repair found poor results: of 23 newly scored pair-v1 decisions, State-space got direction right on 8 and Ridge on 12; neither model had a positive after-spread result in those 23. The forecasts and pairs overlap, so these are not independent trials. Those results remain preserved.

This price-only repair does not incorporate news. The separate joint price/news successor and news admission repair are documented in [the joint report](FOREX_JOINT_PRICE_NEWS_20260907.md). Better coverage and passing engineering tests do not demonstrate improved prediction accuracy or profitability. Orders and promotion remain disabled.

[Source-bound validation](../FOREX_PAIR_FAMILY_REPAIR_VALIDATION_20260907.json) · [Active pipeline](ACTIVE_PIPELINE.md) · [Research index](RESEARCH_INDEX.md).
''',encoding='utf-8')
print(json.dumps({'validation':record(target),'report':record(report)}))
