"""Documentation-only update with exact pre-edit hash checks and preserved history."""
import datetime as dt
import hashlib
import json
from pathlib import Path

out = Path(__file__).resolve().parent
project = Path('C:/Users/zmoor/Documents/forex/trad')
before = json.loads((out / 'before.json').read_text(encoding='utf-8-sig'))
raws = {}
for item in before['bindings']:
    raw = (project / item['name']).read_bytes()
    if hashlib.sha256(raw).hexdigest() != item['sha256']:
        raise SystemExit(f"Documentation changed since snapshot: {item['name']}")
    if raw != (out / 'before' / item['name']).read_bytes():
        raise SystemExit(f"Snapshot differs: {item['name']}")
    raws[item['name']] = raw

readme_insert = '''
**September 11 — change tracking and model reuse:** [Needed changes](docs/FOREX_CHANGE_REGISTER_20260911.md) tracks 26 current gaps and links all 29 historical action IDs; [model reuse and performance](docs/FOREX_MODEL_REUSE_REGISTER_20260911.md) records what was already tried, measured results, evidence limitations, and the comparison required before repeating research. All 140 cold-library family records were located; this is not an independent rescore or successful recreation of every family. The completed two-hour watch and existing-picture inventory remain separately sealed. This update changes documentation only; further buildout remains paused. Earlier operational statements below retain their original observation dates.

'''
pending_insert = '''
## Current — September 11 consolidated change and research-reuse tracking

Use the [working change register](docs/FOREX_CHANGE_REGISTER_20260911.md) for FXG-001 through FXG-026, with evidence, dependencies, closure criteria, and distinctions between completed, inactive, unverified, and deferred work. Its [29-action historical crosswalk](../live_watch_20260910_2200/trial/RELOCATED_CENTRAL_PENDING_CROSSWALK_20260911.md) preserves all earlier INTRA/NEWS/TAG IDs. The older queue remains below as dated evidence; later corrections must be checked before reopening an old item.

Before a model or feature experiment, consult the [model reuse/performance register](docs/FOREX_MODEL_REUSE_REGISTER_20260911.md). Record the nearest prior family/variant/application/run, exact data/features/target/cost/source identity, prior result, and what is materially different. Reuse prior work when no difference exists. A reproduction to resolve missing evidence must be labeled as such. No blanket rerun of the 29,366 catalogue records is planned.

The [completed two-hour watch](../live_watch_20260910_2200/LIVE_WATCH_120_MINUTES_20260911.md) is a dated observation through 04:01 UTC: no new broker transactions or NAV change, three recovered practice-worker errors, five stale-news forecast refusals, and a consumed eight-claim daily cap. It is not a fresh health claim at this documentation update. Required changes include input/issue freshness, durable failure detail, execution counting/preflight, compatible fresh curve/remaining-risk updates, reuse of richer models/news work, matched performance evaluation, and faithful restoration. Existing recovery and candidate fixes are recorded for reuse.

This is documentation-only tracking requested by the user. No model, runtime policy, daily cap, process, broker account, historical result, or sealed audit record was changed. Further project buildout remains paused.

'''
recorded = dt.datetime.now(dt.timezone.utc).isoformat()
log_append = f'''

## 2026-09-11 — consolidated change register and model-reuse guard against repeated research

Recorded {recorded}. User requested continued tracking of all needed changes and an honest account of model-space/performance coverage, with avoiding repeats a major priority. Added docs/FOREX_CHANGE_REGISTER_20260911.md with 26 stable FXG items and links to the preserved 29-action historical crosswalk. Added docs/FOREX_MODEL_REUSE_REGISTER_20260911.md with prior family results, overlap/denominator limitations, reproduction gaps, and a mandatory prior-work comparison for subsequent experiments. The 140 located family records and 29,366 run records are catalogue coverage; they are not 140 independently validated models or 29,366 reruns. Fourteen reference runs are reconstructable but unvalidated, one partial, and 125 unresolved.

Linked both records from README and the current pending section. Preserved exact pre-edit README/pending/log bytes and hashes under C:/Users/zmoor/Documents/forex/change_tracking_20260911T180103581Z/before. The existing-picture and completed two-hour watch seals remain unchanged; new documentation is a later layer, not a rewrite of those observations. The working queue separates implemented/reviewed/inactive/deployed/observed/unverified states, keeps the daily cap/preflight issues explicit, and reuses already built recovery, curve, news, feature and management components. No live query, code deployment, trading-policy or cap change, broker action, model fit/rescore, process change, vault republish, or recurring monitor occurred in this documentation update. Further buildout remains paused.
'''

changes = {}
for name, insertion in [('README.md', readme_insert), ('FOREX_PENDING_IMPROVEMENTS.md', pending_insert)]:
    raw = raws[name]
    newline = b'\r\n' if b'\r\n' in raw[:200] else b'\n'
    end = raw.index(b'\n') + 1
    encoded = insertion.replace('\n', newline.decode()).encode('utf-8')
    changes[name] = raw[:end] + encoded + raw[end:]
raw = raws['FOREX_PROJECT_LOG.md']
newline = b'\r\n' if b'\r\n' in raw[:200] else b'\n'
changes['FOREX_PROJECT_LOG.md'] = raw + log_append.replace('\n', newline.decode()).encode('utf-8')

for name, content in changes.items():
    if (project / name).read_bytes() != raws[name]:
        raise SystemExit(f'Concurrent change before write: {name}')
    (project / name).write_bytes(content)

receipt = {
    'schema': 'forex_documentation_tracking_navigation_update_v1',
    'recorded_utc': recorded,
    'scope': 'Documentation only; history preserved; no live system action.',
    'before_manifest': 'before.json',
    'changes': [{'path': str(project / name), 'before_sha256': hashlib.sha256(raws[name]).hexdigest(),
                 'after_sha256': hashlib.sha256(content).hexdigest(), 'bytes': len(content)}
                for name, content in changes.items()],
}
(out / 'NAVIGATION_UPDATE_20260911.json').write_text(json.dumps(receipt, indent=2) + '\n', encoding='utf-8')
print(json.dumps(receipt))
