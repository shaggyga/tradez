"""Read-only evidence collection. Writes only to this external watch directory."""
import datetime as dt
import hashlib
import importlib.util
import json
import re
import subprocess
import time
import zipfile
from collections import Counter
from pathlib import Path

OUT = Path(__file__).resolve().parent
ROOT = Path(r'C:\Users\zmoor\Documents\forex\trad')
VAULT = Path(r'C:\Users\zmoor\OneDrive\thevault\projects\forex')

def sha(raw):
    return hashlib.sha256(raw).hexdigest()

def inspect(path, expected=None):
    began = time.time()
    row = {'path': str(path), 'read_started_epoch': began}
    try:
        before = path.stat()
        raw = path.read_bytes()
        after = path.stat()
        row.update(bytes=len(raw), sha256=sha(raw), stable_stat_during_read=(before.st_size, before.st_mtime_ns)==(after.st_size, after.st_mtime_ns))
        if expected:
            row['matches_prior_bytes'] = row['sha256'] == expected.get('sha256') and len(raw) == expected.get('size', expected.get('bytes', len(raw)))
        if path.suffix.lower() in {'.json', '.md', '.txt'}:
            text = raw.decode('utf-8-sig')
            row['decoded'] = True
            if path.suffix.lower() == '.json':
                obj = json.loads(text)
                row['json_parsed'] = True
                if isinstance(obj, dict):
                    row['declared_clocks'] = {k:obj[k] for k in ('generated_utc','created_utc','as_of_utc','updated_utc') if isinstance(obj.get(k), str)}
            else:
                row['headings'] = re.findall(r'^#{1,3}\s+(.+)$', text, re.M)[:20]
    except Exception as exc:
        row['error_type'] = type(exc).__name__
    row['read_completed_epoch'] = time.time()
    return row

started = time.time()
manifest_path = VAULT / 'SHARED_PROJECT_STATE_CURRENT.json'
manifest = json.loads(manifest_path.read_text(encoding='utf-8-sig'))
pointer_path = VAULT / 'source/WORKTREE_SOURCE_LATEST.json'
pointer = json.loads(pointer_path.read_text(encoding='utf-8-sig'))
archive_manifest_path = VAULT / 'source' / pointer['manifest']
archive_manifest = json.loads(archive_manifest_path.read_text(encoding='utf-8-sig'))
archive_path = VAULT / 'source' / pointer['archive']
archive_check = inspect(archive_path)
archive_check['matches_pointer_hash'] = archive_check['sha256'] == pointer['archive_sha256']
archive_check['manifest_matches_pointer_hash'] = sha(archive_manifest_path.read_bytes()) == pointer['manifest_sha256']
archive_rows = []
with zipfile.ZipFile(archive_path) as z:
    expected_names = [r['path'] for r in archive_manifest['files']]
    archive_check['exact_member_names'] = len(z.namelist()) == len(expected_names) and set(z.namelist()) == set(expected_names)
    for old in archive_manifest['files']:
        payload = z.read(old['path'])
        current = inspect(ROOT / old['path'], old)
        archive_rows.append({'member': old['path'], 'archive_bytes_match_manifest': len(payload)==old['size'] and sha(payload)==old['sha256'], 'current_source': current})

records = []
for old in manifest['records']:
    local = inspect(VAULT / old['name'], old)
    source_name = old['source'].replace('\\', '/')
    if source_name.startswith('trad/'):
        source_name = source_name[5:]
    source = inspect(ROOT / source_name, old)
    records.append({'name':old['name'], 'prior_manifest_entry':old, 'vault':local, 'canonical_source':source,
                    'current_vault_matches_current_source':local.get('sha256') is not None and local.get('sha256') == source.get('sha256')})

mapped = {r['name'] for r in records}
supplemental = []
for path in sorted(VAULT.rglob('*')):
    if path.is_file() and path.suffix.lower() in {'.json','.md','.txt'} and path.relative_to(VAULT).as_posix() not in mapped:
        supplemental.append(inspect(path))

chat_files = [OUT/'build_chat_recent_messages.json'] + sorted(OUT.glob('build_chat_older_messages_*.json'))
turns = []
chat_sources = []
message_counts = Counter()
keyword_turns = {}
patterns = {
    'recreation_recovery':r'recreat|recover|checkpoint|rebuild|transfer|backup',
    'wide_models_and_curves':r'795|643|\b220\b|\b227\b|gradient|boosting|ARIMA|horizon curve|feature space',
    'news_weather_blurb':r'blurb|weather|seismograph|continuous.{0,30}meter|relative strength',
    'management_accounts':r'position management|practice.?00|paper account|margin|stop.loss',
    'pending_ideas':r'pending|backlog|improvements|remaining gaps',
}
for path in chat_files:
    obj = json.loads(path.read_text(encoding='utf-8-sig'))
    chat_sources.append({**inspect(path), 'turn_count':len(obj['turns']), 'has_more':obj['page']['hasMore']})
    for t in obj['turns']:
        turns.append({'id':t['id'],'startedAt':t.get('startedAt'),'completedAt':t.get('completedAt'),'source':path.name})
        for item in t.get('messages',t.get('items',[])):
            message_counts[item['type']] += 1
            text = item.get('text') or ''
            for label, pattern in patterns.items():
                if re.search(pattern, text, re.I):
                    keyword_turns.setdefault(label, {})[t['id']] = {'turn_id':t['id'],'source':path.name,'startedAt':t.get('startedAt')}
git = subprocess.run(['git','status','--porcelain=v1','-uall'],cwd=ROOT,capture_output=True,text=True,check=True)
git_rows = [{'status':line[:2],'path':line[3:]} for line in git.stdout.splitlines() if line]

report = {
    'schema':'forex_existing_picture_coverage_20260911_v1',
    'started_utc':dt.datetime.fromtimestamp(started,dt.timezone.utc).isoformat(),
    'completed_utc':dt.datetime.now(dt.timezone.utc).isoformat(),
    'scope':'Current static vault records and source correspondence, supplemental readable records, returned original-chat user/final history, and working-tree filename inventory. No model deserialization, training, trading, database scan, reconstruction or source edit.',
    'manifest':inspect(manifest_path),'source_pointer':inspect(pointer_path),
    'archive':archive_check,'archive_member_checks':archive_rows,
    'mapped_records':records,'supplemental_records':supplemental,
    'chat':{'title':'Add forex news feeds','thread_id':'019fae9b-ef27-7cc2-945d-9ab23b43414e','pages':chat_sources,'turns':turns,'unique_turn_count':len({t['id'] for t in turns}), 'message_counts':dict(message_counts),'keyword_locator':{k:list(v.values()) for k,v in keyword_turns.items()},'last_page_has_more':json.loads(chat_files[-1].read_text(encoding='utf-8-sig'))['page']['hasMore'],
       'limits':['Retrieved all available paginated turns through hasMore=false. Retained user and final assistant messages only; tool transcripts and hidden reasoning excluded.','Chat statements are historical claims requiring file/evidence reconciliation, not proof of implementation.','Keyword locators are discovery aids, not a semantic verification of every message. No raw chat text is exported into the vault by this collector.']},
    'git_working_tree_inventory':git_rows,
    'limits':['Current source differences from a dated archive do not establish unrecorded edits or corruption.','A hash match proves byte correspondence, not model quality, causal historical availability, or a full reproducible environment.','Supplemental records may include repeated archived metadata; counts are file counts, not independent experiments.','This check does not claim every database/private artifact is in the vault or all original formulas and results were rerun.']
}
report['summary'] = {
    'mapped_records':len(records),
    'mapped_vault_matches_old_manifest':sum(r['vault'].get('matches_prior_bytes',False) for r in records),
    'mapped_vault_changes':[r['name'] for r in records if not r['vault'].get('matches_prior_bytes',False)],
    'mapped_vault_vs_current_source_differences':[r['name'] for r in records if not r['current_vault_matches_current_source']],
    'source_members':len(archive_rows),
    'source_archive_members_valid':sum(r['archive_bytes_match_manifest'] for r in archive_rows),
    'current_source_matches_archive':sum(r['current_source'].get('matches_prior_bytes',False) for r in archive_rows),
    'current_source_differences':[r['member'] for r in archive_rows if not r['current_source'].get('matches_prior_bytes',False)],
    'supplemental_files':len(supplemental),
    'supplemental_errors':[r['path'] for r in supplemental if 'error_type' in r],
    'chat_pages':len(chat_files),'chat_turns':len(turns),'chat_unique_turns':report['chat']['unique_turn_count'],
    'git_modified_tracked':sum(r['status']!='??' for r in git_rows),'git_untracked':sum(r['status']=='??' for r in git_rows),
}
destination=OUT/'EXISTING_PICTURE_COVERAGE_20260911.json'
destination.write_text(json.dumps(report,indent=2,ensure_ascii=True)+'\n',encoding='utf-8')
print(json.dumps({'report':str(destination),'sha256':sha(destination.read_bytes()),**report['summary']}))
