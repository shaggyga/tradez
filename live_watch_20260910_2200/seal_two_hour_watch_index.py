"""Bind completed audit reports only; no broker, process, or project mutation."""
import datetime as dt
import hashlib
import json
import re
from pathlib import Path

root = Path(__file__).resolve().parent
members = [
    'LIVE_WATCH_120_MINUTES_20260911.md',
    'ROOT_TWO_HOUR_WATCH_COMPLETED_SUMMARY_20260911.json',
    'ROOT_WATCH_COMPLETED_SUMMARY_20260911.json',
    'WATCH_WINDOW_EXTENDED_120MIN.json',
    'EXISTING_PICTURE_RECONCILIATION_20260911.md',
    'EXISTING_PICTURE_INDEX_20260911.json',
    'forecasts/FORECAST_TWO_HOUR_PHASE_COMPARISON_20260911.md',
    'forecasts/FORECAST_TWO_HOUR_PHASE_COMPARISON_20260911.json',
    'forecasts/FORECAST_LIVE_WATCH_TWO_HOURS_20260911.json',
    'forecasts/FORECAST_LIVE_WATCH_FINAL_20260911.json',
    'forecasts/PRICE_V2_PUBLICATION_PLATEAU_1789095920417259100.json',
    'trial/COMBINED_TWO_HOUR_TRIAL_WATCH_20260911.md',
    'trial/COMBINED_TWO_HOUR_TRIAL_WATCH_20260911.json',
    'trial/SECOND_HOUR_TRIAL_WATCH_20260911.json',
    'feeds/FEED_WATCH_FINAL_SUMMARY_20260911.md',
    'feeds/FEED_TWO_HOUR_REPORT_20260911.json',
    'feeds/FEED_COMPACT_FINAL_EVIDENCE_MANIFEST_20260911.json',
    'feeds/TWO_HOUR_SOURCE_LISTENER_CHECK_001.json',
    'feeds/ARCHIVE_QUOTE_CLOCK_COMPARISON_FINAL_20260911.json',
    'feeds/news_context_diagnosis_001/WHY_CURRENT_NEWS_IS_CONTEXT_ONLY_20260911.md',
    'feeds/news_context_diagnosis_001/CURRENT_NEWS_CONTEXT_REASONS_20260911.json',
]
members += [f'forecasts/{pair}_NEW_FAILURE_DIAGNOSTIC_001.json'
            for pair in ('EUR_SEK', 'EUR_CHF', 'EUR_HUF', 'AUD_HKD', 'EUR_JPY')]
if not json.loads((root / 'progress_extension.json').read_text())['complete']:
    raise SystemExit('Requested watch is not complete.')

records = []
for member in members:
    path = root / member
    before = path.stat()
    raw = path.read_bytes()
    after = path.stat()
    if (before.st_size, before.st_mtime_ns) != (after.st_size, after.st_mtime_ns):
        raise RuntimeError(f'Report changed during seal: {member}')
    if path.suffix == '.json':
        json.loads(raw)
    else:
        raw.decode('utf-8')
    records.append({'member': member, 'bytes': len(raw), 'sha256': hashlib.sha256(raw).hexdigest()})

index = {
    'schema': 'forex_completed_two_hour_watch_evidence_index_20260911',
    'created_utc': dt.datetime.now(dt.timezone.utc).isoformat(),
    'status': 'completed',
    'scope': 'Reading reports for the completed in-chat 120-minute watch and the existing-project inventory. Referenced manifests bind deeper evidence separately.',
    'requested_start_utc': '2026-09-11T02:01:04.794949+00:00',
    'requested_end_utc': '2026-09-11T04:01:04.794949+00:00',
    'root_observer_completed_utc': '2026-09-11T04:01:05.661859+00:00',
    'entry_count': len(records),
    'records': records,
    'review_note': 'Forecast/news scope was independently checked; final prose distinguishes missing TRY forecasts from tradeability and five raised errors from fifteen broader diagnostics.',
    'limits': [
        'Raw chat, private article captures, credentials, live databases, and raw observation logs are excluded from direct entries.',
        'Some linked reports bind separately retained raw observations; this index is not a self-contained portable project restoration.',
        'Hash integrity and observation coverage do not establish forecast accuracy or complete recreation of historical models.',
        'Branch sampling gaps and later supplemental endpoint clocks are retained in their reports.',
        'The actual trading/research processes remain running; only the finite audit observers exited.',
    ],
}
dest = root / 'LIVE_WATCH_120_MINUTES_EVIDENCE_INDEX_20260911.json'
dest.write_text(json.dumps(index, indent=2, ensure_ascii=True) + '\n', encoding='utf-8')
for record in records:
    if hashlib.sha256((root / record['member']).read_bytes()).hexdigest() != record['sha256']:
        raise RuntimeError(f"Final binding mismatch: {record['member']}")
report = root / 'LIVE_WATCH_120_MINUTES_20260911.md'
links = re.findall(r'\]\((C:/[^)]+)\)', report.read_text(encoding='utf-8'))
missing = [link for link in links if not Path(link).is_file()]
if missing:
    raise RuntimeError(f'Missing report links: {missing}')
print(json.dumps({'index': str(dest), 'entries': len(records), 'report_links_checked': len(links),
                  'index_sha256': hashlib.sha256(dest.read_bytes()).hexdigest(),
                  'report_sha256': hashlib.sha256(report.read_bytes()).hexdigest()}))
