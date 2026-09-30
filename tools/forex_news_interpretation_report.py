"""Read-only archive interpretation; writes a new report, never relabels history."""
import argparse
import datetime as dt
import hashlib
import json
from pathlib import Path
import sqlite3
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'trad'))
from oanda_news_interpretation_v1 import interpret_headline, VERSION


def build(database, event_ids):
    result = []
    with sqlite3.connect(database.resolve().as_uri() + '?mode=ro', uri=True) as db:
        db.execute('PRAGMA query_only=ON')
        db.row_factory = sqlite3.Row
        for event_id in event_ids:
            row = db.execute('SELECT event_id,headline,first_seen_utc,payload_json FROM articles WHERE event_id=?', (event_id,)).fetchone()
            if row is None:
                raise ValueError(f'Missing requested event: {event_id}')
            old = json.loads(row['payload_json'])
            result.append({
                'event_id': event_id, 'headline': row['headline'],
                'original_first_seen_utc': row['first_seen_utc'],
                'stored_payload_sha256': hashlib.sha256(row['payload_json'].encode()).hexdigest(),
                'stored_classification_version': old.get('classification_version'),
                'stored_currency_scores': old.get('currency_scores'),
                'stored_context_reason': old.get('context_reason'),
                'stored_directional_publish_eligible': old.get('directional_publish_eligible'),
                'interpretation': interpret_headline(row['headline'])})
    return {'schema': VERSION + '.archive_review',
            'interpreted_utc': dt.datetime.now(dt.timezone.utc).isoformat(),
            'database': str(database.resolve()),
            'scope': 'selected stored headlines; not historical decision-time replay or whole-feed evaluation',
            'source_sha256': hashlib.sha256((Path(__file__).resolve().parents[1] / 'trad/oanda_news_interpretation_v1.py').read_bytes()).hexdigest(),
            'historical_outputs_modified': False, 'rows': result}


if __name__ == '__main__':
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--database', type=Path, required=True)
    p.add_argument('--event-id', action='append', required=True)
    p.add_argument('--output', type=Path, required=True)
    args = p.parse_args()
    report = build(args.database, args.event_id)
    with args.output.open('x', encoding='utf-8') as f:
        json.dump(report, f, indent=2)
