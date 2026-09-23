"""Read-only bounded measurements; output contains sizes/hash counts, no article text."""
import collections
import datetime as dt
import hashlib
import json
from pathlib import Path
import sqlite3
import sys
import time
import zlib

ROOT = Path(__file__).resolve().parents[4]
sys.path.insert(0, str(ROOT))
import projection_revision_reader_v1 as reader

def encoded(value):
    return reader.encoded(value)

def sha(raw):
    return hashlib.sha256(raw).hexdigest()

def ro(path):
    db = sqlite3.connect(Path(path).as_uri() + '?mode=ro', uri=True, timeout=2)
    db.row_factory = sqlite3.Row
    db.execute('PRAGMA query_only=ON')
    db.setlimit(sqlite3.SQLITE_LIMIT_LENGTH, 19 * 1024 * 1024)
    end = time.monotonic() + 10
    db.set_progress_handler(lambda: int(time.monotonic() > end), 1000)
    return db

def run():
    started = time.time()
    cfg = json.loads((ROOT / 'config/revision_news_io_base_operational_v3_20260913.json').read_text())
    output = {'started_utc': dt.datetime.now(dt.timezone.utc).isoformat(),
              'scope': 'read_only_current_contiguous_prefix_with_unchanged_projection_validator',
              'publisher_tables': {}, 'publisher_attempts': [], 'source_tables': {}}
    with ro(cfg['publication_path']) as db:
        for table in ('profile', 'attempts', 'acknowledgments', 'published'):
            row = db.execute(f'SELECT COUNT(*) n, COALESCE(SUM(length(CAST(body AS BLOB))),0) bytes, COALESCE(MAX(length(CAST(body AS BLOB))),0) max_bytes FROM {table}').fetchone()
            output['publisher_tables'][table] = dict(row)
        ids = [r[0] for r in db.execute('SELECT attempt_id FROM attempts ORDER BY rowid')]
    for ident in ids:
        with ro(cfg['publication_path']) as db:
            raw = db.execute('SELECT body FROM attempts WHERE attempt_id=?', (ident,)).fetchone()[0]
        body = json.loads(raw)
        s = body['snapshot']
        output['publisher_attempts'].append({'body_bytes': len(raw.encode()),
            'snapshot_bytes': len(encoded(s)), 'evidence_bytes': s['evidence_bytes'],
            'after_seq': s['after_seq'], 'through_seq': s['through_seq'],
            'row_count': s['row_count'], 'snapshot_policy_bytes': len(encoded(s['policy'])),
            'body_zlib_bytes': len(zlib.compress(raw.encode(), 6))})
        del body, s, raw
    dbpath = cfg['input_identity']['path']
    with ro(dbpath) as db:
        target = db.execute('SELECT COALESCE(MAX(rowid),0) FROM article_source_projections_v1').fetchone()[0]
        tables = ('article_source_versions_v1', 'article_source_observations_v1',
                  'article_classification_observations_v1', 'article_source_projections_v1')
        for table in tables:
            columns = [r[1] for r in db.execute(f'PRAGMA table_info({table})')]
            fields = [c for c in columns if c.endswith('_json')]
            counts = {'rows': db.execute(f'SELECT count(*) FROM {table}').fetchone()[0], 'json_fields': {}}
            for field in fields:
                counts['json_fields'][field] = dict(db.execute(f'SELECT COALESCE(SUM(length(CAST({field} AS BLOB))),0) bytes, COALESCE(MAX(length(CAST({field} AS BLOB))),0) max_bytes FROM {table}').fetchone())
            output['source_tables'][table] = counts
    checkpoint = None
    row_seen = {}; blob_seen = {}; descriptor_seen = {}
    role_stats = collections.defaultdict(lambda: collections.Counter())
    field_stats = collections.defaultdict(lambda: collections.Counter())
    field_seen = collections.defaultdict(set)
    evidence_total = compressed_evidence = entries = rows = descriptor_refs = 0
    max_evidence = max_decoded_page_bytes = 0
    roles_equal = collections.Counter()
    batches = []
    deadline = time.monotonic() + 160
    def clock():
        return time.time(), json.loads(Path(cfg['clock_path']).read_text())
    while checkpoint is None or checkpoint['through_seq'] < target:
        if time.monotonic() > deadline:
            raise RuntimeError('diagnostic_time_bound')
        snapshot = reader.read_projection_snapshot(dbpath, checkpoint=checkpoint,
            expected_sources=cfg['policy']['sources'], expected_provenance=cfg['policy']['provenance'],
            clock_provider=clock)
        if not snapshot['entries']:
            raise RuntimeError('prefix_not_completed')
        max_decoded_page_bytes = max(max_decoded_page_bytes, snapshot['evidence_bytes'])
        for evidence in snapshot['entries']:
            raw_evidence = encoded(evidence)
            z = zlib.compress(raw_evidence, 6)
            assert zlib.decompress(z) == raw_evidence
            evidence_total += len(raw_evidence)
            compressed_evidence += len(z)
            max_evidence = max(max_evidence, len(raw_evidence))
            entries += 1
            for a,b in [('source_version','canonical_first_version'), ('source_observation','canonical_first_observation'), ('classification_first','classification_available')]:
                if evidence[a] == evidence[b]: roles_equal[a + '==' + b] += 1
            for role, row in evidence.items():
                rows += 1
                raw_row = encoded(row)
                h = sha(raw_row)
                stats = role_stats[role]
                stats['references'] += 1; stats['encoded_bytes'] += len(raw_row)
                if h not in row_seen:
                    row_seen[h] = (len(raw_row), len(zlib.compress(raw_row, 6)))
                descriptor = {}
                local_blobs = {}
                for field, value in row.items():
                    if field.endswith('_json') and isinstance(value, str):
                        raw_value = value.encode('utf-8'); vh = sha(raw_value)
                        local_blobs[vh] = raw_value
                        descriptor[field] = {'utf8_blob_sha256': vh, 'bytes': len(raw_value)}
                        fs = field_stats[role + '.' + field]
                        fs['references'] += 1; fs['raw_utf8_bytes'] += len(raw_value)
                        field_seen[role + '.' + field].add(vh)
                        if vh not in blob_seen:
                            blob_seen[vh] = (len(raw_value), len(zlib.compress(raw_value, 6)))
                    else:
                        descriptor[field] = value
                rebuilt = {k: local_blobs[v['utf8_blob_sha256']].decode('utf-8')
                           if k.endswith('_json') and isinstance(v, dict) and 'utf8_blob_sha256' in v else v
                           for k,v in descriptor.items()}
                assert encoded(rebuilt) == raw_row
                raw_descriptor = encoded(descriptor); dh = sha(raw_descriptor)
                if dh not in descriptor_seen:
                    descriptor_seen[dh] = (len(raw_descriptor), len(zlib.compress(raw_descriptor, 6)))
                descriptor_refs += len(encoded({'role': role, 'row_sha256': dh}))
        batches.append({k:snapshot[k] for k in ('after_seq','through_seq','row_count','evidence_bytes')})
        checkpoint = snapshot['next_checkpoint']
        del snapshot
    output.update({'completed_utc': dt.datetime.now(dt.timezone.utc).isoformat(),
        'elapsed_sec': time.time()-started, 'initial_projection_high':target,
        'validated_through_seq':checkpoint['through_seq'], 'validated_entries':entries,
        'canonical_evidence_bytes':evidence_total, 'max_single_evidence_bytes':max_evidence,
        'max_page_evidence_bytes':max_decoded_page_bytes,
        'per_evidence_independent_zlib_bytes': compressed_evidence,
        'role_equality_counts':dict(roles_equal), 'evidence_role_references':rows,
        'exact_row_cas':{'unique_rows':len(row_seen), 'unique_raw_bytes':sum(v[0] for v in row_seen.values()),
                         'independent_zlib_bytes':sum(v[1] for v in row_seen.values())},
        'exact_json_string_cas':{'unique_blobs':len(blob_seen), 'unique_raw_bytes':sum(v[0] for v in blob_seen.values()),
            'independent_zlib_blob_bytes':sum(v[1] for v in blob_seen.values()),
            'unique_row_descriptors':len(descriptor_seen),
            'raw_descriptor_bytes':sum(v[0] for v in descriptor_seen.values()),
            'independent_zlib_descriptor_bytes':sum(v[1] for v in descriptor_seen.values()),
            'reference_bytes_uncompressed':descriptor_refs,
            'all_individual_row_reconstructions_byte_exact':True},
        'role_sizes':dict(role_stats),
        'largest_json_fields': sorted([{'field':k,**v,'unique_hashes':len(field_seen[k])} for k,v in field_stats.items()],
                                     key=lambda x:x['raw_utf8_bytes'],reverse=True),
        'batches':batches,
        'limits':['Compressed byte size is not Python decoded-object heap size.',
                  'CAS sizes omit database/index framing; include original UTF-8 bytes without reserialization.',
                  'Measurement does not authorize prior failed-attempt readmission or import publication clocks.',
                  'Single-page decoded evidence only; seen-state keeps hashes/lengths, never all article bodies.']})
    destination = Path(__file__).with_name('revision_capacity_diagnostic.json')
    destination.write_text(json.dumps(output,indent=2,sort_keys=True),encoding='utf-8')
    print(json.dumps({k:output[k] for k in ('completed_utc','elapsed_sec','validated_entries','canonical_evidence_bytes',
          'per_evidence_independent_zlib_bytes','publisher_tables','role_equality_counts','exact_row_cas','exact_json_string_cas')},indent=2))

if __name__ == '__main__':
    run()
