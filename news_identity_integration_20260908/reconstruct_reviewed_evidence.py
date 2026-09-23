"""Offline exact-hash recovery of three parent-authorized prior review versions."""
from datetime import datetime, timezone
from hashlib import sha256
import json
from pathlib import Path

BASE = Path('C:/Users/zmoor/Documents/forex')
PROJECT = BASE / 'trad'
OUT = BASE / 'news_identity_integration_20260908/reconstructed_review_sources'
EXPECTED = {
    'oanda_always_on_supervisor.ps1': '594efb0e5dd870b80cbb3db8c37a1ed977a5aa71b51d05ec8ef07cb947f9376e',
    'test_oanda_joint_news_v3_operational_gate.py': 'f43f9bb110ba00c6b252b8d6e6335d34825b0af9b4415b276f86c8ec90e0a899',
    'oanda_project_runtime_health.py': 'b3f9b118680fb2ab496f148684c89a81eeccd1c39a32523aa21971e85828eb45',
}
rows = []
payloads = {}
for name, expected in EXPECTED.items():
    original = PROJECT / name
    data = original.read_bytes()
    candidates = []
    if name.endswith('.ps1'):
        start = data.index(b'-Name "local_news_sentiment_repair_v1"')
        end = data.index(b'$managed += Start-ManagedProcess', start)
        block = data[start:end]
        assert block.count(b'"--interval-sec", "60"') == 1
        candidates = [(data[:start] + block.replace(b'"--interval-sec", "60"', b'"--interval-sec", "15"') + data[end:],
                       'Only repaired producer interval 60 to 15 in its unique launch block')]
    elif name.startswith('test_'):
        suffix = (b"        if name=='local_news_sentiment_repair_v1':\n"
                  b"            assert '\"--interval-sec\", \"60\"' in block\n"
                  b"            assert 'MaxAgeSec = 90' in block\n")
        assert data.endswith(suffix)
        candidates = [(data[:-len(suffix)], 'Remove only final three newly added cadence assertions')]
    else:
        needle = (b"    'news_source_governance_fast_lane_current_prospective_and_inert':('source_governance_news_fast_lane',),\n"
                  b"    'news_watchlist_uses_current_classification_contract':('news_technical_watchlist',),\n}\n")
        assert data.count(needle) == 1
        for before_newline in (b'\n', b'\r\n'):
            for after_newline in (b'\n', b'\r\n'):
                prior = (b"    'news_source_governance_fast_lane_current_prospective_and_inert':('source_governance_news_fast_lane',),"
                         + before_newline + b'}' + after_newline)
                candidates.append((data.replace(needle, prior),
                                   'Remove only new news-watchlist scope mapping; restore the receipt-matched local line endings'))
    matches = [(candidate, description) for candidate, description in candidates
               if sha256(candidate).hexdigest() == expected]
    assert len(matches) == 1, (name, [sha256(v).hexdigest() for v, _ in candidates])
    candidate, description = matches[0]
    destination = OUT / (name + '.txt')
    payloads[destination] = candidate
    rows.append({'original_current_source_path': str(original),
                 'original_current_source_sha256': sha256(data).hexdigest(),
                 'reconstructed_path': str(destination), 'reconstructed_sha256': expected,
                 'bytes': len(candidate), 'method': description,
                 'status': 'reconstructed_exact_hash_match',
                 'expected_binding': 'OPERATIONAL_INDEPENDENT_REVIEW_20260908.json/source_bindings/' + name})
assert not OUT.exists(), 'Never overwrite prior reconstruction'
OUT.mkdir()
for path, data in payloads.items():
    with path.open('xb') as f:
        f.write(data)
receipt = {'schema_version': 'review_source_exact_hash_reconstruction_20260908',
           'generated_utc': datetime.now(timezone.utc).isoformat(),
           'status': 'passed', 'source_edits': False, 'runtime_actions': False,
           'scope': 'Parent-authorized offline reconstruction only; exact prior review hashes verify recovered bytes. These are not claimed as contemporaneously retained source snapshots.',
           'files': rows}
path = OUT / 'REVIEW_SOURCE_RECONSTRUCTION_RECEIPT_20260908.json'
raw = (json.dumps(receipt, indent=2) + '\n').encode('utf-8')
with path.open('xb') as f:
    f.write(raw)
print(json.dumps({'status': 'passed', 'files': rows, 'receipt': str(path),
                  'receipt_sha256': sha256(raw).hexdigest()}, indent=2))
