from pathlib import Path
import json

base = Path(__file__).resolve().parent
audit = json.loads((base / 'inventory/LEGACY_VAULT_PAYLOAD_AUDIT.json').read_bytes())
root = Path('C:/Users/zmoor/Documents/forex/trad/artifacts/vault_cleanup/review_reset_20260905/payload')
rows = [r for r in audit['files'] if not r.get('matches_receipt')][:4]
for row in rows:
    path = root / row['relative_path']
    extended = Path('\\\\?\\' + str(path))
    print(json.dumps({'path_length': len(str(path)), 'normal_exists': path.is_file(),
                      'extended_exists': extended.is_file(),
                      'extended_bytes': extended.stat().st_size if extended.is_file() else None,
                      'expected_bytes': row['bytes']}))
