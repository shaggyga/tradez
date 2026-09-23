"""Create only the three sealed V4 directories; no evidence or source mutation."""
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import stat

ROOT = Path(r'C:\Users\zmoor\Documents\forex\trad')
AREA = ROOT.parent / 'operational_repairs_20260913'
GENERATION = ROOT / 'data/oanda_training_manager/operational_repair_20260913_v4'
NEWS = GENERATION / 'revision_news_v1'
SEALED = {
    'revision_news_io_base_operational_v4_20260913.json': '6e353421b46c74fac7c8b298285205314502743ffa10788df9e10ef89d204c04',
    'revision_transport_operational_v4_20260913.json': 'ba63aa268bda99176df5847e42c65152fcfa80f3c8c7a501b9d900ff1876ba69',
}

def plain_directory(path):
    for part in (*reversed(path.parents), path):
        info = part.lstat()
        if not stat.S_ISDIR(info.st_mode) or stat.S_ISLNK(info.st_mode) or getattr(info, 'st_file_attributes', 0) & 1024:
            raise ValueError('existing_plain_directory_required')

configs = []
for name, expected in SEALED.items():
    raw = (ROOT / 'config' / name).read_bytes()
    if hashlib.sha256(raw).hexdigest() != expected:
        raise ValueError('sealed_configuration_changed')
    configs.append(json.loads(raw))
base, transport = configs
if Path(base['publication_path']).parent != NEWS or Path(base['observation_path']).parent != NEWS:
    raise ValueError('declared_store_parent_mismatch')
if Path(base['archive_root']) != NEWS / 'capture_archive' or Path(transport['state_root']) != NEWS / 'transport_state':
    raise ValueError('declared_directory_mismatch')
plain_directory(GENERATION)
records = []
for path in (NEWS, NEWS / 'transport_state', NEWS / 'capture_archive'):
    plain_directory(path.parent)
    existed = path.exists()
    if not existed:
        path.mkdir()
    plain_directory(path)
    records.append({'path': str(path), 'created': not existed})
receipt = {'observed_utc': datetime.now(timezone.utc).isoformat(), 'directories': records,
           'configuration_hashes': SEALED, 'sources_modified': False,
           'records_or_store_files_created_by_this_helper': False,
           'scope': 'Deployment prerequisite only; worker guards unchanged.'}
output = AREA / ('NATIVE_V4_DIRECTORY_PROVISION_' + datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S%fZ') + '.json')
with output.open('x', encoding='utf-8') as stream:
    json.dump(receipt, stream, sort_keys=True, indent=2)
print(json.dumps({'receipt': str(output), 'directories': records}))
