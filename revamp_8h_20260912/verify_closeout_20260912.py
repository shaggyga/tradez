"""Read back the final checkpoint and verify its local document links."""
from pathlib import Path
import hashlib,json,re
ROOT=Path(__file__).resolve().parent
receipt=ROOT/'publication_checkpoint_004/PUBLICATION_RECEIPT.json'
value=json.loads(receipt.read_bytes())
sha=lambda p:hashlib.sha256(Path(p).read_bytes()).hexdigest()
for row in value['updated']:
    assert sha(row['path'])==row['after_sha256']
    assert sha(row['backup'])==row['before_sha256']
for row in value['copied_documents']:
    assert sha(row['source'])==row['source_sha256']
    assert sha(row['exact_source_copy'])==row['source_sha256']
    assert sha(row['destination'])==row['readable_sha256']
report=ROOT.parent/'trad/docs/FOREX_RUN_CLOSEOUT_20260912.md'
checked=[]
for text in re.findall(r'\]\(([^)]+)\)',report.read_text(encoding='utf-8')):
    name=text.strip('<>').split('#',1)[0]
    if name.startswith(('http:','https:')) or not name:continue
    path=(report.parent/name).resolve()
    assert path.exists(),str(path)
    checked.append(str(path))
stop=ROOT/'direction_richer_archive_003/USER_STOPPED_001.json'
assert sha(stop)==value['stop_receipt_sha256']
stopped=json.loads(stop.read_bytes())
for row in stopped['retained_files']:
    assert sha(row['path'])==row['sha256']
assert stopped['prepared_pairs']==['AUD_CAD','AUD_CHF'] and stopped['new_richer_fits']==0
out=ROOT/'publication_checkpoint_004/VERIFICATION_RECEIPT.json'
with out.open('x',encoding='utf-8') as f:json.dump({'status':'verified','publication_sha256':sha(receipt),'updated_documents':len(value['updated']),
    'copied_documents':len(value['copied_documents']),'closeout_local_links':checked,'stop_receipt_sha256':sha(stop),'script_sha256':sha(__file__)},f,indent=2)
print(json.dumps({'verified':True,'updated_documents':len(value['updated']),'copied_documents':len(value['copied_documents']),'local_links':len(checked)}))
