"""Preserve the exact interim documents named by their completed fact check."""
import hashlib
import json
from pathlib import Path
import time

BASE=Path(__file__).resolve().parent
ROOT=BASE.parent/'trad'
REVIEW=BASE/'richer_inputs/curve_risk_attachment_v1/CONSOLIDATED_REPORT_INDEPENDENT_FACT_CHECK_20260909.json'
EXPECTED='e6dd5308c938218c9c55cde5ec4cb0852daaeba64d3663cac8c01302f1820108'


def sha(raw):return hashlib.sha256(raw).hexdigest()


def main():
    review=REVIEW.read_bytes()
    if sha(review)!=EXPECTED:raise ValueError('fact_check_changed')
    rows=json.loads(review)['documents'];payloads=[]
    expected={ROOT/'docs/FOREX_OVERNIGHT_CURVE_BUILDOUT_20260909.md',ROOT/'FOREX_PENDING_IMPROVEMENTS.md'}
    if len(rows)!=2 or {Path(r['path']) for r in rows}!=expected:raise ValueError('exact_two_documents_required')
    for row in rows:
        raw=Path(row['path']).read_bytes()
        if sha(raw)!=row['sha256'] or len(raw)!=row['bytes']:raise ValueError('reviewed_document_changed')
        payloads.append(raw)
    out=BASE/'documentation_reviewed_history_1021_20260909';out.mkdir(exist_ok=False)
    retained=[]
    for row,raw in zip(rows,payloads):
        target=out/Path(row['path']).name
        with target.open('xb') as h:h.write(raw)
        if target.read_bytes()!=raw:raise ValueError('history_readback_changed')
        retained.append(dict(original_source=row,retained_path=str(target),sha256=sha(raw),bytes=len(raw)))
    value=dict(schema_version='reviewed_interim_document_history_v1_20260909',retained_epoch=time.time(),
        original_review=dict(path=str(REVIEW),sha256=EXPECTED),documents=retained,
        original_acceptance_or_review_rewritten=False,project_or_vault_writes=False,
        purpose='Exact historical bytes remain available when the current report and queue receive later observations.')
    target=out/'REVIEWED_DOCUMENT_HISTORY_20260909.json';raw=(json.dumps(value,indent=2)+'\n').encode()
    with target.open('xb') as h:h.write(raw)
    print(json.dumps(dict(path=str(target),sha256=sha(raw))))


if __name__=='__main__':main()
