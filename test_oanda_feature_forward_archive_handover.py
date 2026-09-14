"""Real frozen ledger/worker with owned protocol fixtures; no live writes."""
import json
from pathlib import Path
import sys

HERE=Path(__file__).resolve().parent
TRAD=HERE.parents[1]/'trad'
if TRAD.exists():sys.path.insert(0,str(TRAD))
import oanda_feature_forward_ledger_v1 as ledger
import oanda_feature_forward_worker_v1 as worker
from test_oanda_feature_forward_v1 import NOW, PAIR, maps, quote, proof


def test_same_pinned_owner_new_archive_keeps_original_jobs_and_targets(tmp_path):
    now=[NOW]
    dbpath=tmp_path/'retained.sqlite'
    owner=ledger.ForwardLedger(dbpath,clock=lambda:now[0],minimum_free_bytes=0)
    owner.observe_quotes({PAIR:quote(now[0])},read_started_epoch=now[0],read_completed_epoch=now[0],force_reference=True)
    oldmaps=maps(now[0])
    for data in oldmaps.values():data['source_snapshots'][0]['source_schema_id']='previous_producer'
    assert all(r['status']=='published' for r in owner.publish_comparisons(oldmaps))
    meta=dict(owner.db.execute('select key,value from ff_meta'))
    jobs=owner.db.execute('select * from ff_jobs order by id').fetchall()
    batches=owner.db.execute('select * from ff_batches order by id').fetchall()
    publications=owner.db.execute('select * from ff_publications order by batch').fetchall()
    assert len(jobs)==9
    owner.close()

    # Restart exact same source/protocol/resource owner, changing only readroot.
    now[0]=NOW+1
    owner=ledger.ForwardLedger(dbpath,clock=lambda:now[0],minimum_free_bytes=0)
    successor=tmp_path/'successor_archive';successor.mkdir()
    readroots=[]
    def reader(root,**kwargs):
        readroots.append(Path(root))
        values=maps(now[0])
        for data in values.values():data['source_snapshots'][0]['source_schema_id']='successor_producer'
        return values
    def quotes_reader(path,clock):
        at=clock()
        return {PAIR:quote(at,bid='1.1010',ask='1.1012')},at,at,{'owned_fixture':True}
    runner=worker.ForwardWorker(owner,clock_check=lambda:proof(now[0]),archive_root=successor,
        quote_path=tmp_path/'owned_quotes.json',mapping_reader=reader,quote_reader=quotes_reader)
    first=runner.tick()
    assert not first['errors']
    assert owner.db.execute('select * from ff_jobs order by id').fetchall()==jobs
    assert not readroots
    now[0]=NOW+300
    second=runner.tick()
    assert not second['errors'] and second['settlement']['scored']==3
    assert readroots==[successor]
    assert dict(owner.db.execute('select key,value from ff_meta'))==meta
    for old in jobs:assert owner.db.execute('select * from ff_jobs where id=?',(old[0],)).fetchone()==old
    for old in batches:assert owner.db.execute('select * from ff_batches where id=?',(old[0],)).fetchone()==old
    for old in publications:assert owner.db.execute('select * from ff_publications where batch=?',(old[0],)).fetchone()==old
    outcomes=[json.loads(raw) for raw, in owner.db.execute('select body from ff_outcomes')]
    assert all(value['publication_epoch']==NOW and value['target_epoch']==NOW+300 for value in outcomes)
    assert all(value['entry']['market_epoch']==NOW+1 and value['target']['market_epoch']==NOW+300 for value in outcomes)
    schemas=set()
    for raw, in owner.db.execute('select body from ff_batches'):
        schemas.update(row['source_schema_id'] for row in ledger.unpack(raw)['source_snapshots'])
    assert schemas=={'previous_producer','successor_producer'}
    assert owner.db.execute('select count(*) from ff_jobs').fetchone()[0]==18
    owner.close()
