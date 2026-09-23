"""Deterministic real-OHLC support slices from a pinned retained archive."""
import hashlib,json,time,zipfile
from pathlib import Path
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
from publication import sha256_file
def prepare(archive,contract_path,destination):
    c=json.loads(contract_path.read_text(encoding='utf-8'))
    if sha256_file(archive)!=c['archive_sha256']:raise ValueError('rich_archive_identity_mismatch')
    origins=c['origins'];members=c['members']
    if origins!=list(range(1720396860,1722038400,21600)) or c['support_bars']!=603 or len(members)!=68:raise ValueError('declared_rich_campaign_population_required')
    if destination.exists():raise FileExistsError('rich_preparation_destination_must_be_new')
    destination.mkdir(parents=True);rows=[];start=time.monotonic()
    with zipfile.ZipFile(archive) as z:
        if len(z.namelist())!=68 or set(z.namelist())!={m['path'] for m in members}:raise ValueError('exact_retained_archive_inventory_required')
        for m in members:
            if time.monotonic()-start>300:raise TimeoutError('rich_preparation_time_limit')
            raw=z.read(m['path'])
            if len(raw)!=m['bytes'] or hashlib.sha256(raw).hexdigest()!=m['sha256']:raise ValueError('rich_consumed_archive_member_changed')
            table=pq.read_table(pa.BufferReader(raw),columns=['datetime','open','high','low','close','bid_close','ask_close','volume'])
            epochs=pc.divide(pc.cast(pc.cast(table['datetime'],pa.timestamp('us',tz='UTC')),pa.int64()),1000000)
            keep=None
            for origin in origins:
                mask=pc.and_(pc.greater_equal(epochs,origin-60-602*60),pc.less_equal(epochs,origin-60))
                keep=mask if keep is None else pc.or_(keep,mask)
            selected=table.append_column('time',pc.cast(epochs,pa.int64())).filter(keep).select(['time','open','high','low','close','bid_close','ask_close','volume'])
            name=m['instrument']+'.parquet'
            if Path(name).name!=name:raise ValueError('plain_instrument_name_required')
            p=destination/name;pq.write_table(selected,p,compression='zstd')
            rows.append({'instrument':m['instrument'],'source_member_sha256':m['sha256'],'path':name,'bytes':p.stat().st_size,'sha256':sha256_file(p),'pip_size':c['pip_map'][m['instrument']],'rows':len(selected)})
            del raw,table,selected,epochs
    manifest={'schema_version':'forex_rich_campaign_raw_inputs.v1','preparation_contract_sha256':sha256_file(contract_path),'preparation_source_sha256':sha256_file(Path(__file__)),
        'archive_sha256':c['archive_sha256'],'pip_metadata_sha256':c['pip_metadata_sha256'],'origins':origins,'support_bars':603,'members':rows,
        'availability':'retrospective_completed_bar_end_assumption_not_observed_arrival','pip_metadata_assumption':'stationary2026pip_conventions_applied_to2024','no_model_fits':True}
    (destination/'INPUT_MANIFEST.json').write_text(json.dumps(manifest,indent=2)+'\n',encoding='utf-8')
    return {'status':'VERIFIED','members':len(rows),'rows':sum(r['rows'] for r in rows),'bytes':sum(r['bytes'] for r in rows),'input_manifest_sha256':sha256_file(destination/'INPUT_MANIFEST.json'),'elapsed_seconds':time.monotonic()-start}
