"""News research JSON snapshots and immutable report publication; no service API."""
from __future__ import annotations
from datetime import datetime,timezone
import errno
import hashlib
import json
import math
import os
from pathlib import Path
import tempfile
import time
import uuid

MAX_JSON_BYTES=64*1024*1024


def _unique_object(pairs):
    result={}
    for key,value in pairs:
        if key in result:raise ValueError('duplicate_json_key:'+key)
        result[key]=value
    return result


def _nonfinite(value):raise ValueError('nonfinite_json_constant:'+value)


def _finite_float(value):
    result=float(value)
    if not math.isfinite(result):raise ValueError('nonfinite_json_number')
    return result


def read_json_snapshot(path,*,allow_missing=False,maximum_bytes=MAX_JSON_BYTES):
    if type(maximum_bytes) is not int or not 1<=maximum_bytes<=MAX_JSON_BYTES:raise ValueError('bounded_json_size_required')
    path=Path(path)
    try:
        with path.open('rb') as handle:raw=handle.read(maximum_bytes+1)
    except FileNotFoundError:
        if not allow_missing:raise
        return {},{'path':str(path.absolute()),'sha256':None,'bytes':None,'status':'missing_at_read'}
    if len(raw)>maximum_bytes:raise ValueError('json_snapshot_size_bound_exceeded')
    payload=json.loads(raw.decode('utf-8-sig'),object_pairs_hook=_unique_object,parse_constant=_nonfinite,parse_float=_finite_float)
    if not isinstance(payload,dict):raise ValueError('json_snapshot_mapping_required')
    return payload,{'path':str(path.absolute()),'sha256':hashlib.sha256(raw).hexdigest(),'bytes':len(raw),'status':'consumed_exact_bytes'}


def new_run_report_path(template):
    template=Path(template)
    stem=template.stem.removesuffix('_latest')
    return template.with_name(stem+'_run_'+datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S%fZ')+'_'+uuid.uuid4().hex[:12]+'.json')


def publish_new_json_report(path,payload,*,maximum_bytes=MAX_JSON_BYTES):
    if type(maximum_bytes) is not int or not 1<=maximum_bytes<=MAX_JSON_BYTES:raise ValueError('bounded_json_size_required')
    raw=json.dumps(payload,indent=2,sort_keys=True,allow_nan=False).encode('utf-8')
    if len(raw)>maximum_bytes:raise ValueError('json_report_size_bound_exceeded')
    path=Path(path);path.parent.mkdir(parents=True,exist_ok=True)
    descriptor,name=tempfile.mkstemp(prefix=path.name+'.owned_',suffix='.tmp',dir=path.parent)
    temporary=Path(name);published=False
    try:
        with os.fdopen(descriptor,'wb') as handle:handle.write(raw);handle.flush();os.fsync(handle.fileno())
        try:os.link(temporary,path)
        except FileExistsError as exc:raise ValueError('refuse_overwriting_existing_news_report') from exc
        except OSError as exc:raise ValueError('immutable_news_publication_failed:'+type(exc).__name__) from exc
        published=True
    finally:
        for attempt in range(8):
            try:temporary.unlink();break
            except FileNotFoundError:break
            except PermissionError:
                if attempt==7:raise RuntimeError('owned_temp_cleanup_failed:'+str(temporary)+';maximum_bytes='+str(maximum_bytes)+';final_published='+str(published))
                time.sleep(min(.01*(attempt+1),.08))
    return {'path':str(path.absolute()),'sha256':hashlib.sha256(raw).hexdigest(),'bytes':len(raw),'publication':'immutable_new_file_hardlink_no_overwrite'}
