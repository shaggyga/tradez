"""Capture three bounded, unchanged-on-read M1 archives for offline research."""
from datetime import datetime, timezone
import csv
import hashlib
import io
import json
import os
from pathlib import Path
import time

ROOT=Path(r'C:\Users\zmoor\Documents\forex\trad')
OUT=Path(__file__).resolve().parent/'ma_history_audit'
PAIRS=('EUR_USD','GBP_USD','USD_JPY')
MAX_BYTES=20*1024*1024


def main():
    OUT.mkdir(exist_ok=False)
    private=OUT/'private_sources'
    private.mkdir()
    sources={}
    for pair in PAIRS:
        source=ROOT/'data/oanda_training_manager/candles'/(pair+'_M1.csv')
        begun=time.time()
        with source.open('rb') as handle:
            before=os.fstat(handle.fileno())
            raw=handle.read(MAX_BYTES+1)
            after=os.fstat(handle.fileno())
        retained=source.stat()
        completed=time.time()
        identity=lambda s:(s.st_dev,s.st_ino,s.st_size,s.st_mtime_ns)
        if len({identity(s) for s in (before,after,retained)})!=1:
            raise ValueError('source_changed_during_read:'+pair)
        if not 0<len(raw)<=MAX_BYTES or not raw.endswith(b'\n'):
            raise ValueError('source_byte_bound_or_partial_tail:'+pair)
        path=private/(pair+'_M1.csv')
        with path.open('xb') as handle:
            handle.write(raw)
            handle.flush()
            os.fsync(handle.fileno())
        if path.read_bytes()!=raw:
            raise ValueError('copy_readback_mismatch:'+pair)
        rows=list(csv.DictReader(io.StringIO(raw.decode('utf8'))))
        epochs=[]
        for row in rows:
            if row['instrument']!=pair or row['granularity']!='M1':
                raise ValueError('source_identity:'+pair)
            epoch=datetime.fromisoformat(row['time'].replace('Z','+00:00')).timestamp()
            if epoch%60 or (epochs and epoch<=epochs[-1]) or epoch+60>completed:
                raise ValueError('source_clock_order_or_future:'+pair)
            epochs.append(epoch)
        segments=[]
        start=0
        for i in range(1,len(epochs)+1):
            if i==len(epochs) or epochs[i]-epochs[i-1]!=60:
                segments.append(dict(start_index=start,end_index=i-1,row_count=i-start,
                    first_bar_label_epoch=epochs[start],last_bar_label_epoch=epochs[i-1],
                    warmed_feature_rows=max(0,i-start-203)))
                start=i
        sources[pair]=dict(source_path=str(source),retained_path=str(path),
            read_started_epoch=begun,read_completed_epoch=completed,source_stat_unchanged=True,
            source_file_size=before.st_size,source_mtime_ns=before.st_mtime_ns,
            retained_sha256=hashlib.sha256(raw).hexdigest(),retained_bytes=len(raw),
            row_count=len(rows),columns=list(rows[0]),first_bar_label_epoch=epochs[0],
            latest_bar_label_epoch=epochs[-1],latest_price_effective_epoch=epochs[-1]+60,
            price_age_at_capture_sec=completed-epochs[-1]-60,
            contiguous_segments=segments,segment_count=len(segments),
            qualifying_segments_204_rows=sum(s['row_count']>=204 for s in segments),
            total_warmed_feature_rows=sum(s['warmed_feature_rows'] for s in segments),
            maximum_segment_rows=max(s['row_count'] for s in segments))
    value=dict(schema='ma_history_capture_20260909',captured_utc=datetime.now(timezone.utc).isoformat(),
        sources=sources,capture_implementation_sha256=hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
        provenance='existing_archived_complete_M1_mid_and_bid_ask_OHLC_observed_now',
        historical_ingestion_clocks_retained=False,historical_availability_proven=False,
        timestamps='bar_start_labels_price_effective_at_label_plus60',
        intended_use='retrospective_feature_and_model_engineering_only',
        research_only=True,forecast_issued=False,can_place_orders=False,can_promote=False)
    path=OUT/'MA_HISTORY_CAPTURE_20260909.json'
    with path.open('x',encoding='utf8') as handle:
        json.dump(value,handle,sort_keys=True,indent=2,allow_nan=False)
    print(json.dumps(dict(path=str(path),sha256=hashlib.sha256(path.read_bytes()).hexdigest(),
        sources={p:{k:s[k] for k in ('row_count','retained_bytes','segment_count','qualifying_segments_204_rows',
            'total_warmed_feature_rows','maximum_segment_rows','price_age_at_capture_sec')} for p,s in sources.items()})))


if __name__=='__main__':
    main()
