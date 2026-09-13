"""Adapt retained synthetic slim histories through the real admission writer.

This is fixture construction only. No production database migration is offered.
Original member/payload/effective/mapping clocks and hashes are retained.
"""
import hashlib,json,sqlite3
from pathlib import Path
import oanda_causal_forecast_inputs_joint_news_v3 as joint
import oanda_isolated_news_history_v1 as history

def prepare_fixture_admissions(database):
    database=Path(database);root=database.parent.parent
    config=root/'admission_fixture_sources.json';config.write_text(json.dumps({'sources':[{'source_id':'synthetic_retained_history'}]}))
    config_sha=hashlib.sha256(config.read_bytes()).hexdigest()
    path,sources=history.read_config(config,config_sha);paths=history.paths_for(root)
    assert paths['database_path']==database
    policy=history.policy_for(paths,path,config_sha,sources)
    with sqlite3.connect(database) as con:
        for table in ('isolated_news_history_admissions_v1','isolated_news_history_admission_visibility_v1','isolated_news_history_profile_v1','source_contracts'):
            con.execute('DROP TABLE IF EXISTS '+table)
        con.execute('''CREATE TABLE source_contracts(source_contract_id TEXT PRIMARY KEY,source_id TEXT,source_cohort_id TEXT,created_utc TEXT,contract_sha256 TEXT,contract_json TEXT)''')
        con.execute('''CREATE TABLE isolated_news_history_profile_v1(id INTEGER PRIMARY KEY,policy_sha256 TEXT,policy_json TEXT,activated_epoch REAL)''')
        joint._ensure_history_admission_schema(con);history.governance.insert_contracts(con,sources)
        columns={r[1] for r in con.execute('PRAGMA table_info(news_fast_lane_batches_v3)')}
        for name,kind in [('input_identity','TEXT'),('input_rowid','INTEGER'),('input_anchor_event_id','TEXT'),('scan_started_utc','TEXT')]:
            if name not in columns:con.execute('ALTER TABLE news_fast_lane_batches_v3 ADD COLUMN '+name+' '+kind)
        rows=con.execute('SELECT b.batch_seq,v.mapping_visible_utc FROM news_fast_lane_batches_v3 b JOIN news_fast_lane_visibility_v3 v USING(batch_seq) ORDER BY b.batch_seq').fetchall()
        activated=min(joint._epoch(r[1]) for r in rows)
        con.execute('INSERT INTO isolated_news_history_profile_v1 VALUES(1,?,?,?)',(history.sha(history.encoded(policy)),history.encoded(policy).decode(),activated))
        for sequence,visible in rows:
            con.execute('UPDATE news_fast_lane_batches_v3 SET input_identity=?,input_rowid=?,input_anchor_event_id=?,scan_started_utc=? WHERE batch_seq=?',
                ('synthetic-retained-history',sequence,'fixture-batch-'+str(sequence),visible,sequence))
        con.commit()
    # Capacity fixtures explicitly construct synthetic durable receipts using
    # real batch-capture/canonical helpers, then validate every receipt through
    # the real consumer admission validator in a pinned read transaction.
    # End-to-end writer/commit/clock ordering is covered by the separate44 tests.
    with sqlite3.connect(database) as con:
        con.row_factory=sqlite3.Row
        for sequence,visible in rows:
            stamp=joint._epoch(visible)
            state={'schema_version':1,'status':'ok','generated_utc':joint._iso(stamp-1),'timestamp_normalization_trusted':True,
                'host_clock_synchronized':True,'source_fresh':True,'broker_clock_lead_sec':.2,'broker_clock_sample_count':128,'source_age_sec':1}
            history.repair.validate_clock_state(state,stamp)
            receipt={'schema_version':joint.HISTORY_ADMISSION_SCHEMA,'policy_sha256':history.sha(history.encoded(policy)),
                'batch_seq':sequence,'previous_sequence':sequence-1,'capture':joint._history_batch_capture(con,sequence),
                'observed_epoch':stamp,'completed_epoch':stamp,'admitted_epoch':stamp,
                'original_clock_state':state,'completion_clock_state':state}
            receipt_sha=joint._digest(receipt)
            con.execute('INSERT INTO isolated_news_history_admissions_v1 VALUES(?,?,?,?)',
                (sequence,stamp,receipt_sha,joint._encoded(receipt).decode()))
            ack={'schema_version':joint.HISTORY_ADMISSION_SCHEMA,'batch_seq':sequence,'receipt_sha256':receipt_sha,
                'admitted_available_epoch':stamp,'availability_basis':'independent_committed_admission_read','clock_state':state}
            con.execute('INSERT INTO isolated_news_history_admission_visibility_v1 VALUES(?,?,?,?)',
                (sequence,stamp,joint._digest(ack),joint._encoded(ack).decode()))
        con.commit()
    with sqlite3.connect(database) as con:
        con.row_factory=sqlite3.Row;con.execute('PRAGMA query_only=ON');con.execute('BEGIN')
        checked_profile=joint._history_profile(con)
        for sequence,visible in rows:joint._history_admission(con,sequence,joint._epoch(visible),checked_profile)
