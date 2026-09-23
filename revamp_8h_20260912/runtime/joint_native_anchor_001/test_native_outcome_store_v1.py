"""Owned exact CSV/SQLite fixtures; no historical prices or model fits."""
from datetime import datetime,timezone
from pathlib import Path
import copy
import hashlib
import json
import sqlite3
import tempfile
import unittest
from unittest.mock import patch
import native_m1_outcome_v1 as o
import native_m1_ledger_v1 as s

C=1700000000//60*60
def policy():return {'schema_version':s.POLICY,'maximum_retained_source_bytes':8*1024**2,
    'maximum_source_observations':100,'maximum_source_blobs':100,'maximum_metadata_bytes':8*1024**2,
    'maximum_score_attempts':100,'maximum_target_records':100,
    'target_selection':'first_actual_admitted_exact_native_close','score_recipe':o.SCORE_RECIPE}
def anchor(c=C):return o.native.make_native_anchor(instrument='EUR_USD',pip_size=.0001,
    origin_bar_start_epoch=c,origin_mid=1.1,expected_signed_pips=10.,probability_up=.6,
    source_anchor_complete=True,source_observed_epoch=float(c+61),feature_decision_epoch=float(c+62),
    computation_started_epoch=float(c+63),computation_completed_epoch=float(c+65),issued_epoch=float(c+70)).as_dict()
def forecast(c=C):
    a=anchor(c);return {'decision_id':str(c),'forecasts':[{'native_anchor':a}]}
def line(epoch,close,complete='true'):
    return datetime.fromtimestamp(epoch,timezone.utc).isoformat()+','+str(close)+','+complete+'\n'
class Owner:
    def __init__(self,path,clock=C+4000.):
        self.path=path;self.contract={'instrument':'EUR_USD','native_outcome_policy':policy()}
        self.contract_hash=o.digest(self.contract);self.db=sqlite3.connect(path);self.db.row_factory=sqlite3.Row
        self.db.executescript(s.SQL);self.at=clock;self.fail_clock=False
    def clock(self):self.at+=.01;return self.at
    def get_native_clock_proof(self,path):
        if self.fail_clock:raise ValueError('fixture_clock_failure')
        return {'observed_epoch':self.clock(),'fixture_actual_clock_sample':True}
    def validate_native_clock_proof(self,p,at):
        assert set(p)=={'observed_epoch','fixture_actual_clock_sample'} and p['fixture_actual_clock_sample'] is True
        assert type(p['observed_epoch']) is float and p['observed_epoch']<=at
    def close(self):self.db.close()

class NativeFixtures(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.root=Path(self.tmp.name);self.path=self.root/'EUR_USD_M1.csv'
        self.owner=Owner(self.root/'owned.sqlite');self.write()
    def tearDown(self):self.owner.close();self.tmp.cleanup()
    def write(self,rows=None,header='datetime,close,complete\n'):
        self.path.write_text(header+''.join(rows or [line(C,1.1),line(C+3600,1.101)]),encoding='utf-8',newline='')
    def capture(self):return o.capture_source(self.path,'EUR_USD',clock=self.owner.clock)
    def admit(self,**kw):return s.observe_source(self.owner,self.path,clock_path='fixture_clock',**kw)
    def test_exact_target_not_later_quote(self):
        cap=self.capture();target=o.select_exact_target(anchor(),cap)
        self.assertEqual(target['target_mid_hex'],(1.101).hex());self.assertEqual(target['target_close_epoch'],C+3660)
    def test_missing_target_with_later_row_stays_unknown(self):
        self.write([line(C,1.1),line(C+3660,1.2)])
        self.assertEqual(o.select_exact_target(anchor(),self.capture())['status'],'unknown')
    def test_stale_current_source_no_prediction_gate(self):
        self.owner.at=C+100000.;self.assertEqual(self.capture()['status'],'captured')
    def test_optional_complete_and_extra_reordered_columns(self):
        self.write([f'1.1,extra,{datetime.fromtimestamp(C,timezone.utc).isoformat()}\n',
            f'1.101,extra,{datetime.fromtimestamp(C+3600,timezone.utc).isoformat()}\n'],'close,unused,time\n')
        cap=self.capture();self.assertEqual(cap['status'],'captured')
        self.assertEqual(cap['row_evidence'][str(C)]['complete_flag'],'absent_clock_completion_only')
    def test_duplicate_and_conflicting_timestamps_refused(self):
        for value in (1.101,1.2):
            with self.subTest(value=value):
                self.write([line(C,1.1),line(C+3600,1.101),line(C+3600,value)])
                cap=self.capture();self.assertEqual(cap['status'],'refused');self.assertIsNotNone(cap['source'])
    def test_incomplete_future_and_bad_close_refused(self):
        for row in (line(C+3600,1.101,'false'),line(C+6000,1.1),line(C+3600,'nan')):
            with self.subTest(row=row):
                self.write([line(C,1.1),row]);self.assertEqual(self.capture()['status'],'refused')
    def test_missing_file_visible_then_recovers(self):
        self.path.unlink();first=self.admit();self.assertEqual(first['status'],'refused')
        self.write();second=self.admit();self.assertEqual(second['status'],'admitted_exact_source')
        self.assertEqual(self.owner.db.execute('SELECT COUNT(*) FROM native_observations').fetchone()[0],2)
    def test_failed_postsource_commit_not_admitted_by_retry(self):
        def fail(point):
            if point=='after_source_commit':raise ValueError('injected')
        self.assertEqual(self.admit(fault_hook=fail)['status'],'refused')
        self.assertEqual(s.admitted_rows(self.owner),[])
        self.assertEqual(self.admit()['sequence'],2)
        self.assertEqual([r['seq'] for r in s.admitted_rows(self.owner)],[2])
    def test_failed_postadmission_commit_not_rehabilitated(self):
        def fail(point):
            if point=='after_admission_commit':raise ValueError('injected')
        self.admit(fault_hook=fail);self.assertEqual(s.admitted_rows(self.owner),[])
        self.admit();self.assertEqual([r['seq'] for r in s.admitted_rows(self.owner)],[2])
    def test_new_source_cannot_replace_first_target_or_origin(self):
        self.admit();f=forecast();s.process_sources(self.owner,[f]);s.settle(self.owner,[f],clock_path='fixture_clock')
        first=self.owner.db.execute('SELECT body FROM native_scores').fetchone()[0]
        self.write([line(C,1.09),line(C+3600,1.105)])
        self.admit();s.process_sources(self.owner,[f]);s.settle(self.owner,[f],clock_path='fixture_clock')
        self.assertEqual(self.owner.db.execute('SELECT body FROM native_scores').fetchone()[0],first)
        self.assertEqual(self.owner.db.execute('SELECT COUNT(*) FROM native_revisions').fetchone()[0],1)
    def test_interrupted_score_commit_fresh_attempt_recovers(self):
        self.admit();f=forecast();s.process_sources(self.owner,[f])
        def fail(point):
            if point=='after_score_commit':raise ValueError('injected')
        with self.assertRaisesRegex(ValueError,'injected'):s.settle(self.owner,[f],clock_path='fixture_clock',fault_hook=fail)
        self.assertEqual(s.outcome_statuses(self.owner,[f],as_of_epoch=self.owner.at)[0]['status'],'unknown')
        self.assertEqual(s.settle(self.owner,[f],clock_path='fixture_clock'),1)
        self.assertEqual(self.owner.db.execute('SELECT COUNT(*) FROM native_scores').fetchone()[0],2)
        self.assertEqual(self.owner.db.execute('SELECT COUNT(*) FROM native_score_visibility').fetchone()[0],1)
    def test_same_source_blob_shared_between_captures_and_targets(self):
        self.admit();self.admit();self.assertEqual(self.owner.db.execute('SELECT COUNT(*) FROM native_blobs').fetchone()[0],1)
        s.process_sources(self.owner,[forecast()]);self.assertEqual(len(s.admitted_rows(self.owner,unprocessed=True)),0)
        with patch.object(o,'verify_capture',side_effect=AssertionError('must_not_reparse_processed')):
            s.process_sources(self.owner,[forecast()])
    def test_no_target_until_recovered_exact_minute(self):
        self.write([line(C,1.1),line(C+3660,1.11)]);self.admit();f=forecast();s.process_sources(self.owner,[f])
        self.assertEqual(s.settle(self.owner,[f],clock_path='fixture_clock'),0)
        self.write();self.admit();s.process_sources(self.owner,[f]);self.assertEqual(s.settle(self.owner,[f],clock_path='fixture_clock'),1)
    def test_capacity_refuses_before_payload_and_retains_diagnostic(self):
        self.owner.contract['native_outcome_policy']['maximum_retained_source_bytes']=1
        value=self.admit();self.assertEqual(value['status'],'refused')
        self.assertEqual(self.owner.db.execute('SELECT COUNT(*) FROM native_blobs').fetchone()[0],0)
        self.assertEqual(s.usage(self.owner)['source_bytes'],0)
        self.assertEqual(self.owner.db.execute('SELECT COUNT(*) FROM native_diagnostics').fetchone()[0],1)
    def test_metadata_capacity_rolls_back_blob_and_journal(self):
        self.owner.contract['native_outcome_policy']['maximum_metadata_bytes']=1
        self.assertEqual(self.admit()['status'],'refused');self.assertEqual(s.usage(self.owner)['source_bytes'],0)
    def test_immutable_all_tables(self):
        self.admit()
        for table in ('native_capacity','native_observations','native_blobs','native_visibility'):
            with self.subTest(table=table),self.assertRaises(sqlite3.IntegrityError):
                self.owner.db.execute('DELETE FROM '+table)
    def test_capture_rehashed_numeric_alias_refused(self):
        cap=self.capture();cap['row_count']=float(cap['row_count']);cap['capture_sha256']=o.digest({k:v for k,v in cap.items() if k!='capture_sha256'})
        with self.assertRaises(ValueError):o.verify_capture(cap)
    def test_native_score_strict_tie_and_binary64_formula(self):
        a=anchor();tie=o.score_native(a,1.1);self.assertEqual(tie['strict_up_label'],0)
        self.assertIsNone(tie['direction_correct']);self.assertFalse(tie['direction_denominator_eligible'])
        result=o.score_native(a,1.101);actual=(1.101-1.1)/.0001
        self.assertEqual(result['actual_signed_pips_hex'],actual.hex())
        self.assertEqual(result['signed_error_pips_hex'],(10.-actual).hex())
        self.assertIsNone(result['executable_result'])
    def test_target_future_prefix_does_not_relabel_original(self):
        cap=self.capture();before=o.select_exact_target(anchor(),cap)
        self.write([line(C,1.1),line(C+3600,1.101),line(C+3660,1.102)])
        after=o.select_exact_target(anchor(),self.capture())
        self.assertEqual(before['target_mid_hex'],after['target_mid_hex'])
        self.assertEqual(before['target_close_epoch'],after['target_close_epoch'])
    def test_capacity_startup_census_checks_retained_payloads(self):
        self.admit();s.verify_existing(self.owner.db)
        # Additional arbitrary positive journal totals must not pass startup.
        values=s.usage(self.owner);values['source_bytes']+=1
        self.owner.db.execute('INSERT INTO native_capacity('+','.join(s.CAPACITY_FIELDS)+') VALUES(?,?,?,?,?,?)',
            tuple(values[k] for k in s.CAPACITY_FIELDS))
        with self.assertRaisesRegex(ValueError,'startup_census'):s.verify_existing(self.owner.db)
    def test_source_identity_change_after_real_read_retains_failure(self):
        original=o.prices._read_tail
        def changed(path):
            source=original(path)
            with path.open('ab') as file:file.write(line(C+3660,1.102).encode())
            return source
        with patch.object(o.prices,'_read_tail',changed):
            capture=self.capture()
        self.assertEqual(capture['status'],'refused');self.assertIsNotNone(capture['source'])
        self.assertIn('identity_changed',capture['reason_code'])
    def test_failure_after_admission_clock_read_has_no_visibility(self):
        def fail(point):
            if point=='after_admission_commit':self.owner.fail_clock=True
        self.assertEqual(self.admit(fault_hook=fail)['status'],'refused')
        self.assertEqual(s.admitted_rows(self.owner),[])
        self.owner.fail_clock=False;self.admit()
        self.assertEqual([r['seq'] for r in s.admitted_rows(self.owner)],[2])

if __name__=='__main__':unittest.main()
