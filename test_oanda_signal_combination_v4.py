"""Synthetic regressions run actual staged miner, partitioner, reader and inference."""
import copy
from datetime import datetime,timedelta,timezone
import importlib.util
import json
import os
from pathlib import Path
import sqlite3
import sys
import tempfile
import types
import unittest
from unittest.mock import patch
import zlib
import numpy as np  # Load before temporary sys.modules helpers, including standalone runs.

import pytest
from trad import oanda_signal_combination_audit as module
from trad import oanda_signal_combination_fit as fitter

ROOT=Path(__file__).resolve().parent


BASE=datetime(2026,1,1,tzinfo=timezone.utc)
ASOF=(BASE+timedelta(days=5)).isoformat()


def rows(n=600):
    result=[]
    for i in range(n):
        x=(i%20)-9.5
        y=((i*7)%20)-9.5
        sign=1.0 if x+y>=0 else -1.0
        origin=BASE+timedelta(minutes=i)
        result.append({'row_id':i+1,'snapshot_id':f's{i}','instrument':['EUR_USD','GBP_USD','USD_JPY'][i%3],
          'origin_time':origin.isoformat(),'outcome_time':(origin+timedelta(seconds=30)).isoformat(),
          'features':{'return_x':x,'depth_y':y,'session_z':(i%7)-3.0},
          'signed_move_pips':sign*10.0,'long_net_pips':sign*10.0-1,'short_net_pips':-sign*10.0-1})
    return result


def fit(data):
    return module.mine_fuzzy_rules(data,30,minimum_train_support=10,minimum_holdout_support=10,
      max_rules=5,max_conditions=3,beam_width=8,expansion_conditions=6,maximum_base_features=3,
      conditions_per_feature=2,minimum_independent_time_buckets=1,minimum_instruments=1,
      minimum_positive_time_bucket_fraction=0,minimum_positive_instrument_fraction=0,
      maximum_instrument_weight_fraction=1,as_of_utc=ASOF)


def frozen(rules):
    ignored={'final_holdout','final_holdout_independence_audit'}
    return [{k:v for k,v in r.items() if k not in ignored} for r in rules]


def predict(rules,path):
    path.write_text(json.dumps({'schema_version':4,'validation_contract':module.FUZZY_VALIDATION_CONTRACT,'generated_at':ASOF,'search_config':{'chronological_validation_blocks':2},'rules':rules}))
    model=module.SignalCombinationModel(path)
    return model.predict({'return_x':8.,'depth_y':8.,'session_z':2.})


class ChronologyTests(unittest.TestCase):
    def test_aware_offsets_normalize_and_same_origin_pairs_stay_together(self):
        data=rows(30)
        twin=copy.deepcopy(data[10]);twin['row_id']=1000;twin['instrument']='AUD_USD'
        twin['origin_time']=datetime.fromisoformat(twin['origin_time']).astimezone(timezone(timedelta(hours=-5))).isoformat()
        twin['outcome_time']=datetime.fromisoformat(twin['outcome_time']).astimezone(timezone(timedelta(hours=-5))).isoformat()
        data.append(twin)
        ordered,a,b,c,receipt=module.chronological_partitions(list(reversed(data)),30,2,as_of_utc=ASOF)
        for segment in (a,b,c):
            ids=[r['row_id'] for r in ordered[segment]]
            self.assertEqual(11 in ids,1000 in ids)
        self.assertTrue(receipt['actual_maturity_purged'])
        self.assertEqual(sum(len(ordered[s]) for s in (a,b,c)),len(ordered))

    def test_actual_delayed_maturity_and_equality_purged_before_both_boundaries(self):
        data=rows(100)
        # Selection starts minute70; final holdout starts minute85.
        data[0]['outcome_time']=(BASE+timedelta(minutes=70)).isoformat()
        data[1]['outcome_time']=(BASE+timedelta(minutes=70,seconds=1)).isoformat()
        data[70]['outcome_time']=(BASE+timedelta(minutes=85)).isoformat()
        data[71]['outcome_time']=(BASE+timedelta(minutes=90)).isoformat()
        ordered,a,b,c,receipt=module.chronological_partitions(data,30,2,as_of_utc=ASOF)
        self.assertTrue({1,2,71,72}.isdisjoint({r['row_id'] for r in ordered}))
        self.assertEqual(receipt['train_rows_purged'],2);self.assertEqual(receipt['selection_rows_purged'],2)
        self.assertEqual(receipt['input_rows'],100);self.assertEqual(receipt['retained_rows'],96)
        self.assertTrue(all(module._origin_epoch(r['outcome_time'])<receipt['selection_start_epoch'] for r in ordered[a]))
        self.assertTrue(all(module._origin_epoch(r['outcome_time'])<receipt['holdout_start_epoch'] for r in ordered[b]))

    def test_missing_naive_malformed_outcome_or_origin_rejected(self):
        for field in ('origin_time','outcome_time'):
            for invalid in (None,'','garbage','2026-01-01T00:00:00','2026-01-01'):
                with self.subTest(field=field,invalid=invalid):
                    data=rows(30);data[0][field]=invalid
                    with self.assertRaises(ValueError):module.chronological_partitions(data,30,2,as_of_utc=ASOF)

    def test_invalid_horizons_no_nominal_maturity_substitution(self):
        for h in (True,0,-1,30.5,float('nan'),'30'):
            with self.subTest(h=h):
                with self.assertRaises(ValueError):module.chronological_partitions(rows(30),h,2,as_of_utc=ASOF)
        data=rows(30);data[0].pop('outcome_time')
        with self.assertRaises(ValueError):module.chronological_partitions(data,30,2,as_of_utc=ASOF)

    def test_future_and_early_outcomes_rejected(self):
        data=rows(30)
        for time in ((BASE+timedelta(seconds=29)).isoformat(),(BASE+timedelta(days=6)).isoformat()):
            data[0]['outcome_time']=time
            with self.assertRaises(ValueError):module.chronological_partitions(data,30,2,as_of_utc=ASOF)
        for asof in ('2026-01-06T00:00:00',None):
            with patch.object(module,'utc_now',return_value='invalid'):
                with self.assertRaises(ValueError):module.chronological_partitions(rows(30),30,2,as_of_utc=asof)

    def test_no_empty_partition_or_shared_holdout_fallback(self):
        for data,h,blocks in (([],30,2),(rows(2),30,2),(rows(30),30,1)):
            with self.assertRaises(ValueError):module.chronological_partitions(data,h,blocks,as_of_utc=ASOF)
        data=rows(30)
        for r in data:r['outcome_time']=(BASE+timedelta(days=1)).isoformat()
        with self.assertRaises(ValueError):module.chronological_partitions(data,30,2,as_of_utc=ASOF)

    def test_nonfinite_labels_do_not_become_zero(self):
        for key in ('signed_move_pips','long_net_pips','short_net_pips'):
            for bad in (None,True,float('nan'),float('inf'),'bad'):
                with self.subTest(key=key,bad=bad):
                    data=rows(30);data[0][key]=bad
                    with self.assertRaises(ValueError):module.chronological_partitions(data,30,2,as_of_utc=ASOF)


class FrozenSelectionTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):cls.data=rows();cls.rules=fit(cls.data)

    def test_fixture_learns_real_rules(self):
        self.assertTrue(self.rules)
        self.assertTrue(all(r['selection_identity_sha256'] and r['final_holdout']['report_only'] for r in self.rules))
        self.assertTrue(all(not r['account_eligible'] and not r['forward_refit_confirmed'] for r in self.rules))

    def test_holdout_outcome_mutation_cannot_select_reorder_or_recalibrate(self):
        changed=copy.deepcopy(self.data)
        for r in changed[510:]:
            r['signed_move_pips']=-r['signed_move_pips']*7
            r['long_net_pips'],r['short_net_pips']=-30.,-40.
        other=fit(changed)
        self.assertEqual(frozen(self.rules),frozen(other))
        self.assertNotEqual([r['final_holdout'] for r in self.rules],[r['final_holdout'] for r in other])
        with tempfile.TemporaryDirectory(dir=ROOT) as directory:
            first=predict(self.rules,Path(directory)/'a.json');second=predict(other,Path(directory)/'b.json')
            for result in (first,second):
                for r in [result,*result.get('candidates',[])]:r.pop('rule_source',None)
            self.assertEqual(first,second)

    def test_unseen_holdout_features_and_categories_do_not_enter_train_schema(self):
        changed=copy.deepcopy(self.data)
        for r in changed[510:]:
            r['features']['aaa_unseen_numeric']=12345.
            r['features']['unseen_category']='category_only_in_holdout'
            r['features']['return_x']=-1000.
            r['features']['depth_y']=1000.
        other=fit(changed)
        self.assertEqual(frozen(self.rules),frozen(other))
        self.assertTrue(all('unseen' not in c['feature'] for r in other for c in r['conditions']))

    def test_frozen_probability_brier_and_direction_are_not_refitted_on_holdout(self):
        import numpy as np
        report=module._evaluate_frozen_rule(np.ones(3),np.array([-1.,-1.,-1.]),np.array([-2.,-2.,-2.]),np.ones(3),{'predicted_direction':'buy','probability_up':.8})
        self.assertEqual(report['frozen_probability_up'],.8)
        self.assertEqual(report['direction_accuracy'],0)
        self.assertEqual(report['brier'],.64)
        self.assertEqual(report['average_net_pips'],-2)

    def test_v4_cannot_gain_execution_eligibility_from_elapsed_refit_recurrence(self):
        rules=copy.deepcopy(self.rules)
        for r in rules:r.update(account_eligible=True,forward_refit_confirmed=True)
        with tempfile.TemporaryDirectory(dir=ROOT) as directory:
            result=predict(rules,Path(directory)/'state.json')
            self.assertFalse(result['account_eligible']);self.assertFalse(result['validation_policy_eligible'])


class ReaderTests(unittest.TestCase):
    def test_actual_outcome_clock_and_raw_invalid_labels_retained_for_validation(self):
        with tempfile.TemporaryDirectory(dir=ROOT) as directory:
            path=Path(directory)/'fixture.sqlite'
            conn=sqlite3.connect(path)
            conn.execute('CREATE TABLE snapshots(snapshot_id TEXT,instrument TEXT,origin_time TEXT,features_zlib BLOB)')
            conn.execute('CREATE TABLE outcomes(row_id INTEGER,snapshot_id TEXT,horizon_sec INTEGER,signed_move_pips REAL,long_net_pips REAL,short_net_pips REAL,outcome_time TEXT)')
            conn.execute('INSERT INTO snapshots VALUES(?,?,?,?)',('s','EUR_USD',BASE.isoformat(),zlib.compress(b'{"x":1}')))
            actual=(BASE+timedelta(seconds=47)).isoformat()
            conn.execute('INSERT INTO outcomes VALUES(?,?,?,?,?,?,?)',(1,'s',30,None,2.,-3.,actual));conn.commit();conn.close()
            loaded=module.read_training_rows(path,30,10)
            self.assertEqual(loaded[0]['outcome_time'],actual);self.assertEqual(loaded[0]['snapshot_id'],'s')
            self.assertIsNone(loaded[0]['signed_move_pips'])

    def test_missing_source_is_not_created_by_read_only_reader(self):
        with tempfile.TemporaryDirectory(dir=ROOT) as directory:
            path=Path(directory)/'missing.sqlite'
            with self.assertRaises(sqlite3.OperationalError):module.read_training_rows(path,30,10)
            self.assertFalse(path.exists())


class CallerAndStateTests(unittest.TestCase):
    @staticmethod
    def create_db(path,data):
        conn=sqlite3.connect(path)
        conn.execute('CREATE TABLE snapshots(snapshot_id TEXT,instrument TEXT,origin_time TEXT,features_zlib BLOB)')
        conn.execute('CREATE TABLE outcomes(row_id INTEGER,snapshot_id TEXT,horizon_sec INTEGER,signed_move_pips REAL,long_net_pips REAL,short_net_pips REAL,outcome_time TEXT)')
        for r in data:
            conn.execute('INSERT INTO snapshots VALUES(?,?,?,?)',(r['snapshot_id'],r['instrument'],r['origin_time'],zlib.compress(json.dumps(r['features']).encode())))
            conn.execute('INSERT INTO outcomes VALUES(?,?,?,?,?,?,?)',(r['row_id'],r['snapshot_id'],30,r['signed_move_pips'],r['long_net_pips'],r['short_net_pips'],r['outcome_time']))
        conn.commit();conn.close()

    @staticmethod
    def args(database,state):
        with patch.object(sys,'argv',['fitter','--database',str(database),'--state',str(state),'--horizons','30','--minimum-train-support','10','--minimum-holdout-support','10','--max-rules-per-horizon','5','--maximum-base-features','3','--max-conditions','3','--once']):
            return fitter.parse_args()

    def test_full_fit_reader_miner_writer_caller_with_explicit_clock(self):
        with tempfile.TemporaryDirectory(dir=ROOT) as directory:
            path=Path(directory);database=path/'source.sqlite';state=path/'v4.json'
            self.create_db(database,rows())
            output=fitter.fit_once(self.args(database,state),as_of_utc=ASOF)
            self.assertEqual(output['schema_version'],4)
            self.assertEqual(output['validation_contract'],module.FUZZY_VALIDATION_CONTRACT)
            self.assertTrue(output['selection_frozen_research_rule_count'])
            self.assertEqual(output['account_eligible_rule_count'],0)
            self.assertTrue(all(r['chronological_partition']['as_of_utc']==ASOF for r in output['rules']))
            self.assertTrue(all(not r['account_eligible'] for r in output['rules']))
            before=[r['selection_identity_sha256'] for r in output['rules']]
            again=fitter.fit_once(self.args(database,state),as_of_utc=ASOF)
            self.assertEqual(before,[r['selection_identity_sha256'] for r in again['rules']])
            self.assertEqual(again['account_eligible_rule_count'],0)

    def test_writer_and_caller_refuse_legacy_artifact_overwrite(self):
        with tempfile.TemporaryDirectory(dir=ROOT) as directory:
            path=Path(directory);state=path/'old.json';old=b'{"schema_version":3,"rules":[]}'
            state.write_bytes(old)
            with self.assertRaisesRegex(ValueError,'legacy_state_overwrite'):
                module.write_rule_state(state,path/'missing.sqlite',[],{}, {})
            self.assertEqual(state.read_bytes(),old)
            with self.assertRaisesRegex(ValueError,'legacy_state_overwrite'):
                fitter.fit_once(self.args(path/'missing.sqlite',state),as_of_utc=ASOF)
            self.assertEqual(state.read_bytes(),old)

    def test_legacy_rules_cannot_be_relabelled_inside_v4_state(self):
        with tempfile.TemporaryDirectory(dir=ROOT) as directory:
            with self.assertRaisesRegex(ValueError,'mixed_or_legacy'):
                module.write_rule_state(Path(directory)/'v4.json',Path(directory)/'missing.sqlite',[{'rule_id':'old','account_eligible':True}],{}, {})

    def test_elapsed_confirmation_cannot_promote_v4_even_if_flag_injected(self):
        rule=copy.deepcopy(FrozenSelectionTests.rules[0]);rule['account_eligible']=True
        prior=copy.deepcopy(rule);prior['first_replicated_utc']=BASE.isoformat()
        output=fitter.apply_forward_refit_confirmation([rule],[prior],validated_utc=ASOF)
        self.assertFalse(output[0]['account_eligible']);self.assertFalse(output[0]['forward_refit_confirmed'])

    def test_v4_default_state_is_distinct_and_shared_block_cli_rejected(self):
        self.assertEqual(fitter.DEFAULT_STATE.name,'signal_combination_audit_v4.json')
        with patch.object(sys,'argv',['fitter','--chronological-validation-blocks','1']):
            with self.assertRaises(SystemExit):fitter.parse_args()





def state(path,rules,contract=True):
    value={'schema_version':4,'generated_at':ASOF,
        'search_config':{'chronological_validation_blocks':2},'rules':rules}
    if contract:value['validation_contract']=module.FUZZY_VALIDATION_CONTRACT
    path.write_text(json.dumps(value))
    return module.SignalCombinationModel(path)


def test_published_condition_numbers_recreate_selection_metrics():
    data=rows(1200)
    for row in data:
        row['features']={k:v*1.1234567e-6+3.333333e-7 for k,v in row['features'].items()}
    rules=fit(data)
    assert rules,'fixture must expose at least one learned rule'
    ordered,a,b,c,_=module.chronological_partitions(data,30,2,as_of_utc=ASOF)
    sample=ordered[b]
    for rule in rules:
        memberships=[module._condition_membership(np.array([r['features'][p['feature']] for r in sample]),
            p['operator'],p['threshold'],p['width']) for p in rule['conditions']]
        result=module._weighted_rule_metrics(np.minimum.reduce(memberships),
            np.array([r['signed_move_pips'] for r in sample]),
            np.array([r['long_net_pips'] for r in sample]),
            np.array([r['short_net_pips'] for r in sample]))
        assert result==rule['selection_calibration']


@pytest.mark.parametrize('bad',[float('nan'),float('inf'),float('-inf')])
def test_nonfinite_live_features_cannot_saturate_into_a_prediction(tmp_path,bad):
    rules=fit(rows())
    assert rules
    model=state(tmp_path/'rules.json',rules)
    prediction=model.predict({'return_x':bad,'depth_y':bad,'session_z':bad})
    assert prediction['ready'] is False


def test_declared_v4_state_refuses_legacy_rule_with_injected_qualification(tmp_path):
    rules=fit(rows());assert rules
    legacy=copy.deepcopy(rules[0]);legacy.pop('validation_contract')
    legacy['holdout']=legacy['selection_calibration']
    legacy.update(account_eligible=True,forward_refit_confirmed=True)
    model=state(tmp_path/'mixed.json',[legacy])
    prediction=model.predict({'return_x':8.,'depth_y':8.,'session_z':2.})
    assert prediction['ready'] is False


def test_v4_rule_does_not_self_certify_a_missing_state_contract(tmp_path):
    rules=fit(rows());assert rules
    model=state(tmp_path/'no_contract.json',rules,contract=False)
    assert model.predict({'return_x':8.,'depth_y':8.,'session_z':2.})['ready'] is False
