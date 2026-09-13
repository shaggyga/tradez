import copy,json,sqlite3
import pytest
import oanda_isolated_news_history_v1 as history
import oanda_causal_forecast_inputs_joint_news_v3 as joint
import oanda_news_causal_aggregation_guard_v2 as guard
from test_isolated_news_history_v1 import fixture,run,NOW

@pytest.mark.parametrize('target', ['receipt_batch','ack_batch','mapping_count','input_rowid'])
@pytest.mark.parametrize('alias',[True,1.0])
def test_numeric_aliases_do_not_preserve_identity_even_when_hashes_recomputed(tmp_path,target,alias):
    root,config,digest,paths=fixture(tmp_path);run(root,config,digest)
    with sqlite3.connect(paths['database_path']) as con:
        con.execute('DROP TRIGGER isolated_news_history_admissions_v1_no_update')
        con.execute('DROP TRIGGER isolated_news_history_admission_visibility_v1_no_update')
        receipt=json.loads(con.execute('SELECT receipt_json FROM isolated_news_history_admissions_v1').fetchone()[0])
        ack=json.loads(con.execute('SELECT ack_json FROM isolated_news_history_admission_visibility_v1').fetchone()[0])
        if target=='receipt_batch':receipt['batch_seq']=alias
        elif target=='ack_batch':ack['batch_seq']=alias
        elif target=='mapping_count':receipt['capture']['mapping_count']=alias
        elif target=='input_rowid':receipt['capture']['batch']['input_rowid']=alias
        receipt_sha=joint._digest(receipt);ack['receipt_sha256']=receipt_sha
        con.execute('UPDATE isolated_news_history_admissions_v1 SET receipt_json=?,receipt_sha256=?',(joint._encoded(receipt).decode(),receipt_sha))
        con.execute('UPDATE isolated_news_history_admission_visibility_v1 SET ack_json=?,ack_sha256=?',(joint._encoded(ack).decode(),joint._digest(ack)))
    with pytest.raises(ValueError,match='admission'):joint._history(paths['database_path'],NOW+1,guard)

@pytest.mark.parametrize('name',['oanda_local_news_sentiment.py','oanda_local_news_sentiment_repair_v2.py','oanda_news_classification_contract.py'])
def test_entire_policy_source_closure_checked_not_only_writer_sources(tmp_path,monkeypatch,name):
    root,config,digest,paths=fixture(tmp_path);run(root,config,digest)
    original=joint._source_hash
    monkeypatch.setattr(joint,'_source_hash',lambda path:'a'*64 if path.name==name else original(path))
    with pytest.raises(ValueError,match='history_profile_source_mismatch'):joint._history(paths['database_path'],NOW+1,guard)

def test_contract_initialization_failure_leaves_no_complete_marker(tmp_path,monkeypatch):
    root,config,digest,paths=fixture(tmp_path)
    def failure(*args,**kwargs):raise ValueError('fixture_contract_init_failure')
    with monkeypatch.context() as patch:
        patch.setattr(history.governance,'insert_contracts',failure)
        with pytest.raises(ValueError,match='fixture_contract_init_failure'):run(root,config,digest)
    with sqlite3.connect(paths['database_path']) as con:
        assert con.execute('SELECT COUNT(*) FROM isolated_news_history_profile_v1').fetchone()[0]==0
    with pytest.raises(ValueError,match='immutable_history_profile_mismatch'):run(root,config,digest)
