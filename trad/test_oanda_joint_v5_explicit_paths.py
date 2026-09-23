from pathlib import Path
import pytest
import oanda_joint_price_news_forecast_study_v5 as worker
import oanda_causal_forecast_inputs_joint_news_v3 as inputs

def test_isolated_paths_do_not_inherit_canonical_data_root(tmp_path):
    study=tmp_path/'market_open_20260913_v1/joint_price_news_study_v5'
    found=worker.resolve_runtime_paths(study=study)
    assert found['study']==study
    for name,path in found.items():assert path.is_relative_to(study.parent)
    assert found['current_news_path']==study.parent/'local_news_sentiment_repair_v2/current_news_v2.json'
    assert not study.parent.exists()

def test_explicit_readonly_history_can_remain_in_prior_root(tmp_path):
    study=tmp_path/'new/joint_price_news_study_v5'
    history=tmp_path/'old/state/source_governance_v1.sqlite'
    candles=tmp_path/'new/archive/candles';quotes=tmp_path/'new/feed/current.json';news=tmp_path/'new/news/current.json'
    found=worker.resolve_runtime_paths(study=study,candle_root=candles,quote_path=quotes,current_news_path=news,history_path=history)
    assert found==dict(study=study,candle_root=candles,quote_path=quotes,current_news_path=news,history_path=history)
    assert not any(p.exists() for p in found.values())

@pytest.mark.parametrize('path',['relative/joint_price_news_study_v5','D:/never_read/joint_price_news_study_v5','//never/read/joint_price_news_study_v5'])
def test_invalid_drive_rejected_before_metadata(path):
    with pytest.raises(ValueError,match='absolute_c_runtime'):worker.resolve_runtime_paths(study=path)

def test_old_study_name_not_accepted(tmp_path):
    with pytest.raises(ValueError,match='distinct_v5'):worker.resolve_runtime_paths(study=tmp_path/'joint_price_news_study_v3')

def test_capture_routes_explicit_current_and_history_without_global_patch(tmp_path,monkeypatch):
    seen=[];news=tmp_path/'news/current.json';history=tmp_path/'old/history.sqlite';storage=tmp_path/'new/news_captures'
    def capture(data_root,**kwargs):
        seen.append((data_root,kwargs));return {'news_capture_path':str(storage/'sample.json.gz'),'news_capture_sha256':'a'*64}
    monkeypatch.setattr(inputs,'capture_news_inputs',capture)
    monkeypatch.setattr(inputs,'capture_inputs',lambda *a,**k: {'sentinel':True,'descriptor':k['news_capture']})
    result=worker.capture_once(tmp_path/'new/candles','EUR_USD',.0001,storage,current_path=news,history_path=history)
    assert result['sentinel']
    assert seen==[(tmp_path/'new',dict(storage_root=storage,current_path=news,history_path=history))]
    assert not any(tmp_path.iterdir())

def test_source_signature_watches_explicit_files(tmp_path):
    candles=tmp_path/'candles';candles.mkdir();news=tmp_path/'new_news.json';history=tmp_path/'old_history.sqlite'
    first=inputs.source_signature(candles,'EUR_USD',current_path=news,history_path=history)
    assert [p for p,_ in first]==[str(candles/'EUR_USD_M1.csv'),str(news),str(history),str(history)+'-wal']
    news.write_text('{}')
    second=inputs.source_signature(candles,'EUR_USD',current_path=news,history_path=history)
    assert first[1]!=second[1] and first[0]==second[0] and first[2:]==second[2:]
