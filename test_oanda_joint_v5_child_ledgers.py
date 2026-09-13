from pathlib import Path
from types import SimpleNamespace
import os,stat
import pytest
import oanda_joint_price_news_forecast_study_v5 as worker

@pytest.mark.parametrize('part',['pairs','EUR_USD','ridge_price_news_v1','study.sqlite'])
def test_descendant_reparse_refused_before_ledger_resolution_or_open(tmp_path,monkeypatch,part):
    root=tmp_path/'joint_price_news_study_v5';full=root/'pairs/EUR_USD/ridge_price_news_v1/study.sqlite'
    parent=next(x for x in (full,*full.parents) if x.name==part);seen=[]
    def lstat(path,*a,**k):
        path=Path(path);seen.append(path)
        assert not path.is_relative_to(parent) or path==parent
        return SimpleNamespace(st_mode=stat.S_IFDIR,st_file_attributes=0x400 if path==parent else 0)
    def no(*a,**k):raise AssertionError('reparse branch must not resolve or open SQLite')
    registry={'pairs':{'EUR_USD':{'families':{'ridge_price_news_v1':{'contract':{}}}}}}
    with monkeypatch.context() as patch:
        patch.setattr(os,'lstat',lstat);patch.setattr(Path,'resolve',no);patch.setattr(worker.sqlite3,'connect',no)
        with pytest.raises(ValueError,match='reparse_runtime_path_refused'):worker.verify_activated_study(registry,root,clock=lambda:1000)
    assert parent in seen and full not in seen if full!=parent else full in seen
