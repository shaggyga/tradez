from pathlib import Path
from types import SimpleNamespace
import os,stat
import pytest
import oanda_joint_price_news_forecast_study_v5 as worker

@pytest.mark.parametrize('attrs,mode',[(0x400,stat.S_IFDIR),(0x400|0x10,stat.S_IFDIR),(0,stat.S_IFLNK)])
def test_any_parent_reparse_is_refused_before_child_metadata(tmp_path,monkeypatch,attrs,mode):
    parent=tmp_path/'fake_reparse';study=parent/'nested/joint_price_news_study_v5';seen=[]
    def lstat(path,*a,**kw):
        path=Path(path);seen.append(path)
        assert not path.is_relative_to(parent) or path==parent
        return SimpleNamespace(st_file_attributes=attrs if path==parent else 0,st_mode=mode if path==parent else stat.S_IFDIR)
    def refuse(*a,**k):raise AssertionError('must not exists/resolve a refused branch')
    with monkeypatch.context() as patch:
        patch.setattr(os,'lstat',lstat);patch.setattr(Path,'resolve',refuse);patch.setattr(Path,'exists',refuse)
        with pytest.raises(ValueError,match='reparse_runtime_path_refused'):worker.resolve_runtime_paths(study=study)
    assert seen==list(reversed((parent,*parent.parents)))
