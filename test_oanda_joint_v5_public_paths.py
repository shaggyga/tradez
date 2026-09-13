from pathlib import Path
from types import SimpleNamespace
import os,stat
import pytest
import oanda_joint_price_news_forecast_study_v5 as worker
import prepare_joint_price_news_registry_v5 as prepare

@pytest.mark.parametrize('bad',['D:/never-read/config.json','C:relative.json','//never/read/config.json','C:/owned/../unsafe/config.json','C:/owned/registration.json:stream'])
@pytest.mark.parametrize('boundary',['registry','run_config','activation_config','activation_study','verify_study','prepare_output','prepare_metadata'])
def test_invalid_public_paths_refuse_before_forbidden_metadata_or_reads(tmp_path,monkeypatch,bad,boundary):
    original=os.lstat
    def observed(path,*a,**k):
        value=str(path).replace('\\','/').lower()
        assert not value.startswith(('d:','//')) and ':stream' not in value
        return original(path,*a,**k)
    monkeypatch.setattr(os,'lstat',observed)
    study=tmp_path/'joint_price_news_study_v5'
    functions={
      'registry':lambda:worker.load_registry(bad),
      'run_config':lambda:worker.run(bad,study=study,once=True),
      'activation_config':lambda:worker.activate_fresh_study(bad,study=study),
      'activation_study':lambda:worker.activate_fresh_study(tmp_path/'never-open.json',study=bad),
      'verify_study':lambda:worker.verify_activated_study({},bad),
      'prepare_output':lambda:prepare.prepare_registry(bad,scope='fixture',selected_pairs=['EUR_USD']),
      'prepare_metadata':lambda:prepare.make_registry(scope='fixture',selected_pairs=['EUR_USD'],metadata_path=bad)}
    with pytest.raises(ValueError,match='absolute_c_runtime_path_required|alternate_stream_runtime_path_refused'):functions[boundary]()
    assert not study.exists() and not list(tmp_path.iterdir())

@pytest.mark.parametrize('boundary',['registry','activation','output','metadata'])
def test_parent_reparse_in_public_paths_precedes_source_read(tmp_path,monkeypatch,boundary):
    parent=tmp_path/'mocked_reparse';target=parent/('joint_price_news_study_v5' if boundary=='activation' else 'file.json');seen=[]
    def lstat(path,*a,**kw):
        path=Path(path);seen.append(path)
        assert not path.is_relative_to(parent) or path==parent
        return SimpleNamespace(st_mode=stat.S_IFDIR,st_file_attributes=0x400 if path==parent else 0)
    def no_open(*a,**k):raise AssertionError('refused path must not be opened')
    functions={
      'registry':lambda:worker.load_registry(target),
      'activation':lambda:worker.activate_fresh_study(tmp_path/'never-open.json',study=target),
      'output':lambda:prepare.prepare_registry(target,scope='fixture',selected_pairs=['EUR_USD']),
      'metadata':lambda:prepare.make_registry(scope='fixture',selected_pairs=['EUR_USD'],metadata_path=target)}
    with monkeypatch.context() as patch:
        patch.setattr(os,'lstat',lstat);patch.setattr(Path,'open',no_open)
        with pytest.raises(ValueError,match='reparse_runtime_path_refused'):functions[boundary]()
    assert seen==list(reversed((parent,*parent.parents)))
