import copy
import hashlib
import importlib.util
from pathlib import Path
import pytest

spec=importlib.util.spec_from_file_location('completion',Path(__file__).resolve().parents[1]/'tools/forex_completion.py')
m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m)


def fixture(tmp_path):
    (tmp_path/'evidence.txt').write_bytes(b'qualified evidence')
    return dict(schema='forex.design_completion.v1', evidence={'e':dict(path='evidence.txt',sha256=hashlib.sha256(b'qualified evidence').hexdigest())},requirements=[dict(id=i,deferred=False,stages={s:dict(passed=True,reason='reviewed',evidence=['e']) for s in m.STAGES}) for i in m.IDS])


def test_denominator_does_not_shrink_for_deferred_work(tmp_path):
    c=fixture(tmp_path);r=c['requirements'][6];r['deferred']=True
    for stage in r['stages'].values():stage.update(passed=False,evidence=[])
    x=m.calculate(c,tmp_path)
    assert (x['earned'],x['possible'],x['active_earned'],x['active_possible'])==(42,45,42,42)


@pytest.mark.parametrize('change',['duplicate','missing','unsupported','unearned','deferred_credit'])
def test_inflation_and_population_changes_refused(tmp_path,change):
    c=fixture(tmp_path)
    if change=='duplicate':c['requirements'][1]['id']='R01'
    if change=='missing':c['requirements'].pop()
    if change=='unsupported':c['requirements'][0]['stages']['acceptance']['evidence']=['missing']
    if change=='unearned':c['requirements'][0]['stages']['implementation']['passed']=False
    if change=='deferred_credit':c['requirements'][0]['deferred']=True
    with pytest.raises(ValueError):m.calculate(c,tmp_path)


def test_changed_evidence_invalidates_score(tmp_path):
    c=fixture(tmp_path);(tmp_path/'evidence.txt').write_bytes(b'changed')
    with pytest.raises(ValueError,match='stale evidence'):m.calculate(c,tmp_path)


def test_missing_evidence_invalidates_score(tmp_path):
    c=fixture(tmp_path);(tmp_path/'evidence.txt').unlink()
    with pytest.raises(FileNotFoundError):m.calculate(c,tmp_path)


def test_path_escape_refused(tmp_path):
    c=fixture(tmp_path);c['evidence']['e']['path']='../outside'
    with pytest.raises(ValueError,match='outside'):m.calculate(c,tmp_path)


def test_partial_evidence_does_not_imply_acceptance(tmp_path):
    c=fixture(tmp_path)
    for row in c['requirements']:row['stages']['acceptance'].update(passed=False,evidence=[])
    r=m.calculate(c,tmp_path)
    assert r['earned']==30 and r['fully_accepted_requirements']==0
