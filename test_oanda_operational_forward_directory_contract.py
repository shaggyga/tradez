"""Permanent, inert Windows PowerShell regression of both profile validators."""
from copy import deepcopy
import hashlib
import json
from pathlib import Path
import shutil
import os
import subprocess
import pytest

ROOT=Path(__file__).resolve().parent

def prepare(tmp_path):
    HERE=tmp_path
    TRAD=ROOT
    FIXTURE=HERE/'fixture_project/trad'
    FIXTURE.mkdir(parents=True,exist_ok=True)
    (FIXTURE/'config').mkdir(exist_ok=True)
    DATA=FIXTURE/'data/oanda_training_manager';DATA.mkdir(parents=True,exist_ok=True)
    original=json.loads((ROOT/'docs/validation/forward_directory_guard_20260914/profile_shape_fixture.json').read_text().replace('{PROJECT_ROOT}',str(TRAD).replace('\\','\\\\')))
    for service in original['services']:
        target=FIXTURE/service['script'];target.write_text('# isolated source fixture '+service['script']);service['source_sha256']=hashlib.sha256(target.read_bytes()).hexdigest()
    shutil.copyfile(TRAD/'oanda_all68_m1_cadence_v2.py',FIXTURE/'oanda_all68_m1_cadence_v2.py')
    base=json.loads(json.dumps(original).replace(str(TRAD).replace('\\','\\\\'),str(FIXTURE).replace('\\','\\\\')))
    cases=[]


    def add(name,mutation,accept=False):
        value=deepcopy(base)
        forward=next(row for row in value['services'] if row['name']=='research_feature_forward_v2')
        mutation(value,forward)
        path=FIXTURE/'config'/f'{name}.json';path.write_text(json.dumps(value))
        cases.append({'name':name,'project':str(FIXTURE),'path':str(path),'accept':accept})


    def directory(row,path):
        row['arguments'][row['arguments'].index('--directory')+1]=path
        row['needle']='oanda_feature_forward_worker_v1.py*'+path


    add('retained_generation_valid',lambda v,f:directory(f,str(DATA/'operational_repair_20260913_v1/feature_forward_v3')),True)
    add('new_fixture_valid',lambda v,f:None,True)
    add('future_generation_valid',lambda v,f:directory(f,str(DATA/'future_generation/feature_forward_v7')),True)
    add('needle_mismatch',lambda v,f:f.update(needle='oanda_feature_forward_worker_v1.py*'+str(DATA/'other')))
    add('missing_directory',lambda v,f:f['arguments'].__delitem__(slice(f['arguments'].index('--directory'),f['arguments'].index('--directory')+2)))
    add('duplicate_directory',lambda v,f:f['arguments'].extend(['--directory',str(DATA/'duplicate')]))
    add('equals_directory_override',lambda v,f:f['arguments'].append('--directory='+str(DATA/'duplicate')))
    add('directory_missing_value',lambda v,f:f.update(arguments=['--directory']))
    add('nonstring_argument',lambda v,f:f['arguments'].append(4))
    add('relative_directory',lambda v,f:directory(f,'data\\oanda_training_manager\\cohort'))
    add('drive_relative_directory',lambda v,f:directory(f,'C:cohort'))
    add('outside_directory',lambda v,f:directory(f,str(FIXTURE.parent/'outside')))
    add('traversal_outside',lambda v,f:directory(f,str(DATA/'inside/../../../outside')))
    add('sibling_prefix_directory',lambda v,f:directory(f,str(DATA)+'_outside\\cohort'))
    add('wildcard_directory',lambda v,f:directory(f,str(DATA/'cohort*')))
    add('question_directory',lambda v,f:directory(f,str(DATA/'cohort?')))
    add('bracket_directory',lambda v,f:directory(f,str(DATA/'cohort[1]')))
    add('doublequote_directory',lambda v,f:directory(f,str(DATA/'cohort"x')))
    add('singlequote_directory',lambda v,f:directory(f,str(DATA/"cohort'x")))
    add('newline_directory',lambda v,f:directory(f,str(DATA/'cohort\nx')))
    add('alternate_stream_directory',lambda v,f:directory(f,str(DATA/'cohort:stream')))
    add('orders_enabled',lambda v,f:v.update(can_place_orders=True))
    add('source_mismatch',lambda v,f:f.update(source_sha256='f'*64))
    add('extra_service',lambda v,f:v['services'].append(deepcopy(f)))
    (HERE/'test_cases.json').write_text(json.dumps(cases,indent=2))
    return HERE/'test_cases.json'


def test_exact_forward_directory_binding_at_launcher_and_supervisor(tmp_path):
    if os.name!='nt':pytest.skip('Windows path and native PowerShell contract')
    cases=prepare(tmp_path)
    result=subprocess.run([str(Path(os.environ['SystemRoot'])/'System32/WindowsPowerShell/v1.0/powershell.exe'),'-NoLogo','-NoProfile','-NonInteractive','-ExecutionPolicy','Bypass','-File',str(ROOT/'docs/validation/forward_directory_guard_20260914/validate_profile_layers.ps1'),'-CaseFile',str(cases),'-CanonicalRoot',str(ROOT)],capture_output=True,text=True,timeout=45)
    assert result.returncode==0,result.stdout+'\n'+result.stderr
    values=json.loads(result.stdout)
    assert len(values)==48
    assert all(row['accepted']==row['expected'] for row in values)
