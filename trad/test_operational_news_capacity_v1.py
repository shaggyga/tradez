import hashlib
import json
import pytest
from test_oanda_operational_recovery_v6 import fixture, validate, invoke, HELPER


def prepared(tmp_path):
    project,path,value=fixture(tmp_path)
    value.update(enable_news_capacity=True,enable_joint_forecasts=True)
    for row in value['services']:
        if row['name']=='revision_news_transport_v6':
            row.update(name='revision_news_transport_v7',script='revision_transport_v7.py',needle='revision_transport_v7.py')
        elif row['name']=='joint_price_news_isolation_status_v1':
            row.update(name='joint_price_news_study_v10',script='oanda_joint_price_news_forecast_study_v10.py',needle='oanda_joint_price_news_forecast_study_v10.py')
        else:continue
        body=b'# inert capacity fixture\n';(project/row['script']).write_bytes(body)
        row['source_sha256']=hashlib.sha256(body).hexdigest()
    path.write_text(json.dumps(value))
    return project,path,value


def test_capacity_profile_and_historical_retry_keys(tmp_path):
    project,path,value=prepared(tmp_path)
    result=validate(tmp_path,project,path)
    assert result.returncode==0,result.stderr
    result=invoke(tmp_path,f". '{HELPER}'\n@( (Get-OperationalRestartBudgetKey revision_news_transport_v7), (Get-OperationalRestartBudgetKey joint_price_news_study_v10) ) | ConvertTo-Json -Compress")
    assert result.returncode==0,result.stderr
    assert json.loads(result.stdout)==['revision_news_transport_v5','joint_price_news_study_v8']
    service=next(row for row in value['services'] if row['name']=='revision_news_transport_v7')
    body=json.dumps(service).replace("'","''")
    result=invoke(tmp_path,f". '{HELPER}'\n$s='{body}'|ConvertFrom-Json\n@(Get-OperationalInterpreterArguments -Service $s -DataRoot '{project}') | ConvertTo-Json -Compress")
    assert result.returncode==0,result.stderr
    args=json.loads(result.stdout)
    assert args[:2]==['-B','-X'] and args[2].startswith('pycache_prefix=')


@pytest.mark.parametrize('defect',['string','unselected','changed_source','old_interpreter'])
def test_capacity_rejects_mixed_or_unselected_runtime(tmp_path,defect):
    project,path,value=prepared(tmp_path)
    if defect=='string':value['enable_news_capacity']='true'
    if defect=='unselected':value['enable_news_capacity']=False
    if defect=='changed_source':(project/'revision_transport_v7.py').write_bytes(b'changed')
    if defect=='old_interpreter':
        next(row for row in value['services'] if row['name']=='revision_news_transport_v7')['interpreter_mode']='no_bytecode'
    path.write_text(json.dumps(value))
    assert validate(tmp_path,project,path).returncode!=0
