import hashlib
import json
from pathlib import Path
import pytest
from test_oanda_operational_recovery_v6 import fixture, validate


@pytest.mark.parametrize('mode',['valid','disabled','string','source_changed'])
def test_optional_context_role_requires_explicit_exact_selection(tmp_path,mode):
    root,path,value=fixture(tmp_path)
    value['enable_currency_news_context']=True
    row=dict(value['services'][0]);script='oanda_currency_news_context_v1.py'
    body=b'# inert test context worker\n';(root/script).write_bytes(body)
    row.update(name='currency_news_context_v1',script=script,needle=script,
               source_sha256=hashlib.sha256(body).hexdigest(),interpreter_mode='no_bytecode')
    value['services'].append(row)
    if mode=='disabled':value['enable_currency_news_context']=False
    if mode=='string':value['enable_currency_news_context']='true'
    if mode=='source_changed':(root/script).write_bytes(b'changed')
    path.write_text(json.dumps(value))
    result=validate(tmp_path,root,path)
    assert (result.returncode==0)==(mode=='valid'),result.stderr


def test_dashboard_reads_corrected_context_and_escapes_text():
    import oanda_practice_live_dashboard as dashboard
    import oanda_currency_news_context_v1 as context
    from unittest.mock import patch
    with patch.object(context,'read_current',return_value={'status':'current','topics':[]}) as read:
        assert dashboard.current_currency_news_context()['status']=='current'
        assert read.call_args.args[0].name=='current.json'
    page=Path(dashboard.__file__).with_name('oanda_main_signal_dashboard.html').read_text(encoding='utf8')
    assert 'esc(topic.headline)' in page and 'currencyContext(data)' in page
    assert '<th>News context</th>' in page
    assert "const news=data.news_sentiment||{},newsEpoch=" not in page
