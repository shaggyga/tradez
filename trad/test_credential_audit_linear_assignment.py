"""Full-payload scanner parity and bounded synthetic encoded-data performance.

All values are constructed test data; no credential files or market archives
are read. The prior expression is retained only for bounded differential cases.
"""
from __future__ import annotations

import itertools
import json
from pathlib import Path
import random
import re
import subprocess
import sys

import pytest

from trad.tools import credential_audit as credentials
from trad.tools import vault_worktree_snapshot as snapshot


ORIGINAL = re.compile(
    rb"(?i)[\"']?(?:[a-z0-9_-]*(?:api[_-]?key|api[_-]?token|access[_-]?token)"
    rb"|oanda[_-]?(?:token|access[_-]?token)|secret|password)[\"']?"
    rb"\s*[=:]\s*(?:[rubf]{0,2}(?:\"{3}|'{3}|[\"'`])\s*)?"
    rb"([A-Za-z0-9._~+/=-]{16,})(?:\"{3}|'{3}|[\"'`])?"
)
OPTIMIZED = credentials.PATTERNS['credential_assignment']


def signatures(pattern, payload):
    return [(match.regs, match.group(0), match.groups(), match.lastindex)
            for match in pattern.finditer(payload)]


def assert_same(payload):
    assert signatures(OPTIMIZED,payload)==signatures(ORIGINAL,payload)


@pytest.mark.parametrize('key',[
    b'api_key',b'APIKEY',b'api-token',b'access_token',b'prefixed_api_key',
    b'foo-bar_access-token',b'oanda_token',b'prefixoanda-token',b'mysecret',
    b'mypassword',b'notsecret',b'password_suffix',b'api_key_suffix',b'secretary',
])
def test_key_substrings_quotes_and_separators_keep_full_spans(key):
    value=(b'abCd09_-'*3)
    for prefix,quote,separator in itertools.product(
            (b'',b'x',b'-',b' ',b'\xc3\xa9',b'\xff'),
            (b'',b'"',b"'",b'"\'',b'\\"'),
            (b'=',b':',b' \t=\r\n')):
        assert_same(prefix+quote+key+quote+separator+b'"'+value+b'";')


@pytest.mark.parametrize('opening', [b'',b'"',b"'",b'`',b'"""',b"'''",b'r"',b'b"',b'f"',b'br"',b'"\n',b"'''\r\n"])
def test_nested_literals_comments_newlines_and_adjacent_assignments(opening):
    value=b'Xy_'*8
    payload=(b'prefix"'+b'api_key'+b'" = '+opening+value+b'"\n'
             b'# mypassword='+value+b'; secret: '+value+b'\r\n'
             b'message="access_token='+value+b'"\n')
    assert_same(payload)


def test_bounded_randomized_full_payload_differential():
    randomizer=random.Random(20260907)
    alphabet=b'abcXYZ019_- \t\r\n=:\'"`/.,[](){}\\\xff\xc3\xa9'
    keys=[b'api_key',b'access-token',b'oanda_token',b'secret',b'password',b'not_a_key']
    for _ in range(5000):
        noise=bytes(randomizer.choice(alphabet) for _ in range(randomizer.randrange(0,120)))
        key=randomizer.choice(keys)
        prefix=b'prefix_'*randomizer.randrange(0,4)
        quote=randomizer.choice([b'',b'"',b"'",b'\\"'])
        value=bytes(randomizer.choice(b'Ab9._~+/=-') for _ in range(randomizer.randrange(0,40)))
        payload=noise+quote+prefix+key+quote+randomizer.choice([b'=',b':',b'\n=\t'])+quote+value+quote+noise[::-1]
        assert_same(payload)


def exemptions(pattern,payload,monkeypatch):
    monkeypatch.setitem(credentials.PATTERNS,'credential_assignment',pattern)
    credentials._python_reference_spans.cache_clear()
    try:
        return [(m.regs,credentials.credential_assignment_is_reference('source.py',m))
                for m in pattern.finditer(payload)]
    finally:
        credentials._python_reference_spans.cache_clear()


@pytest.mark.parametrize('prefix,encoding',[
    ('','utf-8'),('# Unicode café £\n','utf-8'),('# Unicode café £\n','utf-8-sig'),
    ('# coding: latin-1\n# café\n','latin-1'),
])
@pytest.mark.parametrize('style',['bare','attribute','quoted','embedded','triple','comment','malformed'])
def test_python_reference_exemptions_keep_exact_byte_spans(monkeypatch,prefix,encoding,style):
    reference='configured_'+'credential_reference'
    forms={
        'bare':'api_key = '+reference+'\n',
        'attribute':'password = self.'+reference+'\n',
        'quoted':'password = "'+reference+'"\n',
        'embedded':'message = "password = '+reference+'"\n',
        'triple':"password = '''\n"+reference+"\n'''\n",
        'comment':'# secret = '+reference+'\n',
        'malformed':'api_key = '+reference+'\nunclosed = (\n',
    }
    payload=(prefix+forms[style]).encode(encoding)
    expected=exemptions(ORIGINAL,payload,monkeypatch)
    actual=exemptions(OPTIMIZED,payload,monkeypatch)
    assert actual==expected and actual
    assert all(allowed==(style in ('bare','attribute')) for _,allowed in actual)


@pytest.mark.parametrize('key',[b'api_key',b'prefix_api_token',b'mysecret',b'mypassword',b'prefixoanda_token'])
def test_actual_alerts_still_reject_synthetic_secret_values_without_echo(key):
    value=b'Synthetic'+b'9_'*12
    payload=key+b' = "'+value+b'"'
    assert_same(payload)
    with pytest.raises(RuntimeError,match='credential_assignment') as error:
        snapshot.audit_payload('config.json',payload,set())
    assert value.decode() not in str(error.value)


def test_unrelated_rules_and_existing_exemptions_unchanged():
    assert set(credentials.PATTERNS)=={'private_key_header','known_token_prefix','credential_assignment',
                                      'bearer_token','credential_query','credential_uri'}
    assert 'test_credential_audit_linear_assignment.py' not in credentials.FIXTURE_PATHS
    assert signatures(OPTIMIZED,b'x"api_key"='+b'Q'*20)[0][0][0][0]==1
    assert signatures(OPTIMIZED,b'mysecret='+b'Q'*20)[0][0][0][0]==2


def test_large_encoded_payload_is_fast_and_late_alert_is_not_skipped():
    # A separate disposable process makes regression to quadratic scanning a
    # bounded failure, rather than allowing pytest to stall for many minutes.
    script=r'''
import json,sys,time
sys.dont_write_bytecode=True
from trad.tools import credential_audit as c
run=b'QUJD'*63174
benign=b'{"tail_base64":"'+run+b'","status":"synthetic"}'
secret=b'Z9_'*8
later=benign+b'\n"prefix_api_key"="'+secret+b'"'
start=time.perf_counter()
assert not list(c.PATTERNS['credential_assignment'].finditer(benign))
matches=list(c.PATTERNS['credential_assignment'].finditer(later))
elapsed=time.perf_counter()-start
assert len(matches)==1 and matches[0].group(1)==secret
print(json.dumps({'encoded_run_bytes':len(run),'payload_bytes':len(benign),'seconds':elapsed,'late_alerts':len(matches)}))
'''
    result=subprocess.run([sys.executable,'-c',script],cwd=Path(__file__).resolve().parent.parent,
                          capture_output=True,text=True,check=True,timeout=5,
                          creationflags=getattr(subprocess,'CREATE_NO_WINDOW',0))
    metrics=json.loads(result.stdout)
    assert metrics['encoded_run_bytes']==252696
    assert metrics['seconds']<3
    assert metrics['late_alerts']==1


@pytest.mark.parametrize('relative',['tools/credential_audit.py','test_credential_audit_linear_assignment.py'])
def test_changed_source_itself_remains_archiveable_without_new_exemptions(relative):
    payload=(Path(__file__).parent/relative).read_bytes()
    snapshot.audit_payload(relative,payload,set())
