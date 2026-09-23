"""Fixture-only fail-closed checks of first-start activation preflight."""
from copy import deepcopy
from contextlib import closing
import sqlite3

import pytest

from verify_pair_reload import EMPTY_TABLES, digest, encoded, verify_activations


def fixture(tmp_path, *, activation=99., activation_hash=None, altered_payload=False, evidence=False):
    root = tmp_path/'study'
    pair = 'HKD_JPY'
    contract = {'instrument':pair, 'pip_size':.0001, 'fixture_only':True}
    contract_hash = digest(contract)
    registry = {'pairs':{pair:{'contract':contract, 'contract_sha256':contract_hash}}}
    database = root/'pairs'/pair/'study.sqlite'
    database.parent.mkdir(parents=True)
    with closing(sqlite3.connect(database)) as writer:
        writer.execute('CREATE TABLE contract(id INTEGER,sha TEXT,payload TEXT)')
        writer.execute('CREATE TABLE activation(id INTEGER,epoch REAL,contract_sha TEXT)')
        writer.execute('INSERT INTO contract VALUES(1,?,?)', (contract_hash, encoded(contract).decode() if not altered_payload else '{}'))
        if activation is not None:
            writer.execute('INSERT INTO activation VALUES(1,?,?)', (activation, activation_hash or contract_hash))
        for table in EMPTY_TABLES:
            writer.execute('CREATE TABLE '+table+'(value TEXT)')
        if evidence:
            writer.execute("INSERT INTO quotes VALUES('existing evidence')")
        writer.commit()
    return root, registry, database


def test_matching_preexisting_activation_is_read_without_modifying_database(tmp_path):
    root, registry, database = fixture(tmp_path)
    before = database.read_bytes()
    rows = verify_activations(registry, root, now_epoch=100.)
    assert rows[0]['instrument'] == 'HKD_JPY'
    assert rows[0]['activation_epoch'] == 99.
    assert not any(rows[0]['counts'].values())
    assert database.read_bytes() == before


@pytest.mark.parametrize('changes', [
    {'activation':None}, {'activation':101.}, {'activation':0.},
    {'activation_hash':'wrong'}, {'altered_payload':True}, {'evidence':True},
])
def test_invalid_or_used_registration_fails_without_mutation(tmp_path, changes):
    root, registry, database = fixture(tmp_path, **changes)
    before = database.read_bytes()
    with pytest.raises(ValueError):
        verify_activations(registry, root, now_epoch=100.)
    assert database.read_bytes() == before


def test_missing_database_is_not_created(tmp_path):
    root, registry, database = fixture(tmp_path)
    database.unlink()
    with pytest.raises(ValueError, match='existing_pair_database_required'):
        verify_activations(registry, root, now_epoch=100.)
    assert not database.exists()


def test_registry_contract_mismatch_is_rejected_before_database_reads(tmp_path):
    root, registry, _ = fixture(tmp_path)
    registry['pairs']['HKD_JPY']['contract_sha256'] = 'wrong'
    with pytest.raises(ValueError, match='pair_contract_binding_mismatch'):
        verify_activations(registry, root, now_epoch=100.)


def test_resolved_database_must_stay_inside_study_root(tmp_path):
    contract = {'instrument':'../../outside', 'fixture_only':True}
    registry = {'pairs':{'../../outside':{'contract':contract,'contract_sha256':digest(contract)}}}
    with pytest.raises(ValueError, match='existing_pair_database_required'):
        verify_activations(registry, tmp_path/'root', now_epoch=100.)
    assert not (tmp_path/'outside').exists()


@pytest.mark.parametrize('clock', [True, float('nan'), float('inf')])
def test_invalid_current_clock_fails_closed(tmp_path, clock):
    root, registry, _ = fixture(tmp_path)
    with pytest.raises(ValueError, match='finite_current_clock_required'):
        verify_activations(registry, root, now_epoch=clock)
