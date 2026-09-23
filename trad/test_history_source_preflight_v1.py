import sqlite3
import pytest
from test_isolated_news_history_v1 import fixture,run,append,NOW

def test_unknown_source_fails_before_initial_registry_creation(tmp_path):
    root,config,digest,paths=fixture(tmp_path);append(paths['news_database'],2,source='not_configured')
    with pytest.raises(ValueError,match='unconfigured_pending_news_sources_refused'):run(root,config,digest)
    assert not paths['database_path'].exists()

def test_unknown_source_does_not_advance_existing_checkpoint(tmp_path):
    root,config,digest,paths=fixture(tmp_path);run(root,config,digest)
    append(paths['news_database'],2,source='not_configured')
    with sqlite3.connect(paths['database_path']) as db:before=db.execute('SELECT input_rowid FROM news_fast_lane_batches_v3 ORDER BY batch_seq DESC LIMIT 1').fetchone()
    with pytest.raises(ValueError,match='unconfigured_pending_news_sources_refused'):run(root,config,digest,NOW+2)
    with sqlite3.connect(paths['database_path']) as db:after=db.execute('SELECT input_rowid FROM news_fast_lane_batches_v3 ORDER BY batch_seq DESC LIMIT 1').fetchone()
    assert before==after==(1,)
