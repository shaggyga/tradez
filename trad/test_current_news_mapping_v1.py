import json
import sqlite3
import pytest
import oanda_current_news_mapping_v1 as mapping

def fixtures(tmp_path):
    technical=sqlite3.connect(':memory:');technical.row_factory=sqlite3.Row
    technical.executescript('''CREATE TABLE bars(pair TEXT,t INTEGER,first_observed REAL,body TEXT);
      CREATE TABLE observations(pair TEXT,t INTEGER,published REAL,feature_hash TEXT,values_blob TEXT);
      CREATE TABLE metadata(key TEXT,value TEXT);''')
    technical.execute('INSERT INTO metadata VALUES (?,?)',('contract',json.dumps({'contract':{'feature_names':['momentum','reversion']}})))
    for i in range(70):
        t=9900+i*60;body=json.dumps({'close':1.1+i*.0001,'bid_close':1.0999+i*.0001,'ask_close':1.1001+i*.0001})
        technical.execute('INSERT INTO bars VALUES (?,?,?,?)',('EUR_USD',t,t+61,body))
    technical.execute('INSERT INTO observations VALUES (?,?,?,?,?)',('EUR_USD',9900,9962,'f'*64,'[0.5,-0.2]'))
    arm={'publication_epoch':9950,'consumption_epoch':9951,'target_epoch':10080,'reference_epoch':6480,
         'reference_mid':1.1,'predicted_return_bps':3,'forecast_sha256':'a'*64,'family':'ridge'}
    view={'status':'current','selection_sha256':'c'*64,
          'pair_coverage':{'status':'current','rows':[{'instrument':'EUR_USD','feature_status':'complete'}]},
          'price':{'status':'current','registry_sha256':'d'*64,'rows':[{'instrument':'EUR_USD','active_forecasts':[arm]}]},'joint':{'status':'unavailable'}}
    topic={'available_epoch':9970,'interpretation_id':'e'*64,'headline':'Fed sees no rush',
           'published_utc':'2026-09-30T00:00:00+00:00','first_seen_utc':'2026-09-30T00:01:00+00:00',
           'interpretation':{'claims':[{'currency':'USD','kind':'policy_timing','value':'tightening_not_urgent'}]}}
    context={'status':'current','generated_epoch':9975,'payload_sha256':'b'*64,'topics':[topic]}
    return technical,mapping.open_store(tmp_path/'map.sqlite'),view,context

def test_real_consumer_preserves_forecast_and_feature_clocks(tmp_path):
    t,db,v,c=fixtures(tmp_path)
    try:
        a=mapping.record(db,t,c,v,9980);b=mapping.record(db,t,c,v,9985)
        assert a['counts']=={'forecasts':1,'mappings':1,'features':1} and b['new_mappings']==0
        row=json.loads(db.execute('SELECT body FROM mappings').fetchone()[0])
        assert row['technical']['feature_published_epoch']==9962
        assert row['forecast_references'][0]['first_monitor_observed_epoch']==9980
        assert db.execute('SELECT observed FROM forecasts').fetchone()[0]==9980
        f=json.loads(db.execute('SELECT body FROM forecasts').fetchone()[0]);assert mapping.forecast_outcome(t,'EUR_USD',f,10079)['status']=='pending_target'
        out=mapping.forecast_outcome(t,'EUR_USD',f,10090);assert out['status']=='settled'
        assert out['target_epoch']==10080 and out['no_change_absolute_error_bps']>0
    finally:t.close();db.close()

def test_future_news_and_forecasts_and_unqualified_features_not_backfilled(tmp_path):
    t,db,v,c=fixtures(tmp_path)
    try:
        c['topics'][0]['available_epoch']=9990
        assert mapping.record(db,t,c,v,9980)['counts']['mappings']==0
        c['topics'][0]['available_epoch']=9970
        v['price']['rows'][0]['active_forecasts'][0]['publication_epoch']=9990
        v['pair_coverage']['status']='unavailable'
        # Existing earlier observation stays valid, but future publication is not added.
        v['price']['rows'][0]['active_forecasts'][0]['forecast_sha256']='9'*64
        a=mapping.record(db,t,c,v,9981)
        assert a['counts']['forecasts']==1 and a['recent'][0]['technical']['status']=='unavailable'
    finally:t.close();db.close()

def test_revisions_refuse_settlement(tmp_path):
    t,db,v,c=fixtures(tmp_path)
    try:
        t.execute('CREATE TABLE revisions(pair TEXT)');t.execute("INSERT INTO revisions VALUES ('EUR_USD')")
        out=mapping.forecast_outcome(t,'EUR_USD',v['price']['rows'][0]['active_forecasts'][0],10090)
        assert out['status']=='unavailable'
        c['generated_epoch']=9000
        with pytest.raises(ValueError,match='current_context_required'):mapping.record(db,t,c,v,9980)
    finally:t.close();db.close()

def test_fractional_live_targets_wait_for_actual_next_close(tmp_path):
    t,db,v,c=fixtures(tmp_path)
    try:
        f=dict(v['price']['rows'][0]['active_forecasts'][0],target_epoch=10080.25)
        assert mapping.forecast_outcome(t,'EUR_USD',f,10090)['status']=='pending_target_bar'
        x=mapping.forecast_outcome(t,'EUR_USD',f,10145)
        assert x['status']=='settled' and x['target_close_delay_seconds']==59.75
        assert x['outcome_bar_close_epoch']==10140 and x['target_epoch']==10080.25
        assert 'not original producer settlement' in x['scope']
    finally:t.close();db.close()
