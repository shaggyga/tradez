from pathlib import Path
import sqlite3,json
root=Path(r'C:\Users\zmoor\Documents\forex\trad\data\oanda_training_manager')
out={}
for name in ['spike_blurb_factor_reconstruction_v1.sqlite','movement_news_episode_research_v1.sqlite','source_governance_v1.sqlite']:
    p=root/'state'/name
    c=sqlite3.connect(p.as_uri()+'?mode=ro',uri=True)
    try:
        c.execute('PRAGMA query_only=ON');c.execute('BEGIN')
        tables=[r[0] for r in c.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'")]
        out[name]={t:[x[1] for x in c.execute('PRAGMA table_info("'+t+'")')] for t in tables}
    finally:c.rollback();c.close()
target=Path(__file__).parent/'DISCOVERED_DB_SCHEMAS.json'
target.write_text(json.dumps(out,indent=2)+'\n',encoding='utf-8')
print(json.dumps(out,indent=2))
