"""Create a separate read-only observer without changing the retained baseline helper."""
from pathlib import Path
folder=Path(__file__).resolve().parent/'runtime'
source=(folder/'capture_runtime_baseline.py').read_text(encoding='utf-8')
source=source.replace('revamp_runtime_baseline_v1_20260908','repaired_research_runtime_observation_v1_20260908')
source=source.replace("'joint_v2':coverage", "'selected_joint_study':coverage")
source=source.replace("record['api'].get('joint_v2',{})", "record['api'].get('selected_joint_study',{})")
source=source.replace("'historical_rows_imported']),", "'historical_rows_imported','primary_selection']),")
source=source.replace("paths={'guarded_news':", "paths={'repaired_news':DATA/'local_news_sentiment_repair_v1/current_news_v1.json','repaired_news_producer':DATA/'local_news_sentiment_repair_v1/heartbeat.json','joint_price_news_study_v3':DATA/'joint_price_news_study_v3/heartbeat.json','guarded_news':")
source=source.replace("if name=='guarded_news' else", "if name in ('guarded_news','repaired_news') else")
source=source.replace("'historical_rows_imported','supported_decision'", "'historical_rows_imported','source_status','publication_epoch','snapshot_sha256','last_error','supported_decision'")
source=source.replace("choices=['initial','followup'],", "")
source=source.replace("'guarded_news':record['heartbeats'].get('guarded_news',{}).get('value')", "'repaired_news_producer':record['heartbeats'].get('repaired_news_producer',{}).get('value')")
with (folder/'observe_repaired_runtime.py').open('x',encoding='utf-8') as f:f.write(source)
compile(source,str(folder/'observe_repaired_runtime.py'),'exec')
print('Separate runtime observer created; no live observation yet.')
