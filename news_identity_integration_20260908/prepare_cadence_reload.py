from pathlib import Path
folder=Path(__file__).resolve().parent
source=(folder/'reload_research_services.ps1').read_text(encoding='utf-8')
source=source.replace('SERVICE_RELOAD_RECEIPT_20260908.json','NEWS_CADENCE_RELOAD_RECEIPT_20260908.json')
source=source.replace("@('oanda_practice_live_dashboard.py','oanda_project_integrity_audit.py','oanda_storage_headroom_guard.py')", "@('oanda_local_news_sentiment_repair_v1.py')")
source=source.replace('targeted_research_service_reload_20260908','targeted_news_cadence_reload_20260908')
source=source.replace('model_quote_account_news_collectors_restarted=$false','model_quote_account_original_news_collectors_restarted=$false;repaired_news_producer_restarted=$true;news_refresh_interval_sec=60')
source=source.replace('Stop only observed display/health/storage processes. Leave all study and input workers running.', 'Stop only the observed repaired snapshot producer. Leave all study, original collector and quote workers running.')
with (folder/'reload_news_cadence.ps1').open('x',encoding='utf-8') as f:f.write(source)
print('Separate exact-identity cadence reload helper prepared; no processes changed.')
