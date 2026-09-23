# Forex Research Runtime

This archive contains the source, configuration, documentation, and tests needed to run the Forex dashboard and shadow collectors. Large live databases, active WAL files, model artifacts, credentials, and historical report datasets are intentionally excluded from the portable archive and preserved in the full vault snapshot.

Runtime root: `D:\forex`

Dashboard:

```powershell
D:\forex\trad\data\oanda_training_manager\.research_py313\Scripts\python.exe D:\forex\trad\oanda_practice_live_dashboard.py --port 8765 --max-runs 20 --max-lines 20000
```

Always-on supervisor:

```powershell
powershell.exe -NoProfile -ExecutionPolicy Bypass -File D:\forex\trad\oanda_always_on_supervisor.ps1 -Root D:\forex -AccountKey OANDA_ACCOUNT_ID_DUM4 -RunLabel unified-forecast-matrix-m1-core-v8
```

Crypto shadow dependency:

```powershell
D:\forex\trad\data\oanda_training_manager\.research_py313\Scripts\python.exe -m pip install -r D:\forex\trad\requirements-crypto-shadow.txt
```

No script in this archive authorizes live-money execution. Account-specific guards and credentials must be audited before changing any broker mode.
