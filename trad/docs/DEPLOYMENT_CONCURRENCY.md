# Deployment Concurrency

`D:\forex\trad` is the canonical runtime. Chats do not share working memory, and
two chats can otherwise overwrite the same source file or restart the same
worker with different arguments.

## Rules

1. Use one writer chat for canonical source changes.
2. Stage and test changes outside `D:\forex\trad`.
3. Deploy tracked files with `forex_guarded_deploy.py`. It takes an exclusive
   lock and rejects a target whose hash changed after the manifest was created.
4. Do not reinitialize the manifest to bypass a conflict. Inspect the canonical
   change and merge it deliberately.
5. Run only one `oanda_always_on_supervisor.ps1`. The supervisor uses a global
   Windows mutex and exits when another supervisor owns it.

## Commands

Initialize or deliberately re-baseline the canonical files:

```powershell
python forex_guarded_deploy.py init `
  --target-root D:\forex\trad `
  --manifest D:\forex\trad\data\oanda_training_manager\state\deployment_manifest_v1.json `
  --owner codex-primary `
  --file oanda_practice_shadow_strategy_lab.py `
  --file oanda_practice_live_dashboard.py
```

Deploy tested staged files:

```powershell
python forex_guarded_deploy.py deploy `
  --source-root C:\path\to\staging `
  --target-root D:\forex\trad `
  --manifest D:\forex\trad\data\oanda_training_manager\state\deployment_manifest_v1.json `
  --owner codex-primary `
  --file oanda_practice_shadow_strategy_lab.py `
  --file oanda_practice_live_dashboard.py
```

Verify canonical source has not drifted:

```powershell
python forex_guarded_deploy.py verify `
  --target-root D:\forex\trad `
  --manifest D:\forex\trad\data\oanda_training_manager\state\deployment_manifest_v1.json
```
