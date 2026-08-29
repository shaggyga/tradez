# GPT Orig Constant Rotation Demo

Demo-only all-68 forecast rotation using the same primary forecast rotation stack
that is running on the primary live account.

## Files

- Bot engine: `oanda_primary_forecast_rotation_bot.py`
- Demo config: `config/gpt_orig_constant_rotation_demo.json`
- Demo data: `data/forex_gpt_manager/account_gpt_orig_constant_rotation_demo/`

## Safety

- Mode is `demo`, not `live`.
- The bot resolves `OANDA_API_KEY` plus `OANDA_ACCOUNT_ID_GPT` or
  `OANDA_ACCOUNT_ID_MAJ`.
- `demo` mode uses `https://api-fxpractice.oanda.com`.
- Live execution gates and live account aliases are not used.

## Commands

Print resolved config without secrets:

```powershell
.\data\oanda_training_manager\.research_py313\Scripts\python.exe .\oanda_primary_forecast_rotation_bot.py --config .\config\gpt_orig_constant_rotation_demo.json --mode demo --source oanda --print-config --once
```

Run one demo broker cycle:

```powershell
.\data\oanda_training_manager\.research_py313\Scripts\python.exe .\oanda_primary_forecast_rotation_bot.py --config .\config\gpt_orig_constant_rotation_demo.json --mode demo --source oanda --execute --once
```

Run continuously:

```powershell
.\data\oanda_training_manager\.research_py313\Scripts\python.exe .\oanda_primary_forecast_rotation_bot.py --config .\config\gpt_orig_constant_rotation_demo.json --mode demo --source oanda --execute
```

Monitor:

```powershell
.\data\oanda_training_manager\.research_py313\Scripts\python.exe .\primary_forecast_rotation_monitor.py --data-dir .\data\forex_gpt_manager\account_gpt_orig_constant_rotation_demo --config .\config\gpt_orig_constant_rotation_demo.json
```
