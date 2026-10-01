# Offline replica replay

Use the source commit in ../RETAINED_PROJECTION_CONNECTION_20261001/REVIEW.json and restore the capsule with the command in its RESUME.md. In a separate matching-source checkout, copy CURRENT_INPUT_ROWS.json, CURRENT_INPUT_REPORT.json, CURRENT_OUTPUT.json and run_restored_fixture.py from that immutable packet into the checkout root. Then execute in the qualified environment:

```powershell
python -I -B ./run_restored_fixture.py C:/Users/zmoor/Documents/forex C:/Users/zmoor/OneDrive/thevault/projects/forex
```

Use the original dependency roots as those two positional arguments. The runner reads only copied recorded inputs, relocated source and restored model bytes, and emits RESTORED_REPLAY.json. It refuses original dependency reads outside its own checkout. Do not point the runner at a bare packet directory without the matching source/artifact checkout. No collector or model fitting is started. The retained input fixture reproduces the specific recorded observation; it is not current market data.

Original historical frame/input sources remain identified by exact recipe/manifest hashes in HISTORICAL_REPLAY.json and verify_replay.py. Historical full-run replication is not required to use the recorded current fixture.
