# Stage C — all-68 offline input gate

This stage performs a read-only preflight for the first all-68 offline replay.
It binds the preserved dirty-worktree identity, the audited long-history ZIP
directory, and the reviewed 68-pair pip map. It does not extract market data,
fit a model, create forecasts, access the network, or change the live project.

Run with:

`C:\Users\zmoor\AppData\Local\CodexRuntimes\timeseries312\Scripts\python.exe -I -B all68_input_preflight.py`

The receipt is `ALL68_INPUT_PREFLIGHT.json`. It is a prerequisite for, not
evidence of, the later global-clock replay.
