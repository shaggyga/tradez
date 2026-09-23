# Stage C alignment integrity v2

This isolated offline layer repairs issuance, publication and recovery before
the engineering design's all-68 daily/multiday campaign. The current verified
handoff is `ALL68_INTEGRITY_BATCH_05_20260921.md` in the Forex Vault; resolve the
current queue through `DESIGN_ALIGNMENT_LATEST.json`.

The accounting event layer now adds explicit synthetic fills, financing,
capacity, resting-order events, isolated arms and a batched close kernel.
Read `OPERATOR_CONTRACT_V2.md` for its fixed lower-model run/status/resume/verify
workflow. The reviewed recipe and its external hash are published in the
current Vault package; never regenerate approval to bypass drift. The exact
next engineering item is policy thesis and common-horizon HOLD/EXIT/REPLACE
integration. Full campaign readiness remains false.

The neutral runner uses only the exact origin close to issue a forecast.
Physical archive hashing and Parquet column reads include later bytes; future
values do not determine issuance or predictions. All row groups are inspected,
and nonfinite, nonpositive, missing or duplicate origin values are rejected.
Outcomes are separate PENDING records. No fit or policy evidence is produced.

Publication uses a Windows kernel lock (portable flock on Unix), atomic owner
metadata, dependency-bound identities, immutable payloads, a verified origin
snapshot journal and a required-payload completion manifest written last.
`--resume --report-only` rebuilds interrupted deterministic output without raw
archive access only when the cached origin snapshot verifies. Source, runtime,
config or input identity changes require a new run ID.

The NY session convention is `fx_ny_1700_session_close.v2`: Monday-Friday 17:00
New York closes strictly after the decision. Sunday opens a session; it is not
a completed close. Market-session target calls require explicit dated holiday
and partial-close metadata. Synthetic tests do not establish historical venue
availability. Elapsed, daily-close and counted-close targets remain distinct.

Run:

```powershell
& 'C:\Users\zmoor\Documents\forex\stage_c_alignment_integrity_v2\verify_alignment_integrity.ps1'
```

Real subprocess tests cover ownership competition, safe PID queries, process
death at publication boundaries, archive-free recovery and native launcher
failure. The portable checkpoint includes five frozen runtime sources, exact
dependencies, configuration, synthetic 68-pair input and expected outputs. Its
external hash, member inventory and relocated subprocess replay are verified.
It is a runnable issuance/recovery fixture, not a complete campaign or live-system
backup. `recover_v2_run.py` is the older output-only relocation helper.

Existing v1/earlier v2 outputs and sealed checkpoints remain historical evidence.
The retained historical archive has bid/ask candle fields; the quote qualification
report records 68 valid origins and 67 valid exact 24-hour endpoints (EUR_DKK
missing). These observations do not certify simultaneous executable quotes,
original arrival times, conversion availability or account economics.

Full engineering readiness remains false. Continue Step D using the reviewed
Decimal accounting predecessor, then the first complete causal all-68 campaign.
Feature population and non-GPT macro availability remain independent work.
GPT/advisor comparisons remain deferred.
