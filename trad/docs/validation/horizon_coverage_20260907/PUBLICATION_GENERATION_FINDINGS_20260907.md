# Bounded publication-read diagnosis — September 7, 2026

The later producer summary and heartbeat are coherent. Four retained observations from 23:34:03 to 23:34:14 UTC span two summary generations: every captured summary hash matches its heartbeat, every summary payload seal verifies, and both the fresh direct reader and API report current evidence. Worker heartbeat errors remain zero. Exact file bytes, sizes, identities, modification times and read clocks are retained in `publication_generation_20260907T233403Z/`.

The earlier failure is preserved. Six API requests from 23:31:16 to 23:31:25 UTC returned `pair_summary_generation_mismatch`, with no current combined forecast rows. Five responses shared one full-response hash and the sixth had a new hash. That is evidence of repeated unavailable API observations, not evidence that the worker lost all forecasts. The raw producer files at those earlier failure instants were not captured, so their exact generation relationship cannot be reconstructed from later bytes.

The source reviewed during the later probe has a five-second main-state cache. The later API samples explicitly retain their original observation clocks and repeat an earlier observation across cached responses. This can extend the visible duration of a failed read. The evidence does not prove that caching alone caused the earlier failure, or that a 30-second main-state cache was active. Root separately reviewed the reader and reported recovery at 23:33:24 UTC.

Evidence:

- `EURUSD_JOINT_CONTRIBUTION_20260907T233125Z.json`, SHA-256 `958123e653c82f52da2fde7ad4dd28c62fbe5004e3d783358f093d052840e854`: all six unsuccessful API observations and the preliminary example-script assumption failure. No contribution was invented.
- `publication_generation_20260907T233403Z/PUBLICATION_GENERATION_DIAGNOSIS.json`, SHA-256 `d4044d9dc84aec17a3a3d7c33f195eac931fe52efdd972e5ee015bbec63481eb`: four source/reader/API observations plus exact file copies.
- `EURUSD_RETAINED_CONJUNCTION_EXAMPLE_20260907.json`, SHA-256 `99b6e658079ef39ffb73a3555a36cc554e5d291c7d3d888c509b7538d5896d53`: exact original forecast, publication and consumption rows for decision `bb9c54fd8b55eef32c62b09960842f8b0d450b40ea70a593ed809d16a5a49a0a`; forecast SHA-256 `d921ace1c57a241c798408aad49f341dff4ac0aaa2393e34a07f19e88a4d7670` and receipt hashes verified.

The retained EUR/USD example uses the same-model neutral-news estimate of −0.2720188319794971 pips and a news adjustment of −1.4469349540508356 pips. Its separately fitted matched price-only estimate is −0.17909241329952794 pips. It has 94 retrospective training rows, 92 with broad news context and zero with vetted directional news. This demonstrates that price and broad news context were used jointly in this forecast; it does not demonstrate predictive skill, causal news impact or calibrated uncertainty.

No model fitting, outcome scoring, new news capture, broker requests, worker actions or source/configuration/runtime writes were performed. This bounded diagnosis is not an uptime claim. The four structural missing-forecast counts retain their own earlier observation clocks in the separate missing-pairs audit.
