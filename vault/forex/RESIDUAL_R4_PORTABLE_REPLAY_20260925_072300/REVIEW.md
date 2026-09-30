# Residual R4 portable restore/replay

Verdict: same-task review passed for the R4 portable restore/replay gate. Independent scientific review remains pending and nonblocking.

A fresh isolated source restore replayed `currency_projection_residual_operator_v2.py` with the pinned parent recipe hash and parent paths. The replay exited 0 and produced 59349278 bytes with SHA-256 `a1c518b93224cfeeb7a1f66ed5905ffb5d7138f0021f8c3bee50383b72bb295c`. This matches the preserved residual result exactly.

Focused restored-source tests also passed: `14 passed`.

No base models were fitted or loaded, no policy replay ran, no API/service/broker action occurred, and the live bot was untouched.

Next: `forecast_curve_shape_layer_comparison_v2` coverage repair.
