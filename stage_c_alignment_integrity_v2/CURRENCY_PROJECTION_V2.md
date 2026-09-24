# Fixed currency-factor forecast projection

Use the exact frozen contract and recipe. Reuse the preserved pure currency-state
solver through explicit ForecastEdge records, not historical PairObservation records.
Compare direct/simple-return forecasts, full log-return currency projection and
fixed50percent pair residual retention. No base model loading, training or learned
layer fit. No current-quote ranker or broker operation. All68coverage and connected
component exclusions remain explicit. Original source forecast IDs/availability and
constituent identities are retained. Outcomes enter assessment only after maturity.

Run/resume/verify: currency_projection_operator_v2.py <action> --recipe <file>
--recipe-sha256 <pin> --paths <PATHS.json> --runs-dir <directory>.
Export: currency_projection_checkpoint_v2.py export --package <new.zip> --paths
<PATHS.json> --runs-dir <directory>. Restore with --package <zip> --sha256 <pin>
--destination <new-empty-directory> --run-tests. The capsule includes selected
original parent payloads and the exact two preserved solver modules, not complete
parent copies. Use the pinned source/environment. Ordinary continuation reuses
completed outputs; no refitting or policy replay is needed to inspect them.

This is inspected development, not confirmation, native qualification or live
authorization. Read Vault current pointers for status and exact next action.
