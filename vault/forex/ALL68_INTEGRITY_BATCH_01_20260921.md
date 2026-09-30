# Alignment integrity batch 01 — started

The first implementation step of the design path has started in the isolated
workspace folder `C:\Users\zmoor\Documents\forex\stage_c_alignment_integrity_v2`.
It leaves Stage C v1 reports, data and source unchanged.

The source contracts, acceptance mapping, test scope and next step are recorded
here. The implementation adds maturity-aware training selection, issuance
independent of future endpoint support, forecast/outcome separation, an explicit
UTC diagnostic calendar, dependency-bound publication/resume and native exit
propagation.

The batch verifier passed ten synthetic tests. It did not run an all-68 fit or
history extraction and does not change engineering readiness. The next task is
to bind these contracts into a versioned all-68 runner and make a compact
relocated-restore fixture.
