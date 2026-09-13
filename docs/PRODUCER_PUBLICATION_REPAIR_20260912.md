# Immutable producer summary publication

The separate v4 research worker fixes a reproduced status defect: the prior
worker wrote a summary, retained references to mutable attempt/readiness data,
then hashed those changed objects in a later heartbeat. The heartbeat could
therefore name bytes that were never written. This concerns operational status;
the independent forecast-ledger observer remains necessary.

The implementation reuses the exact retained weekend fair scheduler and the
existing `oanda_immutable_summary_publication_v1.py` boundary. It freezes a
summary to immutable bytes, writes those bytes atomically, reads them back, and
only then acknowledges that generation. Summary counts, hashes and original
generation clocks in subsequent heartbeats come from the acknowledged bytes.
Later mutation of an attempt, diagnostic, state population or display copy
cannot change those fields. Read completion is recorded separately and does
not renew original forecast/reference/target clocks.

Write failures, read failures, mismatched/truncated readback, backward read
clocks and oversized readback do not advance the acknowledged generation.
Publication boundary failures are counted by the worker. The earlier receipt
and files remain diagnostic evidence; a failed write/readback does not guarantee
that the mutable summary filename still contains the previous acknowledged
bytes, so consumers must still require matching generations. Native Windows
file-lock testing verified bounded refusal and recovery after the conflicting
reader closes. This does not claim that every earlier PermissionError cause is
known or that an external reader can never interfere.

The worker and registration preparer are separate v4 files with a new registry
schema, cohort scope and default study directory. They do not overwrite the v3
worker, reuse its study ledgers, reset claims, extend the ended practice trial
or change any broker configuration. No v4 registration or study was activated
by this repair. The immutable helper is unchanged. The existing 34-input joint
numerical model is unchanged; this is not a new predictive model or evidence
that it is the best model.

Validation includes **167 independent checks**: 147 whole worker/registry/fair
scheduler checks adapted for the explicit new identity and publication
contract, 15 whole-caller checks and five adversarial actual-file cases. The
original mutable-alias defect is reproduced from retained source. Exact AST
comparisons preserve scheduling, capture, fitting, summary construction and
scoring functions. Thirteen canonical-compatible caller checks additionally
run against the selected actual module paths and canonical dependencies.
These sets overlap and must not be added as independent scientific evidence.

Evidence is in
`revamp_8h_20260912/runtime/producer_publication_review_001/` and the independent
`producer_publication_storage_review_001/`. Candidate001, its oversized-readback
failure, initial compatibility failures and candidate002 are retained. The
new helper wrapper converts only the existing `source_size_limit` readback
failure into a bounded publication error. The first preparation script's CRLF
anchor failure is also retained; original source bytes were not changed.

Operating closure remains open: freeze the chosen complete source/config/input
profile and compatible environment, bind source versions, establish fresh
clock-valid quote/news inputs, preserve shutdown/restart and capacity behavior,
and observe a separately registered prospective run. The v4 source registration
still uses the prior worker's declared source/dependency inventory; this repair
does not prove that inventory is the complete transitive/native closure. Old
source cohorts and reports remain historical evidence. No profitability,
account authority, service recovery or live deployment follows from these tests.
