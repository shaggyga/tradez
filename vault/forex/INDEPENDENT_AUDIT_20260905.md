**Independent Forex audit — September 5, 2026**

Reviewed the actual working tree at `C:\Users\zmoor\Documents\forex\trad`, using `C:\Users\zmoor\OneDrive\thevault\projects\forex\README.md` and its current audit/recovery records as the guide. This is an independent, risk-based code and evidence review, not an exhaustive correctness certificate for every historical module.

**Conclusion: keep the project stopped and retain `no_trade`.** The archive matches the current source, but execution and prospective-evidence defects remain. This audit reproduced **five additional code defects**, reconfirmed **all three previously registered fast-lane defects**, and found **one source-recovery documentation defect**. A separate, currently unused confirmation-registration API also needs hardening before activation.

No trading workers, supervisors, watchdogs or dashboards were started. No broker requests, orders, account changes, private credential reads, source fixes or production database writes were performed. The existing issue register and vault were left unchanged. All new deliverables are in the sibling `audit_20260905` directory.

| Priority | ID | Finding | Status in this review |
|---|---|---|---|
| P1 | A01 | A properly signed canary revocation or reduced limit is ignored at the final order gate | Newly reproduced |
| P1 | A02 | Missing FX conversion is replaced with 1.0, undermining account-currency exposure caps | Newly reproduced |
| P1 | A03 | Source ranking can record a prospective entry quote from before semantic mapping was ready | Newly reproduced; current prospective branch has no observations |
| P1 | A04 | Fast-lane availability can precede durable receipt commit | Previously registered; reproduced again |
| P2 | A05 | A collector commit crossing the scan boundary can be omitted from subsequent fast-lane intake | Newly reproduced |
| P2 | A06 | Building progress replaces the completed fast-lane integrity snapshot | Previously registered; reproduced again |
| P2 | A07 | Error/interrupted state resets a progressed fast-lane cursor | Previously registered; reproduced again |
| P2 | A08 | Credential preflight accepts quoted alphabetic secrets as code references | Newly reproduced with synthetic data only |
| P3 | A09 | Source-only recreation instructions include a validator requiring excluded local evidence | Newly reproduced documentation mismatch |

P1 denotes a safety or prospective-evidence invariant that needs correction before relying on the affected path. It does not mean a live-money incident was observed. The execution findings require a future otherwise-valid practice authorization; the current lack of confirmed candidates limits present exposure.

1. **A01 — revalidate the exact current authorization before submitting.**

   At [oanda_practice_top_signal_executor.py:710](C:/Users/zmoor/Documents/forex/trad/oanda_practice_top_signal_executor.py:710), the final gate reloads the authorization and verifies its HMAC. It then checks lifecycle/verifier state and the earlier decision's unit cap, without checking whether the newly signed payload still authorizes entry or still contains the exact permitted entry and limits.

   An isolated fixture first authorized and consumed a valid synthetic nonce. Replacing the document with a correctly signed `entry_authorized=false` payload and an empty entry list still returned an empty blocker. A correctly signed reduction from 10 units to 1 also allowed a proposed 10 units. The production submission path calls this hook before the broker POST at [oanda_practice_shadow_strategy_lab.py:5264](C:/Users/zmoor/Documents/forex/trad/oanda_practice_shadow_strategy_lab.py:5264).

   Repair by binding the final check to the current signed authorization generation, exact entry/nonce, account, signal, cohorts, instrument, direction, expiry and limits. Revalidate without consuming the nonce a second time. Add legitimate signed revocation and tightened-limit tests; an invalid-signature test does not cover these transitions. Evidence: [execution reproduction](execution/execution_gate_reproduction.json) and [execution review](execution/EXECUTION_AUDIT.md).

2. **A02 — unavailable conversion must block sizing and exposure assessment.**

   [oanda_practice_shadow_strategy_lab.py:3583](C:/Users/zmoor/Documents/forex/trad/oanda_practice_shadow_strategy_lab.py:3583) returns `1.0` when both direct and inverse account-currency conversion requests fail. Governed existing exposure at [oanda_practice_top_signal_executor.py:660](C:/Users/zmoor/Documents/forex/trad/oanda_practice_top_signal_executor.py:660) and sizing at [line 675](C:/Users/zmoor/Documents/forex/trad/oanda_practice_top_signal_executor.py:675) use this as a valid conversion.

   For a synthetic USD account trading EUR/GBP, an unavailable GBP/USD conversion admitted approximately $1,105 of hypothetical actual exposure against an $850 cap if the true conversion were 1.30. The reproduction also exercises default dynamic sizing and an existing cross-currency position. The 1.30 rate is an illustrative fixture, not a fetched market price. A rate above 1 causes underestimation; other rates can produce a different error.

   Represent an unavailable conversion explicitly and block the affected entry. Require a fresh, finite, positive conversion for both existing positions and proposed exposure, then check the final order against current caps. No actual cap breach or trade was demonstrated.

3. **A03 — distinguish raw observation time from forecast availability.**

   [oanda_causal_source_factor_response_map_v1.py:846](C:/Users/zmoor/Documents/forex/trad/oanda_causal_source_factor_response_map_v1.py:846) derives factor knowledge from raw `first_seen_utc` without the transport's later `mapped_utc`. Its forecast builder sets `issued_utc` to that cutoff at [line 2239](C:/Users/zmoor/Documents/forex/trad/oanda_causal_source_factor_response_map_v1.py:2239), and [line 2311](C:/Users/zmoor/Documents/forex/trad/oanda_causal_source_factor_response_map_v1.py:2311) persists it unchanged. Current V8 delegates to this code. Rank V7 inherits the quote timing checks in [oanda_source_conditioned_currency_rank_v1.py:340](C:/Users/zmoor/Documents/forex/trad/oanda_source_conditioned_currency_rank_v1.py:340).

   With raw observation at 12:00:00 and semantic completion at 12:01:30, the fixture produced source forecasts labeled issued at 12:00:00. The actual V7 rank cycle at 12:01:32 selected and stored a 12:00:10 entry quote: **80 seconds before semantic completion**, while ordinary quote-age and source-lag checks passed. The fixture uses eight independent prior training episodes and exact V8 contracts; it does not bypass the classifier-version gate.

   Raw-event response measurement can legitimately begin at a pre-map quote. The defect is treating that quote as an available prospective directional entry. Preserve raw clocks for response diagnostics, separately attest semantic and forecast publication availability, and require a tradable prospective entry after all needed information is available. Changes need a new cohort; do not rewrite V8 history. Current saved V8 has zero prospective proof events/forecasts, and V7 has zero decisions, so production impact is unestablished. Evidence: [clock reproduction](evidence/evidence_clock_reproduction.json).

4. **A04 — fixed padding is not a commit attestation.**

   At [oanda_source_governance_news_fast_lane.py:340](C:/Users/zmoor/Documents/forex/trad/oanda_source_governance_news_fast_lane.py:340), availability is sampled as current time plus 10 seconds before receipt insertion and the commit at line 376. A virtual 11-second commit delay reproduced an availability timestamp one second before a separate SQLite reader could see the receipt. The production integrity checker still passed.

   This reconfirms `FX-20260905-FASTLANE-DURABLE-AVAILABILITY`. Use an append-only publication/commit attestation observed after durable visibility, and require consumers and independent checks to respect it. Historical exposure was not established, and immutable receipts must not be silently corrected. Evidence: [known fault reproductions](known_fastlane_faults.json).

5. **A05 — advancing by wall-clock time can skip an uncommitted input row.**

   [oanda_source_governance_news_fast_lane.py:255](C:/Users/zmoor/Documents/forex/trad/oanda_source_governance_news_fast_lane.py:255) resumes from the last scan start, and [line 269](C:/Users/zmoor/Documents/forex/trad/oanda_source_governance_news_fast_lane.py:269) reads an interval bounded by that clock. [oanda_source_governance.py:561](C:/Users/zmoor/Documents/forex/trad/oanda_source_governance.py:561) uses a strict lower timestamp bound. The collector assigns its observation clock before classification/insertion and commits afterward at [oanda_local_news_sentiment.py:19714](C:/Users/zmoor/Documents/forex/trad/oanda_local_news_sentiment.py:19714).

   In a real overlapping WAL fixture, an article was first seen at 13:15:20 but remained uncommitted during the 13:15:32 scan. That scan advanced successfully. After the writer committed, the 13:16:02 scan still returned zero new rows because the article's first-seen time was behind the cursor. Two raw articles were visible, but only one had a fast-lane receipt, and integrity still passed.

   Use a durable input sequence/high-water mark tied to committed rows, or a rigorously bounded overlapping replay with immutable deduplication and completeness reconciliation. A full governance reconciliation might ingest the row separately; this demonstrates omission from the fast-lane receipt path, not loss from the entire project. The fixture establishes the race, not its historical incidence. Evidence: [reproduction script](late_commit_reproduction.py) and [result](late_commit_reproduction.json).

6. **A06 — separate progress from the last completed integrity state.**

   The building publication at [oanda_source_governance_news_fast_lane.py:289](C:/Users/zmoor/Documents/forex/trad/oanda_source_governance_news_fast_lane.py:289) replaces the only state file without a completed cutoff/count/commit flag. Sampling it makes the checker reject the same database whose prior completed snapshot passes. This reconfirms `FX-20260905-FASTLANE-PUBLICATION-STATE` and matches the final saved degraded integrity report. It is a false degraded attestation, not a fail-open execution bypass. Publish progress independently while preserving strict freshness checks on completed evidence.

7. **A07 — retain the committed cursor across failures.**

   The resume logic at [oanda_source_governance_news_fast_lane.py:255](C:/Users/zmoor/Documents/forex/trad/oanda_source_governance_news_fast_lane.py:255) accepts prior progress only when status is `ok`. Input errors, registry errors and interrupted `building` states replace that file. Each reproduction reset a cursor progressed to 13:15:32 back to 13:15:00 activation. Immutable deduplication avoided duplicate receipts, but unnecessary rereading remains. This reconfirms `FX-20260905-FASTLANE-RETRY-CURSOR`. Keep the cohort-bound committed cursor separately from transient status.

8. **A08 — quoted secrets can be mistaken for variable names.**

   [tools/credential_audit.py:80](C:/Users/zmoor/Documents/forex/trad/tools/credential_audit.py:80) classifies alphabetic/underscore-only values as code references without preserving whether the value was quoted. [tools/vault_worktree_snapshot.py:134](C:/Users/zmoor/Documents/forex/trad/tools/vault_worktree_snapshot.py:134) and [line 155](C:/Users/zmoor/Documents/forex/trad/tools/vault_worktree_snapshot.py:155) reuse this for private-value collection and export checking.

   A new synthetic alphabetic password matched the assignment detector, was omitted from the known-private set, and was accepted inside a quoted password assignment in a source file. This is a scanner false negative; no actual credential disclosure was found or claimed. Preserve quoting/context, restrict code-reference exceptions to actual expressions, and never discard a known private literal because it resembles an identifier. Evidence: [synthetic reproduction](recovery/credential_bypass_reproduction.json).

9. **A09 — label the register validator's recovery prerequisites.**

   [docs/VAULT_RECREATION_CURRENT.md:45](C:/Users/zmoor/Documents/forex/trad/docs/VAULT_RECREATION_CURRENT.md:45), mirrored in the vault guide, suggests running `python oanda_issue_register_validator.py` after source recreation. In a newly extracted and verified source ZIP, it returns 161 missing-reference errors: 160 evidence references and one validation artifact. On the actual canonical project it passes all 91 entries. This consolidated report assigns P3 because the defect is a recovery-instruction mismatch; the recovery sub-review suggested P2.

   The exclusions are documented and do not indicate a corrupt backup. Label this command as requiring the canonical local evidence tree, or provide a separate source-only inspection mode that reports unavailable evidence without approving it. Evidence: [source-only validation](recovery/source_only_register_validation.json).

**What the records establish**

The current source ZIP safely extracts and verifies: **1,395 members**, zero source/hash mismatches, and **986 Python syntax compilations**. Its base Git commit, dirty-worktree inventory and index/status bindings match the actual project. All **103 shared records / 97,304,608 bytes** match their manifest and canonical source. The configured Python 3.12.10 research environment matches all **142 pinned package versions**. These checks establish identity and reproducibility of source, not behavioral correctness or predictive value.

The legacy archive's receipt and plan match. **5,636 of 5,637 archived files** were independently hashed with zero mismatches; one private-path file was deliberately not read. Complete market/news histories, original causal ledgers and installed credentials are outside source-only recovery. No full production ledger traversal, database integrity rescan or full system restoration was performed.

The existing **91-item issue register** validates with 47 completed repairs, 24 implemented/collecting items, 11 permanently invalid cohorts, two superseded/collecting baselines, four external blockers and three open repairs. That remains the register's pre-review state; the new findings above have not been added or marked repaired.

The saved lifecycle contains **53,357 hypotheses: zero confirmed, 43,872 collecting and 9,485 futility-rejected**. V8 has **13 preactivation diagnostic events and zero prospective proof events/forecasts**; rank V7 has **zero decisions**. Its V152 classifier binding rejects current V164 intake intentionally. Simply restarting cannot make that branch collect compatible current evidence. A green verifier on an empty quote-capture cohort is evidence of checked empty state, not successful live capture.

The frozen September 4 close recap reports **24 official clocks**, **130 material factor episodes**, **zero strict pre-move directional matches** and **zero causal pre-release consensus observations**. The saved account was flat. These are dated local observations; this audit did not refresh broker or external source state. The supported decision remains `no_trade`.

**Verification and scope**

| Verification | Result |
|---|---|
| Original fast-lane / issue-register focused tests | 11 passed |
| Source V8 / rank V7 / prospective-governance focused tests | 18 passed |
| Vault/recreation maintenance tests | 90 passed, 1 Windows symlink-privilege skip |
| Delayed commit, publication and retry fault fixtures | All three known defects reproduced |
| Overlapping collector commit fixture | New omission reproduced; integrity still accepted |
| Execution fixtures with signed payloads and stubbed pricing | Revocation and conversion defects reproduced |
| Semantic-clock/rank fixture | Entry before mapping completion reproduced |
| Synthetic credential and source-only restore fixtures | Both new issues reproduced |

These are separate focused runs with some overlap, not a count of unique tests or a whole-repository regression pass. Passing existing tests did not cover the newly reproduced transitions. Root fast-lane tests blocked network, subprocesses, production SQLite access and writes outside the audit directory. Other diagnostic scripts used temporary files, extracted production methods or inspected offline code. The initial root pytest launch was blocked by its audit write guard before tests began; routing pytest's own log to the audit folder allowed the complete successful run.

Process inspections during the audit found zero canonical Forex workers/supervisors/watchdogs and no port 8765 listener. `ForexSafeCoreAtLogon` and the legacy Forex watchdog task were disabled. The separate BIGTRIAD dashboard remained an enabled logon task. Evidence and timestamps are in [runtime_observation.json](runtime_observation.json).

The confirmation-registration API accepts a caller-supplied historical lock time and definition fields not fully bound to the stored discovery snapshot. Only tests call those APIs in the reviewed source, and the relevant confirmation graduation count is hardcoded to zero. Treat this as unfinished governance functionality to close before wiring it into promotion, not evidence of an active promotion bypass. See the [evidence review](evidence/EVIDENCE_AUDIT.md) for its bounded reproduction. The [recovery review](recovery/RECOVERY_AUDIT.md) documents archive verification and recovery limits in detail.

**Recommended reorientation**

1. Repair A01/A02 and the causal clocks/intake in A03–A07 using these failure scenarios as acceptance tests. Harden export scanning before another source publication. Preserve the current evidence and lineage; do not credit retrospective corrections as prospective success.
2. Keep V8/V7 explicitly frozen. Design a separately identified successor compatible with the chosen current classifier only after knowledge-time and entry-time contracts are sound. A changing classifier cannot continuously reset an experiment and still yield an untouched confirmation cohort.
3. Run one small, predeclared research experiment when a future collection session is authorized: a defined event family, limited liquid pairs, fixed horizons, independent episodes, executable bid/ask costs, a technical baseline, source-only comparison and explicit abstentions. Measure first-observation-to-commit, mapping and forecast latencies as part of the outcome. Expand only when this experiment demonstrates incremental prospective information under the existing independent gates.
4. Treat missing consensus, event-time rates, direct RBNZ transport and secure GDELT transport as explicit gaps. Source counts, historical explanations and extra model families do not fill them. Respect the recorded procurement constraints.
5. Build any later offline demo from dated reports with the timestamp, misses, coverage gaps and `no_trade` visible. Keep demo readers independent of broker startup. Establish a coherent backup of the original causal ledgers before relying on disaster recovery for historical reproduction.

The audit changes no trading authority. Repairs, cohort registration, collection restart and demo implementation remain future work.
