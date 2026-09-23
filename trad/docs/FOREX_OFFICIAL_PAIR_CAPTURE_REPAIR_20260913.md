# Official-event pair capture: September 13 repair

This source update closes the dedicated quote-producer mismatch and connects
new raw official observations to per-pair future outcomes. It does not enable
orders, invent directional sentiment, change the old all-68 capture contract,
or establish profitable prediction. No supervisor or source registry was
changed by this work; activation is a separate operational step.

## Reused implementation and changed boundary

The earlier V1/V2/V3 per-pair capture already distinguished stale and explicitly
nontradeable pairs. However, it expected the former executor-owned quote
producer, and its snapshot metadata still required all 68 supplied instruments.
The current research stream identifies itself as
`practice_007_dedicated_quote_stream` with schema 3. The new V4 path explicitly
binds that producer, requires coherent integer counts and connection generation,
and accepts a consistent partial snapshot. Missing pairs remain in the 68-pair
audit surface; they cannot erase independent eligible quotes.

The V4 path reuses the hash-pinned V1 numerical rows. It keeps exact boolean
tradeability, finite positive numeric quotes, aware venue clocks, the original
30-second pair-age limit and two-second venue-clock tolerance. Snapshot
generation must be at or before the actual post-read clock and no older than
five seconds. Event attachment must finish within 15 seconds of the retained
raw observation's first-seen time. This is the collector's raw-event clock,
not the later semantic forecast-publication clock.

The fast lane opt-in `--pair-capture-v4` initializes an immutable binding with
the actual first-enable UTC and source hashes. Each newly appended raw event
gets its separate terminal capture in the same SQLite transaction. No old
event is imported or reacquired. Bootstrap/revised identities remain
nonprospective. Quote-read failures are retained as terminal refusals while
raw news persists. Old `official_release_quote_capture` records and proof
counts keep their prior definition. The new table is
`official_event_pair_quote_capture`; its owned binding is
`official_pair_capture_v4_binding`.

The new horizon V2 worker reuses the pinned V1 1/5/15/30/60-minute scorer and
append-only outcome tables in a private module namespace. No old module globals
or ledger rows are changed. It records both long and short bid/ask endpoint
probes with the original 0/0.25/0.5-pip cost stress. These are hypothetical
per-pair probes, not selected directions, broker fills or portfolio P/L.
Missing/late outcomes retain null payoffs and are never retried as wins later.
The original 15-second attempt delay and 20-second target quote tolerance stay
explicit. Verified host clock evidence is retained and checked again before
valid outcomes commit. A later successful clock cannot revive missed outcomes.

Each output directory is exclusively owned through an OS file lock. Source
bindings refuse changed-code reuse. The worker bounds input cohorts to 10,000
event captures; larger cohorts require an explicitly reviewed continuation.
It does not implement indefinite evidence rotation.

## Activation sequence

1. Restart only the research fast lane with its existing arguments plus
   `--pair-capture-v4`. Its first successful cycle must expose
   `pair_capture_v4.binding`, supported/skipped source counts and explicit
   exclusion reasons. Preserve the initialized binding thereafter.
2. Run `oanda_official_event_pair_horizon_capture_v2.py` against that same fast
   database, a new empty output directory, the dedicated quote snapshot and
   current verified clock file. The options are `--input-database`,
   `--output-directory`, `--quote-path`, `--clock-path`, `--interval-sec 1`,
   `--duration-sec 0`, and `--quiet`. Zero duration means continuous worker
   operation; supervisor ownership/lifetime remains the caller's responsibility.
3. Verify new prospective events through entry and due-horizon records. A
   running worker with zero eligible new releases proves no prediction result.
   Main-news-only events remain outside this direct fast-lane attachment; the
   earlier dual-ledger cohorts remain preserved and are not relabeled.

## RBNZ coverage remains an external delivery gap

The maintained official policy route is
[RBNZ monetary policy](https://www.rbnz.govt.nz/monetary-policy), which exposes
the current OCR and update time. The bank also documents that website
publication can lag announcements by a few minutes in
[its publication guidance](https://www.rbnz.govt.nz/news-and-events/how-we-release-information).
The existing OCR parser handles the visible page structure; no duplicate
numeric extraction engine was introduced.

`oanda_rbnz_official_policy_adapter_v1.py` reuses that parser through a bounded
normal HTTPS request, validates same-authority secure redirects, rejects
non-HTML/error bodies and future/missing update clocks, and retains the actual
read time separately. It reports a parsed mutable rate snapshot as distinct
from a full policy statement and leaves consensus/direction absent.

The actual secure local probe at **2026-09-13 23:34:05 UTC returned HTTP 403**.
The official policy, decisions, news and home routes were also probed earlier
in this pass and returned 403. The source must remain blocked; successful web
search visibility does not establish local collector access. No TLS bypass,
proxy, browser impersonation or paid service was introduced.
The immutable probe receipt is
`C:/Users/zmoor/Documents/forex/official_live_repair_20260913/RBNZ_MAINTAINED_ROUTE_PROBE_001.json`.

Fast-lane snapshots/heartbeats now explicitly report supported sources,
skipped sources and their configured reasons. Zero transport errors therefore
cannot imply that an unsupported source was actually collected.

## Verification

The focused and inherited suite passed 92 tests. Coverage includes raw official
append → partial pair capture → future bid/ask outcome; explicit nontradeable,
stale, malformed, missing and future inputs; actual read clocks; original
all-68 behavior; append-only storage; duplicate/restart behavior; late and
clock-blocked terminal outcomes; inherited V1/V3 tests; source-change refusal;
secure RBNZ transport/parser and honest unsupported-source telemetry. These are
software fixtures, not prospective market performance.

Recreate using the existing project Python environment:

```powershell
python -B -m pytest -q -p no:cacheprovider test_oanda_rbnz_official_policy_adapter_v1.py test_oanda_official_event_pair_quote_capture_v4.py test_oanda_official_event_pair_horizon_capture_v2.py test_oanda_official_release_fast_lane.py test_oanda_official_release_fast_lane_communications.py test_oanda_official_event_pair_quote_capture_v3.py test_oanda_official_event_pair_horizon_capture_v1.py
```

The original timing/identity/evaluation contracts and all old result ledgers
remain preserved. No new direction, profitable-policy or live-delivery
qualification is claimed by this implementation record.
