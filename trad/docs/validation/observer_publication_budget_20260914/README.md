# Observer publication budget and forward-owner continuity

The observer reserves15seconds within its existing45second prepublication limit. A real68pair25MiB smoke used33.453seconds (sources16.235, build8.984, frame/archive7.406, receipt0.828); all68pair entries remained, with53rich vectors materialized and52fresh. These are observation counts, not forecast wins. Missing, stale, partial or unsupported inputs remain explicit.

The optional base-worker calculation order is an exact permutation of the configured quote universe. V2 rotates by observed minute. Cross-sectional caches are sorted back into the original order before any reduction, preserving unchanged feature math when the available member set is the same. Slow cycles retain unavailable placeholders and the attempted order. No prior model acquires these observation fields.

The heartbeat retains each last failure's original start/failure timestamps during the next read and across a worker restart. Only a later successful publication clears it. Individual local operations cannot be preempted; the unchanged final guard refuses publication after45seconds rather than extending the deadline.

The observer successor archive is separate. The existing feature forward ledger is intentionally retained because its exact eight sources, protocol and resource configuration are unchanged. Its archive input is not ledger ownership. Existing publication, quote, entry and target clocks remain authoritative; pending outcomes continue to settle. New mapper comparisons use only the successor archive, while cumulative forward summary totals include both observation source generations, each retained with its exact source schema identity.

Run from the project directory with the installed Python environment:

```powershell
python -B -m pytest -q -p no:cacheprovider test_oanda_research_feature_observation_budget_v2.py test_oanda_feature_forward_archive_handover.py test_oanda_research_feature_observation_worker_v1.py test_oanda_research_feature_observation_worker_v2.py test_oanda_feature_observation_determinism.py
```

Tests use owned disposable inputs and the unchanged real calculators, archive normalizer and forward protocol/ledger. They cover slow calculators and source reads, original deadline refusal, all-pair missingness, full-universe rotation and exact numeric parity, invalid scheduling lists, immutable archive recreation, failure persistence/restart/clear, and a restarted retained forward owner reading a new archive while scoring old jobs at their unchanged original targets. There are no broker/account/order calls.

Deployment evidence and exact source/profile hashes are retained under `operational_repairs_20260913/observer_publication_budget_repair_v1`. The never-installed empty-forward plan is explicitly superseded there. Recreate artifacts from original retained sources; never rewrite or bless old observations against successor source hashes.
