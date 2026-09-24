# Later chronological forecast-layer comparison

Read the current Vault checkpoint/queue before running. This package extends the
same fixed forecast-layer experiment across24new six-hour development origins.
It does not authorize trading, API calls or base-model refitting.

The frozen recipe authenticates159.3MB of selected original inputs,16saved base
fit pairs, original predictions and seven existing frozen snapshots. Complete
parent identities/manifests accompany selected payloads; these are not complete
copies of those runs. Original26features and every saved training population must
match before deserialization. Original20origin forecast chunks reproduce exactly.

New base predictions use eight consecutive2second modeled slots; diagnostic
layers use eight15second slots after them. Actual phases must meet these bounds.
This is retrospective modeled availability, not observed historical publication
or a native execution-ready curve. Missing observations and unmatured labels remain
explicit; no session-calendar target is substituted. All dates remain development.

Frozen layers are reused byte-for-byte when already present. Only the previously
absent2880minute frozen snapshot and192new expanding snapshots are attempted.
The original support thresholds, normalization, lambda20 and exact maturity rules
remain binding. Coverage-matched raw/signed-only/magnitude variants share support;
unrestricted raw coverage is reported separately. Two overlapping later cohorts
and per-day/horizon strata are descriptive dependent evidence, not significance.

Use pinned Python3.12.10 and the recipe environment:

```text
python -X utf8 -I -B chronological_layer_operator_v2.py status --recipe <recipe> --recipe-sha256 <approved SHA> --paths <PATHS.json> --runs-dir <runs>
python -X utf8 -I -B chronological_layer_operator_v2.py run --recipe <recipe> --recipe-sha256 <approved SHA> --paths <PATHS.json> --runs-dir <runs>
```

Use `resume` for the existing interrupted run, then `verify`. Never start a
duplicate run to recover a partial. Verified payloads are retained; attempt-level
model loads, layer fits, timing and sampled resources are separate from deterministic
scientific outputs. A completed run is reused without model loading or fitting.

```text
python -X utf8 -I -B chronological_layer_checkpoint_v2.py export --package <new.zip> --paths <PATHS.json> --runs-dir <runs>
python -X utf8 -I -B chronological_layer_checkpoint_v2.py restore --package <zip> --sha256 <approved SHA> --destination <new-empty-directory> --run-tests
```

Restore intentionally recomputes new chronological layers for reproducibility;
it reuses base weights and existing frozen snapshots. Ordinary reuse reads completed
outputs instead. Portable reproduction on this machine does not prove another
operator's environment or cloud synchronization. Same-task review and independent
review are distinct. GPT/advisor comparisons remain deferred.
