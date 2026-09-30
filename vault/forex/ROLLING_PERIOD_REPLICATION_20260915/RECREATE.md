# Recreation

The accepted [guide](source/forex/trad/docs/FOREX_ROLLING_PERIOD_REPLICATION_20260915.md) and copied executed-command/config receipts give the exact window commands and parameters.

The source tree preserves forex/trad and its sibling forex/direction_decision_20260911/src. Use the recorded timeseries312 runtime and the package versions retained with each RESULTS.json. Do not apply an earlier full-TRAIN normalizer to an earlier OOF prefix.

Each window retains four prefix and final six-head models, direct-only/context combiners, calibrators, seven additional masked-family fits and the original-recipe comparators. Its exact five-prefix normalizer NPZ and metadata are in the accepted source/forex/trad/docs/validation evidence paths. Family models keep 53 slots: 50 technical fields plus known long/short entry costs and categorical pair ID.

Restore compatible registered feature inputs and retained normalization to replay saved inference. For complete refitting/evaluation, restore the exact local base/endpoint/quote/prepared datasets listed in DEPENDENCIES.json, or regenerate them with the retained source manifests, inventories and commands. The package intentionally does not duplicate those row datasets or claim that they are embedded.

Previous dated packages remain separate pinned historical references. No archived code is executed by publication.
