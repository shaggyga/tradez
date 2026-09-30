# Existing registries and saved-artifact reuse

These are retrieval starting points. The current design/review pointers determine present status; old CURRENT names and catalog classifications do not.

- [Historical model reuse register](../SIGNED_COST_RESEARCH_20260912/FOREX_MODEL_REUSE_REGISTER_20260911.md): existing duplicate-check procedure and predecessor reuse decisions.
- [Model experiment census](../RECOVERY_CHECKPOINT_20260921/audit_20260921/deep_audit_02/MODEL_EXPERIMENT_CENSUS.json):3,124 historical experiment records, including source/spec/dataset/feature hashes and outcomes.
- [Lineage evidence index](../RECOVERY_CHECKPOINT_20260921/audit_20260921/deep_audit_02/MODEL_LINEAGE_EVIDENCE_INDEX.json): parent/evidence links.
- [Older operations catalog](../OPERATIONS_CATALOG_20260922_125442/operations/OPERATIONS_CATALOG.json):20 then-current recipes and7 historical records; anchor predates the macro chain. Its next-item text is historical.
- Broader existing local historical library: C:/Users/zmoor/Documents/vault_cold_archive/20260901_record_consolidation/FOREX_MODEL_LIBRARY/builds/forex_catalogue_20260711_213704_v1/catalog/. Family/variant/application/run/artifact/dataset tables already exist. Cold-library availability on another machine has not been verified. Do not add overlapping census counts.

## Existing fitted remaining-model artifacts

The old MATCHED_REMAINING_20260922_041223/checkpoint/forex_matched_remaining.zip contains source, raw slices and expected replay hashes; it does not contain fitted model binaries and its restore procedure can refit. Original saved run bytes were instead verified at C:/Users/zmoor/Documents/forex/stage_c_alignment_integrity_v2/evidence/timed_20260922_022952/matched_remaining_step/runs_v2/matched-remaining-native-inputs and preserved here under saved_artifacts/. Use its BUILD_RECEIPT.json, README and archive manifest for byte retrieval and hash verification. No training, model deserialization, scientific promotion or current runtime acceptance occurred.

The original run fingerprint is fc0a1423ca93b62e94f215b49a6a55aaf280524aadc1501b231efef4031a2763. Treat copies on different machines as replicas of this same run, not additional experiments. A future artifact loader must use the original locked source/environment and approved consumer.

## Older directional models

DIRECTIONAL_RECOVERY_20260922_031840/checkpoint/local_restoration_001.zip contains eight retained .joblib model members under direction_decision_20260911/evaluation_001/h{5,15,30,60}_{technical,combined}.joblib. Its original archive manifest and directional-recovery recipe describe the required companion. This turn inspected member inventory only for that historical archive; it did not freshly qualify, load or refit those models.
