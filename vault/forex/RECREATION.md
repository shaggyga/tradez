# Restore the current offline research checkpoint

Updated September21,2026. Use [the portable checkpoint](RECOVERY_CHECKPOINT_20260921/README.md) and its included restore_checkpoint.py, not the older forex_vault_import.py or bootstrap launcher. Verify every member and extract into a new absent directory. The restore tool never launches project code.

[Current pointer](RECOVERY_CHECKPOINT_LATEST.json) binds the manifest and successful local restore receipt. Eleven synthetic contracts passed on restored source copies. Read [scope and exclusions](RECOVERY_CHECKPOINT_20260921/SCOPE_AND_LIMITS.md), including private legacy modules, historical runtime databases and separately provisioned dependencies. No standalone legacy-runtime or fresh-environment restoration is claimed.

[Previous guide](maintenance/vault_refresh_20260921/before/projects/forex/RECREATION.md) is preserved. All older sealed source and research packages retain their own dates. OneDrive destination readback remains required.

## Windows destination length

Use a short restore root, such as C:\ForexRecovered. The first deeply nested maintenance-directory attempt hit the 260-character Windows path limit (LongPathsEnabled=0). It did not alter the source package. Retrying at C:\ForexRestore_20260921 kept every archived path at most 216 characters and passed. [Failed attempt record](maintenance/vault_refresh_20260921/RESTORE_ATTEMPT1.json) is retained. No system setting was changed.
