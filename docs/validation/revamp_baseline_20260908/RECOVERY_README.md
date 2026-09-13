# Forex revamp baseline and recovery workspace

Created September 8, 2026. This is a local recovery checkpoint and isolated source copy for the first revamp phase. It is not a deployed replacement bot or an off-device disaster backup.

- Live project: `C:/Users/zmoor/Documents/forex/trad`
- Isolated source: `C:/Users/zmoor/Documents/forex/revamp_baseline_20260908/workspace/trad`
- Static source/model/data recovery and selected coherent ledger backups: `D:/ForexRecovery/revamp_20260908T1353Z`
- Evidence: this directory's `runtime`, `performance`, `reconciliation` and `comparator` subdirectories.

`SOURCE_ARTIFACT_RECOVERY_20260908.json` binds the exact source archive, all 2,202 copied source members, 36 separately preserved D source modules, fitted artifacts, reports and the complete unified training matrix. `ISOLATED_WORKSPACE.json` identifies the extracted source scope. The performance receipt separately identifies each completed SQLite backup, its snapshot interval, integrity checks and hash. Multiple online database backups are individually coherent; they are not a single simultaneous snapshot of the entire bot.

The extracted source retains its original bytes, including historical absolute paths and operational entrypoints. It is an inspection/engineering copy, not a sandbox that intercepts arbitrary filesystem or broker access. Do not start its supervisor, collectors, account tools or original runner merely because the directory is labelled isolated. Bounded offline probes must select copied artifacts and inputs explicitly and must not instantiate database-writing stores or runtimes. Private credentials were not copied.

Before restoring a component, verify the relevant original manifest and source/copy hashes. Use a new empty destination and check that every target remains within it. Source ZIP members are relative to `trad`. The source manifest preserves uncommitted working-tree state; it is not a clean historical commit. An old report filename does not identify current runtime truth.

The D `legacy_intrahour_v5` source is stored separately because its compatibility with the retained unified v4 model is unresolved. MA-grid artifacts and second-ridge JSON are preserved with their retained reports. Keep original artifacts, matrices, source versions, registrations and forecast/outcome records unchanged when preparing new research versions.

The baseline did not back up all raw feeds, all 352 known databases or private credentials. Read each receipt's scope before making a restore claim. No live model activation, trading-setting change or improved prediction performance follows from successful restoration or deterministic inference.
