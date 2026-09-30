# Operational repair checkpoint — September 16, 2026

Read the [operational repair and remaining limits](source/docs/FOREX_OPERATIONAL_REPAIR_20260916.md) and [pending queue](source/FOREX_PENDING_IMPROVEMENTS.md).

This is a scoped source and evidence snapshot, not a complete runtime backup. Original databases, credentials, news object archives and local file identities remain external dependencies. Any private_external_sources in MANIFEST.json are hash-bound local files deliberately omitted without redaction (four approved private legacy dependencies in the production closure); they must be retained securely for recreation. Restore inactive; do not start duplicate writers. The package changes no trading authority or trial cutoff.
