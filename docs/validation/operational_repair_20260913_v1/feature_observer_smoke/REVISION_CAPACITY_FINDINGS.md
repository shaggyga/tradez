# News revision capacity measurement — 2026-09-14 00:21 UTC

Read-only source and publisher inspection. No database mutation, admission, source-code change or broker call. `measure_revision_capacity.py` used the unchanged source-bound projection reader and validator over the current complete observed prefix, keeping at most one 16 MiB evidence page. Hash/length indices were retained, not the article bodies. The 2,753 projections included the initial committed high watermark of 2,748; another five arrived during the bounded scan.

## Measured capacity

| Representation | Bytes |
|---|---:|
| All validated canonical evidence | 276,712,920 |
| Independent zlib-6 compressed evidence envelopes | 27,476,581 |
| Unique exact evidence-role rows | 181,554,325 |
| Unique exact JSON-string blobs | 134,617,201 |
| Row descriptors for those string blobs | 10,749,678 |
| Uncompressed per-evidence role references | 2,128,069 |

Every measured compression round trip and individual row reconstruction was byte-exact. These are serialized byte sizes, **not** measured Python heap memory or complete database/index sizes. Independently compressing many small CAS nodes was less effective than compressing a whole evidence envelope; a bounded compressed-block/CAS hybrid is worth testing.

The live publisher contained seven published attempts through projection 1,172: 117,612,134 attempt-body bytes plus 111,162 profile/acknowledgment/publication bytes. Each attempt retained a different contiguous source prefix of roughly 16.7 MiB evidence; it does **not** duplicate all earlier source prefixes in each attempt. Existing reads nevertheless decode/revalidate all retained attempts. Another normal full-size attempt exceeds the fixed 128 MiB aggregate JSON bound. Raising only that limit does not fix downstream cumulative readback or heap growth.

## Redundancy and lexical evidence

- `classification_first` and `classification_available` were equal in all 2,753 evidence envelopes.
- Current/canonical-first source-version and source-observation rows were equal in 2,659 of 2,753 cases. The 94 exceptions must remain distinct.
- The largest repeated fields were projection row JSON (41.5 MB), projection payload JSON (35.6 MB), each classification derived payload (31.0 MB), and each content JSON copy (23.6 MB).
- The 68,855-byte policy repeated once per page is a relatively small part of total usage.
- A separate sample of the latest 128 rows in each table found all tested JSON strings exactly canonical-minified **except** projection payload JSON. All 128 projection payloads exactly reproduced using `json.dumps(parsed, sort_keys=True, ensure_ascii=True)` with default spaces; none reproduced with minified separators. Codec detection must verify exact original bytes and use a literal UTF-8 fallback. Field names alone cannot authorize normalization.

## Candidate lossless, bounded representation

Use a successor source/cohort with immutable typed references to exact evidence bytes or verified reversible JSON-string codecs. Preserve every projection sequence, original source/classification clock, envelope hash, read clock, admission/acknowledgment/publication chain and consumer observation. Retain literal bytes when a candidate codec cannot reproduce them exactly. A bounded block can compress related repeated content while its uncompressed hash and strict decompression/output limits prevent silent evidence changes or unbounded expansion.

Stream validation one bounded block at a time, retaining only the complete revision index and proof references required for later selection. Do not rehydrate the full historical universe for each pair or cycle. Public clocks remain the actual new admission/publication/consumption clocks; compact storage cannot backdate a newly admitted source row. Failed legacy attempt records remain immutable and cannot be relabeled as successful. Reconstructed entries must continue to pass the existing unchanged pure projection validator, and selected outputs must match an independent full replay fixture.

Detailed counts, field names, sizes and exact-hash statistics are in `revision_capacity_diagnostic.json`; the lexical sample is in `revision_json_reencodability_sample.json`. No article text is written to these outputs.
