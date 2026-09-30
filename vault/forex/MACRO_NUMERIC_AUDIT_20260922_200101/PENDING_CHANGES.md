# WP6 numeric units and vintage evidence audit

The full2696-version projection contains31 numeric/component-bearing versions
from18 sources:29 actual values,16 prior values,one revised prior,and nine release
components. There are no finite retained consensus values. All31 have blank
numeric verification labels; two lack extraction contract IDs.26 actual values
occur numerically in retained summaries, but summaries can be collector-generated:
occurrence is not independent publisher-field verification. Two BLS derived values
match rounded display text rather than their full underlying decimal value.

Nine unit labels omit the percent-change comparison period; two component-only
rows have no top-level unit/reference. Twenty references normalize to explicit
months,eight are explicit dates with unspecified economic frequency,and one
"June quarter" lacks a year and remains unresolved. No inferred year, seasonal
definition, original prior vintage or consensus was supplied. No numeric surprise
was computed. Current reported prior/revised-prior values remain current-snapshot
claims, not historical original vintages. Original numeric clock projections and
two exact parser source fragments are preserved; collector code was not executed.

29 local and29 relocated tests pass; five outputs reproduce exactly. Tests cover
strict numeric parsing, ambiguous locales, unit/reference gaps, exact occurrence
offsets, rounded displays, timezone requirements, preserved priors, source pins
and crash recovery. The initial run is preserved; r2 corrects only an overly generic
operator status description, with unchanged scientific payloads.

Same-implementer review accepted within scope; independent review unperformed.
No model fits, outcome data, paid/GPT calls, live DB or collector actions.


Checkpoint: checkpoint/forex_macro_numeric_audit.zip, SHA256 2f2b589574c019b2f96dc203b8e58afb42260e567af624258ab6d2e997e7bc63. Exact next: review_macro_numeric_audit_checkpoint_v2; then macro_numeric_observation_asof_state_v2. Prior checkpoints remain sealed in their original packages; approvals reference them without duplicating archives.
