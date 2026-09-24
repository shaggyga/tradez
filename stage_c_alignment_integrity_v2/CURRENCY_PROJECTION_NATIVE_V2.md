# Currency projection native qualification

This package recreates saved direct base forecasts at each selected origin, applies
the fixed currency-factor projection, and prepares the existing native curve consumer
at a new measured `origin + 2` clock. The previous diagnostic timestamps are retained
as provenance and are never reused as native issuance times.

The package keeps direct, full projection and half-residual variants for both base
learners. It does not fit a base model or learned layer, replay policy, select a
winner, claim confirmation, or authorize execution.
