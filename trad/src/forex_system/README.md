# Forex System Package Boundary

This tree is the destination architecture for the live Forex project. The
current root-level scripts remain compatibility entrypoints until each one has
an owner, equivalence tests, and a supervisor-safe migration.

The domains are deliberately one-way:

```text
ingestion -> contracts -> features -> research -> evidence
                                           |          |
                                           v          v
                                      monitoring  governance -> authorization -> execution
```

Rules:

- Ingestion records point-in-time facts and revisions; it cannot rewrite past
  knowledge without an explicit revision record.
- Research proposes forecasts but cannot change lifecycle state.
- Evidence matures outcomes but cannot authorize an order.
- Governance independently verifies and assigns lifecycle state.
- Execution accepts only an exact, fresh Practice-007 authorization and cannot
  reinterpret evidence.
- Monitoring is read-only.

The controlling layout is `config/project_layout_v1.json`. Do not bulk-move
root scripts. Migrate one domain at a time behind stable compatibility imports.
