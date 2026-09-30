# Immutable forecast tape checkpoint

Source Git: 4e1f42819bb4d8b255b863d003c649ba5f5da915.

The tape contains 56,476 authenticated preserved forecast records: 51,744 from the
currency-projection parent, 3,786 from the residual layer, and 946 from the curve-shape
layer. It covers 68 instruments and 16 origins. Each tape entry retains its source,
forecast ID, model ID, record/target identity, prediction, origin and availability epoch.
Outcomes are intentionally excluded from the tape.

TAPE.json SHA-256: cbb7c5bfd56c67390482e4363ea6226afa89e09b62f4b7aea2bc0c77e30466c6.
The tape identity is 2f1710453ec03550462866ddd59ed11b3c1a72572714c7249c7bde4ffd8a5063.
Two focused identity/availability tests passed. No model fit, API, external service,
broker, policy or live-bot action occurred. This is an engineering data boundary, not a
forecast-quality, policy, confirmation, or trading claim.
