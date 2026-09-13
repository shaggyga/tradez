# Retained paper-management verification

`verify_observed_episodes.py` performs a bounded, read-only check of the three predeclared paper episodes. It checks the registered 20-file source closure before and after reading, reconstructs each episode contract from its registered nominal start and retained initial curve, and verifies the original published curve and publication bytes. No broker request, fitting, database operation or runtime write occurs.

For every completed step it verifies original quote and optional momentum files, their recorded persistence/consumption clocks, the original curve adapter result, all five management decisions, plan publication, every retained execution observation, settlement, state and completion identities. Pure replay uses the explicitly retained original computation clocks. It does not relabel this later audit as the original computation or claim that an omitted external observation never occurred.

A second calculation independently derives GBP/USD integer units from the USD 2,500 scenario and decision ask, prices each long/short opening and closing leg with the fixed 0.1-bps slippage, and checks dollar P&L and liquidation marks. It does not call the manager's execution arithmetic. The completed curve-minus-momentum difference uses explicit decimal precision and is withheld when any earlier slot lacks either a verified completion or an explicit missed-slot record. An unresolved terminal position is not treated as flat.

Missing initialization, source errors, partial records, recorded missed cadence slots and future episodes remain separate dispositions. A claimed completed episode with missing initialization or unexplained missing slots is an audit issue. `status=passed` means the checked retained evidence reconciles; it is not a profitability, complete-operation or broker-execution claim. The observations are not globally atomic, and the report retains actual read clocks and hashes.

The final focused suite passed 17 cases. It includes end-to-end temporary immutable files through the real frozen curve adapter, manager and worker, long and short terminal accounting, tampered source/target/leg rejection, bounded directory inventory, missed-evidence withholding and low ambient decimal precision. Earlier 9-, 13- and 15-case receipts are preserved. Independent review found no remaining blocker in this scope.

The accepted-code live observation at **2026-09-09 06:51:25–27 UTC** verified seven completed steps and 111 retained files, with zero reconciliation issues. Episode 1 was still open and no matched terminal result was eligible. The curve manager and hold arm each had a hypothetical liquidation mark of −$0.14212469585 and an open position. The USD momentum arm was flat with −$0.4001468153 realized; the legacy selector was flat with −$1.14514242885 realized; no trade remained $0. These are partial paper results under declared cost assumptions, not completed evidence of a predictive or management advantage. Episodes 2 and 3 had not initialized, as expected from their later starts.

One-shot invocation from the configured Python environment:

```powershell
& 'C:\Users\zmoor\AppData\Local\CodexRuntimes\timeseries312\Scripts\python.exe' -B `
  'C:\Users\zmoor\Documents\forex\overnight_curve_buildout_20260909\observed_management\verify_observed_episodes.py' `
  --registry 'C:\Users\zmoor\Documents\forex\trad\config\observed_curve_management_v1_20260909.json' `
  --expected-sha256 60689101fb63a8bcce2465d19f627b9b2ce357d1ee271c65298f255663d991e1 `
  --output 'C:\Users\zmoor\Documents\forex\overnight_curve_buildout_20260909\observed_management\NEW_UNIQUE_REPORT_NAME.json'
```

Output must be a new file in an existing directory outside `trad`. The helper refuses overwriting. It does not schedule or start a watcher.
