# Original paper observations for scientific plotting

`VERIFIED_THREE_EPISODE_PLOT_DATA_20260909.json` contains all three predeclared GBP/USD episodes and all 178 verified steps, without selection by performance. Dataset SHA256: `ebc2ac9c1153afdb9d3b6ff9dcb463a21f035ab0e72d409c538dc6d71102d26c`. It derives from original verifier SHA256 `cdaac1862e6d2d2153e65d394ff5bffada25dd505b43d186197144cf1286e5ce` and rehashes 1,591 selected original files. New derivation reads ran September 9, 09:58:48.206–09:59:06.840 UTC; those clocks do not replace the original observation clocks.

Each `episodes[]` header holds the original reference label and price clocks, reference price, issue/publication/consumer times, model/curve/node identities, fixed expected terminal price, exact original target and its retained training window policy. The reference is the declared official-M S5 close. Quote midpoints below are exact `(bid + ask) / 2` from the actual stream snapshot; these are distinct price conventions and observations.

Each `rows[]` element represents one original completed paper step:

| Field | Meaning for plotting |
|---|---|
| `scheduled_epoch` | Original planned slot; not a substituted actual observation time. |
| `information_cutoff_epoch` | Actual information cutoff used by all arms. |
| `plan_created_epoch`, `plan_publication_completed_epoch` | Actual derived-plan and publication completion. |
| `decision_quote` | Quote available at the information cutoff, or `null` with original refusals. |
| `selected_quote` | Later independently observed quote selected for all virtual arms, or `null`. |
| Quote `market_epoch`, `available_epoch` | Original market tick time and actual receipt/read completion. Repeated ticks remain repeated observations. |
| Quote `bid`, `ask`, `midpoint` | Exact decimal text; midpoint uses 96-digit local arithmetic. No synthetic spread or price. |
| `settlement_computed_epoch` | Actual completed state derivation time. |
| `arms.*.before`, `arms.*.after` | Original recorded side, integer base units and realized virtual USD. Flat is side zero; missing quote is never price zero. |
| `arms.*.position_state_known_epoch` | When the new inventory/cash state became known. |
| `curve_candidate` | Original retained candidate side and signed remaining move, or `null`; no new forecast. |
| `source_files`, `*_quote_sources` | Exact plan, state, settlement, raw quote and receipt hashes linked to the original verifier. |

Use actual quote availability for a chart aligned with portfolio information, and state-known time for state/cash changes. Preserve market timestamps separately if showing quote age. Do not place a completed settlement state at an earlier quote market time. Prices between observations are unknown; use points, or explicitly label connecting segments as visual guides rather than observed paths. Preserve the 16 missing decision quotes and 19 missing candidates; none are filled. All 178 selected execution observations exist.

Draw the fixed terminal level as an endpoint-expectation annotation, not a predicted continuous path. The original reference and target remain fixed; current price crossing that level can mechanically change the retained candidate's signed remaining move. Original probability appears only in the header with its uncalibrated original-event scope; it is not a newly estimated remaining-move probability.

The two inventory series are the original curve manager and fixed curve hold arms. They are isolated USD2,500 research scenarios, not simultaneous account positions. No new direction guard, manager policy, P&L replay, interpolation, quote GET or broker fill is included. The final accepted cost attribution remains the authority for economic totals.
