# Pure curve management replay API

`trad/oanda_curve_management_replay_v1.py` exports `replay(frames, config)`. It performs no file, network, broker, account or database operations. The configuration and every frame are supplied explicitly. This remains an engineering implementation awaiting integration with authenticated curve publication receipts; synthetic fixture output is not predictive validation.

The five separate state chains are the unchanged legacy momentum selector within common deadline constraints, the USD momentum manager, the same USD manager with curve inputs, no trade, and curve entry followed by holding without rotation. Only the USD momentum/curve comparison isolates the changed forecast input. The legacy selector uses common new USD sizing/fill accounting and therefore does not reproduce the historical native-pip ledger economics.

Use the fixture configuration and frames in `SYNTHETIC_API_FIXTURE_FINAL_20260909.json` as a complete executable example. Every quoted price is an exact decimal string and explicitly tradeable. Missing or false tradeability is rejected for decision, fill and currency conversion. Its $1,000 notional is explicitly synthetic research configuration; it does not describe the user's balance or leverage.

## Configuration

Required: `schema_version=curve_management_replay_contract_v1_20260909`, the module's exact inert `SAFETY` fields, `account_currency=USD`, `maximum_open_positions=1`, and a metadata map of explicit base currency, quote currency, pip size and integer unit increment per instrument. Pip metadata is never inferred from symbol suffixes. Supply positive research notional, quote age limit (at most 60 seconds), cadence, execution delay (strictly less than cadence), maximum holding, maximum entry spread and minimum entry cost ratio; supply nonnegative slippage per leg and incremental USD switch hurdle. Supply the explicit legacy policy and all uniformly spaced decision epochs in advance (2–4,096 frames).

Conversion policy is `direct_or_inverse_executable_bid_ask_no_triangulation`. Sizing policy is `fixed_usd_notional_integer_base_units_at_decision`. Base units are floored using the current executable USD purchase cost of base currency. Realized quote-currency profits use executable sale conversion; losses use executable purchase conversion. Conversion quotes cannot be from the future. The module does not model financing, commissions beyond configured slippage/spread, broker fills or leverage.

## Frames and time

Each frame has `decision_epoch`, `execution_epoch`, `terminal`, `management_target_epoch`, `decision_quotes`, `execution_quotes`, `curve_candidates` and `momentum_candidates`. Execution is exactly the scheduled decision plus the configured delay. Traded-pair fill quotes must have exactly that market epoch; there is no nearest-quote fallback. Every nonterminal management target must belong to the predeclared execution grid. All arms enforce original native deadlines, including the legacy wrapper; entry at/past target and holding through it are rejected. Quote maps carry `instrument`, `quote_id`, exact-string `bid`/`ask`, `market_epoch`, and actual `available_epoch`.

All arms choose before any execution quote is used. `execution_observed_epoch` defaults to execution but must instead carry the actual observation when later. Nonterminal observation must be no later than the next decision, or the whole unsupported state chronology is refused. Terminal settlement can be observed later because no following action consumes the state. Archival retrieval is not evidence of old prospective portfolio knowledge.

Optional `feedback_epoch`, `feedback_observed_epoch` and `feedback_quotes` score fixed depth-one alternatives from exactly the same predecision state. Their future values never enter choice or subsequent primary state. They have zero additional independent repetition weight. Explicit `curve_refusals` and `momentum_refusals` remain refusals, never synthetic neutral predictions.

## Curve DTO consumed

Required: instrument, side (+1/-1, or exact zero for neutral incumbent valuation), curve ID/hash, node ID, model ID, cohort, original reference epoch, original issue epoch, actual available epoch, current decision epoch, original target epoch, explicit pip size, remaining seconds, expected terminal price, signed remaining price change, signed remaining pips and source bindings. Remaining price change equals expected terminal minus the actual decision midpoint. Target must equal the frame's declared native management target; no interpolation or implicit horizon ranking. Original probability and other source fields are retained but are never rebased or claimed calibrated for the remaining interval. Parent adapter also retains reference price, trained horizon and native label clocks in the source payload.

For S5 trained models, distinguish bar-start label from close-price availability: reference price clock is label start +5 seconds; target price clock is original label target +5 seconds. The replay consumes the native target *price* clock supplied by the authenticated adapter.

Momentum DTO: instrument, side, positive expected move pips, legacy score, legacy confidence, snapshot ID, original availability, decision epoch and original target. The same USD manager converts this input using explicit pip and quote metadata. Incumbent valuation uses its actual held side even if forecast direction reverses, independently of the new-entry hurdle. An incumbent estimate with a different deadline is explicitly incomparable; the manager exits rather than borrowing later forecast returns.

## Output and limits

Output retains all five trajectories, each action and fill, integer sizing/FX evidence, candidate refusals, exact quote and observation clocks, terminal unresolved positions, depth-one counterfactuals, config/input/report hashes, USD summaries and matched terminal deltas. `mechanics_complete` means all trajectories ended flat, not that the strategy passed validation. Execution rejection counts and unavailable valuations must also be inspected. Terminal comparisons are absent when either trajectory remains open.

The pure DTO boundary checks identities, fields and arithmetic; it does not authenticate external bytes or recreate fitted predictions. The integration caller must verify the source closure and curve issue/publication/consumption evidence. Shared currencies, overlapping horizons and multiple arms do not increase the independent sample count; the module reports that count as unknown. No model or policy is promoted by this module.


## Explicit trained-target window policy

Configuration must name `curve_target_window_policy`: `exact_only` or `nominal_management_boundary`. Each curve DTO must carry the same policy plus original `target_selection_policy` (kind and maximum delay), `target_is_exact` and `target_price_window_end_epoch`. Exact-only refuses nonzero fitted target windows. The recovered original S5 fit can choose the first complete bar at/after nominal within seven seconds; it must not be represented as exact-target training. A separately declared nominal-boundary experiment closes at nominal and visibly preserves that approximation in the output. It is not scoring the original fitted label endpoint.

A legitimate zero remaining curve estimate is retained for incumbent valuation (hold and exit tie under the current rule), but cannot open a new position. Missing evidence is still a refusal. Adapter decision-quote hashes, identities, exact prices and clocks are checked against the frame quote when present.

## Acceptance evidence

The focused final suite passed 106 cases. Earlier receipts are retained, including the first 62-pass/two-failed fixture expectation run, which was corrected without changing historical sources. The final synthetic fixture is hash-bound to current source and includes all three scheduled clocks and all five trajectories. There is no actual forward manager evaluation in this receipt; authenticated integration and a declared collection episode remain separate work.
