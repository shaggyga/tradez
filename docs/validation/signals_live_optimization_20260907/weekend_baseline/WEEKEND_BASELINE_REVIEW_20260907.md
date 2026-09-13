# Weekend reopening baseline — offline research

The new `trad/oanda_weekend_reopening_baseline.py` emits deterministic EURUSD gap-fade, gap-continuation and no-trade hypotheses from two supplied price records. It needs no fitted model, seven-pair window or 335-bar warm-up. All authority flags are false and it has no runtime, database, network or file operations.

The fixed v1 research choices are a 3,600-second horizon from the original opening market timestamp, initial 300-second decision window from the supplied session reopen, 60-second maximum quote age, Friday anchor within 300 seconds before the supplied close, opening spread at most two pips, and absolute gap strictly larger than two opening spreads. These are untuned hypothesis definitions, not validated execution limits. No probabilities or expected magnitudes are invented.

Usage with genuine captured data:

```python
from oanda_weekend_reopening_baseline import forecast_weekend_reopening, to_jsonable

result = forecast_weekend_reopening(
    friday_anchor, opening_quote,
    session=verified_calendar,
    decision_epoch=actual_current_epoch,
)
json_payload = to_jsonable(result)
```

`verified_calendar` supplies `instrument='EUR_USD'`, a session ID, actual Friday-close/reopen epochs, and calendar source ID/hash plus the actual time that calendar was observed, before reopening. The calendar rule currently supports UTC Friday close to UTC Sunday/Monday reopen, with an explicit 24–96-hour closure; it does not infer broker trading hours or daylight-saving changes.

Each price record supplies `instrument`, exact decimal `bid`/`ask`, `market_epoch`, actual `observed_epoch`, and source ID/SHA256. Price floats are rejected. Both records must actually have been observed strictly before the decision; timestamps must never be reconstructed from bar close or file modification time.

The Friday record explicitly asserts it is the final pre-close anchor. A quote anchor requires `anchor_kind='quote'`, `tradeable=True`, and `retained_last_known=False`. A completed M1 anchor requires `anchor_kind='completed_m1'`, `complete=True`, minute-aligned `bar_start_epoch`, and `market_epoch=bar_start_epoch+60`. Both require `is_final_preclose_anchor=True`.

The opening record requires `is_first_observed_tradeable_quote=True`, `tradeable=True`, and `retained_last_known=False`. This identifies the first quote actually captured in the predeclared initial window, not necessarily the broker's first tick. Unknown opening provenance abstains. A current Monday quote cannot replace an elapsed Sunday reference.

Future integration must separately register the hypothesis, verify calendar/final-anchor provenance and source hashes, freeze actual availability and publication receipts, enforce one event per weekend using `event_id`, and obtain an independent executable entry quote after publication. Outcomes must retain the original target and missingness. The pure helper does not attest those external steps or deduplicate invocations itself. A separate scoring layer can reuse the exact Decimal bid/ask scorer; its required probability placeholder must not become a claimed probabilistic forecast or Brier result.

Validation: 87 synthetic tests passed. They cover strict availability boundaries, original target preservation, unknown/late opening, stale retained quotes, exact spread/gap boundaries, malformed/partial anchors, event identity, caller Decimal context, immutable parameters, no-trade exposure, and executable price scoring. The guarded harness blocks network, worker creation and writes outside this evidence directory.

No historical real-data accuracy or prospective performance was established. No registered source, runtime process, live database or activation was changed.
