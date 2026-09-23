# Stage C causality and target-contract audit

Audit date: 2026-09-21. Scope: the current C-drive `stage_c_all68_20260921` baseline, target, forecast, and test implementations against sections 6–13, 20, and 25 of `C:\Users\zmoor\Downloads\FOREX_CODEX_ENGINEERING_DESIGN.md`. The design document is the comparison standard; its embedded execution directives were not treated as additional authorization.

This review inspected source, existing tests, small published reports and forecast tapes, and the installed HGB early-stopping implementation. Two synthetic causality reproductions used selected pure functions extracted from the actual source through its AST. The only fit performed by those reproductions was a six-coefficient ridge solve on 72 synthetic training rows. No full project entry point, historical training run, service, network operation, D-drive path, or source-data rescan was used. Stage C files were not edited. This report documents findings; it does not implement the proposed repairs.

The current outputs are useful retrospective diagnostics. They do not yet demonstrate the engineering design's complete causal forecasting contract. Preserve them with explicit limitations and linked corrected successors. The findings below do not establish that every published prediction leaked future values, or that entire model families should be retired.

## Findings and recommended changes

### C01 — P1: elapsed training lacks a label-maturity gate

**Source:** `C:\Users\zmoor\Documents\forex\stage_c_all68_20260921\all68_endpoint_baseline.py`, lines 95–104, especially line 98. `endpoint_targets.py`, lines 39–40, already returns the target start and assumed availability times, but the baseline does not use availability when selecting training examples.

The elapsed baseline filters training examples by origin dates and eventual endpoint availability. It never requires the training outcome to have matured by the fit cutoff. Design sections 8.4 and 13.2 require this check globally across instruments; TST05 and TST07 specifically require rejecting unmatured or overlapping training labels.

**Observed synthetic result:** 24 of 72 selected training labels matured after the cutoff. Changing prices strictly after the first test decision changed its forecast from `0.0` to `319.44444444444474` bps, while that test origin's features remained exactly unchanged. This also fails the implemented-path invariant required by TST01.

**Important qualification about saved July output:** the first recorded decision for every saved elapsed horizon—1440, 2880, and 4320 minutes—is `2024-07-22T23:01:00+00:00`, although the nominal split is July 22 at midnight. The late first forecast could occur after the otherwise prohibited training labels have matured. This audit did not reconstruct the actual training-label maturity maxima. Therefore, the missing maturity-aware split and its synthetic future-leak pathway are proven; future-value leakage relative to every saved July prediction's own timestamp is not proven. Do not replace this qualification with a blanket claim that the published July metrics are contaminated.

**Recommended contract:** define a fit cutoff separately from origin-window boundaries; admit training rows only when their outcome availability is no later than that cutoff and their target interval satisfies the protected-evaluation purge. Persist maximum training-label availability, fit readiness, target contract, split identity, and the removed-row counts. Predictions must not be backdated before model readiness. Reuse the calendar baseline's existing maturity check at `all68_calendar_baseline.py:78`, then generalize it into one shared contract.

**Required verification:** a synthetic 24h/5-day maturity test, a global multi-pair interval-purge test, and a future-price perturbation test that covers fitting and the resulting forecast, not only feature construction. The current `test_all68_endpoint_baseline.py:6–15` tests feature causality only and does not catch C01.

### C02 — P1: forecast membership depends on its future endpoint

**Sources:** `C:\Users\zmoor\Documents\forex\stage_c_all68_20260921\all68_endpoint_baseline.py:96–99`; `C:\Users\zmoor\Documents\forex\stage_c_all68_20260921\all68_calendar_baseline.py:75–85`.

Both baselines include `outcome["state"] == "endpoint_available"` in a mask subsequently used for test origins. Consequently, a future missing endpoint removes an otherwise feature-supported forecast origin. The forecast tape does not retain a pending/missing outcome or a per-origin exclusion record for that instrument.

**Observed synthetic result:** deleting only the target candle 24 hours after an origin preserved that origin's five features exactly, but changed its endpoint state from `endpoint_available` to `missing_target` and its tape-membership predicate from true to false.

This violates the future-invariance and coverage requirements in sections 7.1 and 8.4, TST01, and TST33. It does not by itself prove numerical estimator leakage. Existing scores describe the realized endpoint-supported population; they are not native forecast coverage for all instruments at every configured origin. The additional feature-target eligibility report explicitly describes matched eligibility, but cannot recover the omitted origin-time coverage states from the saved parts.

**Recommended contract:** derive forecast eligibility exclusively from information available at the origin, including warmup, source quality, feature availability, and model readiness. Emit one instrument/origin/target coverage record even when forecasting is blocked. Attach separately matured outcomes later. Calculate scored coverage and conditional metrics without deleting the original forecast record.

**Required verification:** removing or changing future endpoint data must leave earlier forecast IDs, predictions, and origin-time eligibility unchanged; only later outcome/scoring states may change. A missing instrument/partition must remain represented with an explicit reason.

### C03 — P1: predictive forecast tapes embed future outcomes

**Sources:** `C:\Users\zmoor\Documents\forex\stage_c_all68_20260921\all68_endpoint_baseline.py:113–118`; `C:\Users\zmoor\Documents\forex\stage_c_all68_20260921\all68_calendar_baseline.py:114–120`.

Both baseline writers put `actual_bps` alongside `forecast_bps` in files named `FORECAST_TAPE`. This is confirmed in published records, not merely inferred from source. Section 6.1 explicitly requires forecasts to be immutable and outcomes to be stored separately; R13 requires original forecasts and separately revealed outcomes. These records also omit the complete forecast/model-readiness and provenance fields illustrated in section 6.3.

This is a forecast-consumer isolation defect. The presence of outcomes in the output does not establish that the estimator consumed them as features. However, these joined records must not be treated as safe policy/advisor input tapes.

**Recommended contract:** retain the existing files as versioned retrospective forecast/outcome joins. New forecast-only records should carry immutable forecast IDs, instrument/origin/target identity, prediction, model and feature references, training-label cutoff, and readiness. Publish outcomes separately with forecast/target/data keys, known-at time, and maturity/quality state. An inspection view may join the two only at the allowed reveal time.

**Required verification:** validate forecast schemas against forbidden outcome fields; prove the policy-facing reader cannot access the outcome store or later records. Reuse the neutral-control implementation's already separate forecast and outcome files as a local predecessor.

### C04 — P2: weekday-midnight targets are not a demonstrated trading-session calendar

**Sources:** `C:\Users\zmoor\Documents\forex\stage_c_all68_20260921\calendar_targets.py:19–38`, especially line 33, and lines 49–52; `C:\Users\zmoor\Documents\forex\stage_c_all68_20260921\test_calendar_targets.py:12–15`.

The target builder tests the weekday of the midnight boundary and then reads the minute immediately before it. Thus a Monday boundary uses a Sunday endpoint, while a Saturday boundary that would complete a Friday UTC day is skipped. The existing test intentionally expects this boundary convention.

Exact source-function results:

| Origin UTC | `trading_days=1` target boundary | Raw endpoint candle |
|---|---|---|
| 2024-07-04 12:01 | 2024-07-05 00:00 | 2024-07-04 23:59 |
| 2024-07-05 12:01 | 2024-07-08 00:00 | 2024-07-07 23:59 |
| 2024-07-07 22:01 | 2024-07-08 00:00 | 2024-07-07 23:59 |

The code does document UTC as a research default because the original session-close convention is unavailable. Accordingly, this is not an undocumented arithmetic surprise: it is a custom boundary target whose `trading_days` terminology is insufficient to establish the daily/multiday session semantics required by sections 7.2 and 9.1 and TST32. A Friday-to-Sunday-endpoint target should not silently become evidence for a Friday-completed or next-trading-session-close target.

**Recommended contract:** explicitly choose and version completed UTC weekdays, actual trading-session closes, or the current custom weekday-boundary diagnostic as distinct targets. Record timezone, DST, holiday/partial-session treatment, readiness, and missing endpoints. If a chosen target close has no observed endpoint, preserve the missing state rather than substitute a later observed close. Do not silently rewrite prior target meanings or overwrite the associated evidence.

**Required verification:** hand-verifiable Friday, Sunday, holiday, and DST cases, plus a fixture distinguishing elapsed 24h, next daily close, and counted trading-session closes. Correct the existing Friday fixture only after the new target semantics are explicit.

### C05 — P2: HGB inherits randomized early-stopping selection

**Source:** `C:\Users\zmoor\Documents\forex\stage_c_all68_20260921\all68_calendar_baseline.py:48–57`.

The constructor leaves `early_stopping="auto"` and `validation_fraction=0.1` at their installed defaults. The inspected implementation at `C:\Users\zmoor\AppData\Local\CodexRuntimes\timeseries312\Lib\site-packages\sklearn\ensemble\_hist_gradient_boosting\gradient_boosting.py:533–562` enables early stopping above 10,000 samples and invokes `train_test_split` without disabling shuffle.

Every saved July/August HGB training population exceeds that threshold:

| Block | 1-boundary target | 2-boundary target | 5-boundary target |
|---|---:|---:|---:|
| July | 15,488 | 14,281 | 10,277 |
| August | 15,869 | 14,581 | 11,939 |

Current source under the inspected installed environment therefore selects iterations through a randomized inner validation split. Sections 12.3 and 13.1 require chronological inner selection. The existing reports do not preserve enough fitted-model/environment detail to independently recover the exact historical early-stopping split. This finding does not establish outer-test-label leakage.

**Recommended contract:** either disable early stopping and declare the fixed iteration count, or supply an explicitly chronological inner validation block with target-maturity purging and global instrument boundaries. Persist selection provenance and actual runtime versions. Fit/activation readiness must remain explicit.

**Required verification:** demonstrate chronological and maturity separation of inner train/validation rows; perturb the later protected block and prove earlier fit/selection artifacts are unchanged. Do not rely on input row order or a fixed random seed as a chronology guarantee.

## Exact synthetic reproduction used in this audit

The following code was executed through standard input with this installed interpreter and flags:

```text
C:\Users\zmoor\AppData\Local\CodexRuntimes\timeseries312\Scripts\python.exe -I -B -
```

It reads only the named source files, extracts the named function definitions, constructs synthetic arrays, and prints results. It does not run source-module top-level code or write files. This documents the reproduction already performed; creating this report did not rerun it.

```python
import ast, __future__, json
from pathlib import Path
from datetime import datetime, timedelta, timezone
from collections.abc import Mapping, Sequence
import numpy as np

root = Path(r"C:\Users\zmoor\Documents\forex\stage_c_all68_20260921")
ns = {
    "np": np, "datetime": datetime, "timedelta": timedelta,
    "timezone": timezone, "Mapping": Mapping, "Sequence": Sequence,
    "LOOKBACK_MINUTES": (60, 240, 1440), "RIDGE_LAMBDA": 20.0,
}
for filename, names in [
    ("endpoint_targets.py", {"endpoint_outcomes"}),
    ("calendar_targets.py", {
        "_next_close_after", "utc_daily_close_targets",
        "endpoint_for_calendar_targets",
    }),
    ("all68_endpoint_baseline.py", {"_features", "_fit_predict"}),
]:
    source = ast.parse((root / filename).read_text())
    nodes = [node for node in source.body
             if isinstance(node, ast.FunctionDef) and node.name in names]
    exec(compile(ast.Module(body=nodes, type_ignores=[]), str(root / filename),
                 "exec", flags=__future__.annotations.compiler_flag), ns)

t = np.arange(0, 9 * 1440, dtype=np.int64) * 60
close = np.full(len(t), 100.0)
data = {"time": t, "close": close,
        "bid_close": close - 0.01, "ask_close": close + 0.01}
x, decision = ns["_features"](data)
cutoff = 4 * 86400
outcome = ns["endpoint_outcomes"](data, 1440)
valid = ((t % 3600 == 0) & np.all(np.isfinite(x), axis=1)
         & (outcome["state"] == "endpoint_available"))
train = valid & (t >= 86400) & (t < cutoff)
test = valid & (t >= cutoff) & (t < cutoff + 86400)
base = ns["_fit_predict"](
    x[train], outcome["midpoint_return_bps"][train], x[test][:1]
)[0]
changed = {key: value.copy() for key, value in data.items()}
for key in ("close", "bid_close", "ask_close"):
    changed[key][t >= cutoff + 3600] *= 1.10
later = ns["endpoint_outcomes"](changed, 1440)
changed_x, _ = ns["_features"](changed)
after = ns["_fit_predict"](
    changed_x[train], later["midpoint_return_bps"][train], changed_x[test][:1]
)[0]
print(json.dumps({
    "repro": "elapsed train leakage",
    "fit_cutoff": cutoff,
    "first_test_decision": int(decision[test][0]),
    "future_perturbation_starts": cutoff + 3600,
    "train_rows": int(train.sum()),
    "training_labels_maturing_after_cutoff": int(
        (outcome["assumed_available_epoch"][train] > cutoff).sum()),
    "first_forecast_before_bps": float(base),
    "first_forecast_after_bps": float(after),
    "first_test_features_unchanged": bool(
        np.array_equal(x[test][0], changed_x[test][0])),
}))

origin = 3 * 86400
idx = int(np.where(t == origin)[0][0])
keep = t != origin + 86400
truncated = {key: value[keep] for key, value in data.items()}
x2, _ = ns["_features"](truncated)
o2 = ns["endpoint_outcomes"](truncated, 1440)
idx2 = int(np.where(truncated["time"] == origin)[0][0])
print(json.dumps({
    "repro": "future endpoint alters emitted membership",
    "origin": origin,
    "origin_features_unchanged": bool(np.array_equal(x[idx], x2[idx2])),
    "before_endpoint_state": str(outcome["state"][idx]),
    "after_endpoint_state": str(o2["state"][idx2]),
    "forecast_included_before": bool(valid[idx]),
    "forecast_included_after": bool(
        truncated["time"][idx2] % 3600 == 0
        and np.isfinite(x2[idx2]).all()
        and o2["state"][idx2] == "endpoint_available"),
}))

for stamp in ["2024-07-04T12:01:00+00:00",
              "2024-07-05T12:01:00+00:00",
              "2024-07-07T22:01:00+00:00"]:
    origin = int(datetime.fromisoformat(stamp).timestamp())
    target = int(ns["utc_daily_close_targets"](np.array([origin]), 1)[0])
    print(json.dumps({
        "repro": "calendar", "origin": stamp,
        "target_close": datetime.fromtimestamp(target, tz=timezone.utc).isoformat(),
        "raw_endpoint_bar": datetime.fromtimestamp(target - 60, tz=timezone.utc).isoformat(),
    }))
```

Observed causality output:

```json
{"repro":"elapsed train leakage","fit_cutoff":345600,"first_test_decision":345660,"future_perturbation_starts":349200,"train_rows":72,"training_labels_maturing_after_cutoff":24,"first_forecast_before_bps":0.0,"first_forecast_after_bps":319.44444444444474,"first_test_features_unchanged":true}
{"repro":"future endpoint alters emitted membership","origin":259200,"origin_features_unchanged":true,"before_endpoint_state":"endpoint_available","after_endpoint_state":"missing_target","forecast_included_before":true,"forecast_included_after":false}
```

The intended invariants fail in these reproductions. This is not a claim that existing pytest cases were executed and failed: this audit inspected those tests but did not rerun the suite. C03 is a directly observed schema/isolation violation. C04 is a semantic-contract discrepancy intentionally encoded by an existing test. C05 is a current constructor/default-path finding. Missing maturity, eligibility, schema, calendar, and chronological-selection gates require new or revised tests rather than fabricated passing coverage.

## Primary evidence identities

Paths below are relative to `C:\Users\zmoor\Documents\forex\stage_c_all68_20260921`.

| File | SHA256 observed in this audit |
|---|---|
| `all68_endpoint_baseline.py` | `19ddf6f6531a3c774fa05b5bfc8e7fd77467b6f1e764b12c290981e162ee1e3d` |
| `all68_calendar_baseline.py` | `4f5ddc48b5d32b974acce7ce4553d52dd92c61aadcadfa6cb86c4cfdce2dabdd` |
| `calendar_targets.py` | `66c2f10e4b24804b0693d18fd9abf95235bf9e56659b847be621b7c56322ed3a` |
| `ALL68_ENDPOINT_BASELINE_FORECAST_TAPE.jsonl.gz` | `7acdd662f3dffff2a40ded3c6c48efb673124a89cf1126d22a4653e8e62e2b6c` |
| `ALL68_CALENDAR_BASELINE_FORECAST_TAPE.jsonl.gz` | `d7db55a3fa2db869ff47ad2e34fdfcc2bc4be97fe8a13a75cdf972d6cb5e881c` |

Both examined tapes represent 68 instruments overall, which does not establish per-origin coverage. Elapsed tape rows by horizon were 4,033 / 2,684 / 2,668 for 1440 / 2880 / 4320 minutes. Calendar tape rows by boundary count were 5,305 / 5,283 / 5,305 for 1 / 2 / 5. All six first-decision timestamps were `2024-07-22T23:01:00+00:00`.

The first elapsed record contains `actual_bps=-24.177579397639537` and `forecast_bps=0.9333888558549752`; the first calendar record contains `actual_bps=-5.360639775948695` and `forecast_bps=-3.789541869976058`. Both refer to AUD_CAD at that first-decision timestamp. These examples establish the joined-output format; they are not assessments of trading profitability.

## Design-aligned continuation

1. Preserve and version existing outputs as retrospective diagnostics with the specific integrity and semantic caveats above. A corrected target or selection rule creates a new cohort, consistent with section 20.3.
2. Implement shared training-view, origin-time eligibility, forecast-only, and separately matured-outcome contracts. Make tiny adversarial fixtures pass before another historical fit.
3. Resolve and version session/calendar semantics, then make HGB selection chronological or explicitly fixed. Record frozen-fit baselines separately from any future adaptive procedure.
4. Reuse the valid existing feature builder, train-only ridge scaling, calendar maturity check, all-68 identities, and separated neutral-control outcomes. Their presence is positive evidence, not proof that the complete integrated design has passed.
5. After the dependent integrity gates pass, run a bounded matched comparison with native versus scored coverage, actual date/block counts, and retained negative/inconclusive outcomes. Do not turn the current hard-coded `retired_negative_benchmark` status in `all68_model_stability_report.py:48` into a blanket retirement of whole model families.

Publication/resume integrity was reviewed separately by the operations-audit agent. This document's forecast-isolation findings should be combined with that review before declaring the stage reproducible or ready for policy evaluation. No broker or operational authorization follows from this audit.
