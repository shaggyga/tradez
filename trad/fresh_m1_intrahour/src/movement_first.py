from __future__ import annotations

from typing import Any

import numpy as np
import pandas as pd

from .features import assert_decision_time_feature_columns


try:
    from sklearn.ensemble import HistGradientBoostingClassifier, HistGradientBoostingRegressor
except Exception:  # pragma: no cover - recorded by the caller
    HistGradientBoostingClassifier = None
    HistGradientBoostingRegressor = None


def movement_target_column(horizon: int, margin_pips: float) -> str:
    margin_tag = str(float(margin_pips)).replace(".", "p")
    return f"target_move_exceeds_cost_plus_{margin_tag}_{int(horizon)}m"


def add_movement_targets(
    frame: pd.DataFrame,
    horizons: list[int],
    margins_pips: list[float],
) -> pd.DataFrame:
    out = frame.copy()
    cost = pd.to_numeric(out.get("round_trip_cost_pips_used", 0.0), errors="coerce").fillna(0.0)
    for horizon in horizons:
        future_range = pd.to_numeric(out.get(f"future_range_pips_{horizon}m"), errors="coerce")
        for margin in margins_pips:
            target = movement_target_column(horizon, margin)
            out[target] = np.where(
                future_range.notna(),
                (future_range > (cost + float(margin))).astype(float),
                np.nan,
            )
    return out


def neutral_movement_features(columns: list[str]) -> list[str]:
    neutral = [
        col for col in columns
        if not col.startswith("side_")
        and "xs_rank_side_" not in col
        and col != "xs_opportunity_score"
    ]
    assert_decision_time_feature_columns(neutral)
    return neutral


def _clean_x(frame: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    return frame[columns].replace([np.inf, -np.inf], np.nan).fillna(0.0)


def _movement_training_rows(frame: pd.DataFrame, target: str) -> pd.DataFrame:
    work = frame.replace([np.inf, -np.inf], np.nan).dropna(subset=[target]).copy()
    if "side" in work.columns and (work["side"] == "long").any():
        work = work[work["side"] == "long"].copy()
    keys = [c for c in ["decision_time_utc", "pair"] if c in work.columns]
    return work.drop_duplicates(keys) if keys else work


def fit_predict_movement_first(
    train: pd.DataFrame,
    validation: pd.DataFrame,
    final: pd.DataFrame,
    movement_features: list[str],
    direction_features: list[str],
    horizon: int,
    margin_pips: float,
    direction_model: str,
    seed: int,
) -> tuple[pd.Series, pd.Series, dict[str, Any], str | None]:
    if HistGradientBoostingClassifier is None or HistGradientBoostingRegressor is None:
        return pd.Series(dtype=float), pd.Series(dtype=float), {}, "sklearn histogram gradient boosting unavailable"

    assert_decision_time_feature_columns(movement_features)
    assert_decision_time_feature_columns(direction_features)
    move_target = movement_target_column(horizon, margin_pips)
    ev_target = f"actual_ev_{int(horizon)}m"
    required = [move_target, ev_target]
    if any(col not in train.columns for col in required):
        return pd.Series(dtype=float), pd.Series(dtype=float), {}, f"missing movement-first target: {required}"

    try:
        movement_train = _movement_training_rows(train, move_target)
        if len(movement_train) < 1000:
            return pd.Series(dtype=float), pd.Series(dtype=float), {}, "too few movement training rows"
        y_move = movement_train[move_target].astype(int)
        if y_move.nunique() < 2:
            return pd.Series(dtype=float), pd.Series(dtype=float), {}, "movement target is single-class"

        movement_model = HistGradientBoostingClassifier(
            learning_rate=0.06,
            max_iter=70,
            max_leaf_nodes=21,
            min_samples_leaf=90,
            l2_regularization=0.10,
            random_state=seed,
        )
        movement_model.fit(_clean_x(movement_train, movement_features), y_move)
        positive_index = list(movement_model.classes_).index(1)

        conditional = train.replace([np.inf, -np.inf], np.nan).dropna(subset=[move_target, ev_target]).copy()
        conditional = conditional[conditional[move_target] > 0.5]
        if len(conditional) < 1000:
            return pd.Series(dtype=float), pd.Series(dtype=float), {}, "too few movement-conditioned direction rows"
        y_ev = conditional[ev_target].astype(float)

        if direction_model == "conditional_ev_regressor":
            direction_estimator: Any = HistGradientBoostingRegressor(
                learning_rate=0.06,
                max_iter=80,
                max_leaf_nodes=21,
                min_samples_leaf=90,
                l2_regularization=0.15,
                random_state=seed,
            )
            direction_estimator.fit(_clean_x(conditional, direction_features), y_ev)

            def direction_score(part: pd.DataFrame) -> np.ndarray:
                return direction_estimator.predict(_clean_x(part, direction_features))

            direction_meta = {"conditional_positive_rate": float((y_ev > 0.0).mean())}
        elif direction_model == "conditional_positive_classifier":
            y_positive = (y_ev > 0.0).astype(int)
            if y_positive.nunique() < 2:
                return pd.Series(dtype=float), pd.Series(dtype=float), {}, "conditional direction target is single-class"
            direction_estimator = HistGradientBoostingClassifier(
                learning_rate=0.06,
                max_iter=70,
                max_leaf_nodes=21,
                min_samples_leaf=90,
                l2_regularization=0.10,
                random_state=seed,
            )
            direction_estimator.fit(_clean_x(conditional, direction_features), y_positive)
            direction_positive_index = list(direction_estimator.classes_).index(1)
            win_mean = float(y_ev[y_ev > 0.0].mean())
            loss_mean = float(y_ev[y_ev <= 0.0].mean())

            def direction_score(part: pd.DataFrame) -> np.ndarray:
                probability = direction_estimator.predict_proba(_clean_x(part, direction_features))[:, direction_positive_index]
                return (probability * win_mean) + ((1.0 - probability) * loss_mean)

            direction_meta = {
                "conditional_positive_rate": float(y_positive.mean()),
                "conditional_win_mean": win_mean,
                "conditional_loss_mean": loss_mean,
            }
        else:
            return pd.Series(dtype=float), pd.Series(dtype=float), {}, f"unknown direction model: {direction_model}"

        def score(part: pd.DataFrame) -> pd.Series:
            move_probability = movement_model.predict_proba(_clean_x(part, movement_features))[:, positive_index]
            conditional_ev = direction_score(part)
            return pd.Series(move_probability * conditional_ev, index=part.index, dtype=float)

        metadata = {
            "movement_target": move_target,
            "movement_margin_pips": float(margin_pips),
            "movement_train_rows": int(len(movement_train)),
            "movement_positive_rate": float(y_move.mean()),
            "conditional_direction_rows": int(len(conditional)),
            "movement_feature_count": int(len(movement_features)),
            "direction_feature_count": int(len(direction_features)),
            "direction_model": direction_model,
            **direction_meta,
        }
        return score(validation), score(final), metadata, None
    except Exception as exc:
        return pd.Series(dtype=float), pd.Series(dtype=float), {}, str(exc)
