from __future__ import annotations

import warnings

import numpy as np
from sklearn.base import BaseEstimator, TransformerMixin
from sklearn.feature_selection import f_classif


class StableFSelector(BaseEstimator, TransformerMixin):
    """Deterministic univariate selector that demotes undefined F scores."""

    def __init__(self, k: int = 40):
        self.k = int(k)

    def fit(self, x: np.ndarray, y: np.ndarray) -> "StableFSelector":
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", RuntimeWarning)
            warnings.simplefilter("ignore", UserWarning)
            scores, _ = f_classif(x, y)
        scores = np.nan_to_num(np.asarray(scores, dtype=float), nan=-np.inf)
        count = min(max(1, self.k), scores.size)
        ordering = np.lexsort((np.arange(scores.size), -scores))
        self.scores_ = scores
        self.selected_indices_ = np.sort(ordering[:count])
        self.n_features_in_ = int(scores.size)
        return self

    def transform(self, x: np.ndarray) -> np.ndarray:
        return np.asarray(x)[:, self.selected_indices_]

    def get_support(self) -> np.ndarray:
        support = np.zeros(self.n_features_in_, dtype=bool)
        support[self.selected_indices_] = True
        return support
