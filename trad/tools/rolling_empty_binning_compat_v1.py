"""Scoped sklearn 1.9.0 workaround for continuous columns with no observations.

This module does not alter sklearn files or fill missing feature values. It only
returns zero thresholds when the installed threshold finder would receive zero
effective values after its documented NaN and zero-weight filtering. Every
nonempty column delegates to the unmodified installed function. Apply only around
new family fits; saved sklearn estimators predict without this context manager.
"""
from __future__ import annotations

from contextlib import contextmanager
import hashlib
import inspect
from pathlib import Path
import threading

import numpy as np
import sklearn
from sklearn.ensemble._hist_gradient_boosting import binning

SCHEMA = "rolling_hgb_empty_binning_compat_v1_20260915"
EXPECTED_SKLEARN_VERSION = "1.9.0"
EXPECTED_BINNING_MODULE_SHA256 = "ea9c9247a2a1989d05547d54d6f53d71cf108dc22e4111464aec037cfdab07a3"
EXPECTED_THRESHOLD_FUNCTION_SHA256 = "ab2729a3fe8c8742e42cc69268f51f6289202fe845f486d4e8d0e0a606c3fa51"
_SCOPE_LOCK = threading.RLock()
_COUNT_LOCK = threading.Lock()
_depth = 0
_installed = None
_active_receipt = None


def compatibility_source_record():
    """Return and validate the exact installed implementation being repaired."""
    module_path = Path(inspect.getfile(binning))
    function = binning._find_binning_thresholds
    record = {
        "schema": SCHEMA,
        "sklearn_version": sklearn.__version__,
        "numpy_version": np.__version__,
        "binning_module_path": str(module_path),
        "binning_module_sha256": hashlib.sha256(module_path.read_bytes()).hexdigest(),
        "threshold_function_sha256": hashlib.sha256(inspect.getsource(function).encode()).hexdigest(),
        "helper_path": str(Path(__file__).resolve()),
        "helper_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
    }
    if record["sklearn_version"] != EXPECTED_SKLEARN_VERSION:
        raise RuntimeError("empty-binning compatibility is limited to the reviewed sklearn version")
    if record["binning_module_sha256"] != EXPECTED_BINNING_MODULE_SHA256:
        raise RuntimeError("installed binning module differs from the reviewed source")
    if record["threshold_function_sha256"] != EXPECTED_THRESHOLD_FUNCTION_SHA256:
        raise RuntimeError("installed threshold finder differs from the reviewed source")
    return record


@contextmanager
def empty_binning_compat():
    """Temporarily repair zero-effective-value thresholds, restoring on exit.

    The yielded JSON-safe receipt reports how many empty columns were repaired.
    Nested use shares the outer receipt. Independent callers using this helper
    serialize their scopes; sklearn's own per-column worker threads are allowed.
    No global patch is applied merely by importing this module.
    """
    global _depth, _installed, _active_receipt
    with _SCOPE_LOCK:
        if _depth:
            if binning._find_binning_thresholds is not _installed:
                raise RuntimeError("threshold finder changed inside compatibility scope")
            _depth += 1
            try:
                yield _active_receipt
            finally:
                _depth -= 1
            return
        receipt = compatibility_source_record()
        receipt.update(empty_effective_columns_handled=0, delegated_nonempty_or_unsupported_columns=0,
                       restored_original_function=False,
                       feature_values_imputed=False, installed_package_files_modified=False)
        original = binning._find_binning_thresholds

        def repaired(col_data, max_bins, sample_weight=None):
            supported = isinstance(col_data, np.ndarray) and col_data.ndim == 1
            if sample_weight is not None:
                supported = supported and isinstance(sample_weight, np.ndarray) and sample_weight.shape == col_data.shape
            if supported:
                effective = ~np.isnan(col_data)
                if sample_weight is not None:
                    effective &= sample_weight != 0
                if not np.any(effective):
                    with _COUNT_LOCK:
                        receipt["empty_effective_columns_handled"] += 1
                    return np.empty(0, dtype=binning.X_DTYPE)
            with _COUNT_LOCK:
                receipt["delegated_nonempty_or_unsupported_columns"] += 1
            return original(col_data, max_bins, sample_weight=sample_weight)

        _installed, _active_receipt, _depth = repaired, receipt, 1
        binning._find_binning_thresholds = repaired
        try:
            yield receipt
        finally:
            changed = binning._find_binning_thresholds is not repaired
            if not changed:
                binning._find_binning_thresholds = original
                receipt["restored_original_function"] = True
            _installed, _active_receipt, _depth = None, None, 0
            if changed:
                raise RuntimeError("threshold finder changed externally; refusing to overwrite it")
