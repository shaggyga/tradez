"""Research-only two-pending OCO and account-space replay tools.

The package deliberately has no dependency on a live OANDA manager.  Callers
must provide decision-time signals, executable bid/ask bars, and explicit
account-currency economics.
"""

from .oco import evaluate_oco_candidates
from .portfolio import ReplayResult, no_trade_baseline, replay_account
from .dataset import (
    build_forward_labels,
    iter_normalized_bars,
    load_normalized_bars,
    normalize_cached_bars,
    normalize_instrument,
    pip_size,
    validate_utc_grid,
)
from .events import assign_event_clusters
from .schemas import (
    AccountConfig,
    BufferConfig,
    EconomicsProvider,
    ExitConfig,
    ObjectiveConfig,
    OCOConfig,
    TradeEconomics,
)
from .search import (
    SearchResult,
    objective_for_summary,
    search_account_space,
    search_joint_space,
    stable_fold_objective,
)
from .splits import NestedPurgedFold, PurgedFold, make_nested_purged_splits
from .thresholds import (
    TrainOnlyThresholdModel,
    apply_train_only_thresholds,
    fit_train_only_thresholds,
)
from .assumptions import attach_timestamped_economics, build_project_metadata
from .economics import (
    InstrumentMeta,
    MarketQuote,
    margin_closeout_percent,
    resolve_conversion,
    size_position,
)
from .gate import (
    MovementGateModel,
    audit_vault_columns,
    fit_movement_classifier,
    reject_hindsight_feature_names,
    score_movement_classifier,
)
from .pipeline import (
    ResearchPaths,
    load_config,
    load_prepared_dataset,
    prepare_research_dataset,
    run_nested_research,
)
from .continuation import (
    ContinuationModel,
    add_continuation_labels,
    confidence_sweep,
    continuation_metrics,
    fit_continuation_model,
    run_continuation_research,
    score_continuation_model,
)

__all__ = [
    "AccountConfig",
    "ContinuationModel",
    "InstrumentMeta",
    "MarketQuote",
    "MovementGateModel",
    "ResearchPaths",
    "BufferConfig",
    "EconomicsProvider",
    "ExitConfig",
    "NestedPurgedFold",
    "ObjectiveConfig",
    "OCOConfig",
    "PurgedFold",
    "ReplayResult",
    "SearchResult",
    "TradeEconomics",
    "TrainOnlyThresholdModel",
    "apply_train_only_thresholds",
    "attach_timestamped_economics",
    "audit_vault_columns",
    "add_continuation_labels",
    "assign_event_clusters",
    "confidence_sweep",
    "build_forward_labels",
    "build_project_metadata",
    "evaluate_oco_candidates",
    "fit_train_only_thresholds",
    "fit_movement_classifier",
    "fit_continuation_model",
    "iter_normalized_bars",
    "load_normalized_bars",
    "load_config",
    "load_prepared_dataset",
    "margin_closeout_percent",
    "make_nested_purged_splits",
    "no_trade_baseline",
    "normalize_cached_bars",
    "normalize_instrument",
    "objective_for_summary",
    "pip_size",
    "prepare_research_dataset",
    "replay_account",
    "reject_hindsight_feature_names",
    "resolve_conversion",
    "run_nested_research",
    "run_continuation_research",
    "score_continuation_model",
    "score_movement_classifier",
    "search_account_space",
    "search_joint_space",
    "stable_fold_objective",
    "size_position",
    "validate_utc_grid",
]
