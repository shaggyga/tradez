"""Shared immutable identifiers for the active FX-news classification contract.

Keep this tiny module free of collector imports so read-only integrity tools can
bind to the producer contract without loading network or parser dependencies.
"""

NEWS_CLASSIFICATION_VERSION = (
    "local_fx_news_rules_20260828_v151_pair_breakout_recap_boundary"
)
