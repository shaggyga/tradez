"""Build a durable 2-hour FX movement trading-logic sheet.

The sheet is research-only.  It combines the existing exact 120-minute labels
with causal technical features, event-cluster context, and macro/sentiment
hypotheses.  Macro explanations are separated into verified and heuristic
fields so later manual/news enrichment can improve the same artifact instead
of replacing it.
"""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[1]
TRAD_ROOT = PROJECT_ROOT / "trad"
DEFAULT_SPIKE_ROOT = TRAD_ROOT / "data" / "spike_account_space"
DEFAULT_OUTPUT_ROOT = DEFAULT_SPIKE_ROOT / "trading_logic"

VERIFIED_MACRO_EVENTS = [
    {
        "event_id": "us_cpi_yen_jump_intervention_speculation_2024_07_11",
        "event_timestamp": pd.Timestamp("2024-07-11T12:30:00Z"),
        "tolerance_minutes": 360,
        "event_name": "Cool US CPI, yen jump, and intervention speculation",
        "macro_category": "inflation_release",
        "sentiment_reason": (
            "Cool US CPI triggered a sharp yen rally and speculation that Japanese authorities "
            "had intervened after USD/JPY sold off."
        ),
        "source_url": "https://www.wsj.com/livecoverage/cpi-report-today-inflation-stock-market-07-11-2024/card/japanese-yen-jumps-after-cool-u-s-inflation-data-7vZxOW9ax6BT8wVMc3Rl",
        "source_note": "WSJ live coverage described the yen jumping after cool US inflation data and intervention speculation.",
    },
    {
        "event_id": "us_election_trump_mxn_tariff_risk_2024_11_06",
        "event_timestamp": pd.Timestamp("2024-11-06T06:00:00Z"),
        "tolerance_minutes": 1440,
        "event_name": "US election result and Mexico tariff risk repricing",
        "macro_category": "election_or_political_event",
        "sentiment_reason": (
            "Donald Trump's election win drove Mexican peso weakness as markets repriced "
            "tariff and US-Mexico trade risk."
        ),
        "source_url": "https://elpais.com/mexico/economia/2024-11-06/el-peso-cae-hasta-2080-unidades-por-dolar-tras-la-victoria-de-trump-en-ee-uu.html",
        "source_note": "El Pais reported MXN falling after Trump's victory amid tariff and USMCA concerns.",
    },
    {
        "event_id": "trump_mexico_canada_china_tariff_threat_2024_11_25",
        "event_timestamp": pd.Timestamp("2024-11-26T04:00:00Z"),
        "tolerance_minutes": 1440,
        "event_name": "Trump Mexico/Canada/China tariff threat",
        "macro_category": "fiscal_trade_policy",
        "sentiment_reason": (
            "President-elect Trump threatened 25% tariffs on Mexico and Canada and "
            "additional China tariffs, pressuring MXN/CAD and broader dollar risk."
        ),
        "source_url": "https://www.theguardian.com/us-news/2024/nov/25/trump-mexico-canada-tariffs-border",
        "source_note": "Guardian reported the Nov. 25, 2024 tariff threat tied to Jan. 20 executive orders.",
    },
    {
        "event_id": "fed_hawkish_cut_dot_plot_2024_12_18",
        "event_timestamp": pd.Timestamp("2024-12-18T19:00:00Z"),
        "tolerance_minutes": 360,
        "event_name": "Federal Reserve hawkish cut / dot-plot repricing",
        "macro_category": "central_bank",
        "sentiment_reason": (
            "The Fed cut rates but the updated projections signaled fewer 2025 cuts, "
            "creating a hawkish USD repricing."
        ),
        "source_url": "https://www.federalreserve.gov/newsevents/pressreleases/monetary20241218a.htm",
        "source_note": "Federal Reserve FOMC statement and projections release on December 18, 2024.",
    },
    {
        "event_id": "north_america_tariff_pause_2025_02_03",
        "event_timestamp": pd.Timestamp("2025-02-03T15:00:00Z"),
        "tolerance_minutes": 720,
        "event_name": "Mexico/Canada tariff pause and MXN relief move",
        "macro_category": "fiscal_trade_policy",
        "sentiment_reason": (
            "US tariffs on Mexico and Canada were paused for one month after negotiations, "
            "causing whiplash in MXN and risk assets."
        ),
        "source_url": "https://www.politico.com/news/2025/02/03/mexico-president-tariffs-00202059",
        "source_note": "Politico reported Mexico's president announced a one-month tariff delay agreement.",
    },
    {
        "event_id": "trump_day_one_tariff_delay_trade_review_2025_01_20",
        "event_timestamp": pd.Timestamp("2025-01-20T17:00:00Z"),
        "tolerance_minutes": 1440,
        "event_name": "Trump day-one tariff delay / trade review",
        "macro_category": "fiscal_trade_policy",
        "sentiment_reason": (
            "Markets repriced immediate tariff risk lower after reports and the trade-policy "
            "memorandum pointed to investigation/review rather than day-one broad tariffs."
        ),
        "source_url": "https://www.whitehouse.gov/presidential-actions/2025/01/america-first-trade-policy/",
        "source_note": "White House Jan. 20, 2025 America First Trade Policy memorandum ordered reviews of trade and tariff policy.",
    },
    {
        "event_id": "turkey_political_lira_shock_2025_03_19",
        "event_timestamp": pd.Timestamp("2025-03-19T06:00:00Z"),
        "tolerance_minutes": 540,
        "event_name": "Turkey political shock and lira intervention",
        "macro_category": "political_event",
        "sentiment_reason": (
            "Istanbul mayor Ekrem Imamoglu was detained, Turkish assets sold off, "
            "and reports described record central-bank FX sales after the lira dropped."
        ),
        "source_url": "https://www.marketwatch.com/story/turkish-presidents-main-rival-is-arrested-the-lira-is-crashing-1b9ec08c",
        "source_note": "News reports tied the lira plunge to the March 19, 2025 arrest/detention shock.",
    },
    {
        "event_id": "us_reciprocal_tariff_stress_2025_04_02_10",
        "event_timestamp": pd.Timestamp("2025-04-09T16:00:00Z"),
        "tolerance_minutes": 10080,
        "event_name": "US reciprocal-tariff shock and 90-day pause window",
        "macro_category": "fiscal_trade_policy",
        "sentiment_reason": (
            "The April 2 reciprocal-tariff order and April 9 modifications created a global "
            "risk, dollar, rates, and equity repricing window."
        ),
        "source_url": "https://www.whitehouse.gov/presidential-actions/2025/04/modifying-reciprocal-tariff-rates-to-reflect-trading-partner-retaliation-and-alignment/",
        "source_note": "White House April 9, 2025 order modified reciprocal tariffs and suspended many country-specific rates for 90 days.",
    },
    {
        "event_id": "us_china_tariff_truce_2025_05_12",
        "event_timestamp": pd.Timestamp("2025-05-12T07:00:00Z"),
        "tolerance_minutes": 1440,
        "event_name": "US-China tariff truce and risk rally",
        "macro_category": "fiscal_trade_policy",
        "sentiment_reason": (
            "The US and China agreed to sharply reduce reciprocal tariffs for 90 days, "
            "lifting risk appetite and pressuring safe-haven trades."
        ),
        "source_url": "https://www.wsj.com/livecoverage/stock-market-today-tariffs-trade-war-05-12-2025",
        "source_note": "WSJ live coverage reported the US-China tariff cease-fire and market rally on May 12, 2025.",
    },
    {
        "event_id": "august_2025_tariff_deadline_dollar_rally",
        "event_timestamp": pd.Timestamp("2025-08-01T12:00:00Z"),
        "tolerance_minutes": 1440,
        "event_name": "August 2025 tariff deadline and dollar rally",
        "macro_category": "fiscal_trade_policy",
        "sentiment_reason": (
            "US tariff deadline and July dollar rally shifted FX risk premia; tariff-sensitive "
            "and high-beta currencies saw outsized moves."
        ),
        "source_url": "https://www.mufgresearch.com/fx/monthly-foreign-exchange-outlook-august-2025/",
        "source_note": "MUFG noted tariffs going live and the US dollar's strong July 2025 rally.",
    },
    {
        "event_id": "china_stimulus_cnh_spillover_2024_09_27",
        "event_timestamp": pd.Timestamp("2024-09-27T04:00:00Z"),
        "tolerance_minutes": 1440,
        "event_name": "China stimulus and CNH spillover rally",
        "macro_category": "policy_stimulus",
        "sentiment_reason": (
            "China stimulus measures pushed USD/CNH below 7 and spilled over into Asian FX "
            "and broader risk-sensitive currencies."
        ),
        "source_url": "https://www.mufgresearch.com/fx/asia-fx-talk-china-stimulus-pushes-usdcnh-below-7-27-september-2024/",
        "source_note": "MUFG reported CNH strength and regional FX spillovers after China stimulus measures.",
    },
    {
        "event_id": "yen_carry_unwind_2024_08_05",
        "event_timestamp": pd.Timestamp("2024-08-05T00:00:00Z"),
        "tolerance_minutes": 2880,
        "event_name": "Yen carry-trade unwind and global volatility shock",
        "macro_category": "risk_sentiment_shock",
        "sentiment_reason": (
            "BIS described the August 2024 episode as a carry-trade unwind where JPY and CHF "
            "appreciated and high-yielding currencies such as MXN and ZAR were hit."
        ),
        "source_url": "https://www.bis.org/publ/bisbull90.pdf",
        "source_note": "BIS Bulletin 90, The market turbulence and carry trade unwind of August 2024.",
    },
    {
        "event_id": "fed_fomc_2026_06_17",
        "event_timestamp": pd.Timestamp("2026-06-17T18:00:00Z"),
        "tolerance_minutes": 180,
        "event_name": "Federal Reserve FOMC statement and press conference",
        "macro_category": "central_bank",
        "sentiment_reason": (
            "Fed held rates but statement cited elevated inflation, energy/supply shocks, "
            "and Middle East uncertainty; broad USD repricing is plausible."
        ),
        "source_url": "https://www.federalreserve.gov/newsevents/pressreleases/monetary20260617a.htm",
        "source_note": "Federal Reserve statement released June 17, 2026 at 2:00 p.m. EDT.",
    },
]

HIGH_BETA_OR_EM = {"ZAR", "MXN", "TRY", "HUF", "PLN", "NOK", "SEK", "THB", "CNH", "CZK"}
RISK_CURRENCIES = {"AUD", "NZD", "CAD", "ZAR", "MXN", "NOK", "SEK"}
HAVENS = {"USD", "JPY", "CHF"}


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _session(timestamp: pd.Timestamp) -> str:
    hour = int(timestamp.hour)
    if 7 <= hour < 12:
        return "london"
    if 12 <= hour < 16:
        return "london_new_york_overlap"
    if 16 <= hour < 21:
        return "new_york"
    if 21 <= hour or hour < 7:
        return "asia"
    return "other"


def _currency_move(base: str, quote: str, signed_pips: float) -> tuple[str, str]:
    if signed_pips >= 0:
        return base, quote
    return quote, base


def _technical_reason(row: pd.Series) -> tuple[str, str]:
    signed = float(row.get("forward_signed_pips", 0.0))
    momentum_60 = float(row.get("momentum_60_atr", 0.0))
    momentum_15 = float(row.get("momentum_15_atr", 0.0))
    acceleration = abs(float(row.get("acceleration_15_atr", 0.0)))
    atr_ratio = float(row.get("atr15_to_atr240", 0.0))
    compression = float(row.get("compression_30", 0.0))
    range_60 = float(row.get("range_position_60_centered", 0.0))
    range_240 = float(row.get("range_position_240_centered", 0.0))
    event_count = int(row.get("event_pair_count", 0) or 0)
    flags: list[str] = []
    if event_count >= 5:
        flags.append(f"cross_pair_cluster_{event_count}_pairs")
    if atr_ratio >= 1.5:
        flags.append("short_term_volatility_expansion")
    elif atr_ratio <= 0.8:
        flags.append("impulse_from_low_short_term_atr")
    if compression >= 1.5:
        flags.append("expanded_recent_range")
    if abs(range_60) >= 0.85 or abs(range_240) >= 0.85:
        flags.append("near_recent_range_extreme")
    if acceleration >= 1.0:
        flags.append("large_recent_acceleration")
    if signed and momentum_60:
        same = np.sign(signed) == np.sign(momentum_60)
        flags.append("momentum_continuation" if same else "momentum_reversal_or_stop_run")
    elif signed and momentum_15:
        same = np.sign(signed) == np.sign(momentum_15)
        flags.append("short_momentum_continuation" if same else "short_momentum_reversal")
    if not flags:
        flags.append("large_endpoint_displacement")
    explanation = "; ".join(flags)
    setup = "reversal_or_stop_run" if "momentum_reversal_or_stop_run" in flags else "continuation_or_breakout"
    return setup, explanation


def _sentiment_reason(row: pd.Series) -> tuple[str, str, str]:
    base = str(row["base_currency"])
    quote = str(row["quote_currency"])
    strong, weak = _currency_move(base, quote, float(row["forward_signed_pips"]))
    pair_count = int(row.get("event_pair_count", 0) or 0)
    is_cross = bool(row.get("is_cross_pair_shock", False))
    pair = {base, quote}

    if is_cross and "USD" in pair:
        if strong == "USD":
            return (
                "broad_usd_strength",
                f"USD strengthened against {weak}; cross-pair cluster suggests dollar-flow repricing.",
                "medium",
            )
        return (
            "broad_usd_weakness",
            f"USD weakened against {strong}; cross-pair cluster suggests dollar-flow repricing.",
            "medium",
        )
    if strong in HAVENS and weak in RISK_CURRENCIES:
        return (
            "risk_off_or_carry_unwind",
            f"{strong} strengthened while higher-beta {weak} weakened.",
            "low",
        )
    if weak in HAVENS and strong in RISK_CURRENCIES:
        return (
            "risk_on_or_carry_rebuild",
            f"Higher-beta {strong} strengthened while haven {weak} weakened.",
            "low",
        )
    if pair.intersection(HIGH_BETA_OR_EM):
        return (
            "high_beta_local_or_liquidity_shock",
            f"{'/'.join(sorted(pair.intersection(HIGH_BETA_OR_EM)))} pair produced outsized displacement.",
            "low",
        )
    if pair_count >= 5:
        return (
            "broad_cross_pair_repricing",
            f"{pair_count} pairs moved in the same event cluster.",
            "low",
        )
    return ("pair_specific_technical_move", "No verified broad macro theme assigned yet.", "low")


def _macro_reason(timestamp: pd.Timestamp) -> dict[str, Any]:
    for event in VERIFIED_MACRO_EVENTS:
        delta = abs((timestamp - event["event_timestamp"]).total_seconds()) / 60.0
        if delta <= float(event["tolerance_minutes"]):
            return {
                "macro_event_id": event["event_id"],
                "macro_event_timestamp": event["event_timestamp"],
                "macro_event_name": event["event_name"],
                "macro_category": event["macro_category"],
                "macro_reason_verified": event["sentiment_reason"],
                "macro_source_url": event["source_url"],
                "macro_source_note": event["source_note"],
                "macro_confidence": "verified_time_match",
                "needs_news_review": False,
            }
    return {
        "macro_event_id": "",
        "macro_event_timestamp": pd.NaT,
        "macro_event_name": "",
        "macro_category": "",
        "macro_reason_verified": "",
        "macro_source_url": "",
        "macro_source_note": "",
        "macro_confidence": "heuristic_only",
        "needs_news_review": True,
    }


def _load_frame(spike_root: Path) -> pd.DataFrame:
    features = pd.read_parquet(spike_root / "features" / "decision_features.parquet")
    labels = pd.read_parquet(spike_root / "labels" / "exact_120m_labels.parquet")
    frame = features.merge(
        labels.drop(columns=["timestamp", "instrument"], errors="ignore"),
        on="decision_id",
        how="inner",
        validate="one_to_one",
    )
    parts = frame["instrument"].str.split("_", expand=True)
    frame["base_currency"] = parts[0]
    frame["quote_currency"] = parts[1]
    return frame


def build_trading_logic_sheet(
    spike_root: Path = DEFAULT_SPIKE_ROOT,
    output_root: Path = DEFAULT_OUTPUT_ROOT,
    *,
    top_n: int = 500,
) -> dict[str, Any]:
    output_root.mkdir(parents=True, exist_ok=True)
    frame = _load_frame(spike_root)
    significant = frame.loc[frame["is_significant"].fillna(False)].copy()
    significant = significant.sort_values(
        ["forward_abs_pips", "timestamp", "instrument"],
        ascending=[False, True, True],
        kind="stable",
    ).head(int(top_n))

    rows: list[dict[str, Any]] = []
    for _, row in significant.iterrows():
        timestamp = pd.Timestamp(row["timestamp"])
        setup, technical = _technical_reason(row)
        sentiment_theme, sentiment_reason, sentiment_confidence = _sentiment_reason(row)
        macro = _macro_reason(timestamp)
        strong, weak = _currency_move(
            str(row["base_currency"]),
            str(row["quote_currency"]),
            float(row["forward_signed_pips"]),
        )
        rows.append(
            {
                "decision_id": row["decision_id"],
                "timestamp": timestamp,
                "session": _session(timestamp),
                "instrument": row["instrument"],
                "base_currency": row["base_currency"],
                "quote_currency": row["quote_currency"],
                "strong_currency_in_move": strong,
                "weak_currency_in_move": weak,
                "signed_pips_2h": float(row["forward_signed_pips"]),
                "abs_pips_2h": float(row["forward_abs_pips"]),
                "abs_return_2h": float(row["forward_abs_return"]),
                "event_cluster_id": row.get("event_cluster_id", ""),
                "event_pair_count": row.get("event_pair_count", pd.NA),
                "is_cross_pair_shock": bool(row.get("is_cross_pair_shock", False)),
                "technical_setup": setup,
                "technical_reason": technical,
                "sentiment_theme": sentiment_theme,
                "sentiment_reason": sentiment_reason,
                "sentiment_confidence": sentiment_confidence,
                **macro,
                "movement_score_rule": float(row.get("movement_score_rule", np.nan)),
                "momentum_15_atr": float(row.get("momentum_15_atr", np.nan)),
                "momentum_60_atr": float(row.get("momentum_60_atr", np.nan)),
                "acceleration_15_atr": float(row.get("acceleration_15_atr", np.nan)),
                "atr15_to_atr240": float(row.get("atr15_to_atr240", np.nan)),
                "compression_30": float(row.get("compression_30", np.nan)),
                "range_position_60_centered": float(row.get("range_position_60_centered", np.nan)),
                "range_position_240_centered": float(row.get("range_position_240_centered", np.nan)),
                "strength_gap_rank_15": float(row.get("strength_gap_rank_15", np.nan)),
                "strength_gap_rank_60": float(row.get("strength_gap_rank_60", np.nan)),
                "trading_logic_hypothesis": (
                    f"{setup}: {technical}. {sentiment_reason}"
                ),
            }
        )
    sheet = pd.DataFrame(rows)
    cluster_summary = (
        sheet.groupby(["event_cluster_id"], dropna=False)
        .agg(
            first_timestamp=("timestamp", "min"),
            last_timestamp=("timestamp", "max"),
            rows=("decision_id", "count"),
            max_abs_pips=("abs_pips_2h", "max"),
            instruments=("instrument", lambda values: ",".join(sorted(set(map(str, values)))[:20])),
            dominant_sentiment=("sentiment_theme", lambda values: values.mode().iat[0] if len(values.mode()) else ""),
            verified_macro=("macro_event_name", lambda values: values.mode().iat[0] if len(values.mode()) else ""),
            needs_news_review=("needs_news_review", "max"),
        )
        .reset_index()
        .sort_values(["max_abs_pips", "rows"], ascending=[False, False], kind="stable")
    )
    review_queue = (
        sheet.loc[sheet["needs_news_review"]]
        .groupby(["event_cluster_id"], dropna=False)
        .agg(
            first_timestamp=("timestamp", "min"),
            last_timestamp=("timestamp", "max"),
            rows=("decision_id", "count"),
            max_abs_pips=("abs_pips_2h", "max"),
            instruments=("instrument", lambda values: ",".join(sorted(set(map(str, values)))[:20])),
            dominant_sentiment=("sentiment_theme", lambda values: values.mode().iat[0] if len(values.mode()) else ""),
            dominant_setup=("technical_setup", lambda values: values.mode().iat[0] if len(values.mode()) else ""),
            example_hypothesis=("trading_logic_hypothesis", "first"),
        )
        .reset_index()
        .sort_values(["max_abs_pips", "rows"], ascending=[False, False], kind="stable")
    )

    sheet_path = output_root / "trading_logic_sheet.csv"
    cluster_path = output_root / "trading_logic_cluster_summary.csv"
    queue_path = output_root / "needs_news_review_queue.csv"
    manifest_path = output_root / "manifest.json"
    report_path = output_root / "TRADING_LOGIC_SHEET.md"
    sheet.to_csv(sheet_path, index=False)
    cluster_summary.to_csv(cluster_path, index=False)
    review_queue.to_csv(queue_path, index=False)
    manifest = {
        "generated_utc": _utc_now(),
        "research_only": True,
        "source": "spike_account_space exact 120-minute labels + causal decision features",
        "rows": int(len(sheet)),
        "clusters": int(cluster_summary["event_cluster_id"].nunique(dropna=True)),
        "verified_macro_rows": int(sheet["macro_confidence"].eq("verified_time_match").sum()),
        "needs_news_review_rows": int(sheet["needs_news_review"].sum()),
        "needs_news_review_clusters": int(review_queue["event_cluster_id"].nunique(dropna=True)),
        "verified_macro_sources": VERIFIED_MACRO_EVENTS,
        "warnings": [
            "Technical reasons are derived from causal feature snapshots.",
            "Sentiment themes are rule-based hypotheses unless macro_confidence is verified_time_match.",
            "The sheet is explanatory research, not a live trading rule.",
        ],
    }
    manifest_path.write_text(json.dumps(manifest, indent=2, default=str), encoding="utf-8")
    top = sheet.head(20)[
        [
            "timestamp",
            "instrument",
            "signed_pips_2h",
            "technical_setup",
            "sentiment_theme",
            "macro_event_name",
            "needs_news_review",
        ]
    ]
    queue_preview = review_queue.head(20)[
        [
            "first_timestamp",
            "rows",
            "max_abs_pips",
            "instruments",
            "dominant_sentiment",
            "dominant_setup",
        ]
    ]
    report = [
        "# 2-Hour Trading Logic Sheet",
        "",
        "Research-only explanation sheet for the largest exact 2-hour FX movements.",
        "",
        f"- Rows: {len(sheet):,}",
        f"- Event clusters: {cluster_summary['event_cluster_id'].nunique(dropna=True):,}",
        f"- Verified macro rows: {manifest['verified_macro_rows']:,}",
        f"- Rows still needing manual news review: {manifest['needs_news_review_rows']:,}",
        f"- Clusters still needing manual news review: {manifest['needs_news_review_clusters']:,}",
        "",
        "## Top Rows",
        "",
        "```text",
        top.to_string(index=False),
        "```",
        "",
        "## Review Queue",
        "",
        "```text",
        queue_preview.to_string(index=False),
        "```",
        "",
        "## Source Discipline",
        "",
        "Macro fields are split into verified time matches and heuristic hypotheses. Do not treat heuristic rows as confirmed news attribution.",
    ]
    report_path.write_text("\n".join(report) + "\n", encoding="utf-8")
    return {
        "sheet": sheet_path,
        "clusters": cluster_path,
        "review_queue": queue_path,
        "manifest": manifest_path,
        "report": report_path,
        "rows": len(sheet),
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--spike-root", type=Path, default=DEFAULT_SPIKE_ROOT)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--top-n", type=int, default=500)
    args = parser.parse_args()
    result = build_trading_logic_sheet(
        args.spike_root,
        args.output_root,
        top_n=args.top_n,
    )
    print(json.dumps({key: str(value) for key, value in result.items()}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
