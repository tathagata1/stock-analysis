"""Reporting tables and charts for the multi-index search workflow."""

from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

import config.config as config


SIGNAL_ORDER = [
    "STRONG SELL",
    "WEAK SELL",
    "HOLD",
    "WEAK BUY",
    "STRONG BUY",
]
SIGNAL_COLORS = {
    "STRONG SELL": "#b71c1c",
    "WEAK SELL": "#ef5350",
    "HOLD": "#9e9e9e",
    "WEAK BUY": "#66bb6a",
    "STRONG BUY": "#1b5e20",
}
CANDIDATE_COLUMNS = [
    "Rank",
    "TICKER",
    "Indexes",
    "Signal_Text",
    "Signal",
    "Close",
    "Period_Return_Pct",
    "Annualized_Volatility_Pct",
    "Max_Drawdown_Pct",
    "RSI",
    "ATR_Pct",
    "Volume_vs_Avg",
    "Forward_PE",
    "Target_Upside_Pct",
]
FACTOR_COLUMNS = [
    "Technical_Score",
    "Fundamental_Score",
    "Sentiment_Score",
    "Multifactor_Score",
]
FACTOR_LABELS = ["Technical", "Fundamental", "Sentiment", "Multifactor"]


def _numeric_value(value):
    return pd.to_numeric(pd.Series([value]), errors="coerce").iloc[0]


def save_figure(fig, filename, enabled=False, output_directory="output"):
    """Optionally save a Matplotlib figure and return its output path."""
    if not enabled:
        return None
    output_path = Path(output_directory)
    output_path.mkdir(parents=True, exist_ok=True)
    output_file = output_path / filename
    fig.savefig(output_file, dpi=170, bbox_inches="tight")
    return output_file


def build_scan_summary(
    results_by_index,
    combined_prediction_summary,
    signal_only,
):
    """Summarize universe coverage and actionable signal counts."""
    unique_tickers = (
        combined_prediction_summary["TICKER"].nunique()
        if "TICKER" in combined_prediction_summary
        else 0
    )
    buy_rows = (
        signal_only["Signal_Text"].str.contains("BUY").sum()
        if "Signal_Text" in signal_only
        else 0
    )
    sell_rows = (
        signal_only["Signal_Text"].str.contains("SELL").sum()
        if "Signal_Text" in signal_only
        else 0
    )
    return pd.DataFrame(
        [
            {
                "universes": len(results_by_index),
                "constituent_rows_scanned": len(combined_prediction_summary),
                "unique_tickers": unique_tickers,
                "actionable_rows": len(signal_only),
                "buy_rows": buy_rows,
                "sell_rows": sell_rows,
            }
        ]
    )


def _build_detail_row(index_name, ticker, analysis):
    prediction = analysis["df_pred"]
    latest = prediction.iloc[0]
    stats = analysis.get("stats", pd.Series(dtype=object))
    history = prediction.sort_values("Date")
    close = pd.to_numeric(history["Close"], errors="coerce").dropna()
    daily_returns = close.pct_change().dropna()
    period_return_pct = (
        (close.iloc[-1] / close.iloc[0] - 1) * 100 if len(close) > 1 else np.nan
    )
    annualized_volatility_pct = (
        daily_returns.std() * np.sqrt(252) * 100
        if len(daily_returns) > 1
        else np.nan
    )
    wealth = (1 + daily_returns).cumprod()
    max_drawdown_pct = (
        ((wealth / wealth.cummax()) - 1).min() * 100
        if not wealth.empty
        else np.nan
    )
    current_price = _numeric_value(latest.get("Close"))
    target_price = _numeric_value(stats.get("Target Mean Price", np.nan))
    atr_value = _numeric_value(latest.get("ATR"))
    average_volume = pd.to_numeric(history["Volume"], errors="coerce").mean()
    latest_volume = _numeric_value(latest.get("Volume"))

    return {
        "index_name": index_name,
        "TICKER": ticker,
        "Signal": _numeric_value(latest.get("Signal")),
        "Signal_Text": latest.get("Signal_Text"),
        "Close": current_price,
        "Period_Return_Pct": period_return_pct,
        "Annualized_Volatility_Pct": annualized_volatility_pct,
        "Max_Drawdown_Pct": max_drawdown_pct,
        "RSI": _numeric_value(latest.get("RSI")),
        "ATR_Pct": atr_value / current_price * 100 if current_price else np.nan,
        "Volume_vs_Avg": (
            latest_volume / average_volume if average_volume else np.nan
        ),
        "Technical_Score": _numeric_value(
            latest.get("technical_analysis_buy_score")
        )
        + _numeric_value(latest.get("technical_analysis_sell_score")),
        "Fundamental_Score": _numeric_value(
            latest.get("fundamental_analysis_score")
        ),
        "Sentiment_Score": _numeric_value(latest.get("sentiment_analysis_score")),
        "Multifactor_Score": _numeric_value(
            latest.get("multifactor_analysis_score")
        ),
        "Market_Cap": _numeric_value(stats.get("Market Cap", np.nan)),
        "Forward_PE": _numeric_value(stats.get("Forward P/E", np.nan)),
        "Price_Book": _numeric_value(stats.get("Price/Book", np.nan)),
        "Price_Sales": _numeric_value(stats.get("Price/Sales", np.nan)),
        "Target_Upside_Pct": (
            (target_price / current_price - 1) * 100
            if current_price and pd.notna(target_price)
            else np.nan
        ),
    }


def build_detailed_candidate_data(results_by_index, candidate_limit=20):
    """Build consolidated candidate frames and ticker-analysis lookup data."""
    detail_rows = []
    analysis_by_ticker = {}
    for index_name, index_result in results_by_index.items():
        for ticker, analysis in index_result["analyses"].items():
            analysis_by_ticker.setdefault(ticker, analysis)
            if analysis["df_pred"].empty:
                continue
            detail_rows.append(_build_detail_row(index_name, ticker, analysis))

    raw_detailed = pd.DataFrame(detail_rows)
    if raw_detailed.empty:
        raise RuntimeError(
            "The scan returned no usable stock analyses; review the scan logs "
            "and data connections."
        )

    membership_by_ticker = raw_detailed.groupby("TICKER")["index_name"].agg(
        lambda values: ", ".join(sorted(set(values)))
    )
    detailed_ranked = (
        raw_detailed.sort_values(["Signal", "TICKER"], ascending=[False, True])
        .drop_duplicates("TICKER")
        .drop(columns="index_name")
        .reset_index(drop=True)
    )
    detailed_ranked.insert(
        1,
        "Indexes",
        detailed_ranked["TICKER"].map(membership_by_ticker),
    )
    detailed_ranked.insert(0, "Rank", np.arange(1, len(detailed_ranked) + 1))
    actionable = detailed_ranked[
        detailed_ranked["Signal_Text"] != "HOLD"
    ].copy()
    buy_candidates = actionable[
        actionable["Signal_Text"].str.contains("BUY", na=False)
    ].nlargest(int(candidate_limit), "Signal")
    sell_candidates = actionable[
        actionable["Signal_Text"].str.contains("SELL", na=False)
    ].nsmallest(int(candidate_limit), "Signal")

    return {
        "analysis_by_ticker": analysis_by_ticker,
        "detailed_ranked": detailed_ranked,
        "actionable": actionable,
        "buy_candidates": buy_candidates,
        "sell_candidates": sell_candidates,
    }


def plot_signal_overview(
    detailed_ranked,
    buy_candidates,
    sell_candidates,
    ranking_chart_count=20,
):
    """Plot the signal distribution and strongest buy/sell rankings."""
    if detailed_ranked.empty:
        return None

    signal_counts = (
        detailed_ranked["Signal_Text"]
        .value_counts()
        .reindex(SIGNAL_ORDER, fill_value=0)
    )
    side_count = max(1, int(ranking_chart_count) // 2)
    bullish = buy_candidates.nlargest(side_count, "Signal")
    bearish = sell_candidates.nsmallest(side_count, "Signal")
    ranking_plot = (
        pd.concat([bearish, bullish])
        .drop_duplicates("TICKER")
        .sort_values("Signal")
    )

    fig, (ax_counts, ax_ranks) = plt.subplots(1, 2, figsize=(18, 7))
    count_colors = [SIGNAL_COLORS[label] for label in signal_counts.index]
    bars = ax_counts.bar(signal_counts.index, signal_counts.values, color=count_colors)
    ax_counts.bar_label(bars, padding=3)
    ax_counts.set_title("Unique ticker signal distribution")
    ax_counts.set_ylabel("Tickers")
    ax_counts.tick_params(axis="x", rotation=25)

    rank_colors = [
        SIGNAL_COLORS.get(label, "#9e9e9e") for label in ranking_plot["Signal_Text"]
    ]
    ax_ranks.barh(ranking_plot["TICKER"], ranking_plot["Signal"], color=rank_colors)
    ax_ranks.axvline(0, color="#424242", linewidth=1)
    ax_ranks.axvline(
        config.STRONG_BUY_THRESHOLD,
        color="#1b5e20",
        linestyle="--",
        linewidth=1,
    )
    ax_ranks.axvline(
        config.STRONG_SELL_THRESHOLD,
        color="#b71c1c",
        linestyle="--",
        linewidth=1,
    )
    ax_ranks.set_title("Strongest bullish and bearish signals")
    ax_ranks.set_xlabel("Composite signal score")
    fig.tight_layout()
    return fig


def _select_factor_candidates(
    buy_candidates,
    sell_candidates,
    ranking_chart_count,
):
    side_count = max(1, int(ranking_chart_count) // 2)
    return (
        pd.concat(
            [
                buy_candidates.nlargest(side_count, "Signal"),
                sell_candidates.nsmallest(side_count, "Signal"),
            ]
        )
        .drop_duplicates("TICKER")
        .sort_values("Signal", ascending=False)
    )


def plot_factor_risk_map(
    detailed_ranked,
    buy_candidates,
    sell_candidates,
    period,
    ranking_chart_count=20,
):
    """Plot factor agreement and the universe risk/return map."""
    factor_candidates = _select_factor_candidates(
        buy_candidates,
        sell_candidates,
        ranking_chart_count,
    )
    if factor_candidates.empty:
        return None

    fig, (ax_heatmap, ax_risk) = plt.subplots(1, 2, figsize=(19, 9))
    factor_values = factor_candidates[FACTOR_COLUMNS].fillna(0).to_numpy(dtype=float)
    image = ax_heatmap.imshow(
        factor_values,
        cmap="RdYlGn",
        vmin=-1,
        vmax=1,
        aspect="auto",
    )
    ax_heatmap.set_xticks(
        range(len(FACTOR_LABELS)),
        labels=FACTOR_LABELS,
        rotation=25,
        ha="right",
    )
    ax_heatmap.set_yticks(
        range(len(factor_candidates)),
        labels=factor_candidates["TICKER"],
    )
    ax_heatmap.set_title("Factor agreement for strongest signals")
    for row_index in range(factor_values.shape[0]):
        for column_index in range(factor_values.shape[1]):
            ax_heatmap.text(
                column_index,
                row_index,
                f"{factor_values[row_index, column_index]:.2f}",
                ha="center",
                va="center",
                fontsize=8,
            )
    fig.colorbar(
        image,
        ax=ax_heatmap,
        label="Factor score",
        fraction=0.046,
        pad=0.04,
    )

    scatter = ax_risk.scatter(
        detailed_ranked["Annualized_Volatility_Pct"],
        detailed_ranked["Period_Return_Pct"],
        c=detailed_ranked["Signal"],
        cmap="RdYlGn",
        vmin=-1,
        vmax=1,
        s=55,
        alpha=0.75,
        edgecolor="#424242",
        linewidth=0.4,
    )
    ax_risk.axhline(0, color="#616161", linewidth=1)
    ax_risk.set_title(f"Risk/return map over {period}")
    ax_risk.set_xlabel("Annualized volatility (%)")
    ax_risk.set_ylabel("Period return (%)")
    for _, candidate in factor_candidates.iterrows():
        if pd.notna(candidate["Annualized_Volatility_Pct"]) and pd.notna(
            candidate["Period_Return_Pct"]
        ):
            ax_risk.annotate(
                candidate["TICKER"],
                (
                    candidate["Annualized_Volatility_Pct"],
                    candidate["Period_Return_Pct"],
                ),
                xytext=(4, 4),
                textcoords="offset points",
                fontsize=8,
            )
    fig.colorbar(
        scatter,
        ax=ax_risk,
        label="Composite signal",
        fraction=0.046,
        pad=0.04,
    )
    fig.tight_layout()
    return fig


def select_priority_candidates(
    buy_candidates,
    sell_candidates,
    candidate_detail_count=6,
):
    """Select balanced buy and sell candidates for individual charts."""
    buy_count = max(1, int(candidate_detail_count) // 2)
    sell_count = max(1, int(candidate_detail_count) - buy_count)
    return (
        pd.concat(
            [
                buy_candidates.nlargest(buy_count, "Signal"),
                sell_candidates.nsmallest(sell_count, "Signal"),
            ]
        )
        .drop_duplicates("TICKER")
        .head(int(candidate_detail_count))
    )


def plot_candidate_details(ticker, analysis):
    """Plot price, RSI, MACD, and volume for one candidate."""
    history = analysis["df_pred"].sort_values("Date").copy()
    latest = history.iloc[-1]
    sma_fast = f"SMA{config.sma1}"
    sma_slow = f"SMA{config.sma2}"
    fig, axes = plt.subplots(
        4,
        1,
        figsize=(16, 13),
        sharex=True,
        gridspec_kw={"height_ratios": [2.2, 1, 1, 1]},
    )
    ax_price, ax_rsi, ax_macd, ax_volume = axes

    ax_price.plot(
        history["Date"],
        history["Close"],
        label="Close",
        color="#1565c0",
        linewidth=2,
    )
    for column, color in [(sma_fast, "#ef6c00"), (sma_slow, "#6a1b9a")]:
        if column in history:
            ax_price.plot(
                history["Date"],
                history[column],
                label=column,
                color=color,
                linewidth=1.3,
            )
    if {"Upper_Band", "Lower_Band"}.issubset(history.columns):
        ax_price.plot(
            history["Date"],
            history["Upper_Band"],
            color="#78909c",
            linewidth=1,
            linestyle="--",
            label="Bollinger bands",
        )
        ax_price.plot(
            history["Date"],
            history["Lower_Band"],
            color="#78909c",
            linewidth=1,
            linestyle="--",
        )
        ax_price.fill_between(
            history["Date"],
            history["Lower_Band"],
            history["Upper_Band"],
            color="#90a4ae",
            alpha=0.12,
        )
    buy_rows = history[history["Signal_Text"].str.contains("BUY", na=False)]
    sell_rows = history[history["Signal_Text"].str.contains("SELL", na=False)]
    ax_price.scatter(
        buy_rows["Date"],
        buy_rows["Close"],
        marker="^",
        color="#2e7d32",
        s=55,
        label="Buy signal",
        zorder=5,
    )
    ax_price.scatter(
        sell_rows["Date"],
        sell_rows["Close"],
        marker="v",
        color="#c62828",
        s=55,
        label="Sell signal",
        zorder=5,
    )
    ax_price.set_ylabel("Price")
    ax_price.set_title(
        f"{ticker}: {latest['Signal_Text']} ({latest['Signal']:.3f})"
    )
    ax_price.legend(loc="upper left", ncol=3)

    ax_rsi.plot(history["Date"], history["RSI"], color="#5d4037", linewidth=1.5)
    ax_rsi.axhline(
        config.rsi_buy,
        color="#2e7d32",
        linestyle="--",
        linewidth=1,
        label=f"Buy threshold {config.rsi_buy}",
    )
    ax_rsi.axhline(
        config.rsi_sell,
        color="#c62828",
        linestyle="--",
        linewidth=1,
        label=f"Sell threshold {config.rsi_sell}",
    )
    ax_rsi.set_ylim(0, 100)
    ax_rsi.set_ylabel("RSI")
    ax_rsi.legend(loc="upper left", ncol=2)

    ax_macd.plot(history["Date"], history["MACD"], color="#1565c0", label="MACD")
    ax_macd.plot(
        history["Date"],
        history["Signal_Line"],
        color="#ef6c00",
        label="Signal line",
    )
    macd_histogram = history["MACD"] - history["Signal_Line"]
    histogram_colors = np.where(macd_histogram >= 0, "#66bb6a", "#ef5350")
    ax_macd.bar(
        history["Date"],
        macd_histogram,
        color=histogram_colors,
        alpha=0.35,
    )
    ax_macd.axhline(0, color="#616161", linewidth=1)
    ax_macd.set_ylabel("MACD")
    ax_macd.legend(loc="upper left", ncol=2)

    volume_colors = np.where(
        history["Close"] >= history["Open"],
        "#66bb6a",
        "#ef5350",
    )
    ax_volume.bar(
        history["Date"],
        history["Volume"],
        color=volume_colors,
        alpha=0.7,
    )
    ax_volume.set_ylabel("Volume")
    ax_volume.set_xlabel("Date")
    fig.tight_layout()
    return fig


def build_combined_index_search_output(
    ranked,
    signal_only,
    detailed_ranked,
):
    """Combine ranking, actionability, index membership, and detail metrics."""
    if ranked.empty:
        return pd.DataFrame()

    required_ranked_columns = {"index_name", "TICKER", "Signal", "Signal_Text"}
    missing_ranked_columns = required_ranked_columns.difference(ranked.columns)
    if missing_ranked_columns:
        raise ValueError(
            "ranked is missing required columns: "
            + ", ".join(sorted(missing_ranked_columns))
        )
    required_detail_columns = {"Rank", "TICKER"}
    missing_detail_columns = required_detail_columns.difference(
        detailed_ranked.columns
    )
    if missing_detail_columns:
        raise ValueError(
            "detailed_ranked is missing required columns: "
            + ", ".join(sorted(missing_detail_columns))
        )

    summary = ranked.copy().rename(
        columns={"index_name": "Index_Name", "current_date": "Run_Date"}
    )
    summary.insert(
        summary.columns.get_loc("TICKER"),
        "Index_Rank",
        summary.groupby("Index_Name", sort=False).cumcount() + 1,
    )

    detail_columns = [
        column
        for column in detailed_ranked.columns
        if column not in {"Indexes", "Signal", "Signal_Text"}
    ]
    details = detailed_ranked[detail_columns].rename(
        columns={"Rank": "Overall_Rank"}
    )
    combined = summary.merge(
        details,
        how="left",
        on="TICKER",
        validate="many_to_one",
    )

    actionable_keys = set()
    if not signal_only.empty and {"index_name", "TICKER"}.issubset(
        signal_only.columns
    ):
        actionable_keys = set(
            zip(
                signal_only["index_name"].astype(str),
                signal_only["TICKER"].astype(str),
            )
        )
    combined.insert(
        combined.columns.get_loc("Signal_Text"),
        "Actionable",
        [
            (str(index_name), str(ticker)) in actionable_keys
            for index_name, ticker in zip(
                combined["Index_Name"], combined["TICKER"]
            )
        ],
    )

    leading_columns = [
        column
        for column in [
            "Overall_Rank",
            "Index_Rank",
            "Index_Name",
            "Run_Date",
            "TICKER",
            "Actionable",
            "Signal_Text",
            "Signal",
        ]
        if column in combined.columns
    ]
    remaining_columns = [
        column for column in combined.columns if column not in leading_columns
    ]
    return combined[leading_columns + remaining_columns].sort_values(
        ["Overall_Rank", "Index_Name", "TICKER"],
        na_position="last",
    ).reset_index(drop=True)


def plot_combined_index_insight(combined_output):
    """Plot signal composition and average directional score for each index."""
    if combined_output.empty:
        return None

    required_columns = {"Index_Name", "Signal_Text", "Signal"}
    missing_columns = required_columns.difference(combined_output.columns)
    if missing_columns:
        raise ValueError(
            "combined_output is missing required columns: "
            + ", ".join(sorted(missing_columns))
        )

    chart_data = combined_output.dropna(subset=["Index_Name"]).copy()
    if chart_data.empty:
        return None

    signal_composition = (
        pd.crosstab(chart_data["Index_Name"], chart_data["Signal_Text"])
        .reindex(columns=SIGNAL_ORDER, fill_value=0)
        .sort_index()
    )
    average_signal = (
        chart_data.groupby("Index_Name")["Signal"]
        .mean()
        .reindex(signal_composition.index)
        .fillna(0)
    )

    figure_height = max(5.5, 0.75 * len(signal_composition))
    fig, (ax_composition, ax_direction) = plt.subplots(
        1,
        2,
        figsize=(18, figure_height),
        gridspec_kw={"width_ratios": [1.35, 1]},
    )
    y_positions = np.arange(len(signal_composition))
    left = np.zeros(len(signal_composition), dtype=float)
    for signal_label in SIGNAL_ORDER:
        counts = signal_composition[signal_label].to_numpy(dtype=float)
        ax_composition.barh(
            y_positions,
            counts,
            left=left,
            label=signal_label.title(),
            color=SIGNAL_COLORS[signal_label],
        )
        left += counts
    ax_composition.set_yticks(y_positions, labels=signal_composition.index)
    ax_composition.set_xlabel("Ticker memberships")
    ax_composition.set_title("Signal composition by index")
    ax_composition.legend(
        loc="upper center",
        bbox_to_anchor=(0.5, -0.16),
        ncol=3,
    )
    ax_composition.invert_yaxis()

    direction_colors = [
        SIGNAL_COLORS["WEAK BUY"] if value >= 0 else SIGNAL_COLORS["WEAK SELL"]
        for value in average_signal
    ]
    direction_bars = ax_direction.barh(
        y_positions,
        average_signal,
        color=direction_colors,
    )
    ax_direction.axvline(0, color="#424242", linewidth=1)
    ax_direction.set_yticks(y_positions, labels=average_signal.index)
    ax_direction.set_xlabel("Average composite signal")
    ax_direction.set_title("Directional lean by index")
    ax_direction.invert_yaxis()
    ax_direction.bar_label(direction_bars, fmt="%.3f", padding=4)
    signal_limit = max(
        0.5,
        float(average_signal.abs().max()) * 1.25,
    )
    ax_direction.set_xlim(-signal_limit, signal_limit)

    fig.suptitle("Combined index-search insight", fontsize=15, y=0.98)
    fig.tight_layout(rect=[0, 0.08, 1, 0.92])
    return fig


def export_combined_index_search_output(
    combined_output,
    output_directory="output",
):
    """Export the combined index-search report to one CSV file."""
    output_path = Path(output_directory)
    output_path.mkdir(parents=True, exist_ok=True)
    output_file = output_path / "index_search_combined.csv"
    combined_output.to_csv(output_file, index=False)
    return output_file
