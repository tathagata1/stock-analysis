"""Result preparation, CSV export, and charting for backtests."""

from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd

from .data import clean_symbol_for_filename


def _plot_price_level(ax, dates, values, color, label, linestyle="--"):
    """Plot either a scalar horizontal level or a per-session level series."""
    if values is None:
        return
    if pd.api.types.is_scalar(values):
        ax.axhline(
            float(values),
            color=color,
            linestyle=linestyle,
            linewidth=1.5,
            label=label,
        )
        return

    series = pd.to_numeric(pd.Series(values).reset_index(drop=True), errors="coerce")
    if len(series) != len(dates):
        raise ValueError(f"{label} must have one value per plotted session")
    ax.plot(
        dates,
        series,
        color=color,
        linestyle=linestyle,
        linewidth=1.5,
        label=label,
    )


def prepare_backtest_results(backtest):
    """Return result frames and notebook-friendly display frames."""
    summary = backtest["summary"].copy()
    transactions = backtest["transactions"].copy()
    round_trips = backtest["round_trips"].copy()
    equity_curve = backtest["equity_curve"].copy()

    transactions_display = (
        transactions
        if not transactions.empty
        else pd.DataFrame(
            {
                "message": [
                    "No orders filled: no entry was eligible or capital was insufficient."
                ]
            }
        )
    )
    round_trips_display = (
        round_trips
        if not round_trips.empty
        else pd.DataFrame({"message": ["No completed round trips."]})
    )
    return {
        "summary": summary,
        "transactions": transactions,
        "round_trips": round_trips,
        "equity_curve": equity_curve,
        "summary_display": summary.T.rename(columns={0: "value"}),
        "transactions_display": transactions_display,
        "round_trips_display": round_trips_display,
    }


def prepare_trade_details(backtest, ticker=None):
    """Return one execution-style row for every simulated trade entry."""
    transactions = backtest["transactions"]
    round_trips = backtest["round_trips"]
    equity_curve = backtest["equity_curve"]
    entry_actions = ["BUY", "SELL_SHORT"]
    exit_actions = ["SELL", "BUY_TO_COVER"]

    entries = transactions[transactions["action"].isin(entry_actions)].copy()
    if entries.empty:
        return pd.DataFrame(
            columns=[
                "trade_number",
                "ticker",
                "side",
                "status",
                "buy_date",
                "buy_price",
                "sell_date",
                "sell_price",
                "entry_action",
                "entry_date",
                "entry_price",
                "shares",
                "entry_notional",
                "entry_commission",
                "cash_after_entry",
                "planned_target_price",
                "planned_stop_price",
                "trailing_stop_pct",
                "planned_risk_per_share",
                "planned_reward_per_share",
                "planned_reward_risk_ratio",
                "exit_action",
                "exit_date",
                "exit_price",
                "exit_notional",
                "exit_commission",
                "cash_after_exit",
                "exit_reason",
                "holding_sessions",
                "realized_pnl",
                "return_pct",
            ]
        )

    entries = entries[
        [
            "entry_number",
            "side",
            "action",
            "Date",
            "fill_price",
            "shares",
            "gross_value",
            "commission",
            "cash_after",
        ]
    ].rename(
        columns={
            "action": "entry_action",
            "Date": "entry_date",
            "fill_price": "entry_price",
            "gross_value": "entry_notional",
            "commission": "entry_commission",
            "cash_after": "cash_after_entry",
        }
    )
    exits = transactions[transactions["action"].isin(exit_actions)][
        [
            "entry_number",
            "side",
            "action",
            "Date",
            "fill_price",
            "gross_value",
            "commission",
            "cash_after",
            "reason",
        ]
    ].rename(
        columns={
            "action": "exit_action",
            "Date": "exit_date",
            "fill_price": "exit_price",
            "gross_value": "exit_notional",
            "commission": "exit_commission",
            "cash_after": "cash_after_exit",
            "reason": "exit_reason",
        }
    )
    completed = round_trips[
        ["entry_number", "side", "holding_sessions", "pnl", "return_pct"]
    ].rename(columns={"pnl": "realized_pnl"})
    details = entries.merge(exits, on=["entry_number", "side"], how="left")
    details = details.merge(completed, on=["entry_number", "side"], how="left")

    level_columns = [
        "Date",
        "target",
        "fixed_stop",
        "trailing_pct",
        "short_target",
        "short_fixed_stop",
    ]
    entry_levels = equity_curve[level_columns].rename(
        columns={
            "Date": "entry_date",
            "target": "long_target_at_entry",
            "fixed_stop": "long_stop_at_entry",
            "trailing_pct": "trailing_stop_pct",
            "short_target": "short_target_at_entry",
            "short_fixed_stop": "short_stop_at_entry",
        }
    )
    details = details.merge(entry_levels, on="entry_date", how="left")
    is_long = details["side"] == "LONG"
    details["planned_target_price"] = details["long_target_at_entry"].where(
        is_long, details["short_target_at_entry"]
    )
    details["planned_stop_price"] = details["long_stop_at_entry"].where(
        is_long, details["short_stop_at_entry"]
    )
    details["planned_risk_per_share"] = (
        details["entry_price"] - details["planned_stop_price"]
    ).where(
        is_long,
        details["planned_stop_price"] - details["entry_price"],
    )
    details["planned_reward_per_share"] = (
        details["planned_target_price"] - details["entry_price"]
    ).where(
        is_long,
        details["entry_price"] - details["planned_target_price"],
    )
    details["planned_reward_risk_ratio"] = (
        details["planned_reward_per_share"] / details["planned_risk_per_share"]
    ).where(details["planned_risk_per_share"] > 0)
    details["status"] = details["exit_date"].notna().map(
        {True: "CLOSED", False: "OPEN"}
    )
    details["buy_date"] = details["entry_date"].where(
        is_long, details["exit_date"]
    )
    details["buy_price"] = details["entry_price"].where(
        is_long, details["exit_price"]
    )
    details["sell_date"] = details["exit_date"].where(
        is_long, details["entry_date"]
    )
    details["sell_price"] = details["exit_price"].where(
        is_long, details["entry_price"]
    )
    details["ticker"] = ticker
    details = details.rename(columns={"entry_number": "trade_number"})

    return details[
        [
            "trade_number",
            "ticker",
            "side",
            "status",
            "buy_date",
            "buy_price",
            "sell_date",
            "sell_price",
            "entry_action",
            "entry_date",
            "entry_price",
            "shares",
            "entry_notional",
            "entry_commission",
            "cash_after_entry",
            "planned_target_price",
            "planned_stop_price",
            "trailing_stop_pct",
            "planned_risk_per_share",
            "planned_reward_per_share",
            "planned_reward_risk_ratio",
            "exit_action",
            "exit_date",
            "exit_price",
            "exit_notional",
            "exit_commission",
            "cash_after_exit",
            "exit_reason",
            "holding_sessions",
            "realized_pnl",
            "return_pct",
        ]
    ].sort_values("trade_number", ignore_index=True)


def export_backtest_results(
    backtest,
    ticker,
    start_date,
    output_dir="output",
):
    """Export summary, order, trade, equity, and trade-detail CSV files."""
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    summary = backtest["summary"]
    transactions = backtest["transactions"]
    round_trips = backtest["round_trips"]
    equity_curve = backtest["equity_curve"]
    trade_details = prepare_trade_details(backtest, ticker)
    stem = (
        f"backtest_{clean_symbol_for_filename(ticker)}_"
        f"{pd.Timestamp(start_date):%Y%m%d}_{equity_curve['Date'].max():%Y%m%d}"
    )
    files = {
        "summary": output_path / f"{stem}_summary.csv",
        "orders": output_path / f"{stem}_orders.csv",
        "trades": output_path / f"{stem}_trades.csv",
        "equity": output_path / f"{stem}_equity.csv",
        "trade_details": output_path / f"{stem}_trade_details.csv",
    }
    summary.to_csv(files["summary"], index=False)
    transactions.to_csv(files["orders"], index=False)
    round_trips.to_csv(files["trades"], index=False)
    equity_curve.to_csv(files["equity"], index=False)
    trade_details.to_csv(files["trade_details"], index=False)
    return files


def plot_backtest_results(
    backtest,
    benchmark_history,
    ticker,
    index_name,
    benchmark_ticker,
    buy_price,
    sell_price,
    stop_loss,
    initial_capital,
    chart_output_dir=None,
):
    """Build the price, equity, and drawdown chart and optionally save it."""
    prices = backtest["price_history"]
    summary = backtest["summary"]
    transactions = backtest["transactions"]
    round_trips = backtest["round_trips"]
    equity_curve = backtest["equity_curve"]
    shorts_enabled = backtest.get("allow_short", False)
    summary_row = summary.iloc[0]
    long_entries = transactions[transactions["action"] == "BUY"]
    short_entries = transactions[transactions["action"] == "SELL_SHORT"]
    exit_orders = transactions[
        transactions["action"].isin(["SELL", "BUY_TO_COVER"])
    ]

    fig, (ax_price, ax_equity, ax_drawdown) = plt.subplots(
        3,
        1,
        figsize=(16, 12),
        sharex=True,
        gridspec_kw={"height_ratios": [2.2, 1.4, 0.8]},
    )

    ax_price.plot(
        prices["Date"],
        prices["Close"],
        color="#1769aa",
        linewidth=2,
        label=f"{ticker} close",
    )
    ax_price.fill_between(
        prices["Date"],
        prices["Low"],
        prices["High"],
        color="#1769aa",
        alpha=0.10,
        label="Daily low-high",
    )
    _plot_price_level(
        ax_price,
        prices["Date"],
        buy_price,
        "#2e7d32",
        "Dynamic buy / short target" if shorts_enabled else "Dynamic buy",
    )
    _plot_price_level(
        ax_price,
        prices["Date"],
        sell_price,
        "#6a1b9a",
        "Dynamic sell / short entry" if shorts_enabled else "Dynamic target",
    )
    _plot_price_level(
        ax_price,
        prices["Date"],
        stop_loss,
        "#c62828",
        "Dynamic long stop",
        linestyle=":",
    )
    if shorts_enabled and "short_fixed_stop" in equity_curve:
        _plot_price_level(
            ax_price,
            prices["Date"],
            equity_curve["short_fixed_stop"],
            "#ef6c00",
            "Dynamic short stop",
            linestyle=":",
        )
    if not long_entries.empty:
        ax_price.scatter(
            long_entries["Date"],
            long_entries["fill_price"],
            marker="^",
            s=100,
            color="#2e7d32",
            edgecolor="white",
            linewidth=0.8,
            zorder=5,
            label="Long entry",
        )
    if not short_entries.empty:
        ax_price.scatter(
            short_entries["Date"],
            short_entries["fill_price"],
            marker="v",
            s=100,
            color="#ef6c00",
            edgecolor="white",
            linewidth=0.8,
            zorder=5,
            label="Short entry",
        )
    if not exit_orders.empty:
        target_exits = exit_orders["reason"] == "TARGET"
        ax_price.scatter(
            exit_orders.loc[target_exits, "Date"],
            exit_orders.loc[target_exits, "fill_price"],
            marker="x",
            s=100,
            color="#6a1b9a",
            linewidth=2,
            zorder=5,
            label="Target exit",
        )
        ax_price.scatter(
            exit_orders.loc[~target_exits, "Date"],
            exit_orders.loc[~target_exits, "fill_price"],
            marker="x",
            s=100,
            color="#c62828",
            linewidth=2,
            zorder=5,
            label="Risk/time exit",
        )
    for _, trade in round_trips.iterrows():
        ax_price.axvspan(
            trade["entry_date"],
            trade["exit_date"],
            color="#43a047",
            alpha=0.06,
        )
    ax_price.set_ylabel(f"{ticker} price")
    ax_price.legend(loc="upper left", ncol=2)

    ax_benchmark = ax_price.twinx()
    benchmark_price_column = (
        "Adj Close"
        if "Adj Close" in benchmark_history
        and benchmark_history["Adj Close"].notna().any()
        else "Close"
    )
    benchmark_plot = benchmark_history.set_index("Date")[benchmark_price_column].sort_index()
    benchmark_plot = benchmark_plot.loc[
        (benchmark_plot.index >= prices["Date"].min())
        & (benchmark_plot.index <= prices["Date"].max())
    ]
    if not benchmark_plot.empty:
        benchmark_return_line = (benchmark_plot / benchmark_plot.iloc[0] - 1) * 100
        ax_benchmark.plot(
            benchmark_return_line.index,
            benchmark_return_line,
            color="#616161",
            linestyle=":",
            linewidth=1.6,
            label=f"{index_name} ({benchmark_ticker}) return",
        )
        ax_benchmark.set_ylabel("Benchmark return (%)", color="#616161")
        ax_benchmark.legend(loc="upper right")

    ax_equity.plot(
        equity_curve["Date"],
        equity_curve["strategy_equity"],
        color="#111111",
        linewidth=2.2,
        label="Strategy",
    )
    ax_equity.plot(
        equity_curve["Date"],
        equity_curve["buy_hold_equity"],
        color="#1769aa",
        linewidth=1.7,
        label=f"Buy & hold {ticker}",
    )
    if equity_curve["benchmark_equity"].notna().any():
        ax_equity.plot(
            equity_curve["Date"],
            equity_curve["benchmark_equity"],
            color="#616161",
            linestyle="--",
            linewidth=1.5,
            label=f"{index_name} benchmark",
        )
    ax_equity.axhline(
        initial_capital,
        color="#9e9e9e",
        linewidth=1,
        linestyle=":",
    )
    ax_equity.set_ylabel("Portfolio value")
    ax_equity.legend(loc="upper left")

    ax_drawdown.fill_between(
        equity_curve["Date"],
        equity_curve["drawdown_pct"],
        0,
        color="#c62828",
        alpha=0.30,
    )
    ax_drawdown.plot(
        equity_curve["Date"],
        equity_curve["drawdown_pct"],
        color="#c62828",
        linewidth=1,
    )
    ax_drawdown.set_ylabel("Drawdown %")
    ax_drawdown.set_xlabel("Date")

    fig.suptitle(
        f"{ticker} price-intent backtest | "
        f"Strategy {summary_row['strategy_return_pct']:.2f}% | "
        f"Buy & hold {summary_row['buy_hold_return_pct']:.2f}% | "
        f"Max DD {summary_row['max_drawdown_pct']:.2f}%",
        fontsize=15,
    )
    fig.tight_layout(rect=[0, 0, 1, 0.96])

    chart_file = None
    if chart_output_dir is not None:
        output_path = Path(chart_output_dir)
        output_path.mkdir(parents=True, exist_ok=True)
        chart_file = output_path / f"backtest_{clean_symbol_for_filename(ticker)}.png"
        fig.savefig(chart_file, dpi=160, bbox_inches="tight")

    return fig, chart_file
