import numpy as np
import pandas as pd

import analysis_functions.fundamental_analysis as fundamental_analysis
import analysis_functions.multifactor_analysis as multifactor_analysis
import analysis_functions.sentiment_analysis as sentiment_analysis
import analysis_functions.technical_analysis as technical_analysis
import config.config as config
import dao.dao as dao
from config.logging_config import get_logger

logger = get_logger(__name__)

MISSING_VALUE = "--"
VALUE_STAT_KEYS = [
    "Market Cap",
    "Enterprise Value",
    "Trailing P/E",
    "Forward P/E",
    "PEG Ratio (5yr expected)",
    "Price/Sales",
    "Price/Book",
    "Enterprise Value/Revenue",
    "Enterprise Value/EBITDA",
    "Target Mean Price",
    "TICKER",
]

FUNDAMENTAL_SCORE_KEYS = [
    key for key in VALUE_STAT_KEYS if key not in {"TICKER", "Target Mean Price"}
]


def _normalize_numeric_or_missing(value):
    if value is None or (
        isinstance(value, str) and value.strip() in {"", MISSING_VALUE}
    ):
        return MISSING_VALUE
    try:
        if pd.isna(value):
            return MISSING_VALUE
    except Exception:
        pass
    try:
        return float(value)
    except Exception:
        return MISSING_VALUE


def _safe_ratio(numerator, denominator, fallback=MISSING_VALUE):
    numerator = _normalize_numeric_or_missing(numerator)
    denominator = _normalize_numeric_or_missing(denominator)
    if numerator == MISSING_VALUE or denominator == MISSING_VALUE:
        return fallback
    denominator = float(denominator)
    if denominator <= 0:
        return fallback
    return float(numerator) / denominator


def _safe_growth(current_value, previous_value, fallback=MISSING_VALUE):
    current_value = _normalize_numeric_or_missing(current_value)
    previous_value = _normalize_numeric_or_missing(previous_value)
    if current_value == MISSING_VALUE or previous_value == MISSING_VALUE:
        return fallback
    previous_value = float(previous_value)
    if previous_value == 0:
        return fallback
    return (float(current_value) - previous_value) / abs(previous_value)


def _normalize_trade_date(value):
    parsed = pd.to_datetime(value, errors="coerce")
    if pd.isna(parsed):
        return pd.NaT
    timestamp = pd.Timestamp(parsed)
    if timestamp.tzinfo is not None:
        timestamp = timestamp.tz_localize(None)
    return timestamp.normalize()


def _empty_value_stats(ticker):
    stats = {key: MISSING_VALUE for key in VALUE_STAT_KEYS}
    stats["TICKER"] = ticker
    return stats


def _metric_coverage(metrics, keys):
    if metrics is None:
        return 0.0
    available = sum(
        _normalize_numeric_or_missing(metrics.get(key, MISSING_VALUE)) != MISSING_VALUE
        for key in keys
    )
    return available / len(keys) if keys else 0.0


def _prepare_snapshot_frame(snapshot_frame):
    if snapshot_frame is None or snapshot_frame.empty:
        return pd.DataFrame()
    prepared = snapshot_frame.copy()
    prepared["available_from"] = pd.to_datetime(prepared["available_from"], errors="coerce")
    prepared["report_date"] = pd.to_datetime(prepared["report_date"], errors="coerce")
    prepared["available_from"] = prepared["available_from"].apply(
        lambda value: value.tz_localize(None) if pd.notna(value) and getattr(value, "tzinfo", None) is not None else value
    )
    prepared["report_date"] = prepared["report_date"].apply(
        lambda value: value.tz_localize(None) if pd.notna(value) and getattr(value, "tzinfo", None) is not None else value
    )
    prepared = prepared.dropna(subset=["available_from"]).sort_values("available_from").reset_index(drop=True)
    return prepared


def _snapshot_for_date(snapshot_frame, trade_date):
    if snapshot_frame is None or snapshot_frame.empty or pd.isna(trade_date):
        return None
    eligible = snapshot_frame[snapshot_frame["available_from"] <= trade_date]
    if eligible.empty:
        return None
    return eligible.iloc[-1]


def _build_point_in_time_value_stats(snapshot_row, trade_row):
    ticker = snapshot_row.get("TICKER", trade_row.get("TICKER", "UNKNOWN"))
    close_price = _normalize_numeric_or_missing(trade_row.get("Close"))
    shares_outstanding = _normalize_numeric_or_missing(snapshot_row.get("Shares Outstanding"))
    total_debt = _normalize_numeric_or_missing(snapshot_row.get("Total Debt"))
    cash_and_equivalents = _normalize_numeric_or_missing(snapshot_row.get("Cash And Equivalents"))
    net_income = _normalize_numeric_or_missing(snapshot_row.get("Net Income"))
    total_revenue = _normalize_numeric_or_missing(snapshot_row.get("Total Revenue"))
    stockholders_equity = _normalize_numeric_or_missing(snapshot_row.get("Stockholders Equity"))
    ebitda = _normalize_numeric_or_missing(snapshot_row.get("EBITDA"))

    market_cap = MISSING_VALUE
    if close_price != MISSING_VALUE and shares_outstanding != MISSING_VALUE:
        market_cap = float(close_price) * float(shares_outstanding)

    enterprise_value = MISSING_VALUE
    if market_cap != MISSING_VALUE and total_debt != MISSING_VALUE:
        enterprise_value = float(market_cap) + float(total_debt)
        if cash_and_equivalents != MISSING_VALUE:
            enterprise_value -= float(cash_and_equivalents)

    trailing_pe = MISSING_VALUE
    if market_cap != MISSING_VALUE and net_income != MISSING_VALUE and float(net_income) > 0:
        trailing_pe = float(market_cap) / float(net_income)

    return {
        "TICKER": ticker,
        "Market Cap": market_cap,
        "Enterprise Value": enterprise_value,
        "Trailing P/E": trailing_pe,
        "Forward P/E": MISSING_VALUE,
        "PEG Ratio (5yr expected)": MISSING_VALUE,
        "Price/Sales": _safe_ratio(market_cap, total_revenue),
        "Price/Book": _safe_ratio(market_cap, stockholders_equity),
        "Enterprise Value/Revenue": _safe_ratio(enterprise_value, total_revenue),
        "Enterprise Value/EBITDA": _safe_ratio(enterprise_value, ebitda),
    }


def _build_point_in_time_raw_advanced_data(snapshot_row, price_window):
    latest_row = price_window.iloc[-1]
    ticker = snapshot_row.get("TICKER", latest_row.get("TICKER", "UNKNOWN"))
    close_price = _normalize_numeric_or_missing(latest_row.get("Close"))
    shares_outstanding = _normalize_numeric_or_missing(snapshot_row.get("Shares Outstanding"))
    market_cap = MISSING_VALUE
    if close_price != MISSING_VALUE and shares_outstanding != MISSING_VALUE:
        market_cap = float(close_price) * float(shares_outstanding)

    recent_volume = pd.to_numeric(price_window["Volume"], errors="coerce").dropna().tail(20)
    average_daily_volume = recent_volume.mean() if not recent_volume.empty else MISSING_VALUE

    total_revenue = _normalize_numeric_or_missing(snapshot_row.get("Total Revenue"))
    previous_total_revenue = _normalize_numeric_or_missing(snapshot_row.get("Previous Total Revenue"))
    operating_income = _normalize_numeric_or_missing(snapshot_row.get("Operating Income"))
    stockholders_equity = _normalize_numeric_or_missing(snapshot_row.get("Stockholders Equity"))
    current_assets = _normalize_numeric_or_missing(snapshot_row.get("Current Assets"))
    current_liabilities = _normalize_numeric_or_missing(snapshot_row.get("Current Liabilities"))
    inventory = _normalize_numeric_or_missing(snapshot_row.get("Inventory"))
    total_debt = _normalize_numeric_or_missing(snapshot_row.get("Total Debt"))
    gross_profit = _normalize_numeric_or_missing(snapshot_row.get("Gross Profit"))
    net_income = _normalize_numeric_or_missing(snapshot_row.get("Net Income"))

    raw_data = dict(snapshot_row.to_dict())
    raw_data.update({
        "TICKER": ticker,
        "Average Daily Volume": average_daily_volume,
        "Market Cap": market_cap,
        "Current Price": close_price,
        "Revenue Growth Raw": _safe_growth(total_revenue, previous_total_revenue),
        "Current Ratio Raw": _safe_ratio(current_assets, current_liabilities),
        "Quick Ratio Raw": _safe_ratio(
            float(current_assets) - float(inventory)
            if current_assets != MISSING_VALUE and inventory != MISSING_VALUE
            else MISSING_VALUE,
            current_liabilities,
        ),
        "Debt To Equity Raw": _safe_ratio(total_debt, stockholders_equity),
        "Return On Equity Raw": _safe_ratio(net_income, stockholders_equity),
        "Gross Margins Raw": _safe_ratio(gross_profit, total_revenue),
        "Operating Margins Raw": _safe_ratio(operating_income, total_revenue),
        "Bid": MISSING_VALUE,
        "Ask": MISSING_VALUE,
        "Analyst Recommendation Score": MISSING_VALUE,
        "Target Mean Price": MISSING_VALUE,
        "Beta": MISSING_VALUE,
    })
    return raw_data


def get_prediction(
    df,
    stats=None,
    include_sentiment=True,
    historical_analysis=True,
    allow_latest_fallback=True,
):
    ticker = df["TICKER"].iloc[0] if not df.empty and "TICKER" in df.columns else "UNKNOWN"
    logger.info("Building prediction frame. ticker=%s include_sentiment=%s rows=%s", ticker, include_sentiment, len(df))

    if df is None or df.empty:
        raise ValueError(f"No price history is available for {ticker}")
    required_columns = {"Date", "Open", "High", "Low", "Close", "Adj Close", "Volume", "TICKER"}
    missing_columns = required_columns.difference(df.columns)
    if missing_columns:
        raise ValueError(
            "Price history is missing required columns: " + ", ".join(sorted(missing_columns))
        )

    df = df.copy()
    df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
    df["Date"] = df["Date"].apply(
        lambda value: value.tz_localize(None) if pd.notna(value) and getattr(value, "tzinfo", None) is not None else value
    )
    df = df.dropna(subset=["Date"]).sort_values("Date").reset_index(drop=True)
    df = technical_analysis.get_technical_analysis_calculations(df)
    df["technical_analysis_buy_score"] = df.apply(technical_analysis.calculate_buy_score, axis=1)
    df["technical_analysis_sell_score"] = df.apply(technical_analysis.calculate_sell_score, axis=1)

    snapshot_frame = _prepare_snapshot_frame(dao.get_point_in_time_financial_snapshots(ticker))
    latest_sentiment_score = sentiment_analysis.apply_sentiment_analysis(ticker) if include_sentiment else None

    fallback_stats = stats.to_dict() if hasattr(stats, "to_dict") else _empty_value_stats(ticker)
    fallback_raw_advanced = None
    if snapshot_frame.empty and allow_latest_fallback:
        try:
            fallback_raw_advanced = dao.get_advanced_financial_metrics(ticker).iloc[0].to_dict()
        except Exception:
            logger.exception("Failed to load fallback advanced metrics. ticker=%s", ticker)
            fallback_raw_advanced = {"TICKER": ticker}

    row_count = len(df)
    fundamental_scores = [np.nan] * row_count
    fundamental_coverage = [0.0] * row_count
    sentiment_scores = [np.nan] * row_count
    multifactor_scores = [np.nan] * row_count
    multifactor_coverage = [0.0] * row_count
    snapshot_report_dates = [pd.NaT] * row_count
    snapshot_available_from_dates = [pd.NaT] * row_count
    snapshot_frequencies = [None] * row_count
    analysis_indexes = range(row_count) if historical_analysis else [row_count - 1]

    for index in analysis_indexes:
        row = df.iloc[index]
        trade_date = _normalize_trade_date(row["Date"])
        snapshot_row = _snapshot_for_date(snapshot_frame, trade_date)
        price_window = df.iloc[:index + 1].copy()
        row_sentiment_score = (
            float(latest_sentiment_score)
            if include_sentiment and index == row_count - 1
            else None
        )

        if snapshot_row is not None:
            value_stats = _build_point_in_time_value_stats(snapshot_row, row)
            raw_advanced_data = _build_point_in_time_raw_advanced_data(snapshot_row, price_window)
            snapshot_report_dates[index] = snapshot_row["report_date"]
            snapshot_available_from_dates[index] = snapshot_row["available_from"]
            snapshot_frequencies[index] = snapshot_row.get("statement_frequency")
        elif fallback_raw_advanced is not None and index == row_count - 1:
            value_stats = fallback_stats
            raw_advanced_data = fallback_raw_advanced
        else:
            value_stats = _empty_value_stats(ticker)
            raw_advanced_data = {"TICKER": ticker}

        advanced_stats = multifactor_analysis.derive_advanced_financial_metrics(raw_advanced_data).iloc[0]
        multifactor_result = multifactor_analysis.calculate_multifactor_model(
            price_history=price_window,
            value_stats=value_stats,
            advanced_stats=advanced_stats,
            sentiment_score=row_sentiment_score,
        )

        current_fundamental_coverage = _metric_coverage(value_stats, FUNDAMENTAL_SCORE_KEYS)
        if current_fundamental_coverage > 0:
            fundamental_scores[index] = fundamental_analysis.get_fundamental_analysis(value_stats)
        fundamental_coverage[index] = current_fundamental_coverage
        if row_sentiment_score is not None:
            sentiment_scores[index] = row_sentiment_score
        multifactor_scores[index] = multifactor_result["final_score"]
        multifactor_coverage[index] = multifactor_result["factor_coverage"]

    df["fundamental_snapshot_report_date"] = snapshot_report_dates
    df["fundamental_snapshot_available_from"] = snapshot_available_from_dates
    df["fundamental_snapshot_frequency"] = snapshot_frequencies
    df["fundamental_analysis_score"] = fundamental_scores
    df["fundamental_analysis_coverage"] = fundamental_coverage
    df["sentiment_analysis_score"] = sentiment_scores
    df["multifactor_analysis_score"] = multifactor_scores
    df["multifactor_analysis_coverage"] = multifactor_coverage
    df = df.sort_values("Date", ascending=False).reset_index(drop=True)
    logger.info("Built prediction frame successfully. ticker=%s", ticker)
    return df


def add_total_signal(df):
    ticker = df["TICKER"].iloc[0] if not df.empty and "TICKER" in df.columns else "UNKNOWN"
    logger.info("Calculating total signal. ticker=%s", ticker)
    technical_signal = df["technical_analysis_buy_score"] + df["technical_analysis_sell_score"]
    sentiment_signal = df["sentiment_analysis_score"]
    fundamental_signal = df["fundamental_analysis_score"]
    multifactor_signal = df["multifactor_analysis_score"]

    total_weight = (
        config.TECHNICAL_SIGNAL_WEIGHT
        + config.SENTIMENT_SIGNAL_WEIGHT
        + config.FUNDAMENTAL_SIGNAL_WEIGHT
        + config.MULTIFACTOR_SIGNAL_WEIGHT
    )

    technical_coverage = (
        pd.to_numeric(df["technical_data_available"], errors="coerce").fillna(0).astype(float)
        if "technical_data_available" in df.columns
        else technical_signal.notna().astype(float)
    )
    fundamental_component_coverage = (
        pd.to_numeric(df["fundamental_analysis_coverage"], errors="coerce").fillna(0).clip(0, 1)
        if "fundamental_analysis_coverage" in df.columns
        else fundamental_signal.notna().astype(float)
    )
    multifactor_component_coverage = (
        pd.to_numeric(df["multifactor_analysis_coverage"], errors="coerce").fillna(0).clip(0, 1)
        if "multifactor_analysis_coverage" in df.columns
        else multifactor_signal.notna().astype(float)
    )
    components = [
        (
            technical_signal,
            technical_coverage,
            config.TECHNICAL_SIGNAL_WEIGHT,
        ),
        (
            sentiment_signal,
            sentiment_signal.notna().astype(float),
            config.SENTIMENT_SIGNAL_WEIGHT,
        ),
        (
            fundamental_signal,
            fundamental_component_coverage,
            config.FUNDAMENTAL_SIGNAL_WEIGHT,
        ),
        (
            multifactor_signal,
            multifactor_component_coverage,
            config.MULTIFACTOR_SIGNAL_WEIGHT,
        ),
    ]
    numerator = pd.Series(0.0, index=df.index)
    available_weight = pd.Series(0.0, index=df.index)
    for signal, coverage, configured_weight in components:
        if configured_weight <= 0:
            continue
        valid = signal.notna() & coverage.gt(0)
        effective_weight = configured_weight * coverage.where(valid, 0.0)
        numerator = numerator + signal.fillna(0.0) * effective_weight
        available_weight = available_weight + effective_weight

    df["signal_coverage"] = available_weight / total_weight
    df["Signal"] = (numerator / available_weight.replace(0, np.nan)).where(
        df["signal_coverage"] >= config.MIN_SIGNAL_COVERAGE
    )
    df["analysis_status"] = np.select(
        [
            available_weight.eq(0),
            df["signal_coverage"] < config.MIN_SIGNAL_COVERAGE,
            df["signal_coverage"] < 0.999999,
        ],
        ["NO_DATA", "INSUFFICIENT_DATA", "PARTIAL"],
        default="COMPLETE",
    )
    logger.info("Calculated total signal successfully. ticker=%s", ticker)
    return df


def convert_signal_to_text(df):
    ticker = df["TICKER"].iloc[0] if not df.empty and "TICKER" in df.columns else "UNKNOWN"
    conditions = [
        (df["Signal"] <= config.STRONG_SELL_THRESHOLD),
        (df["Signal"] > config.STRONG_SELL_THRESHOLD) & (df["Signal"] < config.WEAK_SELL_THRESHOLD),
        (df["Signal"] >= config.WEAK_SELL_THRESHOLD) & (df["Signal"] < config.WEAK_BUY_THRESHOLD),
        (df["Signal"] >= config.WEAK_BUY_THRESHOLD) & (df["Signal"] < config.STRONG_BUY_THRESHOLD),
        (df["Signal"] >= config.STRONG_BUY_THRESHOLD),
    ]
    choices = ["STRONG SELL", "WEAK SELL", "HOLD", "WEAK BUY", "STRONG BUY"]
    df["Signal_Text"] = np.select(conditions, choices, default="INSUFFICIENT DATA")
    logger.info("Converted numeric signals to text labels. ticker=%s", ticker)
    return df
