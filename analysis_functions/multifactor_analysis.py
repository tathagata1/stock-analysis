import math
import json

import pandas as pd

import analysis_functions.fundamental_analysis as fundamental_analysis
import analysis_functions.sentiment_analysis as sentiment_analysis
import dao.dao as dao

from config.logging_config import get_logger

logger = get_logger(__name__)

MISSING_VALUE = "--"
TRADING_DAYS_PER_YEAR = 252
MULTIFACTOR_WEIGHTS = {
    "value": 0.30,
    "quality": 0.20,
    "momentum": 0.20,
    "sentiment": 0.15,
    "risk": 0.10,
    "liquidity": 0.05,
}


def _normalize_numeric_or_missing(value):
    if value in (None, "", MISSING_VALUE):
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


def _get_metric(metrics, key):
    if metrics is None:
        return MISSING_VALUE
    if hasattr(metrics, "get"):
        return metrics.get(key, MISSING_VALUE)
    return MISSING_VALUE


def _clamp_score(score, lower=-1.0, upper=1.0):
    if score < lower:
        return lower
    if score > upper:
        return upper
    return score


def _sum_modifiers(*modifiers):
    total = 0.0
    for modifier in modifiers:
        normalized = _normalize_numeric_or_missing(modifier)
        if normalized == MISSING_VALUE:
            continue
        total += float(normalized)
    return total


def _safe_info_value(info, *keys):
    for key in keys:
        normalized = _normalize_numeric_or_missing(info.get(key))
        if normalized != MISSING_VALUE:
            return normalized
    return MISSING_VALUE


def _safe_statement_frame(ticker, *attr_names):
    for attr_name in attr_names:
        try:
            value = getattr(ticker, attr_name, None)
            if callable(value):
                value = value()
            if isinstance(value, pd.DataFrame) and not value.empty:
                return value
        except Exception:
            logger.exception("Failed to load statement frame. attr_name=%s", attr_name)
    return pd.DataFrame()


def _matching_row(frame, candidates):
    if frame is None or frame.empty:
        return None

    normalized_index = {str(index).strip().lower(): index for index in frame.index}
    for candidate in candidates:
        exact_match = normalized_index.get(candidate.strip().lower())
        if exact_match is not None:
            return frame.loc[exact_match]

    for candidate in candidates:
        candidate_normalized = candidate.strip().lower()
        for index in frame.index:
            index_normalized = str(index).strip().lower()
            if candidate_normalized in index_normalized or index_normalized in candidate_normalized:
                return frame.loc[index]
    return None


def _row_numeric_values(frame, candidates):
    row = _matching_row(frame, candidates)
    if row is None:
        return []

    values = []
    for value in row.tolist():
        normalized = _normalize_numeric_or_missing(value)
        if normalized != MISSING_VALUE:
            values.append(float(normalized))
    return values


def _latest_value(frame, candidates, fallback=MISSING_VALUE):
    values = _row_numeric_values(frame, candidates)
    if not values:
        return fallback
    return values[0]


def _previous_value(frame, candidates, offset=1, fallback=MISSING_VALUE):
    values = _row_numeric_values(frame, candidates)
    if len(values) <= offset:
        return fallback
    return values[offset]


def _safe_ratio(numerator, denominator, fallback=MISSING_VALUE):
    normalized_numerator = _normalize_numeric_or_missing(numerator)
    normalized_denominator = _normalize_numeric_or_missing(denominator)
    if normalized_numerator == MISSING_VALUE or normalized_denominator == MISSING_VALUE:
        return fallback
    denominator_value = float(normalized_denominator)
    if denominator_value == 0:
        return fallback
    return float(normalized_numerator) / denominator_value


def _safe_growth(current_value, previous_value, fallback=MISSING_VALUE):
    normalized_current = _normalize_numeric_or_missing(current_value)
    normalized_previous = _normalize_numeric_or_missing(previous_value)
    if normalized_current == MISSING_VALUE or normalized_previous == MISSING_VALUE:
        return fallback
    previous_value = float(normalized_previous)
    if previous_value == 0:
        return fallback
    return (float(normalized_current) - previous_value) / abs(previous_value)


def _safe_cagr(current_value, base_value, periods, fallback=MISSING_VALUE):
    normalized_current = _normalize_numeric_or_missing(current_value)
    normalized_base = _normalize_numeric_or_missing(base_value)
    if normalized_current == MISSING_VALUE or normalized_base == MISSING_VALUE:
        return fallback
    if periods <= 0:
        return fallback
    current_float = float(normalized_current)
    base_float = float(normalized_base)
    if current_float <= 0 or base_float <= 0:
        return fallback
    return (current_float / base_float) ** (1 / periods) - 1


def _safe_tax_rate(tax_provision, pretax_income):
    tax_rate = _safe_ratio(tax_provision, pretax_income, fallback=0.21)
    if tax_rate == MISSING_VALUE:
        return 0.21
    return max(0.0, min(float(tax_rate), 0.35))


def _build_piotroski_score(net_income, operating_cash_flow, roa, previous_roa, long_term_debt, previous_long_term_debt, current_ratio, previous_current_ratio, gross_margin, previous_gross_margin, asset_turnover, previous_asset_turnover):
    metrics = [
        net_income,
        operating_cash_flow,
        roa,
        previous_roa,
        long_term_debt,
        previous_long_term_debt,
        current_ratio,
        previous_current_ratio,
        gross_margin,
        previous_gross_margin,
        asset_turnover,
        previous_asset_turnover,
    ]
    if any(_normalize_numeric_or_missing(metric) == MISSING_VALUE for metric in metrics):
        return MISSING_VALUE

    score = 0
    score += int(float(net_income) > 0)
    score += int(float(operating_cash_flow) > 0)
    score += int(float(roa) > 0)
    score += int(float(operating_cash_flow) > float(net_income))
    score += int(float(roa) > float(previous_roa))
    score += int(float(long_term_debt) <= float(previous_long_term_debt))
    score += int(float(current_ratio) > float(previous_current_ratio))
    score += int(float(gross_margin) > float(previous_gross_margin))
    score += int(float(asset_turnover) > float(previous_asset_turnover))
    return float(score)


def _build_altman_z_score(working_capital, retained_earnings, ebit, market_cap, total_liabilities, revenue, total_assets):
    metrics = [working_capital, retained_earnings, ebit, market_cap, total_liabilities, revenue, total_assets]
    if any(_normalize_numeric_or_missing(metric) == MISSING_VALUE for metric in metrics):
        return MISSING_VALUE

    total_assets = float(total_assets)
    total_liabilities = float(total_liabilities)
    if total_assets == 0 or total_liabilities == 0:
        return MISSING_VALUE

    return (
        1.2 * (float(working_capital) / total_assets)
        + 1.4 * (float(retained_earnings) / total_assets)
        + 3.3 * (float(ebit) / total_assets)
        + 0.6 * (float(market_cap) / total_liabilities)
        + 1.0 * (float(revenue) / total_assets)
    )


def _prepare_price_history(price_history):
    history = price_history.copy()
    history["Date"] = pd.to_datetime(history["Date"], errors="coerce")
    history["Close"] = pd.to_numeric(history["Close"], errors="coerce")
    if "Volume" in history.columns:
        history["Volume"] = pd.to_numeric(history["Volume"], errors="coerce")
    history = history.dropna(subset=["Date", "Close"]).sort_values("Date").reset_index(drop=True)
    return history


def _return_over_window(price_history, trading_days):
    history = _prepare_price_history(price_history)
    if len(history) <= trading_days:
        return MISSING_VALUE
    latest_close = history["Close"].iloc[-1]
    base_close = history["Close"].iloc[-(trading_days + 1)]
    if base_close == 0:
        return MISSING_VALUE
    return (latest_close - base_close) / base_close


def _historical_volatility(price_history):
    history = _prepare_price_history(price_history)
    returns = history["Close"].pct_change().dropna()
    if returns.empty:
        return MISSING_VALUE
    return float(returns.std() * math.sqrt(TRADING_DAYS_PER_YEAR))


def _maximum_drawdown(price_history):
    history = _prepare_price_history(price_history)
    if history.empty:
        return MISSING_VALUE
    rolling_peak = history["Close"].cummax()
    drawdowns = history["Close"] / rolling_peak - 1
    if drawdowns.empty:
        return MISSING_VALUE
    return float(drawdowns.min())


def _sharpe_ratio(price_history, risk_free_rate=0.02):
    history = _prepare_price_history(price_history)
    returns = history["Close"].pct_change().dropna()
    if returns.empty or returns.std() == 0:
        return MISSING_VALUE
    daily_risk_free_rate = risk_free_rate / TRADING_DAYS_PER_YEAR
    excess_returns = returns - daily_risk_free_rate
    if excess_returns.std() == 0:
        return MISSING_VALUE
    return float(math.sqrt(TRADING_DAYS_PER_YEAR) * excess_returns.mean() / excess_returns.std())


def _sortino_ratio(price_history, risk_free_rate=0.02):
    history = _prepare_price_history(price_history)
    returns = history["Close"].pct_change().dropna()
    if returns.empty:
        return MISSING_VALUE
    daily_risk_free_rate = risk_free_rate / TRADING_DAYS_PER_YEAR
    downside_returns = returns[returns < daily_risk_free_rate] - daily_risk_free_rate
    if downside_returns.empty or downside_returns.std() == 0:
        return MISSING_VALUE
    excess_returns = returns - daily_risk_free_rate
    return float(math.sqrt(TRADING_DAYS_PER_YEAR) * excess_returns.mean() / downside_returns.std())


def get_average_daily_volume_modifier(average_daily_volume):
    average_daily_volume = _normalize_numeric_or_missing(average_daily_volume)
    if average_daily_volume == MISSING_VALUE:
        return 0
    average_daily_volume = float(average_daily_volume)
    if average_daily_volume > 10_000_000:
        return 0.05
    if average_daily_volume > 2_000_000:
        return 0.025
    if average_daily_volume > 500_000:
        return 0
    if average_daily_volume > 100_000:
        return -0.05
    return -0.10


def get_volume_to_market_cap_modifier(volume_to_market_cap):
    volume_to_market_cap = _normalize_numeric_or_missing(volume_to_market_cap)
    if volume_to_market_cap == MISSING_VALUE:
        return 0
    volume_to_market_cap = float(volume_to_market_cap)
    if volume_to_market_cap > 0.02:
        return 0.05
    if volume_to_market_cap > 0.01:
        return 0.025
    if volume_to_market_cap > 0.003:
        return 0
    if volume_to_market_cap > 0.001:
        return -0.05
    return -0.10


def get_bid_ask_spread_modifier(bid_ask_spread):
    bid_ask_spread = _normalize_numeric_or_missing(bid_ask_spread)
    if bid_ask_spread == MISSING_VALUE:
        return 0
    bid_ask_spread = float(bid_ask_spread)
    if bid_ask_spread < 0.001:
        return 0.05
    if bid_ask_spread < 0.003:
        return 0.025
    if bid_ask_spread < 0.01:
        return 0
    if bid_ask_spread < 0.02:
        return -0.05
    return -0.10


def get_analyst_recommendation_modifier(analyst_recommendation_score):
    analyst_recommendation_score = _normalize_numeric_or_missing(analyst_recommendation_score)
    if analyst_recommendation_score == MISSING_VALUE:
        return 0
    analyst_recommendation_score = float(analyst_recommendation_score)
    if analyst_recommendation_score <= 1.8:
        return 0.10
    if analyst_recommendation_score <= 2.3:
        return 0.05
    if analyst_recommendation_score <= 3:
        return 0
    if analyst_recommendation_score <= 3.5:
        return -0.05
    return -0.10


def get_price_target_upside_modifier(price_target_upside):
    price_target_upside = _normalize_numeric_or_missing(price_target_upside)
    if price_target_upside == MISSING_VALUE:
        return 0
    price_target_upside = float(price_target_upside)
    if price_target_upside > 0.30:
        return 0.10
    if price_target_upside > 0.15:
        return 0.05
    if price_target_upside > 0:
        return 0
    if price_target_upside > -0.10:
        return -0.05
    return -0.10


def get_historical_volatility_modifier(historical_volatility):
    historical_volatility = _normalize_numeric_or_missing(historical_volatility)
    if historical_volatility == MISSING_VALUE:
        return 0
    historical_volatility = float(historical_volatility)
    if historical_volatility < 0.20:
        return 0.075
    if historical_volatility < 0.30:
        return 0.05
    if historical_volatility < 0.45:
        return 0
    if historical_volatility < 0.60:
        return -0.05
    return -0.10


def get_beta_modifier(beta):
    beta = _normalize_numeric_or_missing(beta)
    if beta == MISSING_VALUE:
        return 0
    beta = float(beta)
    if beta < 0.8:
        return 0.05
    if beta < 1.1:
        return 0.025
    if beta < 1.4:
        return 0
    if beta < 1.8:
        return -0.05
    return -0.10


def get_maximum_drawdown_modifier(maximum_drawdown):
    maximum_drawdown = _normalize_numeric_or_missing(maximum_drawdown)
    if maximum_drawdown == MISSING_VALUE:
        return 0
    maximum_drawdown = float(maximum_drawdown)
    if maximum_drawdown > -0.10:
        return 0.075
    if maximum_drawdown > -0.20:
        return 0.05
    if maximum_drawdown > -0.35:
        return 0
    if maximum_drawdown > -0.50:
        return -0.05
    return -0.10


def get_momentum_modifier(momentum_score):
    momentum_score = _normalize_numeric_or_missing(momentum_score)
    if momentum_score == MISSING_VALUE:
        return 0
    momentum_score = float(momentum_score)
    if momentum_score > 0.25:
        return 0.20
    if momentum_score > 0.10:
        return 0.10
    if momentum_score > 0:
        return 0.05
    if momentum_score > -0.10:
        return 0
    if momentum_score > -0.25:
        return -0.10
    return -0.20


def get_sharpe_ratio_modifier(sharpe_ratio):
    sharpe_ratio = _normalize_numeric_or_missing(sharpe_ratio)
    if sharpe_ratio == MISSING_VALUE:
        return 0
    sharpe_ratio = float(sharpe_ratio)
    if sharpe_ratio > 1.5:
        return 0.075
    if sharpe_ratio > 0.8:
        return 0.05
    if sharpe_ratio > 0:
        return 0
    if sharpe_ratio > -0.5:
        return -0.05
    return -0.10


def get_sortino_ratio_modifier(sortino_ratio):
    sortino_ratio = _normalize_numeric_or_missing(sortino_ratio)
    if sortino_ratio == MISSING_VALUE:
        return 0
    sortino_ratio = float(sortino_ratio)
    if sortino_ratio > 2:
        return 0.075
    if sortino_ratio > 1:
        return 0.05
    if sortino_ratio > 0:
        return 0
    if sortino_ratio > -0.5:
        return -0.05
    return -0.10


def get_price_momentum_factor_score(price_history):
    ticker = price_history["TICKER"].iloc[0] if not price_history.empty and "TICKER" in price_history.columns else "UNKNOWN"
    logger.info("Calculating price momentum score. ticker=%s", ticker)
    three_month_return = _return_over_window(price_history, 63)
    six_month_return = _return_over_window(price_history, 126)
    twelve_month_return = _return_over_window(price_history, 252)
    momentum_composite = _sum_modifiers(
        0.5 * twelve_month_return if _normalize_numeric_or_missing(twelve_month_return) != MISSING_VALUE else 0,
        0.3 * six_month_return if _normalize_numeric_or_missing(six_month_return) != MISSING_VALUE else 0,
        0.2 * three_month_return if _normalize_numeric_or_missing(three_month_return) != MISSING_VALUE else 0,
    )
    score = _clamp_score(get_momentum_modifier(momentum_composite))
    logger.info("Calculated price momentum score. ticker=%s score=%s", ticker, score)
    return {
        "score": score,
        "3_month_return": three_month_return,
        "6_month_return": six_month_return,
        "12_month_return": twelve_month_return,
        "momentum_composite": momentum_composite,
    }


def get_volatility_factor_score(price_history, advanced_stats):
    ticker = price_history["TICKER"].iloc[0] if not price_history.empty and "TICKER" in price_history.columns else "UNKNOWN"
    logger.info("Calculating volatility score. ticker=%s", ticker)
    historical_volatility = _historical_volatility(price_history)
    maximum_drawdown = _maximum_drawdown(price_history)
    sharpe_ratio = _sharpe_ratio(price_history)
    sortino_ratio = _sortino_ratio(price_history)
    score = _sum_modifiers(
        get_historical_volatility_modifier(historical_volatility),
        get_beta_modifier(_get_metric(advanced_stats, "Beta")),
        get_maximum_drawdown_modifier(maximum_drawdown),
        fundamental_analysis.get_altman_z_score_modifier(_get_metric(advanced_stats, "Altman Z-Score")),
        get_sharpe_ratio_modifier(sharpe_ratio),
        get_sortino_ratio_modifier(sortino_ratio),
    )
    score = _clamp_score(score)
    logger.info("Calculated volatility score. ticker=%s score=%s", ticker, score)
    return {
        "score": score,
        "historical_volatility": historical_volatility,
        "maximum_drawdown": maximum_drawdown,
        "sharpe_ratio": sharpe_ratio,
        "sortino_ratio": sortino_ratio,
    }


def get_institutional_liquidity_factor_score(advanced_stats):
    ticker = _get_metric(advanced_stats, "TICKER")
    logger.info("Calculating institutional liquidity score. ticker=%s", ticker)
    score = _sum_modifiers(
        get_average_daily_volume_modifier(_get_metric(advanced_stats, "Average Daily Volume")),
        get_volume_to_market_cap_modifier(_get_metric(advanced_stats, "Volume / Market Cap")),
        get_bid_ask_spread_modifier(_get_metric(advanced_stats, "Bid-Ask Spread")),
    )
    score = _clamp_score(score)
    logger.info("Calculated institutional liquidity score. ticker=%s score=%s", ticker, score)
    return score


def get_analyst_sentiment_factor_score(advanced_stats):
    ticker = _get_metric(advanced_stats, "TICKER")
    logger.info("Calculating analyst sentiment score. ticker=%s", ticker)
    score = _sum_modifiers(
        get_analyst_recommendation_modifier(_get_metric(advanced_stats, "Analyst Recommendation Score")),
        get_price_target_upside_modifier(_get_metric(advanced_stats, "Price Target Upside")),
    )
    score = _clamp_score(score)
    logger.info("Calculated analyst sentiment score. ticker=%s score=%s", ticker, score)
    return score


def calculate_multifactor_model(price_history, value_stats, advanced_stats, sentiment_score=0):
    ticker = price_history["TICKER"].iloc[0] if not price_history.empty and "TICKER" in price_history.columns else _get_metric(advanced_stats, "TICKER")
    logger.info("Calculating multi-factor model. ticker=%s", ticker)

    value_score = _clamp_score(fundamental_analysis.get_fundamental_analysis(value_stats))
    earnings_growth_score = fundamental_analysis.get_earnings_growth_factor_score(advanced_stats)
    quality_score = _clamp_score(fundamental_analysis.get_quality_factor_score(advanced_stats) + 0.5 * earnings_growth_score)
    momentum_details = get_price_momentum_factor_score(price_history)
    volatility_details = get_volatility_factor_score(price_history, advanced_stats)
    financial_strength_score = fundamental_analysis.get_financial_strength_factor_score(advanced_stats)
    analyst_sentiment_score = get_analyst_sentiment_factor_score(advanced_stats)
    combined_sentiment_score = _clamp_score((float(sentiment_score) * 0.7) + (analyst_sentiment_score * 0.3))
    risk_score = _clamp_score((volatility_details["score"] + financial_strength_score) / 2)
    liquidity_score = get_institutional_liquidity_factor_score(advanced_stats)

    final_score = _clamp_score(
        MULTIFACTOR_WEIGHTS["value"] * value_score
        + MULTIFACTOR_WEIGHTS["quality"] * quality_score
        + MULTIFACTOR_WEIGHTS["momentum"] * momentum_details["score"]
        + MULTIFACTOR_WEIGHTS["sentiment"] * combined_sentiment_score
        + MULTIFACTOR_WEIGHTS["risk"] * risk_score
        + MULTIFACTOR_WEIGHTS["liquidity"] * liquidity_score
    )

    result = {
        "ticker": ticker,
        "value_score": value_score,
        "quality_score": quality_score,
        "earnings_growth_score": earnings_growth_score,
        "momentum_score": momentum_details["score"],
        "sentiment_score": float(sentiment_score),
        "analyst_sentiment_score": analyst_sentiment_score,
        "combined_sentiment_score": combined_sentiment_score,
        "volatility_score": volatility_details["score"],
        "financial_strength_score": financial_strength_score,
        "risk_score": risk_score,
        "liquidity_score": liquidity_score,
        "final_score": final_score,
        "3_month_return": momentum_details["3_month_return"],
        "6_month_return": momentum_details["6_month_return"],
        "12_month_return": momentum_details["12_month_return"],
        "momentum_composite": momentum_details["momentum_composite"],
        "historical_volatility": volatility_details["historical_volatility"],
        "maximum_drawdown": volatility_details["maximum_drawdown"],
        "sharpe_ratio": volatility_details["sharpe_ratio"],
        "sortino_ratio": volatility_details["sortino_ratio"],
        "piotroski_f_score": _get_metric(advanced_stats, "Piotroski F-Score"),
        "altman_z_score": _get_metric(advanced_stats, "Altman Z-Score"),
    }
    logger.info("Calculated multi-factor model successfully. ticker=%s final_score=%s", ticker, final_score)
    return result


def calculate_multifactor_model_frame(price_history, value_stats, advanced_stats, sentiment_score=0):
    return pd.DataFrame([calculate_multifactor_model(price_history, value_stats, advanced_stats, sentiment_score=sentiment_score)])


def derive_advanced_financial_metrics(raw_financial_data):
    ticker = _get_metric(raw_financial_data, "TICKER")
    logger.info("Deriving advanced financial metrics. ticker=%s", ticker)

    total_revenue = _get_metric(raw_financial_data, "Total Revenue")
    previous_total_revenue = _get_metric(raw_financial_data, "Previous Total Revenue")
    ebitda = _get_metric(raw_financial_data, "EBITDA")
    previous_ebitda = _get_metric(raw_financial_data, "Previous EBITDA")
    operating_income = _get_metric(raw_financial_data, "Operating Income")
    pretax_income = _get_metric(raw_financial_data, "Pretax Income")
    tax_provision = _get_metric(raw_financial_data, "Tax Provision")
    total_debt = _get_metric(raw_financial_data, "Total Debt")
    long_term_debt = _get_metric(raw_financial_data, "Long Term Debt")
    previous_long_term_debt = _get_metric(raw_financial_data, "Previous Long Term Debt")
    cash_and_equivalents = _get_metric(raw_financial_data, "Cash And Equivalents")
    total_assets = _get_metric(raw_financial_data, "Total Assets")
    previous_total_assets = _get_metric(raw_financial_data, "Previous Total Assets")
    total_liabilities = _get_metric(raw_financial_data, "Total Liabilities")
    stockholders_equity = _get_metric(raw_financial_data, "Stockholders Equity")
    current_assets = _get_metric(raw_financial_data, "Current Assets")
    previous_current_assets = _get_metric(raw_financial_data, "Previous Current Assets")
    current_liabilities = _get_metric(raw_financial_data, "Current Liabilities")
    previous_current_liabilities = _get_metric(raw_financial_data, "Previous Current Liabilities")
    inventory = _get_metric(raw_financial_data, "Inventory")
    retained_earnings = _get_metric(raw_financial_data, "Retained Earnings")
    net_income = _get_metric(raw_financial_data, "Net Income")
    previous_net_income = _get_metric(raw_financial_data, "Previous Net Income")
    gross_profit = _get_metric(raw_financial_data, "Gross Profit")
    previous_gross_profit = _get_metric(raw_financial_data, "Previous Gross Profit")
    interest_expense = _get_metric(raw_financial_data, "Interest Expense")
    operating_cash_flow = _get_metric(raw_financial_data, "Operating Cash Flow")
    capital_expenditure = _get_metric(raw_financial_data, "Capital Expenditure")
    free_cash_flow = _get_metric(raw_financial_data, "Free Cash Flow")

    if free_cash_flow == MISSING_VALUE and _normalize_numeric_or_missing(operating_cash_flow) != MISSING_VALUE:
        free_cash_flow = float(operating_cash_flow)
        if _normalize_numeric_or_missing(capital_expenditure) != MISSING_VALUE:
            free_cash_flow -= abs(float(capital_expenditure))

    tax_rate = _safe_tax_rate(tax_provision, pretax_income)
    nopat = MISSING_VALUE
    if _normalize_numeric_or_missing(operating_income) != MISSING_VALUE:
        nopat = float(operating_income) * (1 - tax_rate)

    invested_capital = MISSING_VALUE
    if _normalize_numeric_or_missing(total_debt) != MISSING_VALUE and _normalize_numeric_or_missing(stockholders_equity) != MISSING_VALUE:
        cash_value = 0 if _normalize_numeric_or_missing(cash_and_equivalents) == MISSING_VALUE else float(cash_and_equivalents)
        invested_capital = float(total_debt) + float(stockholders_equity) - cash_value

    roic = _safe_ratio(nopat, invested_capital)
    roe = _get_metric(raw_financial_data, "Return On Equity Raw")
    if roe == MISSING_VALUE:
        roe = _safe_ratio(net_income, stockholders_equity)
    gross_margin = _get_metric(raw_financial_data, "Gross Margins Raw")
    if gross_margin == MISSING_VALUE:
        gross_margin = _safe_ratio(gross_profit, total_revenue)
    previous_gross_margin = _safe_ratio(previous_gross_profit, previous_total_revenue)
    operating_margin = _get_metric(raw_financial_data, "Operating Margins Raw")
    if operating_margin == MISSING_VALUE:
        operating_margin = _safe_ratio(operating_income, total_revenue)
    free_cash_flow_margin = _safe_ratio(free_cash_flow, total_revenue)
    debt_to_ebitda = _safe_ratio(total_debt, ebitda)
    interest_coverage = _safe_ratio(operating_income, abs(float(interest_expense)) if _normalize_numeric_or_missing(interest_expense) != MISSING_VALUE else MISSING_VALUE)
    eps_growth_1y = _get_metric(raw_financial_data, "Earnings Growth")
    if eps_growth_1y == MISSING_VALUE:
        eps_values = json.loads(_get_metric(raw_financial_data, "Diluted EPS Values")) if _get_metric(raw_financial_data, "Diluted EPS Values") != MISSING_VALUE else []
        if len(eps_values) >= 2:
            eps_growth_1y = _safe_growth(eps_values[0], eps_values[1])
    else:
        eps_values = json.loads(_get_metric(raw_financial_data, "Diluted EPS Values")) if _get_metric(raw_financial_data, "Diluted EPS Values") != MISSING_VALUE else []
    eps_growth_5y = MISSING_VALUE
    if len(eps_values) >= 5:
        eps_growth_5y = _safe_cagr(eps_values[0], eps_values[4], 4)
    elif len(eps_values) >= 3:
        eps_growth_5y = _safe_cagr(eps_values[0], eps_values[-1], len(eps_values) - 1)
    revenue_growth = _get_metric(raw_financial_data, "Revenue Growth Raw")
    if revenue_growth == MISSING_VALUE:
        revenue_growth = _safe_growth(total_revenue, previous_total_revenue)
    ebitda_growth = _safe_growth(ebitda, previous_ebitda)
    current_ratio = _get_metric(raw_financial_data, "Current Ratio Raw")
    if current_ratio == MISSING_VALUE:
        current_ratio = _safe_ratio(current_assets, current_liabilities)
    quick_ratio = _get_metric(raw_financial_data, "Quick Ratio Raw")
    if quick_ratio == MISSING_VALUE and _normalize_numeric_or_missing(current_assets) != MISSING_VALUE and _normalize_numeric_or_missing(inventory) != MISSING_VALUE:
        quick_ratio = _safe_ratio(float(current_assets) - float(inventory), current_liabilities)
    debt_to_equity = _get_metric(raw_financial_data, "Debt To Equity Raw")
    if debt_to_equity == MISSING_VALUE:
        debt_to_equity = _safe_ratio(total_debt, stockholders_equity)
    net_debt_to_ebitda = MISSING_VALUE
    if _normalize_numeric_or_missing(total_debt) != MISSING_VALUE and _normalize_numeric_or_missing(cash_and_equivalents) != MISSING_VALUE:
        net_debt_to_ebitda = _safe_ratio(float(total_debt) - float(cash_and_equivalents), ebitda)
    average_daily_volume = _get_metric(raw_financial_data, "Average Daily Volume")
    market_cap = _get_metric(raw_financial_data, "Market Cap")
    current_price = _get_metric(raw_financial_data, "Current Price")
    volume_to_market_cap = MISSING_VALUE
    if _normalize_numeric_or_missing(average_daily_volume) != MISSING_VALUE and _normalize_numeric_or_missing(current_price) != MISSING_VALUE and _normalize_numeric_or_missing(market_cap) != MISSING_VALUE:
        volume_to_market_cap = _safe_ratio(float(average_daily_volume) * float(current_price), market_cap)
    bid = _get_metric(raw_financial_data, "Bid")
    ask = _get_metric(raw_financial_data, "Ask")
    bid_ask_spread = MISSING_VALUE
    if _normalize_numeric_or_missing(bid) != MISSING_VALUE and _normalize_numeric_or_missing(ask) != MISSING_VALUE:
        midpoint = (float(bid) + float(ask)) / 2
        if midpoint > 0:
            bid_ask_spread = (float(ask) - float(bid)) / midpoint
    analyst_recommendation_score = _get_metric(raw_financial_data, "Analyst Recommendation Score")
    target_mean_price = _get_metric(raw_financial_data, "Target Mean Price")
    price_target_upside = MISSING_VALUE
    if _normalize_numeric_or_missing(target_mean_price) != MISSING_VALUE and _normalize_numeric_or_missing(current_price) != MISSING_VALUE and float(current_price) != 0:
        price_target_upside = (float(target_mean_price) - float(current_price)) / float(current_price)
    previous_current_ratio = _safe_ratio(previous_current_assets, previous_current_liabilities)
    roa = _safe_ratio(net_income, total_assets)
    previous_roa = _safe_ratio(previous_net_income, previous_total_assets)
    asset_turnover = _safe_ratio(total_revenue, total_assets)
    previous_asset_turnover = _safe_ratio(previous_total_revenue, previous_total_assets)
    working_capital = MISSING_VALUE
    if _normalize_numeric_or_missing(current_assets) != MISSING_VALUE and _normalize_numeric_or_missing(current_liabilities) != MISSING_VALUE:
        working_capital = float(current_assets) - float(current_liabilities)
    piotroski_f_score = _build_piotroski_score(net_income, operating_cash_flow, roa, previous_roa, long_term_debt, previous_long_term_debt, current_ratio, previous_current_ratio, gross_margin, previous_gross_margin, asset_turnover, previous_asset_turnover)
    altman_z_score = _build_altman_z_score(working_capital, retained_earnings, operating_income, market_cap, total_liabilities, total_revenue, total_assets)

    metrics = {
        "TICKER": ticker,
        "ROIC": _normalize_numeric_or_missing(roic),
        "ROE": _normalize_numeric_or_missing(roe),
        "Gross Margin": _normalize_numeric_or_missing(gross_margin),
        "Operating Margin": _normalize_numeric_or_missing(operating_margin),
        "Free Cash Flow Margin": _normalize_numeric_or_missing(free_cash_flow_margin),
        "Debt / EBITDA": _normalize_numeric_or_missing(debt_to_ebitda),
        "Interest Coverage Ratio": _normalize_numeric_or_missing(interest_coverage),
        "EPS Growth (1y)": _normalize_numeric_or_missing(eps_growth_1y),
        "EPS Growth (5y)": _normalize_numeric_or_missing(eps_growth_5y),
        "Revenue Growth": _normalize_numeric_or_missing(revenue_growth),
        "EBITDA Growth": _normalize_numeric_or_missing(ebitda_growth),
        "Current Ratio": _normalize_numeric_or_missing(current_ratio),
        "Quick Ratio": _normalize_numeric_or_missing(quick_ratio),
        "Debt / Equity": _normalize_numeric_or_missing(debt_to_equity),
        "Net Debt / EBITDA": _normalize_numeric_or_missing(net_debt_to_ebitda),
        "Average Daily Volume": _normalize_numeric_or_missing(average_daily_volume),
        "Volume / Market Cap": _normalize_numeric_or_missing(volume_to_market_cap),
        "Bid-Ask Spread": _normalize_numeric_or_missing(bid_ask_spread),
        "Analyst Recommendation Score": _normalize_numeric_or_missing(analyst_recommendation_score),
        "Price Target Upside": _normalize_numeric_or_missing(price_target_upside),
        "Piotroski F-Score": _normalize_numeric_or_missing(piotroski_f_score),
        "Altman Z-Score": _normalize_numeric_or_missing(altman_z_score),
        "Beta": _normalize_numeric_or_missing(_get_metric(raw_financial_data, "Beta")),
    }
    logger.info("Derived advanced financial metrics successfully. ticker=%s fields=%s", ticker, len(metrics) - 1)
    return pd.DataFrame([metrics])


def build_multifactor_analysis(ticker, period="1y", include_sentiment=False):
    logger.info(
        "Building multi-factor analysis. ticker=%s period=%s include_sentiment=%s",
        ticker,
        period,
        include_sentiment,
    )
    price_history = pd.DataFrame(dao.get_yahoo_finance(ticker, period))
    value_stats = pd.DataFrame(dao.get_yahoo_finance_key_stats(ticker)).iloc[0]
    raw_advanced_data = pd.DataFrame(dao.get_advanced_financial_metrics(ticker)).iloc[0]
    advanced_stats = derive_advanced_financial_metrics(raw_advanced_data).iloc[0]
    sentiment_score = sentiment_analysis.apply_sentiment_analysis(ticker) if include_sentiment else 0
    score_frame = calculate_multifactor_model_frame(
        price_history=price_history,
        value_stats=value_stats,
        advanced_stats=advanced_stats,
        sentiment_score=sentiment_score,
    )
    logger.info("Built multi-factor analysis successfully. ticker=%s", ticker)
    return {
        "ticker": ticker,
        "price_history": price_history,
        "value_stats": value_stats,
        "raw_advanced_data": raw_advanced_data,
        "advanced_stats": advanced_stats,
        "score_frame": score_frame,
    }
