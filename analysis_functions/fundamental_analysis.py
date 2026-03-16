from config.logging_config import get_logger

logger = get_logger(__name__)
MISSING_VALUE = "--"


def _normalize_numeric_or_missing(value):
    if value in (None, "", MISSING_VALUE):
        return MISSING_VALUE
    try:
        return MISSING_VALUE if float(value) != float(value) else float(value)
    except Exception:
        return MISSING_VALUE


def _get_advanced_metric(metrics, key):
    if metrics is None:
        return MISSING_VALUE
    if hasattr(metrics, "get"):
        return metrics.get(key, MISSING_VALUE)
    return MISSING_VALUE


def _sum_modifiers(*modifiers):
    total = 0.0
    for modifier in modifiers:
        normalized = _normalize_numeric_or_missing(modifier)
        if normalized == MISSING_VALUE:
            continue
        total += float(normalized)
    return total


def _clamp_score(score, lower=-1.0, upper=1.0):
    if score < lower:
        return lower
    if score > upper:
        return upper
    return score


def get_ev_ebitda_modifier(ev_ebitda_ratio):
    
    
    if (ev_ebitda_ratio == "--"):
        return 0
    ev_ebitda_ratio = float(ev_ebitda_ratio)
    
    """
    Adjusts a score based on the Enterprise Value to EBITDA ratio.
    
    A lower EV/EBITDA ratio suggests that a company may be undervalued compared to its earnings potential.
    """
    if ev_ebitda_ratio < 5:  # Significantly undervalued
        return 0.075  # Significant boost for perceived value
    elif 5 <= ev_ebitda_ratio < 10:  # Undervalued
        return 0.0375  # Moderate boost for attractiveness
    elif 10 <= ev_ebitda_ratio < 15:  # Fairly valued
        return 0  # Neutral, no adjustment
    elif 15 <= ev_ebitda_ratio < 20:  # Slightly overvalued
        return -0.0375  # Slight decrease for caution
    else:  # Significantly overvalued
        return -0.075  # Larger decrease for elevated risk

def get_peg_modifier(peg_ratio):
    
    if (peg_ratio == "--" or peg_ratio == "-- "):
        return 0
    peg_ratio = float(peg_ratio)
    """
    Adjusts a score based on the PEG ratio (5-year expected).
    
    The PEG ratio measures the relationship between a company's P/E ratio and its expected growth rate.
    """
    if peg_ratio < 0.5:  # Significantly undervalued
        return 0.1  # Significant boost for perceived value
    elif 0.5 <= peg_ratio < 1:  # Undervalued
        return 0.05  # Moderate boost for attractiveness
    elif 1 <= peg_ratio < 1.5:  # Fairly valued
        return 0  # Neutral, no adjustment
    elif 1.5 <= peg_ratio < 2:  # Slightly overvalued
        return -0.05  # Slight decrease for caution
    else:  # Significantly overvalued
        return -0.1  # Larger decrease for elevated risk

def get_ev_revenue_modifier(ev_revenue_ratio):
    
    if (ev_revenue_ratio == "--"):
        return 0
    ev_revenue_ratio = float(ev_revenue_ratio)
    """
    Adjusts a score based on the Enterprise Value-to-Revenue (EV/Revenue) ratio.
    
    The EV/Revenue ratio helps evaluate the valuation of a company relative to its revenue generation.
    """
    if ev_revenue_ratio < 1:  # Very low EV/Revenue, typically undervalued
        return 0.025  # Significant boost for perceived value
    elif 1 <= ev_revenue_ratio < 3:  # Low to moderate EV/Revenue
        return 0.0125  # Moderate boost for attractiveness
    elif 3 <= ev_revenue_ratio < 5:  # Elevated EV/Revenue, suggests growth potential
        return 0  # Neutral, no adjustment
    elif 5 <= ev_revenue_ratio < 10:  # High EV/Revenue, might indicate overvaluation
        return -0.0125  # Slight decrease for risk
    else:  # Very high EV/Revenue, typically speculative or indicating high growth expectations
        return -0.025  # Larger decrease for elevated risk

def get_price_book_modifier(price_book_ratio):
    
    if (price_book_ratio == "--"):
        return 0
    price_book_ratio = float(price_book_ratio)
    """
    Adjusts a score based on the Price-to-Book (P/B) ratio of a company.
    
    Lower P/B ratios are typically favorable for value investors, while higher ratios may indicate overvaluation.
    """
    if price_book_ratio < 1:  # Very low P/B, possibly undervalued
        return 0.025  # Significant boost for value
    elif 1 <= price_book_ratio < 3:  # Low to moderate P/B, often favorable
        return 0.0125  # Moderate boost for attractiveness
    elif 3 <= price_book_ratio < 5:  # Elevated P/B, suggests growth expectations
        return 0  # Neutral, no adjustment
    elif 5 <= price_book_ratio < 10:  # High P/B, may indicate growth or overvaluation
        return -0.0125  # Slight decrease for risk
    else:  # Very high P/B, typically speculative or overvalued
        return -0.025  # Larger decrease for elevated risk

def get_price_sales_modifier(price_sales_ratio):
    
    if (price_sales_ratio == "--"):
        return 0
    price_sales_ratio = float(price_sales_ratio)
    """
    Adjusts a score based on the Price-to-Sales (P/S) ratio of a company.
    
    A lower P/S can indicate undervaluation or potential growth, while a higher P/S may suggest overvaluation or higher risk.
    """
    if price_sales_ratio < 3:  # Low P/S, often indicating undervaluation
        return 0.075 # Strong boost for attractive valuation
    elif 3 <= price_sales_ratio < 7:  # Moderate P/S, typical for balanced growth
        return 0.0375  # Small boost for reasonable valuation
    elif 7 <= price_sales_ratio < 15:  # Elevated P/S, may indicate growth expectations
        return 0  # Neutral adjustment
    elif 15 <= price_sales_ratio < 25:  # High P/S, signaling overvaluation risk
        return -0.0375  # Small decrease for higher risk
    else:  # Very high P/S, speculative
        return -0.075  # Significant decrease for elevated risk

def get_fpe_ratio_modifier(fpe_ratio):
    
    if (fpe_ratio == "--"):
        return 0
    fpe_ratio = float(fpe_ratio)
    """
    Adjusts a score based on the forward P/E ratio of a company.

    A lower forward P/E can suggest undervaluation or solid growth expectations,
    while a higher forward P/E might indicate elevated risk or overvaluation.
    """
    if fpe_ratio < 15:  # Low forward P/E, indicating undervaluation
        return 0.1  # Strong boost for attractive valuation
    elif 15 <= fpe_ratio < 25:  # Moderate forward P/E, steady growth
        return 0.05  # Small boost for balanced valuation
    elif 25 <= fpe_ratio < 35:  # Higher forward P/E, growth potential
        return 0  # Neutral adjustment
    elif 35 <= fpe_ratio < 50:  # High forward P/E, valuation risk
        return -0.05  # Small decrease for potential overvaluation risk
    else:  # Very high forward P/E, speculative or volatile
        return -0.1  # Larger decrease for higher risk

def get_tpe_ratio_modifier(tpe_ratio):
    
    if (tpe_ratio == "--"):
        return 0
    tpe_ratio = float(tpe_ratio)
    """
    Adjusts a score based on the trailing P/E ratio of a company.

    Lower P/E ratios may indicate undervaluation and more stable companies, 
    while very high P/E ratios could signify overvaluation or higher volatility.
    """
    if tpe_ratio < 15:  # Low P/E ratio, often stable and undervalued
        return 0.1  # Boost for potential undervaluation
    elif 15 <= tpe_ratio < 25:  # Moderate P/E ratio, common in stable companies
        return 0.05  # Slight boost for reasonable valuation
    elif 25 <= tpe_ratio < 35:  # Higher P/E ratio, typical of growth companies
        return 0  # Neutral
    elif 35 <= tpe_ratio < 50:  # High P/E, more risk associated with growth
        return -0.05  # Small decrease for potential overvaluation
    else:  # Very high P/E, possibly overvalued or highly volatile
        return -0.1  # Larger decrease for high risk

def get_market_cap_modifier(market_cap):
    
    if (market_cap == "--"):
        return 0
    market_cap = float(market_cap)
    """
    Adjusts a score based on the market capitalization of a company.
    
    Large caps typically offer more stability and lower risk, while smaller caps
    may be more volatile and higher-risk investments.
    """
    if market_cap > 100_000_000_000:  # Mega-cap
        return 0.25  # Significant boost for mega-cap companies (least risk)
    elif market_cap > 10_000_000_000:  # Large-cap
        return 0.125  # Moderate boost for large-cap companies
    elif market_cap >= 2_000_000_000:  # Mid-cap
        return 0.0  # Neutral for mid-cap companies
    elif market_cap >= 300_000_000:  # Small-cap
        return -0.125  # Slight decrease for small-cap companies (higher risk)
    else:  # Micro-cap and nano-cap
        return -0.25  # Larger decrease for very small caps (highest risk)

def get_enterprise_value_modifier(enterprise_value):
    
    if (enterprise_value == "--"):
        return 0
    enterprise_value = float(enterprise_value)
    """
    Adjusts a score based on the enterprise value of a company.

    Higher enterprise values are typically associated with more stability and lower risk, 
    while lower enterprise values may indicate smaller, higher-risk companies.
    """
    if enterprise_value > 100_000_000_000:  # Mega-sized companies
        return 0.25  # Large boost for very high stability
    elif enterprise_value > 10_000_000_000:  # Large companies
        return 0.125  # Moderate boost for high stability
    elif enterprise_value >= 2_000_000_000:  # Mid-sized companies
        return 0  # Neutral modifier
    elif enterprise_value >= 500_000_000:  # Small companies
        return -0.125  # Small decrease for moderate risk
    else:  # Micro-sized companies
        return -0.25  # Larger decrease for high risk


def get_roic_modifier(roic):
    roic = _normalize_numeric_or_missing(roic)
    if roic == MISSING_VALUE:
        return 0
    if roic > 0.20:
        return 0.10
    if roic > 0.15:
        return 0.05
    if roic > 0.10:
        return 0
    if roic > 0.05:
        return -0.05
    return -0.10


def get_roe_modifier(roe):
    roe = _normalize_numeric_or_missing(roe)
    if roe == MISSING_VALUE:
        return 0
    if roe > 0.25:
        return 0.075
    if roe > 0.15:
        return 0.05
    if roe > 0.08:
        return 0
    if roe > 0:
        return -0.05
    return -0.10


def get_gross_margin_modifier(gross_margin):
    gross_margin = _normalize_numeric_or_missing(gross_margin)
    if gross_margin == MISSING_VALUE:
        return 0
    if gross_margin > 0.60:
        return 0.05
    if gross_margin > 0.40:
        return 0.025
    if gross_margin > 0.20:
        return 0
    if gross_margin > 0.10:
        return -0.025
    return -0.05


def get_operating_margin_modifier(operating_margin):
    operating_margin = _normalize_numeric_or_missing(operating_margin)
    if operating_margin == MISSING_VALUE:
        return 0
    if operating_margin > 0.25:
        return 0.075
    if operating_margin > 0.15:
        return 0.05
    if operating_margin > 0.08:
        return 0
    if operating_margin > 0:
        return -0.05
    return -0.10


def get_free_cash_flow_margin_modifier(free_cash_flow_margin):
    free_cash_flow_margin = _normalize_numeric_or_missing(free_cash_flow_margin)
    if free_cash_flow_margin == MISSING_VALUE:
        return 0
    if free_cash_flow_margin > 0.20:
        return 0.075
    if free_cash_flow_margin > 0.10:
        return 0.05
    if free_cash_flow_margin > 0.05:
        return 0
    if free_cash_flow_margin > 0:
        return -0.05
    return -0.10


def get_debt_to_ebitda_modifier(debt_to_ebitda):
    debt_to_ebitda = _normalize_numeric_or_missing(debt_to_ebitda)
    if debt_to_ebitda == MISSING_VALUE:
        return 0
    if debt_to_ebitda < 1:
        return 0.075
    if debt_to_ebitda < 2:
        return 0.05
    if debt_to_ebitda < 3:
        return 0
    if debt_to_ebitda < 4:
        return -0.05
    return -0.10


def get_interest_coverage_modifier(interest_coverage_ratio):
    interest_coverage_ratio = _normalize_numeric_or_missing(interest_coverage_ratio)
    if interest_coverage_ratio == MISSING_VALUE:
        return 0
    if interest_coverage_ratio > 8:
        return 0.075
    if interest_coverage_ratio > 4:
        return 0.05
    if interest_coverage_ratio > 2:
        return 0
    if interest_coverage_ratio > 1:
        return -0.05
    return -0.10


def get_eps_growth_1y_modifier(eps_growth_1y):
    eps_growth_1y = _normalize_numeric_or_missing(eps_growth_1y)
    if eps_growth_1y == MISSING_VALUE:
        return 0
    if eps_growth_1y > 0.20:
        return 0.10
    if eps_growth_1y > 0.10:
        return 0.05
    if eps_growth_1y > 0:
        return 0
    if eps_growth_1y > -0.10:
        return -0.05
    return -0.10


def get_eps_growth_5y_modifier(eps_growth_5y):
    eps_growth_5y = _normalize_numeric_or_missing(eps_growth_5y)
    if eps_growth_5y == MISSING_VALUE:
        return 0
    if eps_growth_5y > 0.20:
        return 0.10
    if eps_growth_5y > 0.10:
        return 0.05
    if eps_growth_5y > 0:
        return 0
    if eps_growth_5y > -0.05:
        return -0.05
    return -0.10


def get_revenue_growth_modifier(revenue_growth):
    revenue_growth = _normalize_numeric_or_missing(revenue_growth)
    if revenue_growth == MISSING_VALUE:
        return 0
    if revenue_growth > 0.20:
        return 0.075
    if revenue_growth > 0.10:
        return 0.05
    if revenue_growth > 0:
        return 0
    if revenue_growth > -0.05:
        return -0.05
    return -0.10


def get_ebitda_growth_modifier(ebitda_growth):
    ebitda_growth = _normalize_numeric_or_missing(ebitda_growth)
    if ebitda_growth == MISSING_VALUE:
        return 0
    if ebitda_growth > 0.20:
        return 0.075
    if ebitda_growth > 0.10:
        return 0.05
    if ebitda_growth > 0:
        return 0
    if ebitda_growth > -0.05:
        return -0.05
    return -0.10


def get_current_ratio_modifier(current_ratio):
    current_ratio = _normalize_numeric_or_missing(current_ratio)
    if current_ratio == MISSING_VALUE:
        return 0
    if current_ratio > 2:
        return 0.05
    if current_ratio > 1.5:
        return 0.025
    if current_ratio > 1:
        return 0
    if current_ratio > 0.8:
        return -0.05
    return -0.10


def get_quick_ratio_modifier(quick_ratio):
    quick_ratio = _normalize_numeric_or_missing(quick_ratio)
    if quick_ratio == MISSING_VALUE:
        return 0
    if quick_ratio > 1.5:
        return 0.05
    if quick_ratio > 1:
        return 0.025
    if quick_ratio > 0.8:
        return 0
    if quick_ratio > 0.6:
        return -0.05
    return -0.10


def get_debt_to_equity_modifier(debt_to_equity):
    debt_to_equity = _normalize_numeric_or_missing(debt_to_equity)
    if debt_to_equity == MISSING_VALUE:
        return 0
    if debt_to_equity < 0.5:
        return 0.05
    if debt_to_equity < 1:
        return 0.025
    if debt_to_equity < 1.5:
        return 0
    if debt_to_equity < 2.5:
        return -0.05
    return -0.10


def get_net_debt_to_ebitda_modifier(net_debt_to_ebitda):
    net_debt_to_ebitda = _normalize_numeric_or_missing(net_debt_to_ebitda)
    if net_debt_to_ebitda == MISSING_VALUE:
        return 0
    if net_debt_to_ebitda < 1:
        return 0.05
    if net_debt_to_ebitda < 2:
        return 0.025
    if net_debt_to_ebitda < 3:
        return 0
    if net_debt_to_ebitda < 4:
        return -0.05
    return -0.10


def get_piotroski_f_score_modifier(piotroski_f_score):
    piotroski_f_score = _normalize_numeric_or_missing(piotroski_f_score)
    if piotroski_f_score == MISSING_VALUE:
        return 0
    if piotroski_f_score >= 8:
        return 0.075
    if piotroski_f_score >= 6:
        return 0.05
    if piotroski_f_score >= 4:
        return 0
    if piotroski_f_score >= 3:
        return -0.05
    return -0.10


def get_altman_z_score_modifier(altman_z_score):
    altman_z_score = _normalize_numeric_or_missing(altman_z_score)
    if altman_z_score == MISSING_VALUE:
        return 0
    if altman_z_score > 3:
        return 0.075
    if altman_z_score > 2.2:
        return 0.05
    if altman_z_score > 1.8:
        return 0
    if altman_z_score > 1.2:
        return -0.05
    return -0.10


def get_quality_factor_score(advanced_stats):
    ticker = _get_advanced_metric(advanced_stats, "TICKER")
    logger.info("Calculating quality score. ticker=%s", ticker)
    score = _sum_modifiers(
        get_roic_modifier(_get_advanced_metric(advanced_stats, "ROIC")),
        get_roe_modifier(_get_advanced_metric(advanced_stats, "ROE")),
        get_gross_margin_modifier(_get_advanced_metric(advanced_stats, "Gross Margin")),
        get_operating_margin_modifier(_get_advanced_metric(advanced_stats, "Operating Margin")),
        get_free_cash_flow_margin_modifier(_get_advanced_metric(advanced_stats, "Free Cash Flow Margin")),
        get_debt_to_ebitda_modifier(_get_advanced_metric(advanced_stats, "Debt / EBITDA")),
        get_interest_coverage_modifier(_get_advanced_metric(advanced_stats, "Interest Coverage Ratio")),
        get_piotroski_f_score_modifier(_get_advanced_metric(advanced_stats, "Piotroski F-Score")),
    )
    score = _clamp_score(score)
    logger.info("Calculated quality score. ticker=%s score=%s", ticker, score)
    return score


def get_earnings_growth_factor_score(advanced_stats):
    ticker = _get_advanced_metric(advanced_stats, "TICKER")
    logger.info("Calculating earnings growth score. ticker=%s", ticker)
    score = _sum_modifiers(
        get_eps_growth_1y_modifier(_get_advanced_metric(advanced_stats, "EPS Growth (1y)")),
        get_eps_growth_5y_modifier(_get_advanced_metric(advanced_stats, "EPS Growth (5y)")),
        get_revenue_growth_modifier(_get_advanced_metric(advanced_stats, "Revenue Growth")),
        get_ebitda_growth_modifier(_get_advanced_metric(advanced_stats, "EBITDA Growth")),
    )
    score = _clamp_score(score)
    logger.info("Calculated earnings growth score. ticker=%s score=%s", ticker, score)
    return score


def get_financial_strength_factor_score(advanced_stats):
    ticker = _get_advanced_metric(advanced_stats, "TICKER")
    logger.info("Calculating financial strength score. ticker=%s", ticker)
    score = _sum_modifiers(
        get_current_ratio_modifier(_get_advanced_metric(advanced_stats, "Current Ratio")),
        get_quick_ratio_modifier(_get_advanced_metric(advanced_stats, "Quick Ratio")),
        get_debt_to_equity_modifier(_get_advanced_metric(advanced_stats, "Debt / Equity")),
        get_net_debt_to_ebitda_modifier(_get_advanced_metric(advanced_stats, "Net Debt / EBITDA")),
        get_altman_z_score_modifier(_get_advanced_metric(advanced_stats, "Altman Z-Score")),
    )
    score = _clamp_score(score)
    logger.info("Calculated financial strength score. ticker=%s score=%s", ticker, score)
    return score

def get_fundamental_analysis(df_stats):
    ticker = df_stats.get('TICKER', 'UNKNOWN') if hasattr(df_stats, 'get') else 'UNKNOWN'
    logger.info("Calculating fundamental analysis score. ticker=%s", ticker)
    modifiers = [
        get_market_cap_modifier(df_stats['Market Cap']),
        get_enterprise_value_modifier(df_stats['Enterprise Value']),
        get_tpe_ratio_modifier(df_stats['Trailing P/E']),
        get_fpe_ratio_modifier(df_stats['Forward P/E']),
        get_peg_modifier(df_stats['PEG Ratio (5yr expected)']),
        get_price_sales_modifier(df_stats['Price/Sales']),
        get_ev_ebitda_modifier(df_stats['Enterprise Value/EBITDA']),
        get_ev_revenue_modifier(df_stats['Enterprise Value/Revenue']),
        get_price_book_modifier(df_stats['Price/Book']),
    ]
    score = sum(modifiers)
    logger.info("Calculated fundamental analysis score successfully. ticker=%s score=%s", ticker, score)
    return score
