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