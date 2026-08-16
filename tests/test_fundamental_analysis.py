import pytest

from analysis_functions import fundamental_analysis as fa


@pytest.mark.parametrize(
    "modifier",
    [
        fa.get_ev_ebitda_modifier,
        fa.get_peg_modifier,
        fa.get_ev_revenue_modifier,
        fa.get_price_book_modifier,
        fa.get_price_sales_modifier,
        fa.get_fpe_ratio_modifier,
        fa.get_tpe_ratio_modifier,
    ],
)
def test_negative_valuation_ratios_are_not_rewarded(modifier):
    assert modifier(-1) == 0


def test_negative_leverage_ratios_are_not_rewarded():
    assert fa.get_debt_to_ebitda_modifier(-2) == 0
    assert fa.get_debt_to_equity_modifier(-2) == 0


def test_partial_fundamental_mapping_does_not_raise():
    assert isinstance(fa.get_fundamental_analysis({"Market Cap": 20_000_000_000}), float)

