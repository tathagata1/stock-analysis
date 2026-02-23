import os

import pandas as pd
import pdfplumber

import config


def remove_comma(x):
    return x.replace(',', '')


def convert_number(value):
    if value is None:
        return "--"

    if isinstance(value, (int, float)):
        try:
            if pd.isna(value):
                return "--"
        except Exception:
            pass
        return value

    value = str(value).strip()
    if value in {"--", "", "nan", "None"}:
        return "--"

    try:
        if value.endswith('T'):
            return float(value[:-1]) * 1e12
        if value.endswith('B'):
            return float(value[:-1]) * 1e9
        if value.endswith('M'):
            return float(value[:-1]) * 1e6
        if value.endswith('k'):
            return float(value[:-1]) * 1e3
        return float(value)
    except ValueError:
        return value


def extract_text_from_pdf(file_path):
    text = ""
    with pdfplumber.open(file_path) as pdf:
        for page in pdf.pages:
            text += (page.extract_text() or "") + "\n"
    return text


def group_elements_recursively(elements, group_size=3000):
    if not elements:
        return []
    group = " ".join(map(str, elements[:group_size]))
    return [group] + group_elements_recursively(elements[group_size:], group_size)


def get_summary_file(path=config.path_data_dump, name="combined_summary.csv"):
    summary_file = os.path.join(path, name)
    if os.path.exists(summary_file):
        summary = pd.read_csv(summary_file)
        summary.drop(summary.columns[0], axis=1, inplace=True)
        return summary
    return pd.DataFrame(columns=[
        'current_date',
        'TICKER',
        'Signal',
        'Signal_Text',
        'cumulative_return',
        'sharpe_ratio',
        'sortino_ratio',
        'max_drawdown',
        'calmar_ratio',
    ])


def get_simulation_file(summary_file="combined_summary.csv"):
    if os.path.exists(summary_file):
        summary = pd.read_csv(summary_file)
        summary.drop(summary.columns[0], axis=1, inplace=True)
        return summary
    return pd.DataFrame(columns=[
        "current_date",
        "TICKER",
        "closing_stock_price",
        "final_cash_balance",
        "unrealized_gains_losses",
        "unrealized_gain_loss_%",
        "units_held",
        "average_price_per_unit",
        "signal_text",
        "signal_number",
    ])
