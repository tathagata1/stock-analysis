from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.options import Options
import pdfplumber
import config
import os
import pandas as pd


def get_driver(headless):
    chrome_options = Options()
    chrome_options.add_argument("--log-level=3")  # Suppress logs
    chrome_options.add_argument("--ignore-ssl-errors=yes")
    chrome_options.add_argument('--ignore-certificate-errors')  # Ignore SSL errors
    chrome_options.add_argument('--disable-web-security')       # Disable web security (optional)
    chrome_options.add_argument('--allow-running-insecure-content')  # Allow insecure content (optional)
    chrome_options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36")
    if(headless):
        chrome_options.add_argument("--headless")
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)
    return driver

def remove_comma(x):
    return x.replace(',','')

def convert_number(value):
    
    if (value == "--"):
        return value
    else:
        if 'T' in value:
            return float(value.replace('T', '')) * 1e12
        if 'B' in value:
            return float(value.replace('B', '')) * 1e9
        if 'M' in value:
            return float(value.replace('M', '')) * 1e6
        if 'k' in value:
            return float(value.replace('k', '')) * 1e3
        else:
            return value

def extract_text_from_pdf(file_path):
    text = ""
    with pdfplumber.open(file_path) as pdf:
        for page in pdf.pages:
            text += page.extract_text() + "\n"
    return text

def group_elements_recursively(elements, group_size=3000):
    if not elements:
        return []
    group = " ".join(map(str, elements[:group_size]))
    return [group] + group_elements_recursively(elements[group_size:], group_size)

def get_summary_file(path=config.path_data_dump, name="combined_summary.csv"):
    summary_file=os.path.join(path, name)
    if os.path.exists(summary_file):
        summary = pd.read_csv(summary_file)
        summary.drop(summary.columns[0], axis=1, inplace=True)
        return summary
    else:
        return pd.DataFrame(columns=['current_date', 
                                     'TICKER', 
                                     'Signal', 
                                     'Signal_Text', 
                                     'cumulative_return', 
                                     'sharpe_ratio', 
                                     'sortino_ratio', 
                                     'max_drawdown', 
                                     'calmar_ratio'])
        
def get_simulation_file(summary_file="combined_summary.csv"):
    if os.path.exists(summary_file):
        summary = pd.read_csv(summary_file)
        summary.drop(summary.columns[0], axis=1, inplace=True)
        return summary
    else:
        return pd.DataFrame(columns=["current_date",
                                    "TICKER",
                                    "closing_stock_price",
                                    "final_cash_balance",
                                    "unrealized_gains_losses",
                                    "unrealized_gain_loss_%",
                                    "units_held",
                                    "average_price_per_unit",
                                    "signal_text",
                                    "signal_number"])