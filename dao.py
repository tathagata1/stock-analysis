import os
import logging
import json
from datetime import datetime, timedelta
from io import StringIO
from urllib.parse import quote_plus

import pandas as pd
import requests
from bs4 import BeautifulSoup
from openai import OpenAI

import config

try:
    import yfinance as yf
except ImportError:  # pragma: no cover - handled at runtime in helper
    yf = None


REQUEST_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36"
    )
}

INDEX_SOURCES = {
    "sp500": {
        "url": "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies",
        "column": "Symbol",
    },
    "nasdaq100": {
        "url": "https://en.wikipedia.org/wiki/Nasdaq-100",
        "column": "Ticker",
    },
    "dow30": {
        "url": "https://en.wikipedia.org/wiki/Dow_Jones_Industrial_Average",
        "column": "Symbol",
    },
}

STAT_FIELD_MAP = {
    "Market Cap": "marketCap",
    "Enterprise Value": "enterpriseValue",
    "Trailing P/E": "trailingPE",
    "Forward P/E": "forwardPE",
    "PEG Ratio (5yr expected)": "pegRatio",
    "Price/Sales": "priceToSalesTrailing12Months",
    "Price/Book": "priceToBook",
    "Enterprise Value/Revenue": "enterpriseToRevenue",
    "Enterprise Value/EBITDA": "enterpriseToEbitda",
}


def _require_yfinance():
    if yf is None:
        raise ImportError("yfinance is required. Install it with: pip install yfinance")


def _normalize_numeric_or_missing(value):
    if value is None:
        return "--"
    try:
        if pd.isna(value):
            return "--"
    except Exception:
        pass
    return value


def _safe_request(url, timeout=15):
    return requests.get(url, headers=REQUEST_HEADERS, timeout=timeout)


def _extract_visible_text_from_html(html):
    soup = BeautifulSoup(html, 'html.parser')
    for tag in soup(["script", "style", "header", "footer", "nav", "aside"]):
        tag.decompose()
    return soup.get_text(separator=' ', strip=True).replace("'", "")


def _safe_ticker_info(var_stock):
    _require_yfinance()
    ticker = yf.Ticker(var_stock)
    info = {}
    try:
        info = ticker.info or {}
    except Exception as exc:
        logging.error("Error fetching info for %s: %s", var_stock, exc)
    return ticker, info


def get_gpt_score_with_confidence(stock, post):

    try:
        os.environ['OPENAI_API_KEY'] = config.chatgpt_key
        client = OpenAI()
        command='you need to analyse the following reddit post for '+stock+'. you need to respond in the format { "score": "VAR_X", "confidence": "VAR_Y" }. VAR_X can vary from -1.000 to +1.0000. VAR_Y will be your confidence on the buy/sell indication and will vary between 0.00 and 1.00. I do not need anything else other than { "score": "VAR_X", "confidence": "VAR_Y" }. Here is the text: '+post
        completion = client.chat.completions.create(
            model="gpt-5.1",
            messages=[
                {"role": "system", "content": "you are an expert in the stock market analysis."},
                {
                    "role": "user",
                    "content": command
                }
            ]
        )
    except Exception:
        print("error in get_gpt_score_with_confidence")
        return '{ "score": "0", "confidence": "0" }'

    return completion.choices[0].message.content


def write_csv(file_name, tmp_data):
    if os.path.isfile(file_name):
        os.remove(file_name)
    data = pd.DataFrame(data=tmp_data)
    data.to_csv(file_name, index=True)


def write_json(file_name, tmp_data):
    if os.path.isfile(file_name):
        os.remove(file_name)
    with open(file_name, 'w') as file:
        json.dump(tmp_data, file, indent=4)


def read_json(file_name):
    with open(file_name, 'r') as file:
        content = file.read()

    return json.loads(content)


def get_index_constituents(index_name="sp500"):
    source = INDEX_SOURCES.get(index_name.lower())
    if source is None:
        raise ValueError(f"Unsupported index_name: {index_name}. Use one of {sorted(INDEX_SOURCES)}")

    try:
        response = _safe_request(source["url"])
        response.raise_for_status()
        tables = pd.read_html(StringIO(response.text))
    except Exception as exc:
        logging.error("Error reading index constituents for %s: %s", index_name, exc)
        return []

    for table in tables:
        if source["column"] in table.columns:
            tickers = table[source["column"]].astype(str).str.strip().tolist()
            # Yahoo Finance uses '-' instead of '.' for class shares (e.g., BRK.B -> BRK-B)
            return [ticker.replace('.', '-') for ticker in tickers if ticker and ticker != 'nan']

    logging.error("Could not find ticker column %s for %s", source["column"], index_name)
    return []


def get_yahoo_finance_5y(var_stock):
    _require_yfinance()
    try:
        ticker = yf.Ticker(var_stock)
        hist = ticker.history(period="5y", auto_adjust=False, actions=False)
        if hist is None or hist.empty:
            logging.error("Error in get_yahoo_finance_5y for %s", var_stock)
            return pd.DataFrame(columns=['Date', 'Open', 'High', 'Low', 'Close', 'Adj Close', 'Volume', 'TICKER'])

        if 'Adj Close' not in hist.columns:
            hist['Adj Close'] = hist['Close']

        hist = hist.reset_index()
        date_col = hist.columns[0]
        hist.rename(columns={date_col: 'Date'}, inplace=True)
        hist['Date'] = pd.to_datetime(hist['Date']).dt.tz_localize(None)
        hist = hist[['Date', 'Open', 'High', 'Low', 'Close', 'Adj Close', 'Volume']]
        hist['TICKER'] = var_stock
        hist = hist.sort_values('Date', ascending=False).reset_index(drop=True)
        return hist

    except Exception as exc:
        logging.error("Error in get_yahoo_finance_5y for %s: %s", var_stock, exc)
        return pd.DataFrame(columns=['Date', 'Open', 'High', 'Low', 'Close', 'Adj Close', 'Volume', 'TICKER'])


def get_yahoo_finance_key_stats(var_stock):
    try:
        _, info = _safe_ticker_info(var_stock)
        row = {display_name: _normalize_numeric_or_missing(info.get(info_key)) for display_name, info_key in STAT_FIELD_MAP.items()}
        row['TICKER'] = var_stock
        return pd.DataFrame([row])
    except Exception as exc:
        logging.error("Error in get_yahoo_finance_key_stats for %s: %s", var_stock, exc)
        row = {display_name: "--" for display_name in STAT_FIELD_MAP}
        row['TICKER'] = var_stock
        return pd.DataFrame([row])


def get_reddit_links(var_stock):
    try:
        query = quote_plus(var_stock)
        url = f"{config.reddit_url}/search.json?q={query}&sort=new&limit=5"
        response = _safe_request(url)
        response.raise_for_status()
        payload = response.json()

        comment_links = []
        children = payload.get('data', {}).get('children', [])
        for child in children:
            data = child.get('data', {})
            permalink = data.get('permalink')
            if permalink:
                comment_links.append(config.reddit_url + permalink)
        return comment_links[:5]
    except Exception as exc:
        logging.error("Error in get_reddit_links for %s: %s", var_stock, exc)
        return None


def get_reddit_post(link):
    try:
        json_url = link.rstrip('/') + '.json'
        response = _safe_request(json_url)
        response.raise_for_status()
        payload = response.json()

        pieces = []
        if isinstance(payload, list) and payload:
            post_children = payload[0].get('data', {}).get('children', [])
            if post_children:
                post_data = post_children[0].get('data', {})
                pieces.extend([
                    str(post_data.get('title', '')),
                    str(post_data.get('selftext', '')),
                ])

            if len(payload) > 1:
                comment_children = payload[1].get('data', {}).get('children', [])
                for child in comment_children[:15]:
                    body = child.get('data', {}).get('body')
                    if body:
                        pieces.append(str(body))

        text = ' '.join(piece for piece in pieces if piece).replace("'", "")
        return text if text else None

    except Exception as exc:
        logging.error("Error in get_reddit_post for %s: %s", link, exc)
        return None


def get_news_links(stock):
    current_date = (datetime.now() - timedelta(2)).strftime("%Y-%m-%d")
    url = config.newapi_url + config.newsapi_query + stock + config.newsapi_from + current_date + config.newapi_api + config.newsapi_key
    response = requests.get(url, headers=REQUEST_HEADERS, timeout=15)
    data = json.loads(response.text)
    articles = data.get('articles', [])
    return articles


def get_news_post(link):
    try:
        response = _safe_request(link)
        response.raise_for_status()
        page_text = _extract_visible_text_from_html(response.text)
        return page_text[:12000]
    except Exception as exc:
        logging.error("Error in get_news_post for %s: %s", link, exc)
        return None
