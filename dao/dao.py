import logging
import json
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from io import StringIO
from urllib.parse import quote_plus
import xml.etree.ElementTree as ET
import os

import pandas as pd
import requests
from bs4 import BeautifulSoup
from openai import OpenAI

import config.config as config

import json
from pathlib import Path
import yfinance as yf




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
        #command='you need to analyse the following reddit post for '+stock+'. you need to respond in the format { "score": "VAR_X", "confidence": "VAR_Y" }. VAR_X can vary from -1.000 to +1.0000. VAR_Y will be your confidence on the buy/sell indication and will vary between 0.00 and 1.00. I do not need anything else other than { "score": "VAR_X", "confidence": "VAR_Y" }. Here is the text: '+post
        command="""You are a financial sentiment and trading-signal analysis engine.
                    Task:
                    Analyse the following text about the stock:"""+stock+""".
                    The text may come from news, social media, forums, blogs, or reports.
                    Determine whether it indicates a bullish (buy), bearish (sell), or neutral signal.

                    Scoring rules:
                    - Return a sentiment score VAR_X between -1.000 and +1.000
                        - -1.000 = extremely bearish / strong sell signal
                        - -0.500 = moderately bearish
                        -  0.000 = neutral / no clear signal
                        - +0.500 = moderately bullish
                        - +1.000 = extremely bullish / strong buy signal
                    - Score must reflect ONLY the sentiment expressed in the provided text.

                    Confidence rules:
                    - Return VAR_Y between 0.00 and 1.00 indicating confidence in the signal.
                    - Increase confidence when:
                        - Clear directional opinion exists
                        - Evidence, data, financials, catalysts, or concrete reasoning are present
                    - Reduce confidence when:
                        - Speculation, rumours, hype, or emotional language dominates
                        - Sarcasm or ambiguity is present
                        - Mixed signals appear
                        - The text is short or lacks context

                    Signal interpretation guidance:
                    - Bullish indicators:
                        earnings strength, upgrades, growth catalysts, strong guidance, accumulation
                    - Bearish indicators:
                        downgrades, weak outlook, regulatory risk, layoffs, missed earnings, dilution
                    - Neutral:
                        factual reporting without directional opinion

                    Constraints:
                    - Use only the provided text. Do NOT use external data or prior knowledge.
                    - Ignore general market sentiment unless directly tied to the stock.
                    - If the stock is mentioned but no investment signal is present → score = 0.000.
                    - If the stock is not actually discussed → score = 0.000 and confidence ≤ 0.20.
                    - Do NOT explain reasoning.
                    - Output STRICTLY valid JSON only.
                    - No text before or after the JSON.

                    Output format (mandatory):
                    { "score": VAR_X, "confidence": VAR_Y }

                    Text to analyse:""" +post
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


def get_yahoo_finance(var_stock, period="1d"):
    _require_yfinance()
    try:
        ticker = yf.Ticker(var_stock)
        hist = ticker.history(period=period, auto_adjust=False, actions=False)
        if hist is None or hist.empty:
            logging.error("Error in get_yahoo_finance for %s", var_stock)
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
        logging.error("Error in get_yahoo_finance for %s: %s", var_stock, exc)
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
        url = f"{config.reddit_url}/search.json?q={query}&sort=new&limit=10"
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
        return comment_links[:10]
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
    query = quote_plus(f"{stock} stock")
    url = f"{config.google_news_rss_url}{query}&hl=en-US&gl=US&ceid=US:en"

    try:
        response = requests.get(url, headers=REQUEST_HEADERS, timeout=15)
        response.raise_for_status()
        root = ET.fromstring(response.content)
    except Exception as exc:
        logging.error("Error in get_news_links for %s: %s", stock, exc)
        return []

    articles = []
    for item in root.findall("./channel/item"):
        title = (item.findtext("title") or "").strip()
        link = (item.findtext("link") or "").strip()
        description = (item.findtext("description") or "").strip()
        pub_date = (item.findtext("pubDate") or "").strip()
        source_name = (item.findtext("source") or "").strip()

        published_at = None
        if pub_date:
            try:
                dt = parsedate_to_datetime(pub_date)
                if dt.tzinfo is None:
                    published_at = dt.strftime("%Y-%m-%dT%H:%M:%SZ")
                else:
                    published_at = dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
            except Exception:
                published_at = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
        else:
            published_at = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")

        if not source_name and " - " in title:
            source_name = title.rsplit(" - ", 1)[-1].strip()

        articles.append({
            "publishedAt": published_at,
            "source": {"name": source_name},
            "author": "",
            "title": title,
            "content": "",
            "description": BeautifulSoup(description, "html.parser").get_text(" ", strip=True),
            "url": link,
        })

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


def _index_ticker_cache_path(index_name, cache_dir=config.DEFAULT_CACHE_DIR):
    cache_path = Path(cache_dir)
    cache_path.mkdir(parents=True, exist_ok=True)
    return cache_path / f"{index_name.lower()}_tickers.json"


def _read_cached_tickers(cache_file):
    payload = json.loads(cache_file.read_text(encoding="utf-8"))
    if isinstance(payload, dict):
        tickers = payload.get("tickers", [])
    elif isinstance(payload, list):
        tickers = payload
    else:
        tickers = []
    return [str(t).strip() for t in tickers if str(t).strip()]


def get_index_tickers_cached(index_name="sp500", limit=None, cache_dir=config.DEFAULT_CACHE_DIR, max_age_hours=config.DEFAULT_INDEX_CACHE_MAX_AGE_HOURS):
    cache_file = _index_ticker_cache_path(index_name, cache_dir=cache_dir)
    now = datetime.now().timestamp()
    max_age_seconds = max_age_hours * 3600
    cache_used = False
    cache_deleted = False
    tickers = []

    if cache_file.exists():
        cache_age_seconds = now - cache_file.stat().st_mtime
        if cache_age_seconds < max_age_seconds:
            try:
                tickers = _read_cached_tickers(cache_file)
                cache_used = True
            except Exception:
                cache_file.unlink(missing_ok=True)
                cache_deleted = True
        else:
            cache_file.unlink(missing_ok=True)
            cache_deleted = True

    if not cache_used:
        tickers = get_index_constituents(index_name)
        payload = {
            "index_name": index_name,
            "fetched_at": datetime.now().isoformat(timespec="seconds"),
            "tickers": tickers,
        }
        cache_file.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    if limit is not None:
        tickers = tickers[:limit]

    cache_age_seconds = None
    if cache_file.exists():
        cache_age_seconds = max(0, datetime.now().timestamp() - cache_file.stat().st_mtime)

    cache_meta = {
        "cache_file": str(cache_file),
        "cache_used": cache_used,
        "cache_deleted": cache_deleted,
        "max_age_hours": max_age_hours,
        "cache_age_hours": None if cache_age_seconds is None else round(cache_age_seconds / 3600, 3),
    }
    return tickers, cache_meta


def get_index_tickers(index_name="sp500", limit=None):
    tickers = get_index_constituents(index_name)
    if limit is None:
        return tickers
    return tickers[:limit]