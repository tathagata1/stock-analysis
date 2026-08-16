"""Validated project configuration.

Configuration is resolved relative to this file, with an optional environment
override, so imports do not depend on the process working directory.
"""

from __future__ import annotations

import configparser
import os
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parent.parent
CONFIG_PATH = Path(
    os.environ.get("STOCK_ANALYSIS_CONFIG", PROJECT_ROOT / "config" / "config.ini")
).expanduser().resolve()

config = configparser.ConfigParser()
if not config.read(CONFIG_PATH):
    raise FileNotFoundError(
        f"Stock-analysis configuration not found at {CONFIG_PATH}. "
        "Copy config/example.config.ini to config/config.ini or set "
        "STOCK_ANALYSIS_CONFIG."
    )


def _required(section: str, option: str) -> str:
    if not config.has_option(section, option):
        raise ValueError(f"Missing required configuration value [{section}] {option}")
    return config.get(section, option).strip().strip('"').strip("'")


def _positive_int(section: str, option: str) -> int:
    value = config.getint(section, option)
    if value <= 0:
        raise ValueError(f"[{section}] {option} must be greater than zero")
    return value


def _non_negative_float(section: str, option: str) -> float:
    value = config.getfloat(section, option)
    if value < 0:
        raise ValueError(f"[{section}] {option} cannot be negative")
    return value


# Secrets prefer the environment and remain backwards compatible with the local
# ignored INI file.
chatgpt_key = os.environ.get(
    "OPENAI_API_KEY", config.get("API", "chatgpt_key", fallback="")
).strip()
if chatgpt_key and set(chatgpt_key) == {"*"}:
    chatgpt_key = ""
OPENAI_SENTIMENT_MODEL = os.environ.get(
    "OPENAI_SENTIMENT_MODEL",
    config.get("API", "sentiment_model", fallback="gpt-5.1"),
).strip()

google_news_rss_url = _required("Selenium", "google_news_rss_url")

sma1 = _positive_int("SimpleMovingAverage", "SMA1")
sma2 = _positive_int("SimpleMovingAverage", "SMA2")
ema1 = _positive_int("ExponentialMovingAverage", "EMA1")
ema2 = _positive_int("ExponentialMovingAverage", "EMA2")

rsi_buy = config.getint("RelativeStrengthIndex", "RSIBuy")
rsi_sell = config.getint("RelativeStrengthIndex", "RSISell")
rsi_period = _positive_int("RelativeStrengthIndex", "RSIPeriod")

# The legacy ATR setting is now interpreted as percentage points
# (ATR / close * 100), rather than an absolute currency amount.
atr = _non_negative_float("AverageTrueRange", "ATR")
atr_period = _positive_int("AverageTrueRange", "ATRPeriod")
ATR_PERCENT_THRESHOLD = atr

bolinger_period = _positive_int("BollingerBands", "BolingerPeriod")

stoc_buy = config.getint("StochasticOscillator", "StocBuy")
stoc_sell = config.getint("StochasticOscillator", "StocSell")
stochastic_k = _positive_int("StochasticOscillator", "stochastic_k")
stochastic_d = _positive_int("StochasticOscillator", "stochastic_d")

fast_period = _positive_int("MACD", "fast_period")
slow_period = _positive_int("MACD", "slow_period")
signal_period = _positive_int("MACD", "signal_period")

VOLUME_LOOKBACK_PERIOD = config.getint("OTHERS", "VOLUME_LOOKBACK_PERIOD", fallback=20)
HIGH_VOLUME_MULTIPLIER = config.getfloat("OTHERS", "HIGH_VOLUME_MULTIPLIER", fallback=1.5)
VWAP_LOOKBACK_PERIOD = config.getint("OTHERS", "VWAP_LOOKBACK_PERIOD", fallback=20)
MIN_SIGNAL_COVERAGE = config.getfloat("OTHERS", "MIN_SIGNAL_COVERAGE", fallback=0.5)

DEFAULT_CACHE_DIR = _required("OTHERS", "DEFAULT_CACHE_DIR")
DEFAULT_INDEX_CACHE_MAX_AGE_HOURS = _positive_int(
    "OTHERS", "DEFAULT_INDEX_CACHE_MAX_AGE_HOURS"
)
TECHNICAL_SIGNAL_WEIGHT = _non_negative_float("OTHERS", "TECHNICAL_SIGNAL_WEIGHT")
SENTIMENT_SIGNAL_WEIGHT = _non_negative_float("OTHERS", "SENTIMENT_SIGNAL_WEIGHT")
FUNDAMENTAL_SIGNAL_WEIGHT = _non_negative_float("OTHERS", "FUNDAMENTAL_SIGNAL_WEIGHT")
MULTIFACTOR_SIGNAL_WEIGHT = _non_negative_float("OTHERS", "MULTIFACTOR_SIGNAL_WEIGHT")
STRONG_SELL_THRESHOLD = config.getfloat("OTHERS", "STRONG_SELL_THRESHOLD")
WEAK_SELL_THRESHOLD = config.getfloat("OTHERS", "WEAK_SELL_THRESHOLD")
WEAK_BUY_THRESHOLD = config.getfloat("OTHERS", "WEAK_BUY_THRESHOLD")
STRONG_BUY_THRESHOLD = config.getfloat("OTHERS", "STRONG_BUY_THRESHOLD")

LOG_DIR = config.get("Logging", "LOG_DIR", fallback="logs").strip().strip('"').strip("'")
LOG_LEVEL = config.get("Logging", "LOG_LEVEL", fallback="INFO").upper()
LOG_FILE_NAME = config.get("Logging", "LOG_FILE_NAME", fallback="stock_analysis.log")


def _validate() -> None:
    if not OPENAI_SENTIMENT_MODEL:
        raise ValueError("The sentiment model name cannot be empty")
    if sma1 >= sma2:
        raise ValueError("SMA1 must be smaller than SMA2")
    if ema1 >= ema2:
        raise ValueError("EMA1 must be smaller than EMA2")
    if fast_period >= slow_period:
        raise ValueError("MACD fast_period must be smaller than slow_period")
    if not 0 <= rsi_buy < rsi_sell <= 100:
        raise ValueError("RSI thresholds must satisfy 0 <= RSIBuy < RSISell <= 100")
    if not 0 <= stoc_buy < stoc_sell <= 100:
        raise ValueError("Stochastic thresholds must satisfy 0 <= StocBuy < StocSell <= 100")
    if VOLUME_LOOKBACK_PERIOD <= 0 or VWAP_LOOKBACK_PERIOD <= 0:
        raise ValueError("Volume and VWAP lookback periods must be greater than zero")
    if HIGH_VOLUME_MULTIPLIER <= 0:
        raise ValueError("HIGH_VOLUME_MULTIPLIER must be greater than zero")
    if not 0 < MIN_SIGNAL_COVERAGE <= 1:
        raise ValueError("MIN_SIGNAL_COVERAGE must be greater than zero and at most one")
    if not (
        STRONG_SELL_THRESHOLD
        < WEAK_SELL_THRESHOLD
        < WEAK_BUY_THRESHOLD
        < STRONG_BUY_THRESHOLD
    ):
        raise ValueError("Signal thresholds must be strictly increasing")
    if (
        TECHNICAL_SIGNAL_WEIGHT
        + SENTIMENT_SIGNAL_WEIGHT
        + FUNDAMENTAL_SIGNAL_WEIGHT
        + MULTIFACTOR_SIGNAL_WEIGHT
        <= 0
    ):
        raise ValueError("At least one signal weight must be greater than zero")


_validate()


TECHNICAL_MIN_HISTORY = max(
    sma2,
    ema2,
    rsi_period + 1,
    atr_period + 1,
    bolinger_period,
    stochastic_k + stochastic_d - 1,
    slow_period,
    VOLUME_LOOKBACK_PERIOD + 1,
    VWAP_LOOKBACK_PERIOD,
)
