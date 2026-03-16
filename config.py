import configparser
import json

config = configparser.ConfigParser()
config.read('config/config.ini')

chatgpt_key=config['API']['chatgpt_key']

google_news_rss_url=config['Selenium']['google_news_rss_url']

reddit_url=config['Selenium']['redditURL']

sma1=int(config['SimpleMovingAverage']['SMA1'])
sma2=int(config['SimpleMovingAverage']['SMA2'])

ema1=int(config['ExponentialMovingAverage']['EMA1'])
ema2=int(config['ExponentialMovingAverage']['EMA2'])

rsi_buy=int(config['RelativeStrengthIndex']['RSIBuy'])
rsi_sell=int(config['RelativeStrengthIndex']['RSISell'])
rsi_period=int(config['RelativeStrengthIndex']['RSIPeriod'])

atr=float(config['AverageTrueRange']['ATR'])
atr_period=int(config['AverageTrueRange']['ATRPeriod'])

bolinger_period=int(config['BollingerBands']['BolingerPeriod'])

stoc_buy=int(config['StochasticOscillator']['StocBuy'])
stoc_sell=int(config['StochasticOscillator']['StocSell'])
stochastic_k=int(config['StochasticOscillator']['stochastic_k'])
stochastic_d=int(config['StochasticOscillator']['stochastic_d'])

fast_period=int(config['MACD']['fast_period'])
slow_period=int(config['MACD']['slow_period'])
signal_period=int(config['MACD']['signal_period'])

DEFAULT_SIGNAL_LOOKBACK_START = int(config['OTHERS']['DEFAULT_SIGNAL_LOOKBACK_START'])
DEFAULT_SIGNAL_LOOKBACK_END = int(config['OTHERS']['DEFAULT_SIGNAL_LOOKBACK_END'])
DEFAULT_CACHE_DIR = config['OTHERS']['DEFAULT_CACHE_DIR']
DEFAULT_INDEX_CACHE_MAX_AGE_HOURS = int(config['OTHERS']['DEFAULT_INDEX_CACHE_MAX_AGE_HOURS'])
TECHNICAL_SIGNAL_WEIGHT = float(config['OTHERS']['TECHNICAL_SIGNAL_WEIGHT'])
SENTIMENT_SIGNAL_WEIGHT = float(config['OTHERS']['SENTIMENT_SIGNAL_WEIGHT'])
FUNDAMENTAL_SIGNAL_WEIGHT = float(config['OTHERS']['FUNDAMENTAL_SIGNAL_WEIGHT'])
