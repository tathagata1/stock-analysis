import configparser

config = configparser.ConfigParser()
config.read('config.ini')

path_data_dump=config['Misc']['data_dump_folder']
chatgpt_key=config['API']['chatgpt_key']

newsapi_key=config['API']['newsapi_key']
newapi_url=config['Selenium']['newapi_url']
newsapi_query=config['Selenium']['newsapi_query']
newsapi_from=config['Selenium']['newsapi_from']
newapi_api=config['Selenium']['newapi_api']

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
