import json
import numpy as np

import fundamental_analysis
import sentiment_analysis
import technical_analysis

def apply_sentiment_analysis(stock):
    gpt_resp = []
    gpt_resp = sentiment_analysis.get_reddit_sentiment(stock, gpt_resp)
    gpt_resp = sentiment_analysis.get_news_sentiment(stock, gpt_resp)
    try: 
        scores_with_high_confidence = [float(item["score"]) for item in (json.loads(data) for data in gpt_resp) if float(item["confidence"]) >= 0.90]
        if len(scores_with_high_confidence) > 0:
            average_score = sum(scores_with_high_confidence) / len(scores_with_high_confidence)
        else:
            average_score = 0
    except Exception as e:
        print("error in apply_sentiment_analysis")
        return 0
        
    return average_score

def apply_technical_analysis_buy(row):
    return technical_analysis.calculate_buy_score(row)

def apply_technical_analysis_sell(row):
    return technical_analysis.calculate_sell_score(row)

def apply_fundamental_analysis(df_stats):
    modifiers = [
        
        #high importance
        fundamental_analysis.get_market_cap_modifier(df_stats['Market Cap']), #-0.25 to 0.25
        fundamental_analysis.get_enterprise_value_modifier(df_stats['Enterprise Value']), #-0.25 to 0.25
        fundamental_analysis.get_tpe_ratio_modifier(df_stats['Trailing P/E']), #-0.1 to 0.1
        fundamental_analysis.get_fpe_ratio_modifier(df_stats['Forward P/E']), #-0.1 to 0.1
        fundamental_analysis.get_peg_modifier(df_stats['PEG Ratio (5yr expected)']), #-0.1 to 0.1
        
        #medium importance
        fundamental_analysis.get_price_sales_modifier(df_stats['Price/Sales']), #-0.075 to 0.075
        fundamental_analysis.get_ev_ebitda_modifier(df_stats['Enterprise Value/EBITDA']), #-0.075 to 0.075
        
        #low importance
        fundamental_analysis.get_ev_revenue_modifier(df_stats['Enterprise Value/Revenue']), #-0.025 to 0.025
        fundamental_analysis.get_price_book_modifier(df_stats['Price/Book']) #-0.025 to 0.025
    ]
    return sum(modifiers)

def get_prediction(df, stats, include_sentiment=True, include_fundamental=True):

    df = technical_analysis.get_technical_analysis_calculations(df)
    df['technical_analysis_buy_score'] = df.apply(apply_technical_analysis_buy, axis=1)      #0 to +1      
    df['technical_analysis_sell_score'] = df.apply(apply_technical_analysis_sell, axis=1)    #-1 to 0     
    df['fundamental_analysis_score'] = apply_fundamental_analysis(stats) if include_fundamental else 0  #-1 to +1      
    ticker = df["TICKER"].iloc[0]
    df['sentiment_analysis_score'] = apply_sentiment_analysis(ticker) if include_sentiment else 0  #-1 to +1

    return df

def get_statsless_prediction(df, include_sentiment=True):

    df = technical_analysis.get_technical_analysis_calculations(df)
    df['technical_analysis_buy_score'] = df.apply(apply_technical_analysis_buy, axis=1)            #0 to +1
    df['technical_analysis_sell_score'] = df.apply(apply_technical_analysis_sell, axis=1)          #-1 to 0
    df['fundamental_analysis_score'] = 0
    ticker = df["TICKER"].iloc[0]
    df['sentiment_analysis_score'] = apply_sentiment_analysis(ticker) if include_sentiment else 0  #-1 to +1

    return df

def add_total_signal(df):
    df['Signal'] = (
        df['technical_analysis_buy_score']
        + df['technical_analysis_sell_score']
        + df.get('fundamental_analysis_score', 0)
        + df.get('sentiment_analysis_score', 0)
    )
    return df

def convert_signal_to_text(df):
    conditions = [
        (df['Signal'] <= -1),
        (df['Signal'] > -1) & (df['Signal'] < -0.7),
        (df['Signal'] >= -0.7) & (df['Signal'] < 0.7),
        (df['Signal'] >= 0.7) & (df['Signal'] < 1),
        (df['Signal'] >= 1),
    ]
    choices = ['STRONG SELL', 'WEAK SELL', 'HOLD', 'WEAK BUY', 'STRONG BUY']
    df['Signal_Text'] = np.select(conditions, choices, default='HOLD')
    
    return df

def get_weighted_signal(signals):
    # Ignore row 0 and reset index for processing
    
    # Map Signal_Text to numeric values
    signal_mapping = {
        "STRONG SELL": -2,
        "WEAK SELL": -1,
        "HOLD": 0,
        "WEAK BUY": 1,
        "STRONG BUY": 2
    }
    reverse_mapping = {v: k for k, v in signal_mapping.items()}  # Reverse map for final result
    numeric_signals = signals.map(signal_mapping)
    
    # Assign weights (exponentially decreasing, normalized)
    weights = np.array([0.5 ** i for i in range(len(numeric_signals))])
    normalized_weights = weights / weights.sum()
    
    # Compute weighted average
    weighted_avg = np.dot(numeric_signals, normalized_weights)
    
    # Round the weighted average to the nearest category
    closest_signal = round(weighted_avg)
    
    # Map back to Signal_Text
    result_signal = reverse_mapping[closest_signal]
    
    # Return result and supporting details
    return result_signal, closest_signal
