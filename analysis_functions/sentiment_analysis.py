import json

import dao.dao as dao
from datetime import datetime, timedelta
from config.logging_config import get_logger

logger = get_logger(__name__)

MAX_REDDIT_POSTS_FOR_SENTIMENT = 10
MAX_NEWS_ARTICLES_FOR_SENTIMENT = 10

def get_reddit_sentiment(stock, gpt_resp):
    logger.info("Collecting Reddit sentiment. ticker=%s", stock)
    reddit_links=dao.get_reddit_links(stock)
    if reddit_links is not None:
        posts = []
        
        for link in reddit_links[:MAX_REDDIT_POSTS_FOR_SENTIMENT]:
            posts.append(dao.get_reddit_post(link))
        
        if posts is not None:    
            for post in posts:
                if post:
                    gpt_resp.append(dao.get_gpt_score_with_confidence(stock, post))
    logger.info("Collected Reddit sentiment payloads. ticker=%s payload_count=%s", stock, len(gpt_resp))
    return gpt_resp

def get_news_sentiment(stock, gpt_resp):
    logger.info("Collecting news sentiment. ticker=%s", stock)
    try:
        articles = dao.get_news_links(stock)
        if articles is not None:
            recent_articles = []
            for article in articles:
                try:
                    published_at = datetime.strptime(article['publishedAt'], "%Y-%m-%dT%H:%M:%SZ")
                except Exception:
                    continue
                age = datetime.now() - published_at
                if age <= timedelta(days=30):
                    recent_articles.append((published_at, article))

            recent_articles.sort(key=lambda item: item[0], reverse=True)

            for _, article in recent_articles[:MAX_NEWS_ARTICLES_FOR_SENTIMENT]:
                age = datetime.now() - datetime.strptime(article['publishedAt'], "%Y-%m-%dT%H:%M:%SZ")
                if age <= timedelta(days=30):
                    post = ".".join([
                        str((article.get('source') or {}).get('name') or ""),
                        str(article.get('author') or ""),
                        str(article.get('title') or ""),
                        str(article.get('content') or ""),
                        str(article.get('description') or ""),
                        str(dao.get_news_post(article.get('url')) or ""),
                    ])
                    gpt_resp.append(dao.get_gpt_score_with_confidence(stock, post))
    except Exception:
        logger.exception("News sentiment collection failed. ticker=%s", stock)
        return gpt_resp
    logger.info("Collected sentiment payloads after news pass. ticker=%s payload_count=%s", stock, len(gpt_resp))
    return gpt_resp

def apply_sentiment_analysis(stock):
    logger.info("Running sentiment analysis. ticker=%s", stock)
    payloads = get_news_sentiment(
        stock,
        get_reddit_sentiment(stock, []),
    )
    try:
        scores = [
            float(item["score"])
            for item in (json.loads(payload) for payload in payloads)
            if float(item["confidence"]) >= 0.90
        ]
    except Exception:
        logger.exception("Sentiment payload parsing failed. ticker=%s", stock)
        return 0
    score = sum(scores) / len(scores) if scores else 0
    logger.info("Completed sentiment analysis. ticker=%s high_confidence_scores=%s sentiment_score=%s", stock, len(scores), score)
    return score
