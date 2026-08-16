import json

import dao.dao as dao
from datetime import datetime, timedelta, timezone
from config.logging_config import get_logger

logger = get_logger(__name__)

MAX_NEWS_ARTICLES_FOR_SENTIMENT = 10

def get_news_sentiment(stock, gpt_resp):
    logger.info("Collecting news sentiment. ticker=%s", stock)
    try:
        articles = dao.get_news_links(stock)
        if articles is not None:
            recent_articles = []
            for article in articles:
                try:
                    published_at = datetime.strptime(
                        article['publishedAt'], "%Y-%m-%dT%H:%M:%SZ"
                    ).replace(tzinfo=timezone.utc)
                except Exception:
                    continue
                age = datetime.now(timezone.utc) - published_at
                if age <= timedelta(days=30):
                    recent_articles.append((published_at, article))

            recent_articles.sort(key=lambda item: item[0], reverse=True)

            for _, article in recent_articles[:MAX_NEWS_ARTICLES_FOR_SENTIMENT]:
                age = datetime.now(timezone.utc) - datetime.strptime(
                    article['publishedAt'], "%Y-%m-%dT%H:%M:%SZ"
                ).replace(tzinfo=timezone.utc)
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
    payloads = get_news_sentiment(stock, [])
    scores = []
    invalid_payloads = 0
    for payload in payloads:
        try:
            item = json.loads(payload)
            score = float(item["score"])
            confidence = float(item["confidence"])
            if not -1 <= score <= 1 or not 0 <= confidence <= 1:
                raise ValueError("Sentiment values are outside their documented range")
            if confidence >= 0.90:
                scores.append(score)
        except (TypeError, ValueError, KeyError, json.JSONDecodeError):
            invalid_payloads += 1
            logger.warning("Ignoring malformed sentiment payload. ticker=%s", stock)
    score = sum(scores) / len(scores) if scores else 0
    logger.info(
        "Completed sentiment analysis. ticker=%s high_confidence_scores=%s "
        "invalid_payloads=%s sentiment_score=%s",
        stock,
        len(scores),
        invalid_payloads,
        score,
    )
    return score
