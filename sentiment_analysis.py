import dao
from datetime import datetime, timedelta

MAX_REDDIT_POSTS_FOR_SENTIMENT = 10
MAX_NEWS_ARTICLES_FOR_SENTIMENT = 10

def get_reddit_sentiment(stock, gpt_resp):
    
    reddit_links=dao.get_reddit_links(stock)
    if reddit_links is not None:
        posts = []
        
        for link in reddit_links[:MAX_REDDIT_POSTS_FOR_SENTIMENT]:
            posts.append(dao.get_reddit_post(link))
        
        if posts is not None:    
            for post in posts:
                if post:
                    gpt_resp.append(dao.get_gpt_score_with_confidence(stock, post))
    
    return gpt_resp

def get_news_sentiment(stock, gpt_resp):
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
                    post = str(article['source']['name'])+"."+str(article['author'])+"."+str(article['title'])+"."+str(article['content'])+"."+str(article['description'])+"."+dao.get_news_post(article['url'])
                    gpt_resp.append(dao.get_gpt_score_with_confidence(stock, post))
    except:
        return gpt_resp
    
    return gpt_resp
