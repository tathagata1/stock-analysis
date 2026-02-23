import dao
from datetime import datetime, timedelta

def get_reddit_sentiment(stock, gpt_resp):
    
    reddit_links=dao.get_reddit_links(stock)
    if reddit_links is not None:
        posts = []
        
        for link in reddit_links:
            posts.append(dao.get_reddit_post(link))
        
        if posts is not None:    
            for post in posts:
                gpt_resp.append(dao.get_gpt_score_with_confidence(stock, post))
    
    return gpt_resp

def get_news_sentiment(stock, gpt_resp):
    try:
        articles = dao.get_news_links(stock)
        if articles is not None:
            for article in articles:
                age = datetime.now() - datetime.strptime(article['publishedAt'], "%Y-%m-%dT%H:%M:%SZ")
                if age <= timedelta(days=30):
                    post = str(article['source']['name'])+"."+str(article['author'])+"."+str(article['title'])+"."+str(article['content'])+"."+str(article['description'])+"."+dao.get_news_post(article['url'])
                    gpt_resp.append(dao.get_gpt_score_with_confidence(stock, post))
    except:
        return gpt_resp
    
    return gpt_resp
