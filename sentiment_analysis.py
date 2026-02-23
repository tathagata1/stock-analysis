import config
import dao
from datetime import datetime, timedelta
import utils

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

def get_annualreport_sentiment(stock, gpt_resp):
    
    flag=dao.get_annualreport(stock, config.path_data_dump+stock+"_annual_report.pdf")
    if flag:
        report_text = utils.extract_text_from_pdf(config.path_data_dump+stock+"_annual_report.pdf")
        post = shorten_report_recursively(stock, report_text)
        gpt_resp.append(dao.get_gpt_score_with_confidence(stock, post))
    
    return gpt_resp

def shorten_report_recursively(stock, report_text):
    
    primer = "extract relevant technical information for "+stock+", remove any unnecessary details, shorten the text to 200 words without loosing any meaning"

    report_list = report_text.split(" ")
    if (len(report_list)<=4000):
        
        return report_text
    
    report_grouped = utils.group_elements_recursively(report_list, group_size=2000)
    
    report_grouped_shortened = []
    for text in report_grouped:
        report_grouped_shortened.append(dao.get_gpt_misc(text, primer))
    report_grouped_shortened_regrouped = " ".join(map(str, report_grouped_shortened))
    
    return shorten_report_recursively(stock, report_grouped_shortened_regrouped)