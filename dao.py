import os
import time
import utils
import config
import logging
import pandas as pd
from bs4 import BeautifulSoup
from selenium.webdriver.common.by import By
from openai import OpenAI
from datetime import datetime, timedelta
import requests
import json


def get_gpt_score_with_confidence(stock, post):
    
    try:
        os.environ['OPENAI_API_KEY'] = config.chatgpt_key
        client = OpenAI()
        command='you need to analyse the following reddit post for '+stock+'. you need to respond in the format { "score": "VAR_X", "confidence": "VAR_Y" }. VAR_X can vary from -1.000 to +1.0000. VAR_Y will be your confidence on the buy/sell indication and will vary between 0.00 and 1.00. I do not need anything else other than { "score": "VAR_X", "confidence": "VAR_Y" }. Here is the text: '+post
        completion = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": "you are an expert in the stock market analysis."},
                {
                    "role": "user",
                    "content": command
                }
            ]
        )
    except:
        print("error in get_gpt_score_with_confidence")
        return '{ "score": "0", "confidence": "0" }'
    
    return completion.choices[0].message.content

def get_gpt_misc(text, primer):
    try:
        os.environ['OPENAI_API_KEY'] = config.chatgpt_key
        client = OpenAI()
        completion = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": primer},
                {
                    "role": "user",
                    "content": text
                }
            ]
        )
    except:
        print("error in get_gpt_misc")
        return 'unable to parse data'
    
    return completion.choices[0].message.content

def write_csv(file_name, tmp_data):
    if(os.path.isfile(file_name)):
        os.remove(file_name)
    data=pd.DataFrame(data=tmp_data)
    data.to_csv(file_name, index=True)
    
def write_json(file_name, tmp_data):
    if(os.path.isfile(file_name)):
        os.remove(file_name)
    with open(file_name, 'w') as file:
        json.dump(tmp_data, file, indent=4)

def read_json(file_name):
    with open(file_name, 'r') as file:
        content = file.read()

    return json.loads(content)

def get_yahoo_finance_stock_list(criteria, count=1):
    
    stocks = []
    
    try:
        driver = utils.get_driver()
        driver.get(config.stock_list+criteria+str(count))
        time.sleep(config.var_sleep)

        # Handle cookie consent
        try:
            cookie_accept_button = driver.find_element(By.XPATH, '//button[@class="btn secondary accept-all "]')
            cookie_accept_button.click()
            logging.info("Cookie consent accepted.")
        except Exception as e:
            logging.error(f"Could not accept cookie consent: {str(e)}")

        # Sort by 52-week change
        try:
            date_filter = driver.find_element(By.XPATH, '//*[@id="nimbus-app"]/section/section/section/article/section[1]/div/div[2]/div/table/thead/tr/th[11]/div')
            date_filter.click()
            logging.info("52-week change sorted.")
        except Exception as e:
            logging.error(f"Could not sort: {str(e)}")

        time.sleep(config.var_sleep)
        
        # Locate the table element containing stock data
        try:
            table = driver.find_element(By.XPATH, '//tbody')
            table_html = table.get_attribute('innerHTML')
            
            # Parse the table HTML with BeautifulSoup
            soup = BeautifulSoup(table_html, 'html.parser')

            # Extract all stock symbols
            for stock in soup.select('a[data-testid="table-cell-ticker"] .symbol'):
                stocks.append(stock.get_text(strip=True))
                
            return stocks

        except Exception as e:
            logging.error(f"Error in get_yahoo_finance_stock_list")

    finally:
        driver.close()
        logging.info("Browser closed.")

def get_yahoo_finance_5y(var_stock):
    
    try:
        driver=utils.get_driver()
        driver.get(config.base_url+var_stock+config.history)
        time.sleep(config.var_sleep)

        # Handle cookie consent
        cookie_accept_button = driver.find_element(By.XPATH, '//button[@class="btn secondary accept-all "]')
        cookie_accept_button.click()
        logging.info("Cookie consent accepted.")

        # Handle date filter
        date_filter = driver.find_element(By.XPATH, '//span[@class="label yf-1th5n0r"]')
        date_filter.click()
        logging.info("Date filter clicked.")
        
        five_year_button = driver.find_element(By.XPATH, '//button[@value="5_Y"]')
        five_year_button.click()
        logging.info("5-year filter clicked.")
        time.sleep(config.var_sleep)
        
        # Locate the table element containing historical stock data
        table = driver.find_element(By.XPATH, '//tbody')
        table_html = table.get_attribute('innerHTML')
        soup = BeautifulSoup(table_html, 'html.parser')
        rows = []
        for row in soup.find_all('tr'):
            cols = [col.get_text() for col in row.find_all('td')]
            if cols and "Dividend" not in cols[0]:  # Filter out rows where the first column contains 'Dividend'
                rows.append(cols)

        df = pd.DataFrame(data=rows, columns=['Date', 'Open', 'High', 'Low', 'Close', 'Adj Close', 'Volume'])
        df.dropna(inplace=True)
        #format df
        df.set_index('Date', inplace=True)
        df.index = pd.to_datetime(df.index, errors='coerce')
        df['Date']=df.index
        df.reset_index(drop=True, inplace=True)
        df['Volume'] = df['Volume'].apply(utils.remove_comma)
        df['TICKER']=var_stock
        return df

    except Exception as e:
        logging.error(f"Error in get_yahoo_finance_5y for "+var_stock)

    finally:
        driver.close()
        logging.info("Browser closed.")

def get_yahoo_finance_key_stats(var_stock):
    
    try:
        driver=utils.get_driver()
        driver.get(config.base_url+var_stock+config.stats)
        time.sleep(config.var_sleep)

        # Handle cookie consent
        try:
            cookie_accept_button = driver.find_element(By.XPATH, '//button[@class="btn secondary accept-all "]')
            cookie_accept_button.click()
            logging.info("Cookie consent accepted.")
        except Exception as e:
            logging.error(f"Could not accept cookie consent: {str(e)}")

        time.sleep(config.var_sleep)
        
        #get the header
        table = driver.find_element(By.XPATH, '//*[@id="nimbus-app"]/section/section/section/article/section[2]/div/table/thead')
        table_html = table.get_attribute('innerHTML')
        soup = BeautifulSoup(table_html, 'html.parser')
        for row in soup.find_all('tr'):
            header = [col.get_text() for col in row.find_all('th')]
         
        try:
            table = driver.find_element(By.XPATH, '//tbody')
            table_html = table.get_attribute('innerHTML')            
            soup = BeautifulSoup(table_html, 'html.parser')
            rows = []
            for row in soup.find_all('tr'):
                cols = [col.get_text() for col in row.find_all('td')]
                rows.append(cols)
                       
            # Convert the rows into a DataFrame
            df = pd.DataFrame(data=rows, columns=header)
            df.dropna(inplace=True)
            df.rename(columns={ df.columns[0]: "Date" }, inplace = True)
            df = df.transpose()
            df.columns = df.iloc[0]
            df = df.iloc[1:]
            df['Market Cap'] = df['Market Cap'].apply(utils.convert_number)
            df['Enterprise Value'] = df['Enterprise Value'].apply(utils.convert_number)
            df['Trailing P/E'] = df['Trailing P/E'].apply(utils.convert_number)
            df['Forward P/E'] = df['Forward P/E'].apply(utils.convert_number)
            df['PEG Ratio (5yr expected)'] = df['PEG Ratio (5yr expected)'].apply(utils.convert_number)
            df['Price/Sales'] = df['Price/Sales'].apply(utils.convert_number)
            df['Price/Book'] = df['Price/Book'].apply(utils.convert_number)
            df['Enterprise Value/Revenue'] = df['Enterprise Value/Revenue'].apply(utils.convert_number)
            df['Enterprise Value/EBITDA'] = df['Enterprise Value/EBITDA'].apply(utils.convert_number)
            df['TICKER']=var_stock
            return df

        except Exception as e:
            logging.error(f"Error in get_yahoo_finance_key_stats for "+var_stock)

    finally:
        driver.close()
        logging.info("Browser closed.")
        
def get_reddit_links(var_stock):
    
    try:
        driver=utils.get_driver()
        driver.get(config.reddit_url+config.reddit_param_1+var_stock+config.reddit_param_2)
   
        table = driver.find_element(By.XPATH, '//*[@id="main-content"]/div/reddit-feed')
        table_html = table.get_attribute('innerHTML')

        # Parse the table HTML with BeautifulSoup
        soup = BeautifulSoup(table_html, 'html.parser')

        # Extract all stock symbols
        comment_links = []
        for link in soup.select('a[data-testid="post-title"]'):
            # Extract the href attribute for each link
            href = link.get('href')
            if href:
                full_url = config.reddit_url+href  # Construct full URL
                comment_links.append(full_url)
        
        return comment_links[:5]
    except Exception as e:
        logging.error(f"Error in get_reddit_links for "+var_stock)
        return None

    finally:
        driver.close()
        logging.info("Browser closed.")

def get_reddit_post(link):
    
    try:
        # Initialize the driver and open the Reddit post link
        driver = utils.get_driver()
        driver.get(link)
        time.sleep(config.var_sleep)
        
        # Parse the loaded page HTML with BeautifulSoup
        soup = BeautifulSoup(driver.page_source, 'html.parser')

        # Remove scripts, styles, and other non-content elements
        for tag in soup(["script", "style", "header", "footer", "nav", "aside"]):
            tag.decompose()  # Remove the element from the soup

        # Extract all visible text as a single cleaned string
        page_text = soup.get_text(separator=' ', strip=True)
        
        # Define the text to be removed
        unwanted_text = (
            "Reddit and its partners use cookies and similar technologies to provide you with a better experience. "
            "By accepting all cookies, you agree to our use of cookies to deliver and maintain our services and site, "
            "improve the quality of Reddit, personalize Reddit content and advertising, and measure the effectiveness "
            "of advertising. By rejecting non-essential cookies, Reddit may still use certain cookies to ensure the "
            "proper functionality of our platform. For more information, please see our Cookie Notice and our Privacy "
            "Policy . Get the Reddit app Scan this QR code to download the app now Or check it out in the app stores"
        )

        # Remove the unwanted text from page_text
        page_text = page_text.replace(unwanted_text, "")
        page_text = page_text.replace("'", "")
        
        # Return the cleaned text
        return page_text

    except Exception as e:
        logging.error(f"Error in get_reddit_post for "+link)
        return None

    finally:
        # Ensure the driver is properly closed
        driver.quit()
        logging.info("Browser closed.")
        
def get_news_links(stock):
    current_date = (datetime.now() - timedelta(2)).strftime("%Y-%m-%d")
    url = config.newapi_url+config.newsapi_query+stock+config.newsapi_from+current_date+config.newapi_api+config.newsapi_key
    response = requests.get(url)
    data = json.loads(response.text)
    articles = data['articles']
    return articles
        
def get_news_post(link):
    
    try:
        # Initialize the driver and open the Reddit post link
        driver = utils.get_driver()
        driver.get(link)
        time.sleep(config.var_sleep)
        
        # Parse the loaded page HTML with BeautifulSoup
        soup = BeautifulSoup(driver.page_source, 'html.parser')

        # Remove scripts, styles, and other non-content elements
        for tag in soup(["script", "style", "header", "footer", "nav", "aside"]):
            tag.decompose()  # Remove the element from the soup

        # Extract all visible text as a single cleaned string
        page_text = soup.get_text(separator=' ', strip=True)
        
        page_text = page_text.replace("'", "")
        
        # Return the cleaned text
        return page_text

    except Exception as e:
        logging.error(f"Error in get_news_post for "+link)
        return None

    finally:
        # Ensure the driver is properly closed
        driver.quit()
        logging.info("Browser closed.")

def get_annualreport(stock, filename):
    
    driver = utils.get_driver()

    try:
        driver.get(config.annualreports+stock)
        time.sleep(config.var_sleep)

        link_element = driver.find_element(By.XPATH, '/html/body/div[8]/section[1]/div[2]/ul/li[2]/span[1]/a')
        link_element.click()
        time.sleep(config.var_sleep)

        #get the annual report
        annualreport_element = driver.find_element(By.XPATH, '/html/body/div[8]/section/div[2]/div[2]/div[1]/div[2]/div[2]/a[1]')
        annualreport_url = annualreport_element.get_attribute('data-uw-original-href')
        annualreport_response = requests.get(annualreport_url)
        if annualreport_response.status_code == 200:
            with open(filename, "wb") as pdf_file:
                pdf_file.write(annualreport_response.content)
            flag = True
        else:
            flag = False
        
    except Exception as e:
        logging.error(f"Error in get_annualreport for "+stock)
        flag = False

    finally:
        # Ensure the driver is properly closed
        driver.quit()
        logging.info("Browser closed.")
        return flag
