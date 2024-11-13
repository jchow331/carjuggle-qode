import os
import re
import logging
import pandas as pd
from datetime import date

import requests
from decouple import config
import concurrent.futures
import undetected_chromedriver as uc
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.by import By

from base import log_config
from base.aws_handler import AWSHandler
from base.logging_decorator import handle_exceptions

logger_name = 'carcanada'
email_subject = 'carcanada Bot Alert'
email_toaddrs = ['jordan@qodemedia.com', 'karan@qodemedia.net', 'bikin@nerdplatoon.com']
logger = logging.getLogger(__name__)
scraper_dir = os.path.dirname(os.path.abspath(__file__))

local_file_path = os.path.join(os.path.join(scraper_dir, "results"), "carcanada_result.csv")
aws_bucket_file_path = "MasterCode1/scraping/carcanada/carcanada_result.csv"
aws_bucket_folder_path = "MasterCode1/scraping/carcanada"
website_url = 'https://www.carcanada.com/vehicles/?view=grid&pg={}'


def get_urls(driver, page_num):
    
    driver.get(website_url.format(page_num))
    soup = BeautifulSoup(driver.page_source, 'html.parser')

    listings = soup.find_all('div',{'class':'vehicle-card__full-details'})
    listings = [re.findall('a href="(.+?)"', str(x))[0] for x in listings]
    return listings


def scrape_url(listing_url):
    
    info = [None] * 31
    
    page = requests.get(listing_url)
    soup = BeautifulSoup(page.text, 'html.parser')
    
    info_chunk = re.findall('"vehicle":\{(.+?)\};', str(soup))[0]
    
    info[0] = next(iter(re.findall('"stock_number":"(.+?)"', info_chunk)), None)
    info[1] = listing_url
    info[2] = next(iter(re.findall('"vin":"(.+?)"', info_chunk)), None)
    info[4] = next(iter(re.findall('"make":"(.+?)"', info_chunk)), None)
    info[5] = next(iter(re.findall('"model":"(.+?)"', info_chunk)), None)
    info[6] = next(iter(re.findall('"year":(.+?),', info_chunk)), None)
    info[7] = next(iter(re.findall('"odometer":(.+?),', info_chunk)), None)
    info[8] = next(iter(re.findall('"asking_price":(.+?),', info_chunk)), None)
    info[9] = next(iter(re.findall('"sale_class":"(.+?)"', info_chunk)), None)
    info[10] = next(iter(re.findall('"company_province":"(.+?)"', info_chunk)), None)
    info[11] = next(iter(re.findall('"company_city":"(.+?)"', info_chunk)), None)
    info[12] = next(iter(re.findall('"transmission":"(.+?)"', info_chunk)), None)
    info[13] = next(iter(re.findall('"drive_train":"(.+?)"', info_chunk)), None)
    info[14] = next(iter(re.findall('"body_style":"(.+?)"', info_chunk)), None)
    info[15] = next(iter(re.findall('"engine":"(.+?)"', info_chunk)), None)
    info[16] = next(iter(re.findall('"exterior_color":"(.+?)"', info_chunk)), None)
    info[17] = next(iter(re.findall('"fuel_type":"(.+?)"', info_chunk)), None)
    info[18] = next(iter(re.findall('"trim":"(.+?)"', info_chunk)), None)
    info[3] = f'{info[4]} {info[5]} {info[6]} {info[18]}'.replace('None','')
    info[20] = next(iter(re.findall('"image_original":"(.+?)"', info_chunk)), None)
    info[24] = next(iter(re.findall('"date_added":"(.+?) ', info_chunk)), None)
    info[27] = str({info[0]:info[10]}).replace("\'", "\"")
    info[28] = info[11]
    info[29] = next(iter(re.findall('"image_original":"(.+?)"', info_chunk)), None)
    
    all_info.append(info)
    
    
# Checks previous urls
def check_url(index):
    
    page = requests.get(df.loc[index,'url'])
    
    if page.history:
        removed.append(index)
    else:
        soup = BeautifulSoup(page.text, 'html.parser')
        info_chunk = re.findall('"vehicle":\{(.+?)\};', str(soup))[0]
        old_price = df.loc[index,'price']
        new_price = next(iter(re.findall('"asking_price":(.+?),', info_chunk)), None)
        if int(new_price) != int(old_price):
            price_changed[index] = new_price
    
        
@handle_exceptions(logger_name, email_subject, email_toaddrs)
def main():
    
    global all_info, df, removed, price_changed
    
    a = AWSHandler()
    a.download_from_aws(aws_bucket_file_path, local_file_path)
    df = pd.read_csv(local_file_path)
    
    chrome_options = webdriver.ChromeOptions()
    chrome_options.add_argument("--headless")
    # driver = uc.Chrome(options=chrome_options, version_main=107)
    driver = webdriver.Chrome(executable_path=config('CHROME_DRIVER'), options=chrome_options)
    driver.get(website_url.format('1'))
    
    #Get num pages
    num_pages = int(driver.find_element(By.CLASS_NAME, 'pagination__numbers').text.split('\n')[-1].replace('of ',''))
    
    #Iterate through page numbers and get urls
    all_listings = []
    for page in range(num_pages):
        all_listings.extend(get_urls(driver, page))
    all_listings = list(set(all_listings))
    all_listings = [x for x in all_listings if x not in df['url'].unique()]
    
    #Scrapes listings in parallel
    all_info = []
    with concurrent.futures.ThreadPoolExecutor() as executor:
        executor.map(scrape_url, all_listings)
    df = pd.concat([df, pd.DataFrame(all_info, columns=df.columns.tolist())])
    df = df.drop_duplicates(subset=['url'], keep='first')
    df = df.reset_index(drop=True)
        
    # Checks previous urls
    to_check = df[(df['date_added'] != str(date.today())) & (df['date_removed'].isnull())].index.values
    removed, price_changed = [], {}
    with concurrent.futures.ThreadPoolExecutor() as executor:
        executor.map(check_url, to_check)
        
    # Adds date removed for listings that have gone down
    df.loc[removed, 'date_removed'] = str(date.today())
    df.sort_values(by=['date_removed', 'date_added'],inplace=True)
    df.to_csv(local_file_path, index=False)

    a.upload_to_aws(local_file_path, aws_bucket_folder_path)


if __name__ == "__main__":
    main()