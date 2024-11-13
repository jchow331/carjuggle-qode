import logging
import time
import re
import os
from datetime import date

import pandas as pd
import numpy as np
import requests
import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium import webdriver
from bs4 import BeautifulSoup
from decouple import config

from base import log_config
from base import retry_decorator
from base.aws_handler import AWSHandler
from base.logging_decorator import handle_exceptions

logger_name = 'autocan'
email_subject = 'autocan Bot Alert'
email_toaddrs = ['jordan@qodemedia.com','karan@qodemedia.net']
logger = logging.getLogger(__name__)
scraper_dir = os.path.dirname(os.path.abspath(__file__))

local_file_path = os.path.join(os.path.join(scraper_dir, "results"), "autocan_result.csv")
aws_bucket_file_path = "MasterCode1/scraping/autocan/autocan_result.csv"
aws_bucket_folder_path = "MasterCode1/scraping/autocan"
website_url = 'https://www.autocan.ca/used-vehicles/'


def get_urls(driver) -> list:
    """Iterates through pages of listings to get all listing urls"""

    driver.get(website_url)
    time.sleep(10)

    soup = BeautifulSoup(driver.page_source, 'html.parser')
    pages = soup.find('div',{'class':'pagination-state'}).text
    pages = list(pages.split(" "))[-1]
    pages = '200'

    all_urls = []
    for i in range(0,int(pages)):
        driver.get(f'https://www.autocan.ca/used-vehicles/?_p={i}&_dFR%5Btype%5D%5B0%5D=Used&_dFR%5Btype%5D%5B1%5D=Certified%2520Used&_paymentType=our_price')
        time.sleep(2)
        links = driver.find_elements(By.CLASS_NAME, 'hit-content-title-wrap')
        links = ['https://www.autocan.ca/inventory' + re.findall('href="https://www.autocan.ca/inventory(.+?)"', x.get_attribute('innerHTML'))[0] for x in links]
        all_urls.extend(links)

    return all_urls


def scrape_listing(driver, url) -> None or list:
    """Scrapes the listing url, returns the scraped info or None if the page has expired"""
    
    info = [None]*21
    
    driver.get(url)
    if driver.current_url == 'https://www.autocan.ca/new-vehicles/':
        return None
    soup = BeautifulSoup(driver.page_source, 'html.parser')

    info[0] = str(date.today())
    info[2] = re.findall('"vin":"(.+?)"',str(soup))[-1]
    info[3] = url
    info[4] = re.findall('"make":"(.+?)"',str(soup))[-1]
    info[5] = re.findall('"model":"(.+?)"',str(soup))[-1]
    info[6] = re.findall('"year":(.+?),',str(soup))[-1].replace('"', '')
    info[7] = re.findall('"miles":"(.+?)",',str(soup))[-1]
    info[8] = re.findall('"price":(.+?),',str(soup))[-1]
    info[9] = re.findall('"type":"(.+?)"',str(soup))
    if "Certified Used" in info[9]:
        info[9] = 'Certified Used'
    elif 'Used' in info[9]:
        info[9] = 'Used'
    elif 'New' in info[9]:
        info[9] = 'New'
    try:
        location = re.findall('"location":"(.+?)"', str(soup))[-1]
        info[10] = re.findall('<br\\\/>(.+?), (.+?) ', location)[0][-1]
        info[11] = re.findall('<br\\\/>(.+?),', location)[0]
    except:
        pass
    
    details_list = soup.find_all('li', {'class':'basic-info-item'})
    details_list = [re.sub('<(.+?)>', '', str(x)).replace('\n', '') for x in details_list]
    
    info[12] = next(iter([x.replace('Transmission:','') for x in details_list if 'Transmission' in x]), None)
    info[13] = next(iter([x.replace('Drivetrain:','') for x in details_list if 'Drivetrain' in x]), None)
    info[14] = re.findall('"bodytype":"(.+?)"',str(soup))[-1]
    info[15] = re.findall('"ext_color":"(.+?)"',str(soup))[-1]
    info[16] = next(iter([x.replace('Engine:','') for x in details_list if 'Engine' in x]), None)
    info[17] = re.findall('"trim":"(.+?)"',str(soup))[-1]
    info[18] = re.findall('"image": "(.+?)"',str(soup))[0]
    
    #metadata
    info[20] = re.findall('"vehicle":\{(.+?)\}', str(soup))[0]
    
    #Deals with the 0km used discrepency
    if (info[7] == 0) and (info[9] == 'Used'):
        info[7] = np.nan
    
    #Gets rid of empty lists
    info = [None if i == '",' else i for i in info]
    info = [x if x else None for x in info]
    
    return info



@handle_exceptions(logger_name, email_subject, email_toaddrs)
@retry_decorator.retry(requests.exceptions.ConnectionError, tries=3, delay=240)
def main():

    aws_handler = AWSHandler()
    aws_handler.download_from_aws(aws_bucket_file_path, local_file_path)
    df = pd.read_csv(local_file_path, low_memory=False)
    all_urls = df['url'].tolist()

    options = uc.ChromeOptions()
    #options.add_argument('--headless')
    driver = webdriver.Chrome(options=options)

    #Gets all urls, excluding the previously scraped urls
    all_listings = get_urls(driver)
    all_listings = [x for x in all_listings if x not in all_urls]

    #Scrapes each url, removes None values after which occurs when the listing has expired
    all_info = []
    for url in all_listings:
        all_info.append(scrape_listing(driver, url))
    all_info = [x for x in all_info if x]

    driver.close()

    df = pd.concat([df, pd.DataFrame(all_info, columns=df.columns)])
    df.to_csv(local_file_path, index=False)
    aws_handler.upload_to_aws(local_file_path, aws_bucket_folder_path)


if __name__ == "__main__":
    main()
    