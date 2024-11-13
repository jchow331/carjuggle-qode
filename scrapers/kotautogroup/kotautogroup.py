import logging
import time
import math
import re
import os
from datetime import date

import pandas as pd
import numpy as np
import requests
import concurrent.futures
import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup
from decouple import config

from base import log_config
from base import retry_decorator
from base.aws_handler import AWSHandler
from base.logging_decorator import handle_exceptions

logger_name = 'kotautogroup'
email_subject = 'kotautogroup Bot Alert'
email_toaddrs = ['jordan@qodemedia.com','karan@qodemedia.net']
logger = logging.getLogger(__name__)
scraper_dir = os.path.dirname(os.path.abspath(__file__))

local_file_path = os.path.join(os.path.join(scraper_dir, "results"), "kotautogroup_result.csv")
aws_bucket_file_path = "MasterCode1/scraping/kotautogroup/kotautogroup_result.csv"
aws_bucket_folder_path = "MasterCode1/scraping/kotautogroup"
website_url = "https://www.kotautogroup.com/inventory.html?filterid=aAb21q{}-10x0-0-0"


def get_urls(driver, page_number) -> list:
    """Takes page number and gets the urls from that page"""

    driver.get(website_url.format(page_number))
    soup = BeautifulSoup(driver.page_source, 'html.parser')

    urls = soup.find_all('li', {'class':'carBoxWrapper'})
    urls = [x.find('a') for x in urls]
    urls = [x.get('href') for x in urls]
    urls = [f'https://www.kotautogroup.com{x}' for x in urls if x!='javascript:void(0);']

    return urls


def get_history(url) -> list:
    """Loads carfax url and returns [NumOwners, PrevAccident]"""
    
    options = uc.ChromeOptions()
    options.add_argument("--headless=new")
    driver = uc.Chrome(options=options)

    info = [None,None]
    driver.get(url)
    try:
        element = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.ID, 'vehicle-details-section')))
    except:
        return info
    soup = BeautifulSoup(driver.page_source, 'html.parser')
    owners = soup.find_all('div',{'class','mobile-table-cell'})
    
    if 'This Report Has Expired' in str(soup) or not owners:
        return info
    
    if 'There are no accidents/damage reported on this vehicle.' in str(soup):
        info[1] = 0
    else:
        info[1] = 1

    owners = str(owners).count('New Owner Reported') + 1
    info[0] = owners

    driver.quit()
    
    return info


def scrape_listing(url) -> list:
    """Gets info from soup by requests"""

    page = requests.get(url)
    soup = BeautifulSoup(page.text, 'html.parser')
    info_chunk = str(soup.find_all('script', {'type':'application/ld+json'})[-1])

    info = [None] * 26
    info[0] = str(date.today())
    info[2] = next(iter(re.findall('"sku":"(.+?)"', info_chunk)), None)
    info[3] = next(iter(re.findall('data-vin="(.+?)"', str(soup))), None)
    info[5] = url
    info[6] = next(iter(re.findall('data-make="(.+?)"', str(soup))), None)
    info[7] = next(iter(re.findall('data-model="(.+?)"', str(soup))), None)
    info[8] = next(iter(re.findall('data-year="(.+?)"', str(soup))), None)
    info[9] = next(iter(re.findall('data-km="(.+?)"', str(soup))), None)
    info[10] = next(iter(re.findall('"price":"(.+?)"', info_chunk)), None)
    info[11] = next(iter(re.findall('data-condition="(.+?)"', str(soup))), '').title()
    info[12] = next(iter(re.findall('"addressRegion": "(.+?)"', str(soup))), None)
    info[13] = next(iter(re.findall('"addressLocality": "(.+?)"', str(soup))), None)
    info[14] = soup.find('span', {'id':'specsTransmission'})
    if info[14]:
        info[14] = info[14].text.replace('Transmission: ','')
    info[15] = soup.find('span', {'id':'specsDriveTrain'})
    if info[15]:
        info[15] = info[15].text.replace('Drive train: ','')
    info[16] = soup.find('span', {'id':'specsBodyType'})
    if info[16]:
        info[16] = info[16].text.replace('Category: ','')
    info[17] = soup.find('span', {'id':'specsExtColor'})
    if info[17]:
        info[17] = info[17].text.replace('Exterior Color: ','')
    info[18] = soup.find('span', {'id':'specsFuel'})
    if info[18]:
        info[18] = info[18].text.replace('Fuel: ', '')
    info[19] = next(iter(re.findall('data-trim="(.+?)"', str(soup))), None)
    try:
        info[20] = soup.find('li', {'class':'slide'}).find('a').get('href')
    except:
        pass
    info[21] = soup.find('a', {'title':'Get Carproof'})
    if info[21]:
        info[21] = info[21].get('href')
        info[22], info[23] = get_history(info[21])
    info[24] = str({info[0]:info[10]}).replace("\'", "\"")

    return info



@handle_exceptions(logger_name, email_subject, email_toaddrs)
@retry_decorator.retry(requests.exceptions.ConnectionError, tries=3, delay=240)
def main():

    aws_handler = AWSHandler()
    aws_handler.download_from_aws(aws_bucket_file_path, local_file_path)
    df = pd.read_csv(local_file_path, low_memory=False)

    #Page numbers go from 0-9, A-E as of the writing of the bot; not that many listings so just scrape 1-9
    options = uc.ChromeOptions()
    options.add_argument("--headless=new")
    driver = uc.Chrome(options=options)

    #Get all urls
    all_listings = []
    for page_numbers in range(0,10):
        all_listings.extend(get_urls(driver, page_numbers))
    all_listings = [x for x in all_listings if x not in df['url'].tolist()]
    driver.close()


    #Scrape urls in parallel
    all_info = []
    for url in all_listings:
        all_info.append(scrape_listing(url))
    
    df = pd.concat([df, pd.DataFrame(all_info, columns=df.columns)])
    df.to_csv(local_file_path, index=False)
    aws_handler.upload_to_aws(local_file_path, aws_bucket_folder_path)



if __name__ == "__main__":
    main()
    