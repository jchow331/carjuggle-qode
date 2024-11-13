import re
import os
import time
import logging
from datetime import date

import requests
from decouple import config
import pandas as pd
from selenium import webdriver
import undetected_chromedriver as uc
from bs4 import BeautifulSoup
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

from base import log_config
from base import retry_decorator
from base.aws_handler import AWSHandler
from base.logging_decorator import handle_exceptions

logger_name = 'mycar'
email_subject = 'mycar Bot Alert'
email_toaddrs = ['jordan@qodemedia.com']#, 'karan@qodemedia.net']
logger = logging.getLogger(__name__)
scraper_dir = os.path.dirname(os.path.abspath(__file__))

local_file_path = os.path.join(os.path.join(scraper_dir, "results"), "mycar_result.csv")
aws_bucket_file_path = "MasterCode1/scraping/mycar/mycar_result.csv"
aws_bucket_folder_path = "MasterCode1/scraping/mycar"
website_url = 'https://www.mycar.ca/used/hf/mycarottawa/'


def scroll_to_bottom(driver):
    """Scrolls to bottom of driver page"""

    SCROLL_PAUSE_TIME = 3

    # Get scroll height
    last_height = driver.execute_script("return document.body.scrollHeight")

    while True:
        # Scroll down to bottom
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")

        # Wait to load page
        time.sleep(SCROLL_PAUSE_TIME)

        # Calculate new scroll height and compare with last scroll height
        new_height = driver.execute_script("return document.body.scrollHeight")
        if new_height == last_height:
            break
        last_height = new_height
    
    return driver


def get_listings(driver) -> list:
    """Gets all listing urls on page after scrolling to bottom"""
    
    #Scroll to bottom
    driver = scroll_to_bottom(driver)

    #Get listings
    soup = BeautifulSoup(driver.page_source, 'html.parser')
    listings = soup.find_all('div',{'class':'btn-instock-inv-1 select-trim-mobile'})
    listings = ['https://mycar.ca' + re.findall('href="(.+?)"', str(x))[0] for x in listings]

    return listings


def get_history(driver, url) -> list:
    """Loads carfax url and returns [NumOwners, PrevAccident]"""
    
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
    
    return info


def scrape_listing(driver, url) -> list:
    """Scrapes an individual url and returns info scraped"""
    
    #driver.get(url)
    #soup = BeautifulSoup(driver.page_source, 'html.parser')
    page = requests.get(url)
    soup = BeautifulSoup(page.text, 'html.parser')


    info = [None]*26
    info_chunk = soup.find('table',{'class':'table mobile-margin'})
    info_chunk = info_chunk.find_all('tr')
    compact_info = next(iter(re.findall('"LowestPaymentWidgetParameters":"(.+?)"', str(soup))), None)
    image_list = soup.find('ul', {'id':'imgList'})
    image_list = [next(iter(re.findall('src="(.+?)"', str(x))), None) for x in image_list]
    carfax_info = soup.find('div', {'data-ga-event':'carfax_logo_vdp'})
    carfax_url = next(iter(re.findall('href="(.+?)"', str(carfax_info))), None)
    

    info[0] = str(date.today())
    info[2] = next(iter([x.text.replace('\n', '').replace('Stock #:', '').strip() \
              for x in info_chunk if 'Stock #:' in str(x)]), None)
    info[3] = next(iter(re.findall('vin=(.+?)&', compact_info)), None)
    info[5] = url
    info[6] = next(iter(re.findall('mk=(.+?)&', compact_info)), None)
    info[7] = next(iter(re.findall('model=(.+?)&', compact_info)), None)
    info[8] = next(iter(re.findall('yr=(.+?)&', compact_info)), None)
    info[9] = next(iter(re.findall('kms=(.+?)&', compact_info)), '').replace(',','')
    info[10] = next(iter(re.findall('cp=(.+?)&', compact_info)), None)
    info[11] = next(iter(re.findall('cond=(.+?)&', compact_info)), '').replace('U', 'Used')
    info[12] = 'Ontario'
    info[13] = 'Ottawa'
    info[14] = next(iter(re.findall('trans=(.+?)&', compact_info)), '').replace('AT', 'Automatic').replace('MT', 'Manual').split('&')[0]
    info[15] = next(iter(re.findall('drv=(.+?)&', compact_info)), '').split('&')[0]
    info[16] = next(iter(re.findall('style=(.+?)&', compact_info)), None)
    info[17] = next(iter([x.text.replace('\n', '').replace('Exterior:', '').strip() \
               for x in info_chunk if 'Exterior:' in str(x)]), None)
    info[18] = next(iter(re.findall('fuelType=(.+?)&', compact_info)), None)
    info[19] = next(iter(re.findall('trim=(.+?)&', compact_info)), None)
    info[20] = list(filter(lambda item: item is not None, image_list))
    if carfax_url:
        info[21] = carfax_url
        info[22], info[23] = get_history(driver, carfax_url)
    info[24] = str({info[0]:info[10]}).replace("\'", "\"")
    info

    return info

@handle_exceptions(logger_name, email_subject, email_toaddrs)
@retry_decorator.retry(requests.exceptions.ConnectionError, tries=3, delay=240)
def main():

    aws_handler = AWSHandler()
    aws_handler.download_from_aws(aws_bucket_file_path, local_file_path)
    df = pd.read_csv(local_file_path)

    chrome_options = uc.ChromeOptions()
    chrome_options.add_argument("--start-maximized")
    chrome_options.add_argument("--headless")
    # driver = uc.Chrome(options=chrome_options, version_main=107)
    driver = webdriver.Chrome(executable_path=config('CHROME_DRIVER'), options=chrome_options)
    driver.get(website_url)

    #Get listings
    all_listings = get_listings(driver)
    all_listings = [x for x in all_listings if x not in df['url']]
    
    #Scrape listings
    all_info = []
    for url in all_listings:
        all_info.append(scrape_listing(driver, url))

    driver.close()

    df = pd.concat([df, pd.DataFrame(all_info, columns=df.columns)])
    df.to_csv(local_file_path, index=False)
    aws_handler.upload_to_aws(local_file_path, aws_bucket_folder_path)
    


if __name__ == "__main__":
    main()
