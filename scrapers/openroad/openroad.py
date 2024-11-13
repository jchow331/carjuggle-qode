import logging
import time
import json
import sys
import os
import re
import concurrent.futures
from datetime import date

import pandas as pd
import numpy as np
import requests
from selenium import webdriver
from selenium.webdriver.common.by import By
import undetected_chromedriver as uc
from bs4 import BeautifulSoup
from decouple import config

from base import retry_decorator
from base import log_config
from base.logging_decorator import handle_exceptions

logger_name = 'openroad'
email_subject = 'openroad Bot Alert'
email_toaddrs = ['jordan@qodemedia.com', 'karan@qodemedia.net', 'bikin@nerdplatoon.com']
logger = logging.getLogger(__name__)

from base.aws_handler import AWSHandler
aws_handler = AWSHandler()

scraper_dir = os.path.dirname(os.path.abspath(__file__))
local_file_path = os.path.join(os.path.join(scraper_dir, "results"), "openroad_result.csv")
aws_bucket_file_path = "MasterCode1/scraping/openroad/openroad_result.csv"
aws_bucket_folder_path = "MasterCode1/scraping/openroad"


def scroll_to_bottom(driver):
    SCROLL_PAUSE_TIME = 0.5
    last_height = driver.execute_script("return document.body.scrollHeight")
    while True:
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(SCROLL_PAUSE_TIME)
        new_height = driver.execute_script("return document.body.scrollHeight")
        if new_height == last_height:
            break
        last_height = new_height
    return

        
def get_listings(page_num):
    driver.get(url.format(page_num))
    #scroll_to_bottom(driver)
    soup = BeautifulSoup(driver.page_source, 'html.parser')
    listings = soup.find_all('div',{'class':'vehicleTileWrapper'})
    listings = [re.findall('href="(.+?)"', str(x))[-1] for x in listings]
    return listings

def get_locations(url):
    
    page = requests.get(url)
    soup = BeautifulSoup(page.text, 'html.parser')
    
    info_chunk = re.findall('buList = \[(.+?)\n', str(soup))[-1]
    locations = re.findall('"urlName":"(.+?)"(.+?)"city":"(.+?)"(.+?)"abbrev":"(.+?)"', info_chunk)
    locations = [[x[0], x[2], x[4]] for x in locations]
    
    return locations


def scrape_listing(url):
    
    page = requests.get(f'https://openroadautogroup.com{url}')
    soup = BeautifulSoup(page.text, 'html.parser')
    
    info = [None]*26
    
    info_chunk = re.findall('vehicleVM = (.+?)\n', str(soup))[-1]
    
    info[0] = str(date.today())
    info[2] = next(iter(re.findall('"stockNumber":"(.+?)"', info_chunk)), None)
    info[3] = next(iter(re.findall('"vin":"(.+?)"', info_chunk)), None)
    info[5] = f'https://openroadautogroup.com{url}'
    info[6] = next(iter(re.findall('"make":"(.+?)"', info_chunk)), None)
    info[7] = next(iter(re.findall('"model":"(.+?)"', info_chunk)), None)
    info[8] = next(iter(re.findall('"vehicleYear":(.+?),', info_chunk)), None)
    info[9] = next(iter(re.findall('"mileage":(.+?),', info_chunk)), None)
    info[10] = int(float(next(iter(re.findall('"salePrice":(.+?),', info_chunk)), None)))
    info[11] = next(iter(re.findall('"status":\{"(.+?)"\}', info_chunk)), None)
    info[11] = next(iter(re.findall('"name":"(.+?)"', info[11])), None)
    dealer = next(iter(re.findall('"dealership":\{"(.+?)"\}', info_chunk)), None)
    if dealer:
        dealer = next(iter(re.findall('"urlName":"(.+?)"', dealer)), None)
        dealer = next(iter([x for x in locations if dealer==x[0]]), None)
        if dealer:
            info[12] = dealer[2]
            info[13] = dealer[1]
    info[14] = next(iter(re.findall('"transmission":\{"(.+?)"\}', info_chunk)), None)
    info[14] = next(iter(re.findall('"name":"(.+?)"', info[14])), None)
    info[15] = next(iter(re.findall('"drivetrain":\{"(.+?)"\}', info_chunk)), None)
    info[15] = next(iter(re.findall('"name":"(.+?)"', info[15])), None)
    info[16] = next(iter(re.findall('"bodyTypeName":"(.+?)"', info_chunk)), None)
    info[17] = next(iter(re.findall('"exterior_color_name":\["(.+?)"', info_chunk)), None)
    info[18] = next(iter(re.findall('"fuelType":"(.+?)"', info_chunk)), None)
    info[19] = next(iter(re.findall('"trim":"(.+?)"', info_chunk)), None)
    info[20] = re.findall('"thumbImageUrl":"(.+?)"', info_chunk)
    if '/images/comingSoon.jpg' in info[20]:
        info[20].remove('/images/comingSoon.jpg')
    info[24] = str({info[0]:info[10]}).replace("\'", "\"")
    info[25] = next(iter(re.findall('^(.+?)\},', info_chunk)), None)
    
    new_info.append(info)
    
    
def get_hist(url):
    
    info = [None,None]
    page = requests.get(url)
    soup = BeautifulSoup(page.text,'html.parser')
    
    if 'This Report Has Expired' in str(soup):
        return info
    
    if 'There are no accidents/damage reported on this vehicle.' in str(soup):
        info[0] = 0
    else:
        info[0] = 1
        
    owners = soup.find_all('div',{'class','mobile-table-cell'})
    owners = str(owners).count('New Owner Reported') + 1
    info[1] = owners
    
    return info


def check_status(x):
    
    page = requests.get(df.loc[x,'url'])
    soup = BeautifulSoup(page.text, 'html.parser')
    
    #If redirect, the listing has gone down
    if '404 Page Not Found' in str(soup):
        removed.append(x)
    #If no redirect, check for price change
    else:
        old_price = df.loc[x,'price']
        new_price = int(float(next(iter(re.findall('"salePrice":(.+?),', re.findall('vehicleVM = (.+?)\n', str(soup))[-1])), None)))
        
        if float(new_price) != old_price:
            price_changed[x] = new_price
    
    
def change_price(x):
    
    new_price = price_changed[x]
    try:
        history = json.loads(df.loc[x,'price_history'])
        history[str(date.today())] = new_price
    
        df.loc[x,'price'] = new_price
        df.loc[x,'price_history'] = str(history).replace("\'", "\"")
    except:
        pass


@handle_exceptions(logger_name, email_subject, email_toaddrs)
@retry_decorator.retry(requests.exceptions.ConnectionError, tries=3, delay=240)
def main():
    
    global df, removed, price_changed, new_info, makes, url, locations, driver

    aws_handler.download_from_aws(aws_bucket_file_path, local_file_path)
    df = pd.read_csv(local_file_path)
    try:
        df.drop('Unnamed: 0', inplace=True, axis=1)
    except:
        pass
    
    options = uc.ChromeOptions()
    options.headless=True
    options.add_argument('--headless')
    driver = webdriver.Chrome(options=options, executable_path=config('CHROME_DRIVER'))
    
    url = 'https://openroadautogroup.com/inventory?page={}&page_size=96&sort_by=created&sort_order=desc&init_ds=1'
    # driver = webdriver.Chrome()
    driver.get(url)
    scroll_to_bottom(driver)    #Scroll to bottom then get num pages
    soup = BeautifulSoup(driver.page_source, 'html.parser')
    pages = int(soup.find_all('button', {'class':'pageButton'})[-1].text)
    
    new_listings = []
    for page_num in range(1, pages+1):
        new_listings.extend(get_listings(page_num))
    new_listings = [x for x in new_listings if x not in df['url'].unique()]
    logger.info('{} new listings added.'.format(len(new_listings)))
    
    driver.quit()
    
    
    #Gets a dictionary of the dealerships on this website; this is necessary because location info isn't on the page
    locations = get_locations('https://openroadautogroup.com/locations')
    
    
    new_info = []
    with concurrent.futures.ThreadPoolExecutor() as executor:
        executor.map(scrape_listing, new_listings)
    df = pd.concat([df, pd.DataFrame(new_info, columns=['date_added','date_removed','_id','_VIN','is_featured','url','make','model','year','kilometers',\
                                                'price','condition','province','City','Vehicle_information.transmission',\
                                                'Vehicle_information.drivetrain','Vehicle_information.body_style',\
                                                'Vehicle_information.exterior_colour','Vehicle_information.fuel_type',\
                                                'Vehicle_information.trim','img_url','hist_url','NumOwners','PrevAccident',\
                                                'price_history','metadata'])])
    df = df.reset_index(drop=True)
    
    #Get urls to check
    to_check = df[(df['date_added'] != str(date.today())) & df['date_removed'].isnull()]
    to_check = to_check['url'].index.values
    
    #Checks which urls have been removed and which prices have changed
    removed, price_changed = [], {}
    with concurrent.futures.ThreadPoolExecutor() as executor:
        executor.map(check_status, to_check)
    
    #Updates prices and price histories for those with changes
    for x in price_changed:
        change_price(x)
    
    df.loc[removed, 'date_removed'] = str(date.today())
    df.sort_values(by=['date_removed', 'date_added'],inplace=True)
    
    df.to_csv(local_file_path, index=False)
    aws_handler.upload_to_aws(local_file_path, aws_bucket_folder_path)

    
if __name__ == "__main__":
    main()