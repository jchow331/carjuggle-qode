import json
import logging
import re
import os
import concurrent.futures
from datetime import date

import requests
import pandas as pd
import numpy as np
from bs4 import BeautifulSoup
import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.chrome.options import Options
from selenium import webdriver
from selenium.webdriver.support import expected_conditions as EC
from decouple import config

from base import log_config
from base.aws_handler import AWSHandler
from base.logging_decorator import handle_exceptions

logger_name = 'kbb_ca'
email_subject = 'kbb_ca Bot Alert'
email_toaddrs = ['jordan@qodemedia.com', 'karan@qodemedia.net', 'bikin@nerdplatoon.com']
logger = logging.getLogger(__name__)
scraper_dir = os.path.dirname(os.path.abspath(__file__))

#chrome_options = uc.ChromeOptions()
#chrome_options.headless=True
#chrome_options.add_argument('--headless')
#chrome_options.add_argument('--no-sandbox')
#driver = webdriver.Chrome(options=chrome_options,version_main=config('CHROME_VERSION'))
#driver = webdriver.Chrome(options=chrome_options)

# Gets all concurrent urls
def get_urls(page_num):
    
    chrome_options = uc.ChromeOptions()
    chrome_options.headless=True
    chrome_options.add_argument('--headless')
    chrome_options.add_argument('--no-sandbox')
    driver = webdriver.Chrome(options=chrome_options, executable_path=config('CHROME_DRIVER'))
    #driver = webdriver.Chrome(options=chrome_options,version_main=config('CHROME_VERSION'))
    
    driver.get(f'https://www.kbb.ca/cars-for-sale/all/?nv=true&uv=true&pag={page_num}&srt=7')
    try:
        WebDriverWait(driver, 60).until(EC.visibility_of_element_located((By.ID, "preferred_listing")))
    except:
        driver.quit()
        return
    
    listings = driver.find_element(By.CLASS_NAME, 'saleContent')
    listings = re.findall('/car-details/(.+?)" id="(.+?)"', listings.get_attribute('innerHTML'))
    listings = [['https://kbb.ca/car-details/' + x[0], x[1]] for x in listings]
    all_urls.extend(listings)
    
    driver.quit()
    

# Checks previous urls
def check_url(index):
    
    page = requests.get(df.loc[index,'url'])
    if '302' in str(page.history):
        removed.append(index)
    else:
        soup = BeautifulSoup(page.text, 'html.parser')
        old_price = df.loc[index,'price']
        new_price = soup.find('h2',{'class':'price-title'}).text.replace(',','').replace('$','').strip()
        if int(new_price) != int(old_price):
            price_changed[index] = new_price


# Adds to price history for changed prices
def change_price(index):
    
    new_price = price_changed[index]
    history = json.loads(df.loc[index,'price_history'])
    history[str(date.today())] = new_price
    
    df.loc[index,'price'] = new_price
    df.loc[index,'price_history'] = str(history).replace("\'", "\"")


# Scrapes url
def scrape(listing_id):
    
    url = listing_id[0]
    page = requests.get(url)
    soup = BeautifulSoup(page.text, 'html.parser')
    try:
        info = [None]*23
        info[0] = str(date.today())
        info[2] = url.split('=')[-1]
        info[3] = next(iter(re.findall('"vin":"(.+?)"', str(soup))), None)
        if listing_id[1] == 'srp_listing':
            info[4] = 'No'
        elif listing_id[1] == 'preferred_listing':
            info[4] = 'Yes'
        info[5] = url
        info[6] = next(iter(re.findall('"make-name":"(.+?)"', str(soup))), None)
        info[7] = next(iter(re.findall('"model-name":"(.+?)"', str(soup))), None)
        info[8] = next(iter(re.findall('"year":"(.+?)"', str(soup))), None)
        # info[9] = soup.find('span',{'class':'carBlockOdomiter'}).text.replace(',','').replace('km','').strip()
        info[9] = soup.find('div',{'id':'mileageValue'}).text.replace('kilometers','').strip()
        # info[10] = soup.find('strong',{'class':'priceOn'}).text.replace(',','').replace('$','').strip()
        info[10] = soup.find('h2',{'class':'price-title'}).text.replace(',','').replace('$','').strip()
        if 'New ' in soup.find('div',{'class':'contentTop'}).text:
            info[11] = 'New'
        else:
            info[11] = "Used"
        # info[12] = soup.find('div',{'class':'dealerAddress'}).text.split('\n')[2].split(',')[1].split(' ')[1]
        # info[13] = soup.find('div',{'class':'dealerAddress'}).text.split('\n')[2].split(',')[0]
        address = soup.find_all('span',{'class':'dealer-address'})[1].text.replace(',', '///')
        info[12] = list(address.split("///"))[1]
        info[13] = list(address.split("///"))[0]
        features = soup.find_all('li',{'class':'list-bordered list-item'})
        features.extend(soup.find_all('li',{'class':'list-bordered list-item mobile-only'}))
        info[14] = next(iter([x.text.replace('\n','') for x in features if 'TRANSMISSION' in str(x)]), None)
        info[15] = next(iter(re.findall('>(.+?)wheel drive', str(soup).lower())), None)
        if info[15]:
            info[15] = info[15].split()[-1].title() + 'Wheel Drive'
        info[16] = next(iter([x.text.replace('\n','') for x in features if 'BODY' in str(x)]), None)
        info[17] = next(iter([x.text.replace('\n','') for x in features if 'Exterior' in str(x)]), None)
        info[18] = next(iter([x.text.replace('\n','') for x in features if 'FUEL' in str(x)]), None)
        info[19] = next(iter(re.findall('"trim":"(.+?)"', str(soup))), None)
        img = soup.find('div',{'id':info[2]})
        info[20] = next(iter(re.findall('content.homenetiol.com/(.+?).jpg', str(img))), None)
        if info[20]:
            info[20] = 'https://content.homenetiol.com/{}.jpg'.format(info[20])
        info[21] = str({info[0]:info[10]}).replace("\'", "\"")
        info[22] = next(iter(re.findall('_pxam.push\((.+?)\)',str(soup))), None)
        all_info.append(info)
    except Exception as e:
        logger.info("Error scraping vehicke information:".format(e))


@handle_exceptions(logger_name, email_subject, email_toaddrs)
def main():
    
    global df, all_urls, all_info, removed, price_changed
    a = AWSHandler()

    local_file_path = os.path.join(scraper_dir, "results/kbbCA_result.csv")
    aws_bucket_file_path = "MasterCode1/scraping/kbbCA/kbbCA_result.csv"
    aws_bucket_folder_path = "MasterCode1/scraping/kbbCA"

    a.download_from_aws(aws_bucket_file_path, local_file_path)
    df = pd.read_csv(local_file_path, low_memory=False)

    page_num = range(1,500)
    # This really can't be dynamic; upon testing, page 6 is the first page with no new listings which gives ~50 new.
    # Even using a reverse stopping point (no unique listings for x amount of pages),
    # there is no discernable point in which new listings don't appear entirely.

    # Gets all urls by page number
    logger.info('Getting URLs...')
    all_urls = []
    for page in page_num:
        get_urls(page)
    
    all_urls = [x for x in all_urls if x[0] not in df['url'].values.tolist()]

    # Scrapes all gotten urls
    logger.info('Scraping {} new listings'.format(len(all_urls)))
    all_info = []
    with concurrent.futures.ThreadPoolExecutor() as executor:
        executor.map(scrape, all_urls)
    df = pd.concat([df, pd.DataFrame(all_info, columns=['date_added','date_removed','_id','_VIN','is_featured','url','make','model','year',\
                                                   'kilometers','price','condition','province','City','Vehicle_information.transmission',\
                                                   'Vehicle_information.drivetrain','Vehicle_information.body_style',\
                                                   'Vehicle_information.exterior_colour','Vehicle_information.fuel_type',\
                                                   'Vehicle_information.trim','img_url','price_history','metadata'])])
    df = df.drop_duplicates(subset=['url'], keep='first')
    df = df.reset_index(drop=True)

    # Checks previous urls
    to_check = df[(df['date_added'] != str(date.today())) & (df['date_removed'].isnull())].index.values
    logger.info('Checking {} previous URLs...'.format(len(to_check)))
    removed, price_changed = [], {}
    with concurrent.futures.ThreadPoolExecutor() as executor:
        executor.map(check_url, to_check)

    # Updates price changes
    logger.info('Updating {} price changes...'.format(len(price_changed)))
    for index in price_changed:
        change_price(index)

    # Adds date removed for listings that have gone down
    df.loc[removed, 'date_removed'] = str(date.today())
    df.sort_values(by=['date_removed', 'date_added'],inplace=True)
    df.to_csv(local_file_path, index=False)
    
    a.upload_to_aws(local_file_path, aws_bucket_folder_path)

    
if __name__ == "__main__":
    main()
