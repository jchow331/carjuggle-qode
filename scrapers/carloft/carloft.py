import re
import os
import time
import logging
from datetime import date

import requests
from decouple import config
import pandas as pd
import concurrent.futures
import undetected_chromedriver as uc
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException

from base import log_config
from base.aws_handler import AWSHandler
from base.logging_decorator import handle_exceptions

logger_name = 'carloft'
email_subject = 'carloft Bot Alert'
email_toaddrs = ['jordan@qodemedia.com']#, 'karan@qodemedia.net']
logger = logging.getLogger(__name__)
scraper_dir = os.path.dirname(os.path.abspath(__file__))

local_file_path = os.path.join(os.path.join(scraper_dir, "results"), "carloft_result.csv")
aws_bucket_file_path = "MasterCode1/scraping/carloft/carloft_result.csv"
aws_bucket_folder_path = "MasterCode1/scraping/carloft"
website_url = 'https://www.carloft.ca/used-inventory/index.htm?start={}'


def scroll_to_bottom(driver):

    page_y = 1000
    for timer in range(0,50):
        driver.execute_script("window.scrollTo(0, "+str(page_y)+")")
        page_y += 1000  
        time.sleep(1)

    return driver


def get_page_urls(driver, num_listing_range) -> list:

    driver.get(website_url.format(num_listing_range*99))
    driver = scroll_to_bottom(driver)
    soup = BeautifulSoup(driver.page_source, 'html.parser')

    listings = soup.find_all('div', {'class':'vehicle-card-details-container'})
    listings = [next(iter(re.findall('a href="(.+?)"', str(x))),None) for x in listings]
    listings = [f'https://www.carloft.ca{x}' for x in listings if x]

    return listings


def scrape_listing(url) -> None:

    page = requests.get(url)
    soup = BeautifulSoup(page.text, 'html.parser')

    info_chunk = re.findall("DDC.dataLayer\['vehicles'\](.+?)\];", str(soup), re.DOTALL)[0]
    if not info_chunk:  #Break if no info
        return
    info = [None]*26

    info[0] = next(iter(re.findall('"inventoryDate": "(.+?)"', info_chunk)), '').replace('\\x2F','/')
    info[2] = next(iter(re.findall('"autodataCaId": "(.+?)"', info_chunk)), None)
    info[3] = next(iter(re.findall('"vin": "(.+?)"', info_chunk)), None)
    info[5] = url
    info[6] = next(iter(re.findall('"make": "(.+?)"', info_chunk)), None)
    info[7] = next(iter(re.findall('"model": "(.+?)"', info_chunk)), '').replace('\\x20', ' ').replace('\\x2D', '-')
    info[8] = next(iter(re.findall('"modelYear": "(.+?)"', info_chunk)), None)
    info[9] = next(iter(re.findall('"odometer": (.+?),', info_chunk)), None)
    info[10] = next(iter(re.findall('"askingPrice": "(.+?)"', info_chunk)), None)
    info[11] = next(iter(re.findall('"newOrUsed": "(.+?)"', info_chunk)), None)
    info[12] = next(iter(re.findall('"state":"(.+?)"', info_chunk)), None)
    info[13] = next(iter(re.findall('"city":"(.+?)"', info_chunk)), None)
    info[14] = next(iter(re.findall('"transmission": "(.+?)"', info_chunk)), '').replace('\\x20', ' ')
    info[15] = next(iter(re.findall('"driveLine": "(.+?)"', info_chunk)), '').replace('\\x20', ' ').replace('\\x2D', '-')
    info[16] = next(iter(re.findall('"bodyStyle": "(.+?)"', info_chunk)), '').replace('\\x20', ' ')
    info[17] = next(iter(re.findall('"exteriorColor": "(.+?)"', info_chunk)), '').replace('\\x20', ' ').replace('\\x2D', '-')
    info[18] = next(iter(re.findall('"fuelType": "(.+?)"', info_chunk)), '').replace('\\x20', ' ')
    info[19] = next(iter(re.findall('"trim": "(.+?)"', info_chunk)), '').replace('\\x20', ' ').replace('\\x7C', '|').replace('\\x2D', '-')
    info[20] = next(iter(re.findall('"uri":"(.+?)"', info_chunk)), None)
    info[24] = str({info[0]:info[10]}).replace("\'", "\"")

    all_info.append(info)



def main():

    global all_info
    
    a = AWSHandler()
    a.download_from_aws(aws_bucket_file_path, local_file_path)
    df = pd.read_csv(local_file_path)

    chrome_options = uc.ChromeOptions()
    chrome_options.add_argument("--headless")
    # driver = uc.Chrome(options=chrome_options, version_main=107)
    driver = webdriver.Chrome(executable_path=config('CHROME_DRIVER'), options=chrome_options)
    driver.get(website_url.format('0'))

    #Iterate through ranges of listings until no more
    all_listings = []
    for num_listing_range in range(10):
        all_listings.extend(get_page_urls(driver, num_listing_range))
    all_listings = [x for x in all_listings if x not in df['url'].tolist()]

    #Scrape listings
    all_info = []
    with concurrent.futures.ThreadPoolExecutor() as executor:
        executor.map(scrape_listing, all_listings)

    #Append to old df and save
    df = pd.concat([df, pd.DataFrame(all_info, columns=df.columns.tolist())])
    df.sort_values(by=['date_removed', 'date_added'], inplace=True)
    df.to_csv(local_file_path, index=False)
    a.upload_to_aws(local_file_path, aws_bucket_folder_path)


if __name__ == "__main__":
    main()


