import re
import os
import time
import logging
from datetime import date

import requests
import pandas as pd
import concurrent.futures
import undetected_chromedriver as uc
from bs4 import BeautifulSoup
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

from base import log_config
from base import retry_decorator
from base.aws_handler import AWSHandler
from base.logging_decorator import handle_exceptions

logger_name = 'basantmotors'
email_subject = 'basantmotors Bot Alert'
email_toaddrs = ['jordan@qodemedia.com', 'karan@qodemedia.net']
logger = logging.getLogger(__name__)
scraper_dir = os.path.dirname(os.path.abspath(__file__))

local_file_path = os.path.join(os.path.join(scraper_dir, "results"), "basantmotors_result.csv")
aws_bucket_file_path = "MasterCode1/scraping/basantmotors/basantmotors_result.csv"
aws_bucket_folder_path = "MasterCode1/scraping/basantmotors"
website_url = 'https://www.basantmotors.com/vehicles/used/?st=year,desc&view=list&sc=used'


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
    """Scrolls down to bottom of page after changing view to 'list' and returns all urls"""

    driver.find_element(By.XPATH, '/html/body/main/div/div[2]/div[2]/div[1]/div[1]/select').click()
    time.sleep(1)
    driver.find_element(By.XPATH, '/html/body/main/div/div[2]/div[2]/div[1]/div[1]/select/option[1]').click()
    time.sleep(1)
    driver = scroll_to_bottom(driver)

    soup = BeautifulSoup(driver.page_source, 'html.parser')
    listings = soup.find_all('div',{'class':'vehicle-card__image-link gtm_vehicle_tile_cta'})
    listings = [x.get('href') for x in listings]

    return listings


def scrape_url(url) -> list:
    """Gets info from soup by request and returns info"""

    page = requests.get(url)
    soup = BeautifulSoup(page.text, 'html.parser')
    
    info_chunk = next(iter(re.findall('"vehicle":\{(.+?)\}', str(soup))), None)

    info = [None]*26
    info[0] = str(date.today())
    if not info_chunk:
        return info
    info[0] = next(iter(re.findall('"date_added":"(.+?) ', info_chunk)), None)
    info[2] = next(iter(re.findall('"stock_number":"(.+?)"', info_chunk)), None)
    info[3] = next(iter(re.findall('"vin":"(.+?)"', info_chunk)), None)
    info[5] = url
    info[6] = next(iter(re.findall('"make":"(.+?)"', info_chunk)), None)
    info[7] = next(iter(re.findall('"model":"(.+?)"', info_chunk)), None)
    info[8] = next(iter(re.findall('"year":(.+?),', info_chunk)), None)
    info[9] = next(iter(re.findall('"odometer":(.+?),', info_chunk)), None)
    info[10] = next(iter(re.findall('"asking_price":(.+?),', info_chunk)), None)
    info[11] = next(iter(re.findall('"sale_class":"(.+?)"', info_chunk)), None)
    info[12] = "BC"
    info[13] = "Surrey"
    info[14] = next(iter(re.findall('"transmission":"(.+?)"', info_chunk)), None)
    info[15] = next(iter(re.findall('"drive_train":"(.+?)"', info_chunk)), None)
    info[16] = next(iter(re.findall('"body_style":"(.+?)"', info_chunk)), None)
    info[17] = next(iter(re.findall('"exterior_color":"(.+?)"', info_chunk)), None)
    info[18] = next(iter(re.findall('"fuel_type":"(.+?)"', info_chunk)), None)
    info[19] = next(iter(re.findall('"trim":"(.+?)"', info_chunk)), None)
    info[20] = next(iter(re.findall('"image_original":"(.+?)"', info_chunk)), None)
    info[24] = str({info[0]:info[10]}).replace("\'", "\"")

    return info



@handle_exceptions(logger_name, email_subject, email_toaddrs)
@retry_decorator.retry(requests.exceptions.ConnectionError, tries=3, delay=240)
def main():
    
    aws_handler = AWSHandler()
    aws_handler.download_from_aws(aws_bucket_file_path, local_file_path)
    df = pd.read_csv(local_file_path)

    chrome_options = uc.ChromeOptions()
    chrome_options.add_argument("--headless=new")
    driver = uc.Chrome(options=chrome_options)#, version_main=107)
    driver.get(website_url)

    #Get all urls
    all_listings = get_listings(driver)
    driver.close()

    #Scrape urls
    with concurrent.futures.ThreadPoolExecutor() as executor:
        all_info = list(executor.map(scrape_url, all_listings))

    df = pd.concat([df, pd.DataFrame(all_info, columns=df.columns)])
    df.to_csv(local_file_path, index=False)
    aws_handler.upload_to_aws(local_file_path, aws_bucket_folder_path)



if __name__ == "__main__":
    main()
