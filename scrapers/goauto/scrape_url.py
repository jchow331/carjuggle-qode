import time
import math
import os
import re
import logging

import pandas as pd
import undetected_chromedriver as uc
from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
import requests
from decouple import config

from base import retry_decorator
# from base import log_config
from base.logging_decorator import handle_exceptions

logger_name = 'goauto_scrapper_bots_canada'
email_subject = 'Goauto Bot Alert'
email_toaddrs = ['karan@qodemedia.net', 'jordan@qodemedia.com','bikin@nerdplatoon.com']
logger = logging.getLogger(__name__)
scraper_dir = os.path.dirname(os.path.abspath(__file__))
local_file_path = os.path.join(scraper_dir, "results/all_links.csv")


def scrape_url(link):
    
    all_links = []
    
    driver.get(link)
    time.sleep(10)
    #Gets all listings instead of location based
    driver.find_element(By.XPATH, '//*[@id="app"]/main/div/div[2]/div/div[1]/div[2]/button').click()
    time.sleep(1)
    driver.find_element(By.XPATH, '//*[@id="app"]/main/div/div[2]/dialog/div/form/div/button[2]').click()
    time.sleep(1)
    
    num_cars = driver.find_element(By.XPATH, '//*[@id="app"]/main/div/div[2]/div/div[1]/div[2]/div').text
    num_cars = num_cars.replace(' vehicles', '').replace(',','')
    num_cars = int(num_cars)
    # logging.info(f'num_cars{num_cars}')

    # Get the number of pages to iterate through
    pages = math.ceil(num_cars/21)
    pages = 49
    #Nvm, don't use the above because the max pages will be 48 anyways (1000 car max)

    # click 'see more' results to extract all the links of the cars
    for i in range(1, pages):
        driver.get(f'https://www.goauto.ca/vehicles?page={i}')
        e = WebDriverWait(driver, 10).until(EC.visibility_of_element_located((By.CLASS_NAME, 'inventory_content__DIqP5')))
        e = driver.find_elements(By.CLASS_NAME, 'inventory_content__DIqP5')
        e = ['https://goauto.ca' + next(iter(re.findall('href="(.+?)"', x.get_attribute('innerHTML'))), '') for x in e]
        e = [x for x in e if x != 'https://goauto.ca']
        
        all_links.extend(e)
        
    all_links = list(set(all_links))
    driver.close()
  
    return all_links



@handle_exceptions(logger_name, email_subject, email_toaddrs)
@retry_decorator.retry(requests.exceptions.ConnectionError, tries=3, delay=120)
def main():
    global driver
    chrome_options = Options()
    # chrome_options.add_argument("--disable-extensions")
    chrome_options.add_argument('--no-sandbox')
    chrome_options.add_argument('--headless')
    # chrome_options.add_argument('--disable-gpu')
    # chrome_options.add_argument('--disable-dev-shm-usage')
    # chrome_options.add_argument('--profile-directory=Default')
    # chrome_options.add_argument('--user-data-dir=~/.config/google-chrome')
    # driver = uc.Chrome(options=chrome_options)
    # driver = uc.Chrome(version_main=config('CHROME_VERSION'))
    driver = webdriver.Chrome(options=chrome_options, executable_path=config('CHROME_DRIVER'))
    driver.maximize_window()
    time.sleep(5)
    # chrome_options = uc.ChromeOptions()
    # chrome_options.add_argument("--headless")
    all_links = scrape_url("https://www.goauto.ca/inventory/search?page=1&sort_by=_score&sort_order=DESC")
    df = pd.DataFrame(all_links, columns=['links'])
    df.to_csv(local_file_path, index=False)

if __name__ == '__main__':
    main()

