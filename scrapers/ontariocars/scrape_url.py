import os
import time
from datetime import date
from random import randint

import pandas as pd
from bs4 import BeautifulSoup
import undetected_chromedriver as uc
from decouple import config

# selenium options
options = uc.ChromeOptions()
options.headless=True
options.add_argument('--headless')
driver = uc.Chrome(options=options, version_main=config('CHROME_VERSION'))

scraper_dir = os.path.dirname(os.path.abspath(__file__))


def check_exists_by(title, by_type, driver):
    time.sleep(randint(8, 12))
    try:
        if by_type == 'id':
            (driver.find_element_by_id(title)).click()
        elif by_type == 'xpath':
            (driver.find_element_by_xpath(title)).click()
        elif by_type == 'class':
            (driver.find_elements_by_class_name(title))[0].click()
    except:
        try:
            time.sleep(randint(4, 8))
            if by_type == 'id':
                (driver.find_element_by_id(title)).click()
            elif by_type == 'xpath':
                (driver.find_element_by_xpath(title)).click()
            elif by_type == 'class':
                (driver.find_elements_by_class_name(title))[0].click()
        except:
            return False
    return True


def url_scrape():
    """Scraping all urls exist in OntarioCars"""
    local_url_file_path = os.path.join(scraper_dir, "results/url.csv")
    url = "https://www.ontariocars.ca/all-car-for-sale"
    driver.get(url)
    time.sleep(randint(5,10))
    soup = BeautifulSoup(driver.page_source, 'html.parser')
    check_exists_by('/html/body/div[1]/div[2]/div[4]/div[2]/div/button', 'xpath', driver)
    max_num = int(soup.find('span',{'data-bind':'text: showingCount'}).text.split(' ')[-1])
    try:
        no_of_loads = int(int(max_num) % 15) +1
    except:
        no_of_loads = 3392
    url_df = pd.DataFrame()
    for j in range(0, no_of_loads):
        urls = "https://www.ontariocars.ca/all-car-for-sale?page=" + str(j)
        driver.get(urls)
        time.sleep(randint(3, 4))
        soup = BeautifulSoup(driver.page_source, 'html.parser')
        for card in soup.find_all('header'):
            if 'VehicleTypeCategory' in card.find('a').get('href'):
                url_df = url_df.append({'url' : url + card.find('a').get('href')}, ignore_index=True)
                urls = url + card.find('a').get('href')
    url_df = url_df.drop_duplicates(keep="last")
    url_df.to_csv(local_url_file_path, index=False)
