from datetime import date
import time
import logging
from random import randint
import os

import pandas as pd
import requests
from decouple import config
import undetected_chromedriver as uc
from selenium import webdriver
from selenium.common.exceptions import NoSuchElementException
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options

from base import retry_decorator
# from base import log_config, retry_decorator
from base.logging_decorator import handle_exceptions

logger_name = 'carnationcanada scrapper_bots_canada'
email_subject = 'Carnationcanada Scrape_url  Bot Alert'
email_toaddrs=['karan@qodemedia.net', 'prabin@qodemedia.net', 'bikin@nerdplatoon.com', 'jordan@qodemedia.com']
# email_toaddrs = ['ajay.qode@gmail.com']
logger = logging.getLogger(__name__)
scraper_dir = os.path.dirname(os.path.abspath(__file__))
local_file_path = os.path.join(scraper_dir, "results/urls.csv")


def collect_url():
    df = pd.DataFrame(columns=['url', 'date_added'])
    base_link = 'https://www.carnationcanadadirect.ca/vehicles/?sc=used'
    driver.get(base_link)
    time.sleep(5)
    try:
        num_cars = WebDriverWait(driver, 20).until(
            EC.presence_of_element_located((By.XPATH, "/html/body/main/div/div[3]/div[1]/div[2]/div[2]/h5"))).text
        num_cars = num_cars.split(' ')[0].replace(",", "")
    except TimeoutException:
        try:
            num_cars = WebDriverWait(driver, 20).until(
                EC.presence_of_element_located((By.XPATH, '/html/body/main/div/div[2]/div[1]/div[2]/div[2]/h5'))).text
            num_cars = num_cars.split(' ')[0].replace(",", "")
        except :
            #avg num of cars in this site
            num_cars =100
    # print(num_cars)

    num_pages_per_scroll = 20
    load = int((int(num_cars) - 2) / num_pages_per_scroll)

    for load in range(1, load):
        driver.execute_script("window.scrollTo(0,document.body.scrollHeight)")
        time.sleep(randint(5, 7))

    for i in range(1, int(num_cars)):
        try:

            urls = driver.find_element_by_xpath(
                '/html/body/main/div/div[2]/div[1]/div/div[2]/div[3]/div[{}]/div/div[1]/a'.format(i)).get_attribute(
                'href')
            df.loc[i, 'url'] = urls
            df.loc[i, 'date_added'] = str(date.today())
        except NoSuchElementException:
            try:
                urls = driver.find_element_by_xpath(
                    '/html/body/main/div/div[2]/div[1]/div[2]/div[2]/div[3]/div[{}]/div/div[1]/a'.format(
                        i)).get_attribute('href')
                df.loc[i, 'url'] = urls
                df.loc[i, 'date_added'] = str(date.today())
            except NoSuchElementException:
                try:
                    urls = driver.find_element_by_xpath(
                        '/html/body/main/div/div[3]/div[1]/div[2]/div[2]/div[3]/div[{}]/div/div[1]/a'.format(
                            i)).get_attribute('href')
                    df.loc[i, 'url'] = urls
                    df.loc[i, 'date_added'] = str(date.today())
                except NoSuchElementException:
                    try:
                        urls = driver.find_element_by_xpath(
                            '/html/body/main/div/div[3]/div[1]/div[2]/div[2]/div[3]/div[{}]/div/div[1]/a'.format(
                                i)).get_attribute('href')
                        df.loc[i, 'url'] = urls
                        df.loc[i, 'date_added'] = str(date.today())
                    except NoSuchElementException:
                        pass
        # print(df.head())
    driver.quit()
    df.to_csv(local_file_path)

@handle_exceptions(logger_name, email_subject, email_toaddrs)
@retry_decorator.retry(requests.exceptions.ConnectionError, tries=3, delay=120)
def main():
    global driver
    chrome_options = Options()
    # options.add_argument('--headless')
    # chrome_options = Options()
    # chrome_options.add_argument("--disable-extensions")
    # chrome_options.add_argument('--no-sandbox')
    # # chrome_options.add_argument('--headless')
    # chrome_options.add_argument('--disable-gpu')
    # chrome_options.add_argument('--disable-dev-shm-usage')
    # chrome_options.add_argument('--profile-directory=Default')
    # chrome_options.add_argument('--user-data-dir=~/.config/google-chrome')
    # chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument('--no-sandbox')
    # driver = uc.Chrome(version_main=config('CHROME_VERSION'), options=chrome_options)
    driver = webdriver.Chrome(executable_path=config('CHROME_DRIVER'),options=chrome_options)
    collect_url()


if __name__ == "__main__":
    main()
