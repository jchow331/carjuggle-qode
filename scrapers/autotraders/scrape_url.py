import os
# import re
import time
import random
import logging

import requests
import pandas as pd
import undetected_chromedriver as uc
from selenium import webdriver
from selenium.common.exceptions import NoSuchElementException
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
from decouple import config

from base import retry_decorator
# from base import log_config
from base.logging_decorator import handle_exceptions
from base.aws_handler import AWSHandler

logger_name = 'autotraders_scrapper_bots_canada'
email_subject = 'Autotraders Bot Alert'
email_toaddrs = ['karan@qodemedia.net', 'sana@qodemedia.com', 'jordan@qodemedia.com', 'bikin@nerdplatoon.com']
logger = logging.getLogger(__name__)

#scraper_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "results")
scraper_dir = os.path.dirname(os.path.abspath(__file__))
local_result_path = os.path.join(scraper_dir, "results/autotraders_result.csv")
local_links_path = os.path.join(scraper_dir, "results/all_links.csv")
aws_bucket_file_path = "MasterCode1/scraping/autotraders/autotraders_result.csv"
aws_bucket_folder_path = "MasterCode1/scraping/autotraders"


def scrape_url(page_num):
    """
    @param page_num: page_num to scrape the urls from
    @return: bool indicating if the previous link is found in the current scraping file
    """
    # CARS_PER_PAGE = 125
    all_links = []
    if not os.path.exists(local_links_path):
        old_df = pd.DataFrame(columns=["links", "featured"])
    else:
        old_df = pd.read_csv(local_links_path)

    driver.get(
        f"https://www.autotrader.ca/cars/?rcp=100&rcs={page_num * 100}&srt=9&prx=-1&loc=K2J6M5&hprc=True&wcp=True&sts=New-Used&inMarket=advancedSearch")
    # print(page_num)

    driver.find_element(By.XPATH, '//body').send_keys(Keys.CONTROL + Keys.END)
    time.sleep(random.randint(2, 5))
    try:
        WebDriverWait(driver, 20).until(
            EC.presence_of_element_located((By.XPATH, "//div[@class='listing-details']/h2/a")))
    except TimeoutException:
        pass

    priority_hrefs = driver.find_elements(By.XPATH,
        "//div[@class='listing-details']/h2/a")
    for href in priority_hrefs:
        try:
            all_links.append((href.get_attribute("href").split("?")[0], 'featured'))
        except AttributeError:
            pass
    all_listing_hrefs = driver.find_elements(By.XPATH,
        "//div[@class='listing-details organic']/h2/a")
    for href in all_listing_hrefs:
        try:
            all_links.append((href.get_attribute("href").split("?")[0], 'not featured'))
        except AttributeError:
            pass
        # We are only checking the previous listings for the all links in the all_listings section because
        # for the priority listings they can put the old cars to the front page
        if href.get_attribute("href") in all_previous_links:
            # print("previous link found stopping scraping the url")
            return True

    # print("len links -> ", len(all_links))
    new_df = pd.DataFrame(all_links, columns=["links", "featured"])
    old_df = pd.concat([old_df, new_df])
    old_df.drop_duplicates(inplace=True)
    old_df.to_csv(local_links_path, index=False)
    # print("writing to csv")
    all_links.clear()
    # driver.delete_all_cookies()

    return False


@handle_exceptions(logger_name, email_subject, email_toaddrs)
@retry_decorator.retry(requests.exceptions.ConnectionError, tries=3, delay=120)
def main():
    
    global all_previous_links, driver
    a = AWSHandler()
    MAX_PAGE_NUMBER = 1000
    
    a.download_from_aws(aws_bucket_file_path, local_result_path)
    old_df = pd.read_csv(local_links_path)
    all_previous_links = old_df["links"].tolist()

    all_previous_links = [link.split("?")[0] for link in all_previous_links]
    for i in range(1, MAX_PAGE_NUMBER):
        
        time.sleep(5)
        chrome_options = uc.ChromeOptions()
        #chrome_options.add_argument("--headless")
        #chrome_options.add_argument("--no-sandbox")
        # driver = uc.Chrome(options=chrome_options, version_main=config('CHROME_VERSION'))
        driver = webdriver.Chrome(executable_path=config('CHROME_DRIVER'),options=chrome_options)
        # driver = webdriver.Chrome(options=chrome_options)

        prev_link_found_bool = scrape_url(i)
        
        driver.quit()

        if prev_link_found_bool:
            break


if __name__ == '__main__':
    main()
