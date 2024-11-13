import logging
import math
import os
import random
import time

import pandas as pd
import requests
import undetected_chromedriver as uc
from decouple import config
from selenium import webdriver
from selenium.common.exceptions import NoSuchElementException
from selenium.webdriver.chrome.options import Options

from base import log_config, retry_decorator
from base.aws_handler import AWSHandler
from base.logging_decorator import handle_exceptions

logger_name = "Shopwilson_scrapper_bots_canada"
email_subject = "Shopwilson Url Bot Alert"
email_toaddrs = [
    "prabin@qodemedia.net",
    "ajay.qode@gmail.com",
    "karan@qodemedia.net",
    "jordan@qodemedia.com",
    "bikin@nerdplatoon.com",
]
# email_toaddrs = []
logger = logging.getLogger(__name__)

chrome_options = Options() 
chrome_options.headless = True
chrome_options.add_argument("--headless")
chrome_options.add_argument("--no-sandbox")
chrome_options.add_argument(
    "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/79.0.3945.79 Safari/537.36"
)
# chrome_options.add_argument('--disable-gpu')
# chrome_options.add_argument('--disable-dev-shm-usage')
# chrome_options.add_argument('--profile-directory=Default')
# chrome_options.add_argument('--user-data-dir=~/.config/google-chrome')
# uc.TARGET_VERSION = 100

# driver = uc.Chrome(options=chrome_options,version_main=config('CHROME_VERSION'))

driver = webdriver.Chrome(
    options=chrome_options, executable_path=config("CHROME_DRIVER")
)
# # driver.maximize_window()
driver.minimize_window()
scraper_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "results")
local_file_path = os.path.join(scraper_dir, "all_links.csv")


@handle_exceptions(logger_name, email_subject, email_toaddrs)
@retry_decorator.retry(requests.exceptions.ConnectionError, tries=3, delay=120)
def main():

    global driver
    df = pd.DataFrame( columns=["links", "feautures"])
    driver.get(
        "https://www.shopwilsons.com/used-vehicles/?_p=1&_dFR%5Btype%5D%5B0%5D=Pre-Owned&_dFR%5Btype%5D%5B1%5D=Certified%2520Pre-Owned&_paymentType=our_price"
    )
    # time.sleep(10)
    try:
        num_cars = driver.find_element_by_xpath('//*[@id="results-title"]/h1/span').text
        # num_cars = int(num_cars.split()[0].replace(",", ""))
        num_cars = int(num_cars.split(" ")[0])
        print("num cars:", num_cars)
    except NoSuchElementException:
        print("Inside exception")
        num_cars = 600

    num_page = math.ceil(num_cars / 20)

  
    

    driver.close()

    for i in range(1, num_page):
        print("Loop",i)
        all_links = []

        # Closing and reopening uc should get around the cloudflare block
        # chrome_options = Options()
        # # chrome_options.headless = True
        # chrome_options.add_argument("--headless")
        # chrome_options.add_argument("--no-sandbox")
        # driver = uc.Chrome(
        #     options=chrome_options, version_main=config("CHROME_VERSION")
        # )
        # chrome_options.add_argument(
        #     "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/79.0.3945.79 Safari/537.36"
        # )
        driver = webdriver.Chrome(
            executable_path=config("CHROME_DRIVER"), options=chrome_options
        )
        # driver.maximize_window()
        driver.get(
            f"https://www.shopwilsons.com/used-vehicles/?_p={i}&_dFR[type][0]=Pre-Owned&_dFR[type][1]=Certified%2520Pre-Owned&_paymentType=our_price"
        )
        # dynamic no of cars
        try:
            CARS_PER_PAGE = len(driver.find_elements_by_xpath("//*[@id='hits']/div"))
        except Exception as exc:
            print(exc)
            
        # time.sleep(10)
        # print(CARS_PER_PAGE)
        for j in range(1, CARS_PER_PAGE + 1):
            print("here")
            row = []
            try:
                
                # driver.get_screenshot_as_file("screenshot.png")
                row.append(
                    driver.find_element_by_xpath(
                        f'//*[@id="hits"]/div[{j}]/div/a'
                    ).get_attribute("href")
                )
                row.append(
                    driver.find_element_by_xpath(
                        f'//*[@id="hits"]/div[{j}]'
                    ).get_attribute("data-vehicle")
                )
                # print(type(row[1]))
                # print(all_links)
            except NoSuchElementException as nse:
                # print(nse)
                df.to_csv(local_file_path, index=False)
                # driver.get_screenshot_as_file("screenshot.png")
                logging.info("exception", nse.msg)
            else:
                all_links.append(row)
        # may give access denied after a while
        if i % 5 == 0:
            sleep_for = random.randint(1, 3) * 60
            time.sleep(sleep_for)
        # print("all links",all_links)
        df = pd.concat([df,pd.DataFrame(all_links, columns=["links", "feautures"])])
        
        # print("\n\n\n\n df",df)
        df.to_csv(local_file_path, index=False)
    # print("scraped")
    driver.quit()

    # print("Completed")


if __name__ == "__main__":
    main()
