import math
import os.path
import logging
import requests
import sys

from selenium import webdriver
from decouple import config
import undetected_chromedriver as uc
import pandas as pd
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import NoSuchElementException
from base import retry_decorator
from base.aws_handler import AWSHandler
from base.logging_decorator import handle_exceptions

logger_name = "Usedcarcanada_scrapper_bots_canada"
email_subject = "Usedcarcanda Bot Alert"
email_toaddrs = ['jordan@qodemedia.com', 'ajay.qode@gmail.com', 'karan@qodemedia.net', 'bikin@nerdplatoon.com']
# email_toaddrs = []
logger = logging.getLogger(__name__)

scraper_dir = os.path.dirname(os.path.abspath(__file__))
local_url_path = os.path.join(os.path.join(scraper_dir, "results"), "all_links.csv")


chrome_options = uc.ChromeOptions()
chrome_options.add_argument("--no-sandbox")
# chrome_options.add_argument('--headless')
chrome_options.add_argument("--log-level=3")
chrome_options.add_argument("--window-size=1920x1080")
# chrome_options.add_argument('--disable-gpu')
# chrome_options.add_argument('--disable-dev-shm-usage')
# chrome_options.add_argument('--profile-directory=Default')
# chrome_options.add_argument('--user-data-dir=~/.config/google-chrome')
driver = uc.Chrome(chrome_options=chrome_options)
#driver = webdriver.Chrome(
#    executable_path=config("CHROME_DRIVER"), options=chrome_options
#)


def get_province_list():
    """
    :return: List of all the province and  numbers of cars each province have
    """
    province_list = driver.find_element(
        By.XPATH, "/html/body/div[2]/ul/li[1]/div/div/ul"
    )
    province_urls = province_list.find_elements(By.TAG_NAME, "li")
    province_urls_lists = []
    for elem in province_urls:
        a_tag = elem.find_element(By.TAG_NAME, "a")
        province_urls_lists.append(
            (a_tag.get_attribute("href"), a_tag.text.split("(")[-1][:-1])
        )
    return province_urls_lists


def get_car_urls_from_province(link, num_cars):
    """
    :param link: link of the car_page giving the cars in that province
    :param num_cars: Total number of cars in that province
    :return: List of link of all the cars in the given province and their 'city'
    """
    default_num_cars = num_cars
    car_conditions = ["condition-used", "condition-new"]
    info_list = []
    for car_condition in car_conditions:

        # Loads and gets total pages to iterate through
        try:
            driver.get(link + "/" + car_condition)
            WebDriverWait(driver, 10).until(
                EC.presence_of_element_located(
                    (By.XPATH, "/html/body/div[1]/div/div/div/div[2]/div/div[2]")
                )
            )
            num_cars = driver.find_element(
                By.XPATH, "/html/body/div[1]/div/div/div/div[2]/div/div[2]"
            ).text.split()[0]
            num_page = math.ceil(float(int(num_cars) / 15))
        except Exception:
            num_page = math.ceil(float(int(default_num_cars) / 15))

        # Iterates through pages
        for i in range(1, num_page + 1):
            # try:
            driver.get(link + "/" + car_condition + "?page=" + str(i))
            # Each page has a total of 15 cars
            try:
                WebDriverWait(driver, 10).until(
                    EC.presence_of_element_located(
                        (
                            By.XPATH,
                            "/html/body/div[1]/div/div/div/div[2]/div/div[4]/div[1]/a",
                        )
                    )
                )
            except Exception:
                pass
            no_of_items = len(
                driver.find_elements(By.CSS_SELECTOR, ".result-container .item")
            )
            for i in range(1, no_of_items):
                try:
                    car_link = driver.find_element(
                        By.CSS_SELECTOR,
                        f".result-container .item:nth-child({i}) a",
                    ).get_attribute("href")
                    city_province = driver.find_elements(
                        By.CSS_SELECTOR,
                        f".result-container .item:nth-child({i}) .item-detail-right p",
                    )[0].text
                except NoSuchElementException as e:
                    continue
                else:
                    info_list.append((car_link, city_province, car_condition))

    return info_list


@handle_exceptions(logger_name, email_subject, email_toaddrs)
@retry_decorator.retry(requests.exceptions.ConnectionError, tries=3, delay=120)
def main():
    link = "https://usedcarcanada.ca/"
    driver.get(link)
    # driver.maximize_window()

    province_urls_lists = get_province_list()

    for link, num_cars in province_urls_lists:
        # check if a given cars have cars to scrape
        if int(num_cars) > 0:
            scraped_cars_list = get_car_urls_from_province(link, num_cars)
            print("new_list:", len(scraped_cars_list))
            new_df = pd.DataFrame(
                scraped_cars_list, columns=["links", "city_province", "car_condition"]
            )
            if not os.path.exists(local_url_path):
                old_df = pd.DataFrame(
                    columns=["links", "city_province", "car_condition"]
                )
            else:
                old_df = pd.read_csv(local_url_path)
            df = pd.concat([old_df, new_df])
            df.drop_duplicates(inplace=True)
            df.to_csv(local_url_path, index=False)
    driver.quit()


if __name__ == "__main__":
    main()
