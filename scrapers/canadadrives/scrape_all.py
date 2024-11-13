import datetime
import logging
import os
import time
import json
from psutil import version_info

import requests
import pandas as pd

# uncomment if selenium is required
from selenium import webdriver
import undetected_chromedriver as uc
from selenium.webdriver.chrome.options import Options
from decouple import config
# from selenium.webdriver.common.by import By
# from selenium.webdriver.support.ui import WebDriverWait
# from selenium.webdriver.support import expected_conditions as EC
# from selenium.common.exceptions import TimeoutException

# from base import log_config
from base import retry_decorator
from base.aws_handler import AWSHandler
from base.logging_decorator import handle_exceptions

logger_name = 'Canadadrives_scrapper_bots_canada'
email_subject = 'Canadadrives Bot Alert'
email_toaddrs = ['ajay.qode@gmail.com', 'karan@qodemedia.net', 'jordan@qodemedia.com', 'bikin@nerdplatoon.com']
logger = logging.getLogger(__name__)
scraper_dir = os.path.dirname(os.path.abspath(__file__))


# logging.basicConfig(format='%(levelname)s:%(message)s', level=logging.INFO)

root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))+'/{}'
chrome_options = Options()
# chrome_options.add_argument("--disable-extensions")
chrome_options.add_argument('--no-sandbox')
chrome_options.add_argument('--headless')
# chrome_options.add_argument('--disable-gpu')
# chrome_options.add_argument('--disable-dev-shm-usage')
# chrome_options.add_argument('--profile-directory=Default')
# chrome_options.add_argument('--user-data-dir=~/.config/google-chrome')
# driver = webdriver.Chrome(executable_path=root.format('chromedriver'),options=chrome_options)
# driver = uc.Chrome(version_main=config('CHROME_VERSION'))
driver = webdriver.Chrome(executable_path=config('CHROME_DRIVER'), options=chrome_options)
driver.maximize_window()

def scrape_data(car_dict):
    info = [None] * 27
    try:
        info[0] = str(datetime.date.today())
    except Exception:
        pass
    try:
        info[1] = None
    except KeyError:
        pass

    try:
        info[2] = car_dict["handle"]
    except KeyError:
        pass
    # url
    try:
        info[3] = "https://shop.canadadrives.ca/cars/" + car_dict["selling_province"].lower() + "/" + car_dict["handle"]
    except KeyError:
        pass
    # make
    try:
        info[4] = car_dict["make"]
    except KeyError:
        pass
    # model
    try:
        info[5] = car_dict["model"]
    except KeyError:
        pass
    # year
    try:
        info[6] = car_dict["year"]
    except KeyError:
        pass
    # mileage
    try:
        info[7] = car_dict["kms"]
    except KeyError:
        pass
    # Price
    try:
        info[8] = car_dict["variants"][0]["price"]
    except KeyError:
        pass
    # state
    try:
        info[10] = car_dict["selling_province"]
    except KeyError:
        pass
    # city
    try:
        if info[10] == 'ON':
            info[11] = 'Brampton'
        elif info[10] == 'BC':
            info[11] = 'Richmond'
        else:
            info[11] = 'Airdrie'
    except :
        pass
    # condition
    try:
        info[9] = "Used"
    except KeyError:
        pass

    try:
        info[12] = car_dict['transmission']
    except KeyError:
        pass

    try:
        info[13] = car_dict['drivetrain']
    except KeyError:
        pass

    try:
        info[14] = car_dict['body_type']
    except KeyError:
        pass
    try:
        info[15] = car_dict['exterior_colour']
    except KeyError:
        pass
    try:
        info[16] = car_dict['fuel_type']
    except KeyError:
        pass
    try:
        # trim
        info[17] = car_dict["trim"]
    except KeyError:
        pass
    try:
        info[19] = car_dict['vin']
    except KeyError:
        pass

    # info[20] = car_dict['vehicle']['OwnerCount']
    # info[21] = info_dict['vehicle']['AccidentCount']

    try:
        info[18] = str(car_dict["images"])
    except KeyError:
        pass

    price_history_dict = str({info[0]: info[8]}).replace("\'", "\"")
    info[22] = str(price_history_dict)
    try:
        info[23] = car_dict['Engine:']
    except KeyError:
        pass
    try:
        info[24] = car_dict['id']
    except KeyError:
        pass
    # info[25] = str(car_dict)
    # Carfax_url
    info[26] = None

    new_info.append(info)


def get_limit(shortform_province):
    # driver = uc.Chrome()
    link = 'https://shop.canadadrives.ca/cars/' + shortform_province
    driver.get(link)
    time.sleep(10)
    num_cars = driver.find_element_by_xpath(
        '//*[@id="app"]/div/main/div/div[1]/section/div/div/div[2]/div[1]/div[4]/div/strong').text
    return num_cars


@handle_exceptions(logger_name, email_subject, email_toaddrs)
@retry_decorator.retry(requests.exceptions.ConnectionError, tries=3, delay=120)
def main():
    global new_info
    new_info = []

    aws_handler = AWSHandler()
    local_file_path = os.path.join(scraper_dir, "results/canadadrives_result.csv")
    aws_bucket_file_path = "MasterCode1/scraping/canadadrives/canadadrives_result.csv"
    aws_bucket_folder_path = "MasterCode1/scraping/canadadrives"

    if not os.path.exists(local_file_path):
        aws_handler.download_from_aws(aws_bucket_file_path, local_file_path)

    scrape_df = pd.read_csv(local_file_path)
    province_list = {'Ontario': 'on', 'British Columbia': 'bc', 'Alberta': 'ab'}
    for province in province_list.values():
        logger.info(f"Scraping -> {province}")
        limit = get_limit(province)
        url = "https://d2cbackend.canadadrives.ca/graphql"
        payload = "{\"query\":\"\\r\\nquery FetchProducts ($sort_by:String, $sort_type:String, $page:Int, $limit:Int, $selling_province:String, $test_record:String) {\\r\\n          product (sort_by:$sort_by, sort_type:$sort_type, page:$page, limit:$limit, selling_province:$selling_province, test_record:$test_record) {\\r\\n            data {\\r\\n  \\r\\n  body_type\\r\\n  drivetrain\\r\\n  exterior_colour\\r\\n  fuel_type\\r\\n  handle\\r\\n  id\\r\\n  images {\\r\\n    id\\r\\n    src\\r\\n  }\\r\\n  kms\\r\\n  make\\r\\n  model\\r\\n  reservation {\\r\\n    vin\\r\\n    reservation_end_time\\r\\n    customer_id\\r\\n  }\\r\\n  selling_province\\r\\n  status\\r\\n  style\\r\\n  stock_number\\r\\n  salesforce_id\\r\\n  transmission\\r\\n  trim\\r\\n  variants {\\r\\n    id\\r\\n    price\\r\\n    sku\\r\\n    title\\r\\n  }\\r\\n  vin\\r\\n  year\\r\\n\\r\\n}\\r\\n            total\\r\\n            per_page\\r\\n          }\\r\\n        }\\r\\n      \",\"variables\":{\"limit\":limit_number_unique,\"page\":1,\"selling_province\":\"province_name_unique\",\"sort_by\":\"Featured\",\"sort_type\":\"desc\",\"test_record\":\"false\"}}"
        payload = payload.replace("limit_number_unique", limit).replace("province_name_unique", province.upper())
        headers = {
            'Content-Type': 'application/json',
            'Cookie': '__cflb=02DiuFJqGQ3ei2HjLwRSgBa3uN47UKJrMXpRLcVAHaJFn'
        }

        response = requests.request("POST", url, headers=headers, data=payload)
        car_dicts = json.loads(response.text)
        car_dicts = car_dicts["data"]["product"]["data"]
        # print(car_dicts[0])

        # break
        for car_dict in car_dicts:
            scrape_data(car_dict)
            # break

        # # print("WRITING TO CSV..")
        new_df = pd.DataFrame(new_info,
                              columns=['date_added', 'date_removed', 'title', 'url', 'make', 'model', 'year',
                                       'kilometers', 'price', 'condition', 'province', 'City',
                                       'Vehicle_information.transmission', 'Vehicle_information.drivetrain',
                                       'Vehicle_information.body_style',
                                       'Vehicle_information.exterior_colour', 'Vehicle_information.fuel_type',
                                       'Vehicle_information.trim', 'img_url', 'vin', 'NumOwners',
                                       'PrevAccident', 'price_history', 'Vehicle_information.engine', '_id',
                                       'metadata', 'car_fax_url'])

        scrape_df = pd.concat([scrape_df, new_df])
        scrape_df.to_csv(local_file_path, index=False)
        new_info = []

    # #For uploading to aws we need to provide only the bucket folder path instead of the full path as it tends to make unnecessary folders
    aws_handler.upload_to_aws(local_file_path, aws_bucket_folder_path)
    # # a.download_from_aws("MasterCodeUS/scraping/autolist/try.txt", "try_download.txt")


if __name__ == "__main__":
    main()
