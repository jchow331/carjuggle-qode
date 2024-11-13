import time
import re
import json
import os

import pandas as pd
import requests
import datetime
import logging
import undetected_chromedriver as uc
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from decouple import config

from base import retry_decorator
# from base import log_config
from base.logging_decorator import handle_exceptions
from base.aws_handler import AWSHandler


scraper_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "results")
print(scraper_dir)
local_file_path = os.path.join(scraper_dir, "kitchenerford_result.csv")
# print(local_file_path)
logger_name = 'kitchenerford_scrapper_bots_canada'
email_subject = 'kitchenerford Bot Alert'
# email_toaddrs = ['ajay.qode@gmail.com', 'summit@qodemedia.com']
email_toaddrs=['ajay.qode@gmail.com', 'karan@qodemedia.net', 'jordan@qodemedia.net', 'bikin@nerdplatoon.com']
logger = logging.getLogger(__name__)
aws_bucket_file_path = "MasterCode1/scraping/kitchenerford/kitchenerford_result.csv"
aws_bucket_folder_path = "MasterCode1/scraping/kitchenerford"

def get_car_number(link):
    # driver = uc.Chrome(version_main=config('CHROME_VERSION'))\
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument('--no-sandbox')
    driver = webdriver.Chrome(executable_path=config('CHROME_DRIVER'), options=chrome_options)
    driver.get(link)
    time.sleep(5)
    cars_num = driver.find_element_by_xpath('//*[@id="total-vehicle-number"]').text.split()[0].replace(",", "")
    driver.quit()
    return int(cars_num)


def extract_info(json_text):
    car_dicts=json.loads(json_text)
    new_info=[]
    # print(car_dicts)
    for car_dict in car_dicts.values():
        # print(car_dict)
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
            info[2] = str(car_dict["year"])+car_dict["make"]+car_dict["model"]
        except KeyError:
            pass
        # url
        try:
            info[3] = "https://www.kitchenerford.com"+car_dict["detailUrl"]
        except:
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
            info[7] = car_dict["mileage"]
        except KeyError:
            pass
        # Price
        try:
            info[8] = car_dict["msrp_total"]
        except KeyError:
            pass
        # state
        try:
            info[10] = car_dict["province"]
        except KeyError:
            pass
        # city
        try:
            info[11] = car_dict["city"]
        except KeyError:
            pass
        # condition
        try:
            info[9] = car_dict["condition"]
        except KeyError:
            pass

        try:
            info[12] = car_dict['transmission']
        except KeyError:
            pass

        try:
            info[13] = car_dict['driveTrain']
        except KeyError:
            pass

        try:
            info[14] = car_dict['bodyStyle']
        except KeyError:
            pass
        try:
            info[15] = car_dict['exteriorColour']
        except KeyError:
            pass
        try:
            info[16] = car_dict['fuelType']
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
            info[18] = car_dict["images"]["adminToolNoImage"]
        except KeyError:
            pass

        price_history_dict = str({info[0]: info[8]}).replace("\'", "\"")
        info[22] = str(price_history_dict)
        try:
            info[23] = car_dict['engine']
        except KeyError:
            pass
        try:
            info[24] = car_dict['vehicleId']
        except KeyError:
            pass
        info[25] = str(car_dict)
        # Carfax_url
        info[26] = None

        new_info.append(info)
    return new_info

@handle_exceptions(logger_name, email_subject, email_toaddrs)
@retry_decorator.retry(requests.exceptions.ConnectionError, tries=3, delay=540)
def main():
    #For used and new cars
    car_urls = ["https://www.kitchenerford.com/used/pg/", "https://www.kitchenerford.com/new/pg/"]

    # cars_number = []
    # for car_url in car_urls:
    #     cars_number.append(get_car_number(car_url))
    # NUMBER_OF_CARS_PER_PAGE = 9
    # num_pages_required =[car_number//NUMBER_OF_CARS_PER_PAGE for car_number in cars_number]

    #Uncomment for dynamic
    num_pages_required =[87, 68]
    # print(num_pages_required)

    aws_handler=AWSHandler()
    if not os.path.exists(local_file_path):
        aws_handler.download_from_aws(aws_bucket_file_path, local_file_path)
    scrape_df =pd.read_csv(local_file_path)

    for j,car_url in enumerate(car_urls):
        new_infos=[]
        for i in range(1,num_pages_required[j]):
            payload = {}
            headers = {
                'Cookie': 'eDealer=%7B%22guid%22%3A%22%7B771667CF-482C-81BB-433F-6F9F5D693F72%7D%22%2C%22parent%22%3A%22%7B4DA2A8E3-124B-4371-49C9-44566E5163B8%7D%22%2C%22server%22%3A%22.kitchenerford.com%22%2C%22referrer%22%3Anull%7D'
            }
            print(car_url+str(i))
            response = requests.request("POST", car_url+str(i), headers=headers, data=payload)

            json_text = re.findall('vehicleArray =(.+?);\n|$', response.text)
            # print(json_text)
            try:
                new_info=extract_info(json_text[1])
                new_infos.extend(new_info)
            except:
                pass
        new_df=pd.DataFrame(new_infos,columns=['date_added', 'date_removed', 'title', 'url', 'make', 'model', 'year',
                                               'kilometers', 'price', 'condition', 'state', 'City',
                                               'Vehicle_information.transmission', 'Vehicle_information.drivetrain',
                                               'Vehicle_information.body_style',
                                               'Vehicle_information.exterior_colour', 'Vehicle_information.fuel_type',
                                               'Vehicle_information.trim', 'img_url', 'vin', 'NumOwners',
                                               'PrevAccident', 'price_history', 'Vehicle_information.engine', '_id',
                                               'metadata', 'car_fax_url'])
        scrape_df = pd.concat([scrape_df,new_df])
        scrape_df.to_csv(local_file_path, index=False)
    aws_handler.upload_to_aws(local_file_path, aws_bucket_folder_path)





if __name__ == '__main__':
    main()
