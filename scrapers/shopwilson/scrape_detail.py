from datetime import date
import os
import re
import time
import logging
import requests

import pandas as pd
import undetected_chromedriver as uc
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium import webdriver
from selenium.webdriver.common.by import By
from decouple import config

from base.aws_handler import AWSHandler
from base import log_config
from base import retry_decorator
from base.aws_handler import AWSHandler
from base.logging_decorator import handle_exceptions

logger_name = 'Shopwilson_scrapper_bots_canada'
email_subject = 'Shopwilson Listing Bot Alert'
email_toaddrs = ['bikin@nerdplatoon.com', 'prabin@qodemedia.net', 'ajay.qode@gmail.com', 'karan@qodemedia.net','jordan@qodemedia.com']
logger = logging.getLogger(__name__)

scraper_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "results")
local_file_path = os.path.join(scraper_dir, "shopwilson_result.csv")

aws_bucket_file_path = "MasterCode1/scraping/shopwilsons/shopwilson_result.csv"
aws_bucket_folder_path = "MasterCode1/scraping/shopwilsons"

root = os.path.dirname(os.path.abspath(__file__))+'/{}'
#chrome_options = Options()
# chrome_options.add_argument('--no-sandbox')
#chrome_options.add_argument('--headless')
# chrome_options.add_argument('--disable-gpu')
# chrome_options.add_argument('--disable-dev-shm-usage')
# chrome_options.add_argument('--profile-directory=Default')
# # chrome_options.add_argument('--user-data-dir=~/.config/google-chrome')
#driver = uc.Chrome(options=chrome_options)#,version_main=config('CHROME_VERSION'))
# driver = webdriver.Chrome(executable_path=config('CHROME_DRIVER'),options=chrome_options)
#driver.maximize_window()


def get_address():
    driver.get("https://www.shopwilsons.com/")
    time.sleep(5)
    addr = driver.find_element_by_xpath('//*[@id="header"]/div[1]/div/div/div[1]/span[5]').text
    address = str(addr).split(",")
    city = address[0].split()[-1]
    state = address[-1].split()[0]
    return city, state


city, state = 'Guelph', 'ON'
# city,state = get_address()

def scrape_detail(info):
    
    chrome_options = Options()
    chrome_options.add_argument('--headless')
    driver = uc.Chrome(options=chrome_options, version_main=100)
    
    link, info_dict = info
    info_dict = eval(info_dict)
    # print(link)
    driver.get(link)
    info = [None] * 26
    info[0] = date.today().strftime('%Y/%m/%d')
    info[1] = None
    # title
    info[3] = link
    try:
        script=WebDriverWait(driver, 3).until(
            EC.presence_of_element_located(
                (By.XPATH, '//*[@id="whitewrap"]/script[1]'))).get_attribute("innerHTML")
        meta_data_dict = eval(script)
        info[25] = str(meta_data_dict)
        info[2] = meta_data_dict["name"]
    except:
        driver.save_screenshot(r'C:\Users\imffb\project\carjuggle-scrapers\scrapers\shopwilson\results\screenie.png')
        print('huh')
        meta_data_dict=''

    try:
        info[4] = info_dict["make"]
    except KeyError:
        pass

    try:
        info[5] = info_dict["model"]
    except KeyError:
        pass

    try:
        # year
        # info[6] = re.findall('(\d+)',info[2])[0]
        info[6] = info_dict["year"]
    except AttributeError:
        pass
    try:
        kilo = WebDriverWait(driver, 5).until(EC.presence_of_element_located((By.CLASS_NAME, 'basic-info-wrapper'))).text
    except Exception:
        pass
    try:
        info[7] = re.findall('Kilometers:\\n(.+?)\\n', kilo)[0].replace(',','')
    except Exception:
        pass

    try:
        # price
        info[8] = info_dict["msrp"]
    except Exception:
        pass

    # condition
    try:
        info[9] = info_dict["type"]
    except Exception:
        pass

    # city
    try:
        info[11] = city
    except Exception:
        pass

    try:
        info[10] = state
    except Exception:
        pass

    # drivetrain
    # try:
    #     info[13] = re.findall('"drivetrain":(.+?),|$', str(soup))[0].replace("\"","")
    # except KeyError:
    #     pass

    # trim
    try:
        info[17] = info_dict["trim"]
    except KeyError:
        pass

    # img_url
    try:
        info[18] = meta_data_dict["image"]
    except Exception:
        pass

    # fuel_type
    try:
        info[16] = info_dict["fueltype"]
    except KeyError:
        pass

    # 'Body Style'
    try:
        info[14] = info_dict["bodystyle"]
    except KeyError:
        pass

    # transmission
    try:
        info[12] = re.findall('Trans:\\n(.+?)$', kilo)[0]
    except Exception:
        pass

    try:
        info[15] = info_dict["ext_color"]
        # print(type(info_dict['enh_vehic_variant']))
    except KeyError as e:
        pass

    try:
        info[19] = info_dict['vin']
    except KeyError as e:
        pass

    try:
        # engine
        info[23] = re.findall('Engine:\\n(.+?)\\n', kilo)[0]
    except :
        pass
    # num_owners
    # try:
    #     info[20] = None
    # except Exception:
    #     pass
    # # num_accidents
    # try:
    #     info[21] = None
    # except Exception:
    #     pass

    try:
        price_dict = {date.today().strftime('%Y/%m/%d'): info[8]}
        info[22] = str(price_dict)
        # print(info)
    except Exception:
        pass

    try:
        info[24] = info_dict["stock"]
    except KeyError as e:
        pass

    driver.close()
    new_info.append(info)

@handle_exceptions(logger_name, email_subject, email_toaddrs)
@retry_decorator.retry(requests.exceptions.ConnectionError, tries=3, delay=120)
def main():
    global new_info, driver
    new_info = []
    aws_handler = AWSHandler()
    links = pd.read_csv(os.path.join(scraper_dir, "all_links.csv"))
    new_urls = links.values.tolist()

    if not os.path.exists(local_file_path):
        aws_handler.download_from_aws(aws_bucket_file_path, local_file_path)
    scrape_df = pd.read_csv(local_file_path)

    for i, link in enumerate(new_urls):
        #print(i,'------',link)
        scrape_detail(link)
        #print(new_info)
        if i % 50 == 0 or i == len(new_urls) - 1:
            df_scraped = pd.DataFrame(new_info,
                                      columns=['date_added', 'date_removed', 'title', 'url', 'make', 'model', 'year',
                                               'kilometers', 'price', 'condition', 'province', 'City',
                                               'Vehicle_information.transmission', 'Vehicle_information.drivetrain',
                                               'Vehicle_information.body_style',
                                               'Vehicle_information.exterior_colour', 'Vehicle_information.fuel_type',
                                               'Vehicle_information.trim', 'img_url', 'vin', 'NumOwners',
                                               'PrevAccident', 'price_history', 'Vehicle_information.engine', '_id',
                                               'metadata'])
            scrape_df = pd.concat([scrape_df, df_scraped])
            # print(scrape_df)
            scrape_df.drop_duplicates(inplace=True)
            scrape_df.to_csv(local_file_path, index=False)
            new_info = []
    aws_handler.upload_to_aws(local_file_path, aws_bucket_folder_path)

if __name__ == '__main__':
    main()
