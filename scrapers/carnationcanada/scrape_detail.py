from datetime import date
import logging
import os

import requests
import pandas as pd
from bs4 import BeautifulSoup
import undetected_chromedriver as uc
from decouple import config
from selenium import webdriver

# from scrape_url import collect_url
# from base import log_config,retry_decorator
from base import retry_decorator
from base.aws_handler import AWSHandler
from base.logging_decorator import handle_exceptions

logger_name = 'carnationcanada scrapper_bots_canada'
email_subject = 'Carnationcanada Scrape_details  Bot Alert'
# email_toaddrs=['summit@qodemedia.com', 'karan@nerdplatoon.com', 'prabin@qodemedia.net']
email_toaddrs = ['ajay.qode@gmail.com', 'karan@qodemedia.net', 'jordan@qodemedia.com']
logger = logging.getLogger(__name__)
scraper_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "results")
local_file_path = os.path.join(scraper_dir, "carnationcanada_result.csv")
local_url_path = os.path.join(scraper_dir, "urls.csv")

# print(local_file_path)
aws_bucket_file_path = "MasterCode1/scraping/carnationcanada/carnationcanada_result.csv"
aws_bucket_folder_path = "MasterCode1/scraping/carnationcanada"
aws_handler = AWSHandler()


# converting list to dict
def convert(lis):
    res_dct = {lis[i].split(':')[0]: lis[i].split(':')[1] for i in range(0, len(lis))}
    return res_dct


# scraping all details of url
def scrape_info(tup):
    global df_scrapee
    info = [None] * 24
    url = tup[1]
    try:
        driver.get(url)
        soup = BeautifulSoup(driver.page_source, 'html.parser')
        details = [j.text for j in soup.find_all(class_="detailed-specs__single")]
        tab = convert(details)

        try:
            title = soup.find('h1').get_text().strip()
        except:
            title = ''
        try:
            gdp_table = soup.find("div", attrs={"class": "main-price"})
            gdp_table_data = [k.parent.text for k in gdp_table.find_all(class_="convertus-dollar-sign")]
        except:
            pass
        try:
            initial_price = gdp_table_data[0]
        except:
            pass
        try:
            final_price = gdp_table_data[-1]
        except:
            final_price = 'Get price alert'
        try:
            loc = soup.find(class_="fas fa-map-marker-alt fa-fw").text
        except:
            pass

        try:
            info[0] = tab.get('Stock #')
        except:
            pass
        try:
            info[1] = url
        except:
            pass
        try:
            info[2] = str(date.today())
        except:
            pass
        try:
            info[4] = tab.get('VIN')
        except:
            pass
        try:
            info[5] = title
        except:
            pass
        try:
            info[6] = title.split(' ')[1]
        except:
            pass
        try:
            info[7] = title.split(' ')[2]
        except:
            pass
        try:
            info[8] = title.split(' ')[0]
        except:
            pass
        try:
            info[9] = final_price
        except:
            pass
        try:
            info[10] = tab.get('Kilometres')
        except:
            pass
        try:
            info[11] = str({'unknown': initial_price, str(date.today()): final_price})
        except:
            info[11] = str({str(date.today()): final_price})
        try:
            info[12] = 'Ontario'
        except:
            pass
        try:
            info[13] = 'Burlington'
        except:
            pass
        try:
            info[14] = tab.get('Condition')
        except:
            pass
        try:
            info[15] = tab.get('Transmission')
        except:
            pass
        try:
            info[16] = tab.get('Body Style')
        except:
            pass
        try:
            info[17] = tab.get('Exterior Colour')
        except:
            pass
        try:
            info[18] = tab.get('Trim')
        except:
            pass
        try:
            info[19] = tab.get('Fuel Type')
        except:
            pass
        try:
            info[20] = tab.get('Engine')
        except:
            pass
        try:
            info[21] = tab.get('Drive Train')
        except:
            pass
        try:
            info[22] = ''
        except:
            pass
        metadata = {}
        try:
            metadata['Interior Colour'] = tab.get('Interior Colour')
            metadata['Door'] = tab.get('Doors')
            metadata['Hwy Fuel Economy'] = tab.get('Hwy Fuel Economy')
            metadata['City Fuel Economy'] = tab.get('City Fuel Economy')
        except:
            pass
        info[23] = str(metadata)
        new_info.append(info)
    except Exception as e:
        logger.info(f'{e}')


@handle_exceptions(logger_name, email_subject, email_toaddrs)
@retry_decorator.retry(requests.exceptions.ConnectionError, tries=3, delay=120)
def main():
    global driver
    global new_info
    options = uc.ChromeOptions()
    # options.headless = True
    options.add_argument('--headless')
    options.add_argument('--no-sandbox')
    # driver = uc.Chrome(options=options,version_main=config('CHROME_VERSION'))
    driver = webdriver.Chrome(executable_path=config('CHROME_DRIVER'), options=options)
    # collect_url()
    url_df = pd.read_csv(local_url_path)

    if not os.path.exists(local_file_path):
        aws_handler.download_from_aws(aws_bucket_file_path, local_file_path)

    df_current = pd.read_csv(local_file_path)
    urls = url_df[~url_df.url.isin(df_current.url)]
    tup = list(urls.to_records(index=False))
    new_info = []

    for info in tup:
        scrape_info(info)

    df_scrapee = pd.DataFrame(new_info,
                              columns=['id', 'url', 'date_added', 'date_removed', 'vin', 'title', 'make', 'model',
                                       'year', 'price', 'kilometers', 'price_history',
                                       'province', 'city', 'condition', 'Vehicle_information.transmission',
                                       'Vehicle_information.body_style', 'Vehicle_information.exterior_colour',
                                       'Vehicle_information.trim', 'Vehicle_information.fuel_type',
                                       'Vehicle_information.engine', 'Vehicle_information.drive_train',
                                       'image_url', 'metadata'])
    df = pd.read_csv(local_file_path)

    df_i = pd.concat([df, df_scrapee], ignore_index=True)

    df_i.to_csv(local_file_path,index=False)
    driver.quit()

    aws_handler.upload_to_aws(local_file_path,aws_bucket_folder_path)


if __name__ == "__main__":
    main()
