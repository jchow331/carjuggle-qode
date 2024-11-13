from datetime import date
import logging
from concurrent.futures import ThreadPoolExecutor
import os

import pandas as pd
from bs4 import BeautifulSoup
import requests

from .scrape_url import collect_url
from base.logging_decorator import handle_exceptions
from base import retry_decorator
# from base import log_config
from base.aws_handler import AWSHandler


logger_name = 'canadablack_book scrapper_bots_canada'
email_subject = 'Canadablackbook Scrape_Detail  Bot Alert'
email_toaddrs = ['karan@qodemedia.net', 'prabin@qodemedia.net','ajay.qode@gmail.com', 'jordan@qodemedia.com', 'bikin@nerdplatoon.com']
logger = logging.getLogger(__name__)

scraper_dir = os.path.dirname(os.path.abspath(__file__))
local_url_path = os.path.join(os.path.join(scraper_dir, "results"), "all_links.csv")
local_file_path = os.path.join(os.path.join(scraper_dir, "results"), "canadablackbook_result.csv")
aws_bucket_file_path = "MasterCode1/scraping/canadablackbook/canadablackbook_result.csv"
aws_bucket_folder_path = "MasterCode1/scraping/canadablackbook"

aws_handler = AWSHandler()
new_info=[]

# converting list to dict
def convert(lis):
    it = iter(lis)
    res_dct = dict(zip(it, it))
    return res_dct


# scraping all details of url
def scrape_info(url):
    info = [None] * 22
    goto = True
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/98.0.4758.81 Safari/537.36'}
    page = requests.get(url,headers=headers)


    soup = BeautifulSoup(page.content, 'html.parser')
    if page.status_code == '400':
        goto = False
    try:
        if goto == True:
            # tab = {}
            te = soup.find(class_="vehicle-detaied-info-specs").text.strip()
            sto = te.split('\n')
            tab = convert(sto)

            try:
                title = soup.find(class_='make-model-name col-6').get_text().strip()
            except:
                title = ''
            try:
                price = soup.find(class_="price col-6").get_text()

                price = price.split('|')[1].strip()
            except:
                price = ''
            try:
                state = soup.find_all('h4')[2]
            except:
                state = ''
            try:
                city = soup.find_all('h4')[1]
            except:
                city = ''

            info[0] = tab.get('Stock Number')
            info[1] = url
            info[2] = date.today()
            info[4] = title
            info[5] = title.split(' ')[1]
            info[6] = title.split(' ')[2]
            info[7] = title.split(' ')[0]
            info[8] = price
            info[9] = tab.get('Kilometres')
            info[10] = str({str(date.today()): price})
            info[11] = state
            info[12] = city
            info[13] = tab.get('Status')
            info[14] = tab.get('Transmission')
            info[15] = tab.get('Body')
            info[16] = tab.get('Exterior Colour')
            info[17] = tab.get('Trim')
            info[18] = tab.get('Fuel')
            info[19] = tab.get('Engine')
            info[20] = tab.get('Drivetrain')

            metadata = {}
            try:
                metadata['Interior Colour'] = tab.get('Interior Colour')
                metadata['Door'] = tab.get('Doors')
                metadata['Passengers'] = tab.get('Passengers')
                metadata['Cylinder'] = tab.get('Cylinder')
            except:
                pass
            info[21] = str(metadata)

            # print(info)
            new_info.append(info)

    except Exception:
        raise


@handle_exceptions(logger_name, email_subject, email_toaddrs)
@retry_decorator.retry(requests.exceptions.ConnectionError, tries=3, delay=120)
def main():
    collect_url()
    url_df = pd.read_csv(local_url_path)
    if not os.path.exists(local_file_path):
        aws_handler.download_from_aws(aws_bucket_file_path, local_file_path)
    df_current = pd.read_csv(local_file_path)
    urls = url_df[~url_df.links.isin(df_current.url)]
    tup = list(urls['links'])

    # print(f"Number of listings remaining to scrape ={len(tup)}")

    with ThreadPoolExecutor(max_workers=16) as executor:
        executor.map(scrape_info, tup)

    logger.info(f'appeding details in no {len(new_info)}')

    df_scraped = pd.DataFrame(new_info,
                              columns=['id', 'url', 'date_added', 'date_removed', 'title', 'make', 'model', 'year',
                                      'price', 'kilometers',
                                       'price_history', 'province', 'city', 'condition',
                                       'Vehicle_information.transmission', 'Vehicle_information.body_style',
                                       'Vehicle_information.exterior_colour', 'Vehicle_information.trim',
                                       'Vehicle_information.fuel_type', 'Vehicle_information.engine',
                                       'Vehicle_information.drive_train', 'metadata'])
    df_current = pd.concat([df_scraped,df_current])
    df_current.to_csv(local_file_path)
    aws_handler.upload_to_aws(local_file_path,aws_bucket_folder_path)


if __name__ == "__main__":
    main()
    #abc
