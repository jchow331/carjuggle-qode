import logging
import re
import os
import concurrent.futures
from datetime import date

import requests
import pandas as pd
import numpy as np
from bs4 import BeautifulSoup

# Necessary imports for logging
# from base import log_config
from base.aws_handler import AWSHandler
from base.logging_decorator import handle_exceptions

# Set global variables for logger and emails (change this according to your needs)
logger_name = 'carpages_scraper_bots_canada'
email_subject = 'Carpages URL_Detail Scraper Bot Alert'
email_toaddrs = ['karan@qodemedia.net', 'jordan@qodemedia.com', 'bikin@nerdplatoon.com']
# email_toaddrs = []
logger = logging.getLogger(__name__)
scraper_dir = os.path.dirname(os.path.abspath(__file__))
local_url_file_path = os.path.join(scraper_dir, "results/url.csv")
local_file_path = os.path.join(scraper_dir, "results/carpages_result.csv")
aws_bucket_file_path = "MasterCode1/scraping/carpages/carpages_result.csv"
aws_folder_path = "MasterCode1/scraping/carpages"
# logging.basicConfig(filename='example.log', encoding='utf-8', level=logging.INFO)
'''Collecting all urls according to make 
condition, page number 
'''


def get_listings(make):
    new_listings = []
    conditions = ['new', 'used']
    url = 'https://www.carpages.ca'

    # Filter by make, condition, and page number to bypass the 10000 listing limit
    for condition in conditions:
        page_num = 1
        global remaining
        make_url = url + '/used-cars/search/?num_results=50&make_name={}&sale_type={}&p=1'.format(make, condition)
        page = requests.get(make_url)
        soup = BeautifulSoup(page.text, 'html.parser')
        try:
            no_of_loads = soup.find(class_="jc-sb fd-c fd-r--medium ai-c delta no-shadow push-none srp-sort").find(
                'strong').text.split(' ')[-1].replace(',', '')
        except:
            pass
        try:
            no_of_load = int(int(no_of_loads) / 50 + 1)
        except:
            no_of_load = 200

        while page_num <= no_of_load:
            make_url = url + '/used-cars/search/?num_results=50&make_name={}&sale_type={}&p={}'.format(make, condition,
                                                                                                       page_num)
            page = requests.get(make_url)
            soup = BeautifulSoup(page.text, 'html.parser')

            listings = soup.find_all('div', {'class': 'media soft push-none rule'})
            if not listings:
                break
            listings = list(set(re.findall('href="(.+?)"', str(listings))))
            if '/buy-from-home/' in listings:
                listings.remove('/buy-from-home/')
            listings = ['https://www.carpages.ca' + s for s in listings]

            new_listings.extend(listings)
            page_num += 1
        remaining -= 1
        logger.info(f'makes {remaining}')
    try:
        old_df = pd.read_csv(local_url_file_path)
    except FileNotFoundError:
        old_df = pd.DataFrame(columns=['url'])
    df_url = pd.DataFrame(new_listings, columns=['url'])
    old_df = pd.concat([old_df, df_url])
    old_df.drop_duplicates(inplace=True)
    old_df.to_csv(local_url_file_path, index=False)


def scrape_url(url):
    info = [None] * 22
    try:
        page = requests.get(url)
        soup = BeautifulSoup(page.text, 'html.parser')
        info[0] = str(date.today())
        info[1] = ''
        info[2] = re.findall('"id": (.+?),|$', str(soup))[0]
        info[3] = re.findall('"vehicleIdentificationNumber":"(.+?)"|$', str(soup))[0]
        if info[3] == '",':
            info[3] = np.nan
        info[4] = url
        info[5] = re.findall('"make":"(.+?)"|$', str(soup))[0]
        info[6] = re.findall('"model":"(.+?)"|$', str(soup))[0]
        info[7] = re.findall('"year": (.+?),|$', str(soup))[0]
        info[8] = re.findall('"mileage": "(.+?)"|$', str(soup))[0]
        info[9] = int(float(re.findall('"price":"(.+?)"', str(soup))[0]))
        if 'new-cars' in url:
            info[10] = 'New'
        elif 'used-cars' in url:
            info[10] = 'Used'
        info[11] = re.findall('"provinceCode": "(.+?)"|$', str(soup))[0]
        info[12] = re.findall('"city": "(.+?)"|$', str(soup))[0]
        info[13] = re.findall('"vehicleTransmission":"(.+?)"|$', str(soup))[0]
        info[14] = re.findall('"driveWheelConfiguration":"(.+?)"|$', str(soup))[0]
        info[15] = re.findall('"bodyStyle": "(.+?)"|$', str(soup))[0]
        info[16] = re.findall('"extColour": "(.+?)"|$', str(soup))[0]
        info[17] = re.findall('"fuelType": "(.+?)"|$', str(soup))[0]
        info[18] = re.findall('"title": "(.+?)"|$', str(soup))[0]
        info[19] = re.findall('"image":"(.+?)"|$', str(soup))[0]
        price_dict = {date.today().strftime('%Y/%m/%d'): info[9]}
        info[20] = str(price_dict)
        info[21] = re.findall('\{"@context"(.+?)\}</script>|$', str(soup))[0]

        info = [str(x).replace('\/', '/') for x in info]
        # print(info)

        all_info.append(info)
    except Exception as e:
        logger.info(f'{e}')
        raise


@handle_exceptions(logger_name, email_subject, email_toaddrs)
def main():
    global remaining, all_info
    all_info = []
    aws_handler = AWSHandler()

    if not os.path.exists(local_file_path):
        aws_handler.download_from_aws(aws_bucket_file_path, local_file_path)

    df = pd.read_csv(local_file_path)
    url = 'https://www.carpages.ca'
    all_listings = df['url'].tolist()

    page = requests.get(url + '/used-cars/search/')
    soup = BeautifulSoup(page.text, 'html.parser')

    # Get all makes to filter through, since there is a max page listing of 10000 cars
    all_makes = soup.find_all('ul', {'class': 'multi-list multi-list--vertical multi-list--medium-3'})
    all_makes = re.findall('a href="/used-cars/(.+?)/\?ref=nav"', str(all_makes))
    # print(all_makes)
    remaining = len(all_makes)

    # #Scrape for all listings
    logging.info('Getting all listings...\n')
    
    #for make in all_makes:
    #    get_listings(make)
    with concurrent.futures.ThreadPoolExecutor() as executor:
        executor.map(get_listings, all_makes)

    df_url = pd.read_csv(local_url_file_path)
    new_listings = df_url['url'].tolist()

    # Isolates new listings
    new_listings = list(set(new_listings) - set(all_listings))
    logging.info(f"Number of new listings = {len(new_listings)}")

    # Scrape new listings
    logging.info('Scraping new listings...\n')
    NUM_WORKERS = 32

    for i in range(0, len(new_listings) - NUM_WORKERS, NUM_WORKERS):
        urls_chunk = new_listings[i:i + NUM_WORKERS]
        with concurrent.futures.ThreadPoolExecutor() as executor:
            executor.map(scrape_url, urls_chunk)

        
    df_temp = pd.DataFrame(all_info,
                           columns=['date_added', 'date_removed', '_id', 'vin', 'url', 'make', 'model', 'year',
                                    'kilometers',
                                    'price', 'condition', 'province', 'City',
                                    'Vehicle_information.transmission',
                                    'Vehicle_information.drivetrain', 'Vehicle_information.body_style',
                                    'Vehicle_information.exterior_colour', 'Vehicle_information.fuel_type',
                                    'Vehicle_information.trim', 'img_url', 'price_history', 'metadata'])

    df1 = pd.read_csv(local_file_path, low_memory=False)
    df1 = pd.concat([df1, df_temp])
    df1 = df1.drop_duplicates(subset=['url'])
    df1.to_csv(local_file_path, index=False)
    logging.info('Uploading in all data in csv')

    aws_handler.upload_to_aws(local_file_path, aws_folder_path)


if __name__ == "__main__":
    main()
