#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import logging
import os
from datetime import date
from random import randint
from concurrent.futures import ThreadPoolExecutor

import pandas as pd
import undetected_chromedriver as uc
from bs4 import BeautifulSoup
from decouple import config

from base import log_config
from base.aws_handler import AWSHandler
from base.logging_decorator import handle_exceptions
from scrapers.ontariocars.scrape_url import url_scrape

# selenium driver options
options = uc.ChromeOptions()
options.headless=True
options.add_argument('--headless')
driver = uc.Chrome(options=options, version_main=config('CHROME_VERSION'))

# logger
logger_name = 'Ontariocars scrapper_bots_canada'
email_subject = 'Ontariocars Scrape_Detail Bot Alert'
email_toaddrs=['bikin@nerdplatoon.com', 'karan@qodemedia.net', 'ajay.qode@gmail.com', 'jordan@qodemedia.com']
logger = logging.getLogger(__name__)
scraper_dir = os.path.dirname(os.path.abspath(__file__))


def scrape_posting(tup):
    new_info = []
    try:
        url = tup
    except:
        _, url = tup
    try:
        driver.get(url)
        info = [None] * 23
        soup = BeautifulSoup(driver.page_source, 'html.parser')
        info[21] = url
        info[20] = soup.find('h2').text
        info[16] = soup.find('div', {'class':'b-detail__head-price-num'}).text.replace('$','').replace(',','')
        info[17] = str({str(date.today()) : info[16]})

        info[0] = soup.find('a', {'data-bind':'text: location,  attr: {href: locationUrl}'}).text
        info[18] = "ON"
        info[13] = soup.find('a', {'data-bind':'text: make, attr: {href: makeUrl}'}).text
        info[15] = soup.find('a', {'data-bind':'text: model, attr: {href: modelUrl}'}).text

        if info[20].split(' ')[0].isdigit():
            info[22] = info[20].split(' ')[0]
        try:
            info[12] = soup.find('span', {'class':'km-info'}).text
        except:
            pass

        meta = {}
        for div in soup.find_all('div',{'class':'feature-item print-50'}):
            if 'Stock #' in div.find('span').text:
                info[19] = div.find('strong').text
            if 'Body Style' in div.find('span').text:
                info[1] = div.find('strong').text
            if 'Engine' in div.find('span').text:
                info[3] = div.find('strong').text
            if 'Transmission' in div.find('span').text:
                info[6] = div.find('strong').text
            if 'Drive Train' in div.find('span').text:
                info[2] = div.find('strong').text
            if 'Fuel Type' in div.find('span').text:
                info[5] = div.find('strong').text
            if 'Trim' in div.find('span').text:
                info[7] = div.find('strong').text
            if 'Exterior Colour' in div.find('span').text:
                info[4] = div.find('strong').text
            if 'Condition' in div.find('span').text:
                info[8] = div.find('strong').text
            meta[div.find('span').text.strip()] = div.find('strong').text.strip()

        meta['comments'] = soup.find('h5', {'data-bind':'html: vehicle.comment'}).text

        info[14] = str(meta)
        if info[8] == 'Used' and soup.find('div', {'class':'cpo-wrap cpo-color vertical-align-middle font-weight-600'})['style'] == '':
            info[8] = 'Certified Pre-owned'
        info[9] = str(date.today())
        info[11] =  soup.find('img', {'alt':'vehicle-image'}).get('src')
        new_info.append(info)
        return new_info
    except Exception as e:
        logger.info(f'Execption at Ontariocars: {e}')
        raise


@handle_exceptions(logger_name, email_subject, email_toaddrs)        
def main():
    a = AWSHandler()
    local_url_file_path = os.path.join(scraper_dir, "results/url.csv")
    local_result_file_path = os.path.join(scraper_dir, "results/ontariocars_result.csv")
    aws_bucket_folder_path = "MasterCode1/scraping/ontariocars"
    aws_bucket_file_path = "MasterCode1/scraping/ontariocars/ontariocars_result.csv"

    # scrape urls and save to a csv file
    url_scrape()

    if not os.path.exists(local_result_file_path):
        a.download_from_aws(aws_bucket_file_path, local_result_file_path)

    try:
        df_current = pd.read_csv(local_result_file_path).drop(columns='Unnamed: 0')
    except:
        df_current = pd.read_csv(local_result_file_path)

    try:  
        url_df = pd.read_csv(local_url_file_path).drop(columns='Unnamed: 0')
    except:
        url_df = pd.read_csv(local_url_file_path)

    urls = url_df[~url_df.url.isin(df_current.url)]
    tup = list(urls.url)

    with ThreadPoolExecutor(max_workers = 4) as executor:
        all_data = executor.map(scrape_posting, tup)

    df = pd.DataFrame(all_data, columns = ['City','Vehicle_information.body_style','Vehicle_information.drivetrain',
                                           'Vehicle_information.engine','Vehicle_information.exterior_colour',
                                           'Vehicle_information.fuel_type','Vehicle_information.transmission',
                                           'Vehicle_information.trim','condition','date_added','date_removed',
                                           'image_url','kilometers','make','meta','model','price','price_history',
                                           'province','id','title','url','year'])

    df2 = pd.read_csv(local_result_file_path)
    df1 = pd.concat([df2, df], ignore_index=True)
    try:
        df1 = df1.loc[:, ~df1.columns.str.match("Unnamed")]
    except:
        pass
    df1 = df1.drop_duplicates(subset='url')
    try:
        df1 = df1.loc[:, ~df1.columns.str.match("Unnamed")]
    except:
        pass

    df1.to_csv(local_result_file_path, index=False)
    a.upload_to_aws(local_result_file_path, aws_bucket_folder_path)


if __name__ == '__main__':
    main()
