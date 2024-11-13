import re
import os
import time
import logging
from datetime import date

import requests
import pandas as pd
import concurrent.futures
import undetected_chromedriver as uc
from bs4 import BeautifulSoup
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

from base import log_config
from base import retry_decorator
from base.aws_handler import AWSHandler
from base.logging_decorator import handle_exceptions

logger_name = 'bcautocentre'
email_subject = 'bcautocentre Bot Alert'
email_toaddrs = ['jordan@qodemedia.com']#, 'karan@qodemedia.net']
logger = logging.getLogger(__name__)
scraper_dir = os.path.dirname(os.path.abspath(__file__))

local_file_path = os.path.join(os.path.join(scraper_dir, "results"), "bcautocentre_result.csv")
aws_bucket_file_path = "MasterCode1/scraping/bcautocentre/bcautocentre_result.csv"
aws_bucket_folder_path = "MasterCode1/scraping/bcautocentre"
website_url = 'https://www.bcautocentre.com/used-cars-burnaby?pn={}'


def get_listings(page_number) -> list:
    """Takes page number and returns listings from that page"""

    page = requests.get(website_url.format(page_number))
    soup = BeautifulSoup(page.text, 'html.parser')
    listings = soup.find_all('a', {'style':'border-radius:0'})
    listings = [next(iter(re.findall('href="(.+?)"', str(x))), None) for x in listings]
    listings = [f'https://bcautocentre.com{x}' for x in listings if x]

    return listings


def scrape_url(url) -> list:
    """Gets info from soup by request and returns info"""

    page = requests.get(url)
    soup = BeautifulSoup(page.text, 'html.parser')

    info_chunk = soup.find('div',{'class':'eziVInfo row mb10'}).text.split('\n')
    location = soup.find('h1',{'class':'detailPageH1'})
    
    info = [None]*26
    info[0] = str(date.today())
    info[2] = next(iter([x.replace('Stock No:','') for x in info_chunk if 'Stock No:' in x]), None)
    info[3] = next(iter([x.replace('VIN:','') for x in info_chunk if 'VIN:' in x]), None)
    info[5] = url
    info[6] = soup.find('h4',{'id':'H1'}).text.replace('/r','').replace('/n','').replace('Photos','').strip()
    info[7] = info[6]
    info[8] = info[6][0:4]
    info[9] = next(iter([x.replace('Odometer:','') for x in info_chunk if 'Odometer:' in x]), '').replace(',','').replace(' km','')
    info[10] = soup.find('span',{'class':'eziPriceValue'}).text.replace('$','').replace(',','')
    info[11] = next(iter([x.replace('Condition:','') for x in info_chunk if 'Condition:' in x]), None)
    #if location:
    #    info[12] = location.text.split(',')[-1].strip()
    #    info[13] = location.text.split(',')[-2].strip()
    info[12] = 'British Columbia'
    info[13] = 'Burnaby'
    info[14] = next(iter([x.replace('Transmission:','') for x in info_chunk if 'Transmission:' in x]), None)
    info[15] = next(iter([x.replace('Drive Type:','') for x in info_chunk if 'Drive Type:' in x]), None)
    info[17] = next(iter([x.replace('Exterior:','') for x in info_chunk if 'Exterior:' in x]), None)
    info[18] = next(iter([x.replace('Fuel Type:','') for x in info_chunk if 'Fuel Type:' in x]), None)
    info[19] = soup.find('div',{'class':'eziVehicleName'}).text.replace('/r','').replace('/n','').strip()
    try:
        info[20] = soup.find('img',{'title':'Zoom image #1'}).get('src')
    except:
        pass
    info[24] = str({info[0]:info[10]}).replace("\'", "\"")

    return info



@handle_exceptions(logger_name, email_subject, email_toaddrs)
@retry_decorator.retry(requests.exceptions.ConnectionError, tries=3, delay=240)
def main():

    aws_handler = AWSHandler()
    aws_handler.download_from_aws(aws_bucket_file_path, local_file_path)
    df = pd.read_csv(local_file_path)

    #Get the number of listings
    page = requests.get(website_url.format(1))
    soup = BeautifulSoup(page.text, 'html.parser')
    num_pages = soup.find('h2',{'class','mb20'}).text.replace(' Vehicle(s) Found.','')
    num_pages = range(1,int(int(num_pages)/12)+1)

    #Get all urls; parallel webscraping messes up output, so we don't use it
    all_listings = []
    for page in num_pages:
        all_listings.extend(get_listings(page))
    all_listings = [x for x in all_listings if x not in df['url']]

    #Scrape urls; parallel webscraping messes up output, so we don't use it
    all_info = []
    for url in all_listings:
        all_info.append(scrape_url(url))

    df = pd.concat([df, pd.DataFrame(all_info, columns=df.columns)])
    df.to_csv(local_file_path, index=False)
    aws_handler.upload_to_aws(local_file_path, aws_bucket_folder_path)
    

if __name__ == "__main__":
    main()
