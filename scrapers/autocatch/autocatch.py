import itertools
import requests
import logging
import urllib3
import sys
import re
import os
import concurrent.futures
from datetime import date

import pandas as pd
import numpy as np
from bs4 import BeautifulSoup

from base import retry_decorator
from base import log_config
from base.logging_decorator import handle_exceptions

    
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

logger_name = 'autocatch'
email_subject = 'autocatch Bot Alert'
email_toaddrs = ['jordan@qodemedia.com', 'karan@qodemedia.net', 'bikin@nerdplatoon.com']
logger = logging.getLogger(__name__)

from base.aws_handler import AWSHandler
aws_handler = AWSHandler()

scraper_dir = os.path.dirname(os.path.abspath(__file__))
local_file_path = os.path.join(os.path.join(scraper_dir, "results"), "autocatch_result.csv")
aws_bucket_file_path = "MasterCode1/scraping/autocatch/autocatch_result.csv"
aws_bucket_folder_path = "MasterCode1/scraping/autocatch"


def get_urls(page_num):
    
    page = requests.get(url.format(page_num), verify=False)
    soup = BeautifulSoup(page.text, 'html.parser')
    listings = soup.find_all('div', {'class':'slisting-section'})
    listings = [[re.findall('href="/used-cars(.+?)"', str(x))[0], 
                 next(iter(re.findall('(.+?) km', str(x))), '').replace(',','')] for x in listings]
    all_listings.extend(listings)

    
def scrape_urls(listing):
    
    url = listing[0]
    km = listing[1]
    
    page = requests.get(url.replace('www.',''), verify=False)
    soup = BeautifulSoup(page.text, 'html.parser')
    
    info = [None]*20
    info[0] = str(date.today())
    info[2] = url.split('/')[-1]
    info[3] = url
    info[4] = re.findall('s.prop22="(.+?)"', str(soup))[0].title()
    info[5] = re.findall('s.prop23="(.+?)"', str(soup))[0].title()
    info[6] = url.split('used-cars/')[-1].split('/')[0]
    info[7] = km
    info[8] = next(iter(re.findall('s.prop11="(.+?)"', str(soup))), None)
    if info[8] == '0':
        info[8] = None
    info[9] = re.findall('s.prop30="(.+?)"', str(soup))[0].title()
    if info[9] == 'New':
        info[8] = '0'
    info[10] = re.findall('s.prop20="(.+?)"', str(soup))[0].title()
    info[11] = re.findall('s.prop21="(.+?)"', str(soup))[0].title().split('-')[0]
    info[12] = re.findall('>Transmission</span>(.+?)<', str(soup))[0].strip()
    if info[12] == 'Unknown':
        info[12] = None
    info[14] = re.findall('s.prop13="(.+?)"', str(soup))[0].title()
    if info[14] == 'Unknown':
        info[14] = None
    info[15] = re.findall('>Colour</span>(.+?)<', str(soup))[0].strip()
    if info[15] == 'N/A' or info[15] == '-':
        info[15] = None
    info[16] = re.findall('>Fuel Type</span>(.+?)<', str(soup))[0].strip()
    if info[16] == '-':
        info[16] = None
    info[17] = re.findall('<title>(.+?)<', str(soup))[0]
    info[18] = list(set(re.findall('https://media.wheels.ca/vehicles(.+?)"', str(soup))))
    if info[18]:
        info[18] = ['https://media.wheels.ca/vehicles' + x for x in info[18]]
    info[19] = str({info[0]:info[8]}).replace("\'", "\"")
    all_info.append(info)


@handle_exceptions(logger_name, email_subject, email_toaddrs)
@retry_decorator.retry(requests.exceptions.ConnectionError, tries=3, delay=120)
def main():
    
    global all_info, all_listings, url
    
    if not os.path.exists(local_file_path):
        aws_handler.download_from_aws(aws_bucket_file_path, local_file_path)
    df = pd.read_csv(local_file_path)

    
    url = 'https://autocatch.com/vehicle/refine?&se=750&p={}&r=10&s=date&o=desc'
    pages = list(range(1,1000))
    
    #Get urls
    all_listings = []
    all_urls = df['url'].tolist()
    with concurrent.futures.ThreadPoolExecutor() as executor:
        executor.map(get_urls, pages)
    all_listings.sort()
    list(k for k,_ in itertools.groupby(all_listings))
    all_listings = [['https://www.autocatch.com/used-cars' + x[0], x[1]] for x in all_listings]
    #Isolates the listings to scrape
    no_kms = [x[0] for x in all_listings]
    dup_urls = np.setdiff1d(no_kms, all_urls).tolist()
    index_list = [no_kms.index(i) for i in dup_urls]
    to_scrape = [all_listings[i] for i in index_list]
    
    #Scrape urls
    print ('Scraping {} listings...'.format(len(to_scrape)))
    all_info = []
    with concurrent.futures.ThreadPoolExecutor() as executor:
        executor.map(scrape_urls, to_scrape)

    df = pd.concat([df,pd.DataFrame(all_info,columns=df.columns.values.tolist())])
    df = df.drop_duplicates(subset=['_id'],keep='first')
    
    df.to_csv(local_file_path, index=False)
    aws_handler.upload_to_aws(local_file_path, aws_bucket_folder_path)

    
if __name__ == "__main__":
    main()