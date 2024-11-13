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

logger_name = 'auto123'
email_subject = 'auto123 Bot Alert'
email_toaddrs = ['jordan@qodemedia.com']#, 'karan@qodemedia.net']
logger = logging.getLogger(__name__)
scraper_dir = os.path.dirname(os.path.abspath(__file__))

local_file_path = os.path.join(os.path.join(scraper_dir, "results"), "auto123_result.csv")
aws_bucket_file_path = "MasterCode1/scraping/auto123/auto123_result.csv"
aws_bucket_folder_path = "MasterCode1/scraping/auto123"
website_url = 'https://inventory.auto123.com/en-ca/cars?PageIndex=1&PageSize=96'


def get_listings(driver) -> list:
    """Iterates through listings and scrapes page"""

    all_listings = []

    #Go to first car, where we will hit right arrow until the end; sometimes it needs a couple tries to find the next arrow page
    for try_again in range(0,3):
        time.sleep(1)
        driver.find_element(By.XPATH, '//*[@id="__layout"]/div/div[1]/div[1]/div[4]/div[2]/div[1]/div/div[1]/div/div[2]/div[5]/div[2]/a').click()
        time.sleep(1)
        try:
            element = WebDriverWait(driver, 10).until(
                EC.visibility_of_element_located((By.CLASS_NAME, 'counter')))
            break
        except:
            driver.get(website_url)
    
    try:
        element
    except:
        return all_listings

    #Gets number of times to click
    num_listings = driver.find_element(By.CLASS_NAME, 'counter').text
    num_listings = int(re.findall('\/ (.+?)\)', num_listings)[0]) - 1

    for num in range(0, num_listings):
        time.sleep(0.5)
        #if driver.current_url in all_urls:
        #    return all_listings
        try:
            element = WebDriverWait(driver, 10).until(
                EC.element_to_be_clickable((By.CLASS_NAME, 'next')))
        except:
            return all_listings

        all_listings.append(driver.current_url)
        driver.find_element(By.CLASS_NAME, 'next').click()

    return all_listings


def scrape_url(url) -> None:
    """Gets info from soup by request"""

    page = requests.get(url)
    soup = BeautifulSoup(page.text, 'html.parser')

    #soup = BeautifulSoup(driver.page_source, 'html.parser')
    info_chunk = re.findall('NUXT__=(.+?)\n', str(soup))[0]

    info = [None]*26
    info[0] = next(iter(re.findall('DateEntry:"(.+?)"', info_chunk)), None)
    info[2] = next(iter(re.findall('StockNumber:"(.+?)"', info_chunk)), None)
    info[3] = next(iter(re.findall('SerialNumber:"(.+?)"', info_chunk)), None)
    info[4] = next(iter(re.findall('Certified:(.+?),', info_chunk)), None)
    info[5] = url
    info[6] = soup.find('span', {'class':'make'}).text
    info[7] = soup.find('span', {'class':'model'}).text
    info[8] = soup.find('span', {'class':'year'}).text
    info[9] = next(iter(re.findall('Mileage:<\/span> <span class="value">(.+?) KM', str(soup))), None)
    info[10] = soup.find('div',{'class':'price'}).text.split('\n')[0].replace('$','').replace(',','')
    info[11] = next(iter(re.findall('>Used<', str(soup))), 'New').replace('>','').replace('<','')
    info[12] = 'Ontario'
    info[13] = next(iter(re.findall('car-for-sale\/(.+?)\/', info[5])), '').title()
    info[14] = next(iter([x.text for x in soup.find_all('span',{'class','value'}) 
                        if 'Automatic' in x.text or 'Manual' in x.text]), None)
    info[15] = next(iter([x.text for x in soup.find_all('span',{'class','value'}) 
                        if 'wheel drive' in x.text]), None)
    info[16] = next(iter(re.findall('Body:"(.+?)"', info_chunk)), None)
    info[17] = next(iter(re.findall('ExteriorColor:"(.+?)"', info_chunk)), None)
    info[18] = next(iter([x.text.split('\n')[-2].strip() for x in soup.find_all('div',{'class','item fuel'})]), None)
    info[19] = next(iter(re.findall('Version:"(.+?)"', info_chunk)), None)
    info[20] = re.sub('\\\\u002F', "/", next(iter(re.findall('Url:"(.+?)"', info_chunk)), ''))
    info[24] = str({info[0]:info[10]}).replace("\'", "\"")

    all_info.append(info)


@handle_exceptions(logger_name, email_subject, email_toaddrs)
@retry_decorator.retry(requests.exceptions.ConnectionError, tries=3, delay=240)
def main():

    global all_info
    
    aws_handler = AWSHandler()
    aws_handler.download_from_aws(aws_bucket_file_path, local_file_path)
    df = pd.read_csv(local_file_path)
    df['url'] = df['url'].str.replace('inventory.', 'www.').str.replace('.com', '.ca')
    all_urls = df['url'].tolist()
    

    chrome_options = uc.ChromeOptions()
    chrome_options.add_argument("--start-maximized")
    #chrome_options.add_argument("--headless=new")
    chrome_options.add_argument("--no-sandbox")
    driver = uc.Chrome(options=chrome_options)#, version_main=107)
    driver.get(website_url)

    #Get all urls
    all_listings = get_listings(driver)
    all_listings = [x for x in all_listings if x not in all_urls]
    driver.close()

    #Scrape urls in parallel
    all_info = []
    with concurrent.futures.ThreadPoolExecutor() as executor:
        executor.map(scrape_url, all_listings)

    df = pd.concat([df, pd.DataFrame(all_info, columns=df.columns)])
    df.to_csv(local_file_path, index=False)
    aws_handler.upload_to_aws(local_file_path, aws_bucket_folder_path)



if __name__ == "__main__":
    main()
