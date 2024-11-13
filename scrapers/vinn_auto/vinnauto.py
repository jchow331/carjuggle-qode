import logging
import time
import os
import re
import requests
import concurrent.futures
from datetime import date
from decouple import config

import pandas as pd
import undetected_chromedriver as uc
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

from base import log_config
from base import retry_decorator
from base.aws_handler import AWSHandler
from base.logging_decorator import handle_exceptions

logger_name = 'Vinnauto_scraper_bots_canada'
email_subject = 'Vinnauto Bot Alert'
email_toaddrs = ['jordan@qodemedia.com', 'karan@qodemedia.net', 'bikin@nerdplatoon.com']
logger = logging.getLogger(__name__)
scraper_dir = os.path.dirname(os.path.abspath(__file__))


def get_all_makes(driver):

    try:
        WebDriverWait(driver, 60).until(EC.visibility_of_element_located((By.XPATH, '//*[@id="__next"]/div/div[2]/div[1]/header/div/button')))
    except:
        pass

    time.sleep(2)
    driver.find_element(By.XPATH, '//*[@id="__next"]/div/div[2]/div[1]/header/div/button').click()
    time.sleep(2)
    driver.find_element(By.ID, 'mui-component-select-make-select').click()
    time.sleep(2)
    makes = driver.find_element(By.XPATH, '//*[@id="menu-make-select"]/div[3]').text
    makes = makes.splitlines()[1:]    
    
    return makes

def scrape_make(driver):
    
    #driver.get('https://vinnauto.com/cars/{}'.format(make))
    #driver.get('https://www.vinnauto.com/cars')
    time.sleep(10)        
        
    #Sorts by Canada first to get as many listings as it can
    driver.find_element(By.XPATH, '/html/body/div[1]/div/div[2]/div[1]/header/div/div/div/div/div/input').send_keys('Canada')
    time.sleep(3)
    driver.find_element(By.CLASS_NAME, 'MuiAutocomplete-option').click()
    time.sleep(10)
    driver.set_window_size(700, 1080)
    #driver.find_element(By.XPATH, '//*[@id="__next"]/div/div[2]/div[3]/div[3]/button').click()
        
    #Scrapes listings by page
    all_listings = []
    while True:
        
        try:
            WebDriverWait(driver, 10).until(EC.visibility_of_element_located((By.XPATH, '/html/body/div[1]/div/div[2]/div[2]/div/div/div/div/div[1]')))
        except:
            pass
        time.sleep(2)
        #listings = driver.find_element(By.CLASS_NAME, 'jss142').get_attribute('innerHTML')
        #listings = re.findall('/car/(.+?)"', listings)
        listings = re.findall('href="/car/(.+?)"', driver.page_source)
        listings = ['https://www.vinnauto.com/car/{}'.format(x) for x in listings]
        all_listings.extend(listings)
        
        #Keeps clicking next button until all listings are scraped
        try:
            time.sleep(1)
            driver.find_element(By.XPATH, '//*[@id="paginatedInventoryMobile"]/div/div[11]/div/div[2]/a[2]').click()
        except:
            driver.save_screenshot('screenie.png')
            break
        
    all_listings = list(set(all_listings))
        
    return all_listings

def scrape_info(url):
    
    info = [None] * 27
    
    page = requests.get(url)
    soup = BeautifulSoup(page.content, 'html.parser')
    info[18] = str(date.today())

    info[0] = url
    info[1] = re.findall('"year":(.+?)}', str(soup))[0]
    info[2] = re.findall('"make":"(.+?)"', str(soup))[0]
    info[3] = re.findall('"model":"(.+?)"', str(soup))[0]
    info[4] = re.findall('"price":"(.+?)"', str(soup))
    if info[4]:
        info[4] = info[4][0]
    else:
        info[4] = None
    info[5] = re.findall('"kilometers":(.+?),', str(soup))[0]
    info[6] = re.findall('"condition":"(.+?)"', str(soup))[0]
    info[7] = re.findall('"trim":"(.+?)"', str(soup))
    if info[7]:
        info[7] = info[7][0]
    else:
        info[7] = None
    info[8] = re.findall('"transmission":"(.+?)"', str(soup))
    if info[8]:
        info[8] = info[8][0]
    else:
        info[8] = None
    info[9] = re.findall('"vin":"(.+?)"', str(soup))
    if info[9]:
        info[9] = info[9][0]
    else:
        info[9] = None
    info[10] = re.findall('"bodyType":"(.+?)"', str(soup))
    if info[10]:
        info[10] = info[10][0]
    else:
        info[10] = None
    info[11] = re.findall('"province":"(.+?)"', str(soup))[0]
    info[12] = re.findall('"city":"(.+?)"', str(soup))[0]
    info[13] = re.findall('"https://images(.+?)"', str(soup))
    if info[13]:
        info[13] = ['https://images' + x for x in info[13]][0] #Added the [0] because the old format of the df only had one image, remove if you want all urls
    else:
        info[13] = re.findall('"originalUrl":"(.+?)"', str(soup))[0] #Same here ^
        if not info[13]:
            info[13] = None
    info[14] = re.findall('"driveType":"(.+?)"', str(soup))
    if info[14]:
        info[14] = info[14][0]
    else:
        info[14] = None
    info[15] = re.findall('"exteriorColour":"(.+?)"', str(soup))
    if info[15]:
        info[15] = info[15][0]
    else:
        info[15] = None
    info[16] = re.findall('"fuelType":"(.+?)"', str(soup))
    if info[16]:
        info[16] = info[16][0]
    else:
        info[16] = None
    info[17] = str({info[19]:info[4]}).replace("\'", "\"")
    info[21] = re.findall('"vehicle":{(.+?)"photos"', str(soup))[0]
    if info[6] == 'New':
        info[22] = '0'
    info[26] = re.findall('"engineSize":"(.+?)"', str(soup))
    if info[26]:
        info[26] = info[26][0]
    else:
        info[26] = None
    
    all_info.append(info)
    
    
@handle_exceptions(logger_name, email_subject, email_toaddrs)
@retry_decorator.retry(requests.exceptions.ConnectionError, tries=3, delay=120)
def main():
    
    global all_info
    
    a = AWSHandler()
    local_file_path = os.path.join(scraper_dir, "results/vinnauto_result.csv")
    #local_file_path = os.path.join(scraper_dir, f"results/vinn_auto_{str(date.today())}.csv")
    aws_bucket_file_path = "MasterCode1/scraping/vinnauto/vinnauto_result.csv"
    aws_bucket_folder_path = "MasterCode1/scraping/vinnauto"
    
    chrome_options = webdriver.ChromeOptions()
    chrome_options.add_argument("javascript.enabled")
    chrome_options.add_argument('--lang=en_US')
    chrome_options.add_argument('--deny-permission-prompts')
    chrome_options.add_argument('--headless')
    chrome_options.add_argument("--window-size=1920x1080")
    
    #driver = uc.Chrome(options=chrome_options, version_main=100)
    driver = webdriver.Chrome(options=chrome_options, executable_path=config('CHROME_DRIVER'))
    driver.implicitly_wait(2)
    link = "https://vinnauto.com/cars"
    driver.get(link)
    driver.maximize_window()
    
    #Get all makes to iterate through
    #all_makes = get_all_makes(driver)
    #driver.set_window_size(900, 1080)
    
    all_info = []
    #for make in all_makes:
        #Gets listings by make
    #all_listings = scrape_make(make, driver)
    all_listings = scrape_make(driver)
    
    #Scrapes each listing
    with concurrent.futures.ThreadPoolExecutor() as executor:
        executor.map(scrape_info, all_listings)
    
    driver.quit()
    
    df_new = pd.DataFrame(all_info, columns=['url', 'year', 'make', 'model', 'price', 'kilometers', 'condition',
       'Vehicle_information.trim', 'Vehicle_information.transmission', 'vin',
       'Vehicle_information.body_style', 'province', 'city', 'img_url',
       'Vehicle_information.drivetrain', 'Vehicle_information.exterior_colour',
       'Vehicle_information.fuel_type', 'price_history', 'date_added',
       'date_removed', 'City', 'metadata', 'NumOwners', 'PrevAccident',
       'carfax_url', 'is_featured', 'Vehicle_information.engine'])

    #Download, read, concat, upload
    a.download_from_aws(aws_bucket_file_path, local_file_path)
    df = pd.read_csv(local_file_path)
    df_new = pd.concat([df, df_new])
    df_new.to_csv(local_file_path, index=False)
    a.upload_to_aws(local_file_path, aws_bucket_folder_path)
    
    
if __name__ == '__main__':
    main()
        