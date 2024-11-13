import time
import pandas as pd
import os
from random import seed
from random import randint
import logging 
from datetime import date

import undetected_chromedriver as uc
options = uc.ChromeOptions()
# options.headless=True
# options.add_argument('--headless')
driver = uc.Chrome(options=options,use_subprocess=True)

# Necessary imports for logging
import logging
# from base import log_config
from base.logging_decorator import handle_exceptions

# Set global variables for logger and emails (change this according to your needs)
scraper_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "../canadadrives/results")
local_urls_path = os.path.join(scraper_dir, "all_links.csv")
# print(local_file_path)
logger_name = 'canadadrives_scrapper_bots_canada'
email_subject = 'canadadrives Bot Alert'

body = ['Sedan','Coupe','Hatchback', 'Truck', 'SUV', 'Minivan', 'Convertible']
loc = {'Ontario' : 'on','British Columbia': 'bc', 'Alberta' :'ab'}
df = pd.DataFrame()


def geturl(link):
    print(link)
    urls = []
    driver.get(link)
    #get the total number of cars in the used section of the website
    time.sleep(randint(5,10))
    try:
        num_cars=driver.find_element_by_xpath('//*[@id="app"]/div/main/div/div[1]/section/div/div/div[2]/div[1]/div[4]/div[1]/strong').text.replace(',','')
        logging.info(f'num_cars{num_cars}')

        # we get the total of 36 cars after each  click of button
        total_iter_req = int(int(num_cars)//20)
        print(total_iter_req)
        j = 29

        '//*[@id="app"]/div/main/div/div[1]/section/div/div/div[2]/div[1]/div[6]/div/div/button'
        for i in range(total_iter_req):
            time.sleep(randint(4, 6))
            try:
                d = 24
                print(i)
                if(i == 0):
                    try:
                        el=driver.find_element_by_xpath('//*[@id="app"]/div/main/div/div[1]/section/div/div/div[2]/div[1]/div[6]/div/div/button')
                        driver.execute_script("arguments[0].click();", el)
                    except:
                        el=driver.find_element_by_xpath("/html/body/div[1]/div/div/div[1]/main/div/div[1]/section/div/div/div[2]/div[1]/div[29]/div/div/button")
                        driver.execute_script("arguments[0].click();", el)
                else:
                    try:
                        j = j + d
                        
                        el=driver.find_element_by_xpath('//*[@id="app"]/div/main/div/div[1]/section/div/div/div[2]/div[1]/div[6]/div/div/button')
                        driver.execute_script("arguments[0].click();", el)
                        
                    except:
                        j = j + d
                        
                        el=driver.find_element_by_xpath("/html/body/div[1]/div/div/div[1]/main/div/div[1]/section/div/div/div[2]/div[1]/div[{}]/div/div/button/span".format(j))
                        driver.execute_script("arguments[0].click();", el)
            except Exception:
                raise
        elems = driver.find_elements_by_class_name('vehicle-card__link')
        for elem in elems:
            urls.append(elem.get_attribute("href"))
        return urls
    except Exception:
        raise


def collect_url():
    i = 0
    for province, shortform_province in loc.items():
        for b in body:
            link = 'https://shop.canadadrives.ca/cars/' + shortform_province + '?sort_by=Featured&body_type=' + str(b)
            url = geturl(link)
            for li in url:
                df.loc[i, 'url'] = li
                df.loc[i, 'province'] = province 
                df.loc[i, 'vehicle_information_body_style'] = b
                df.loc[i, 'date_added'] = str(date.today())
                i = i + 1
            df.to_csv(local_urls_path)
