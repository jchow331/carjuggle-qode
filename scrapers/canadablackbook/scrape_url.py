import time
import  logging
import os

import pandas as pd
import undetected_chromedriver as uc
from selenium import webdriver
from selenium.common.exceptions import NoSuchElementException
from decouple import config

from base.logging_decorator import handle_exceptions
# from base import log_config


options = uc.ChromeOptions()
options.add_argument('--headless')
# driver = uc.Chrome(options=options, use_subprocess=True, version_main=100)
driver = webdriver.Chrome(executable_path=config('CHROME_DRIVER'), options=options)

scraper_dir = os.path.dirname(os.path.abspath(__file__))
local_file_path = os.path.join(os.path.join(scraper_dir, "results"), "all_links.csv")

logger_name = 'canadablack_book scrapper_bots_canada'
email_subject = 'Canadablackbook Scrape_url  Bot Alert'
email_toaddrs=['karan@qodemedia.net', 'prabin@qodemedia.net','ajay.qode@gmail.com', 'jordan@qodemedia.com', 'bikin@nerdplatoon.com']
logger = logging.getLogger(__name__)


def collect_url():
    all_links=[]
    max_price_in_this_web=100
    num_cars_per_page=15
    links = ['https://www.canadianblackbook.com/search/used/?price=1%2C1000000&pg=', 'https://www.canadianblackbook.com/search/new/?price=1%2C1000000&pg=']
    for link in links:
        for j in range(1, max_price_in_this_web):
                url = link + str(j)
                driver.get(url)
                try:
                    text = driver.find_element_by_xpath('/html/body/div[2]/div/h2').text
                except NoSuchElementException:
                    text = None
                if text == 'Sorry there are no listings for that particular vehicle':
                    break
                time.sleep(2)
                for k in range(1, num_cars_per_page):
                    try:
                        all_links.append(driver.find_element_by_xpath('//*[@id="search-results"]/div[2]/a[{}]'.format(k)).get_attribute('href'))

                    except NoSuchElementException:
                        pass
                # print(len(all_links))
    return all_links

@handle_exceptions(logger_name, email_subject, email_toaddrs)
def main():
    all_links = collect_url()
    df =pd.DataFrame(all_links,columns=['links'])
    df.dropna(inplace=True)
    df.to_csv(local_file_path,index=False)

if __name__ == "__main__":
    main()
    
