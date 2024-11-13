
from decouple import config
from selenium.webdriver.chrome.options import Options
import pandas as pd
import undetected_chromedriver as uc

options = uc.ChromeOptions()
options.headless=True
options.add_argument('--headless')
driver = uc.Chrome(options=options,version_main=config('CHROME_VERSION'))


from base.logging_decorator import handle_exceptions
import  logging
logger_name = 'murrayautogroup_scrapper_bots_canada'
email_subject = 'murrayautogroup url Bot Alert'
email_toaddrs=['karan@qodemedia.net', 'prabin@qodemedia.net', 'ajay.qode@gmail.com', 'jordan@qodemedia.com', 'bikin@nerdplatoon.com']
logger = logging.getLogger(__name__)
from base.aws_handler import AWSHandler
a = AWSHandler()


def get_Count_Pages(driver):
    count_pages = driver.find_element_by_class_name("pagination__numbers").find_element_by_tag_name("span")
    count_pages = count_pages.text.split(' ', 2)[1]
    return count_pages

def get_Url_Page(driver):
    list_urls = []
    url_vehicles = driver.find_elements_by_class_name("vehicle-card__details")
    url_vehicles = driver.find_elements_by_class_name("vehicle-card__details")
    for url in url_vehicles:
        list_urls.append(url.find_element_by_tag_name('a').get_attribute('href'))
    return list_urls

def Save_Url(list_all_urls):
    df = pd.DataFrame(list_all_urls, columns=['url'])
    df.to_csv("scrapers/murrayautogroup/results/urls.csv", index=False)

def collect_url(url):
    try:
        driver.get(url)
        #get urls from first page
        list_total_urls = get_Url_Page(driver)

        #get urls from second page until last page
        count_pages = get_Count_Pages(driver)
        print('count pages',count_pages)
        driver.quit()
        c = 2

        while c <= int(count_pages):
            new_url = url+"&pg=1/?view=grid&pg=%s" % str(c)
            print(new_url)
            c = c+1
            driver.get(new_url)
            list_urls = get_Url_Page(driver)
            list_total_urls = list_total_urls + list_urls
            driver.quit()

        Save_Url(list_total_urls)
    except Exception as e:
        logger.info(f'{e}')

if __name__ == "__main__":
    collect_url("https://www.murrayautogroup.ca/vehicles")




