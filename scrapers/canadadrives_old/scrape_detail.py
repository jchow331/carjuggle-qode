import os
import pandas as pd
from bs4 import BeautifulSoup
import concurrent.futures
from datetime import date
import requests
from selenium.common.exceptions import NoSuchElementException, TimeoutException
import undetected_chromedriver as uc

from .scrape_url import collect_url
from base.logging_decorator import handle_exceptions
import logging
from base.aws_handler import AWSHandler

options = uc.ChromeOptions()
# options.headless = True
# options.add_argument('--headless')
driver = uc.Chrome(options=options, use_subprocess=True)

logger_name = 'Canadadrives scrapper_bots_canada'
email_subject = 'Canadadrives Scrape Detail  Bot Alert'
email_toaddrs=['ajay.qode@gmail.com','summit@qodemedia.com', 'karan@qodemedia.net', 'prabin@qodemedia.net']
logger = logging.getLogger(__name__)

scraper_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "../canadadrives/results")
local_file_path = os.path.join(scraper_dir, "canadadrives_result.csv")
local_urls_path = os.path.join(scraper_dir, "all_links.csv")

aws_bucket_file_path = "MasterCode1/scraping/canadadrives/canadadrives_result.csv"
aws_folder_path = "MasterCode1/scraping/canadadrives"

aws_handler = AWSHandler()


def check_removed_and_price_change(URL, prev_price):
    '''
    :param link: carlink
    :param price_list: list of price from price history
    :return: tuple containning the date removed and price change respectively returns boolean False if no change or removed
    '''
    page = requests.get(URL)
    soup = BeautifulSoup(page.source_content, 'html_praser')
    if page.status_code == 404:
        return (str(date.today()), False)
    elif soup.find(class_='heading-year-make-model') == None:
        print('Car is Sold')
        return (str(date.today()), False)

    soup = BeautifulSoup(page.text, 'html.parser')
    price = soup.find(class_='vehicle-price-3').get_text().split('\n')[1]
    # check if the current price is in the price_history
    if str(price) != str(prev_price):
        return (False, price)
    else:
        # if the link is not removed and the price is not changed then we return both parameters False
        return (False, False)


def scrape_info(information):
    try:
        _, url, province, body, _ = information
        print(_, url, province, body)
    except:
        url, province, body, _ = information
    driver.get(url)
    soup = BeautifulSoup(driver.page_source, 'html.parser')

    try:
        car_sold = soup.find(class_="subtitle__message font-weight-medium subtitle__vehicle-not-available").text.strip()
        if car_sold == 'Sorry, this vehicle is no longer available. Please click Search All Vehicles below to look at the rest of our inventory!':
            pass
        else:
            pass
    except:

        try:
            title = soup.find("title").text
        except:
            title = ""

        price_history = {}
        try:
            price = soup.find(class_="text-sm-h6 primary--text font-weight-medium text-body-1").text.replace('$',
                                                                                                             '').replace(
                ',', '')
        except:
            price = ''

        try:
            price_history[str(date.today())] = price
        except:
            price_history = ''

        try:
            keys1, Values1 = [i.text for i in soup.find_all(class_="py-1 col col-6")], [i.text for i in soup.find_all(
                class_="py-1 col")]
            tab = dict(zip(keys1, Values1))
        except:
            pass
        try:
            image_url = driver.find_element_by_xpath(
                '/html/body/div[1]/div/div/div/main/div/div[1]/div[1]/div[2]/div[1]/div/div[1]/div[1]/div[1]/div/div[2]').get_attribute(
                'style').split(' ')[1]
            image_url = image_url.split('("')[1]
            image_url = image_url.split('")')[0]
        except NoSuchElementException:
            pass
        except TimeoutException:
            pass
        except Exception:
            raise

        info = [None] * 24

        info[0] = str(date.today())
        info[2] = url
        info[3] = title
        try:
            info[4] = title.split(' ')[1].strip()
        except:
            pass
        try:
            info[5] = title.split(' ')[-1].strip()
        except:
            pass

        try:
            info[6] = title.split(' ')[0].strip()
        except:
            pass
        try:
            info[7] = tab.get('Kilometers')
        except:
            pass
        try:
            info[8] = price.split('.')[0]
        except:
            pass
        info[9] = str(price_history)

        if province == 'Ontorio':
            info[10] = 'Ontorio'
            info[11] = 'Brampton'
        elif province == 'British Columbia':
            info[10] = 'British Columbia'
            info[11] = 'Richmond'
        else:
            info[10] = 'Alberta'
            info[11] = 'Airdrie'
        try:
            info[12] = title[1]
        except:
            pass
        try:
            info[13] = tab.get('Stock#').replace('\n', '').strip()
        except:
            pass
        try:
            info[14] = tab.get('Status').replace('\n', '').strip()
        except:
            pass
        try:
            info[15] = tab.get('Transmission').replace('\n', '').strip()
        except:
            pass
        try:
            info[16] = tab.get('Drivetrain').replace('\n', '').strip()
        except:
            pass
        try:
            info[17] = body
        except:
            pass
        try:
            info[18] = tab.get('Engine Type').replace('\n', '').strip()
        except:
            pass
        try:
            info[19] = tab.gey('Exterior Colour').replace('\n', '').strip()
        except:
            pass
        try:
            info[20] = tab.get('Fuel Type').replace('\n', '').strip()
        except:
            pass
        try:
            info[21] = tab.get('VIN').replace('\n', '').strip()
        except:
            pass
        try:
            info[22] = image_url
        except:
            pass
        info[23] = str(tab)
        new_info.append(info)
        print(len(new_info))


@handle_exceptions(logger_name, email_subject, email_toaddrs)
def main():
    # collect_url()
    global new_info
    new_info = []
    if not os.path.exists(local_file_path):
        aws_handler.download_from_aws(aws_bucket_file_path, local_file_path)
    df_current = pd.read_csv(local_file_path)
    url_df = pd.read_csv(local_urls_path)
    urls = url_df[~url_df.url.isin(df_current.url)]
    tups = list(urls.to_records(index=False))
    num_workers = 16
    j=0
    for tup in tups:
        j+=1
        scrape_info(tup)
        break
        if j>=10:
            break
    # with concurrent.futures.ThreadPoolExecutor(max_workers=num_workers) as executor:
    #     executor.map(scrape_info, tup)
    logger.info(f'No of Loading:: {len(new_info)}')
    df_scrape = pd.DataFrame(new_info, columns=['date_added', 'date_removed', 'url', 'title',
                                                'make', 'model', 'year', 'kilometers', 'price', 'price_history',
                                                'province', 'city', 'Vehicle_information.trim', 'id', 'Condition',
                                                'Vehicle_information.transmission',
                                                'Vehicle_information.drivetrain',
                                                'Vehicle_information.body_style', 'Vehicle_information.engine',
                                                'Vehicle_information.exterior_colour',
                                                'Vehicle_information.fuel_type',
                                                'vin', 'image_url', 'metadata'])

    df = pd.read_csv(local_file_path)
    df3 = pd.concat([df, df_scrape], ignore_index=True)
    try:
        df3 = df3.loc[:, ~df3.columns.str.match("Unnamed")]
    except:
        pass
    df3.to_csv(local_file_path, index=False)
    # df = pd.read_csv(local_urls_path)
    # aws_handler.upload_csv_object(df, aws_bucket_file_path)


if __name__ == "__main__":
    main()
