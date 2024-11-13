import re
import os
from datetime import date
import concurrent.futures
import logging

import pandas as pd
from bs4 import BeautifulSoup
import requests

# from base import log_config
from base import retry_decorator
from base.aws_handler import AWSHandler
from base.logging_decorator import handle_exceptions

logger_name = 'goauto_scrapper_bots_canada'
email_subject = 'Goauto Bot Alert'
email_toaddrs=['karan@qodemedia.net', 'jordan@qodemedia.com', 'bikin@nerdplatoon.com']
logger = logging.getLogger(__name__)
scraper_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "results")
local_file_path = os.path.join(scraper_dir, "goauto_result.csv")
# print(local_file_path)
aws_bucket_file_path = "MasterCode1/scraping/goauto/goauto_result.csv"
aws_bucket_folder_path = "MasterCode1/scraping/goauto"


def check_removed_and_price_change(url, prev_price):
    '''
    :param link: carlink
    :param price_list: list of price from price history
    :return: tuple containning the date removed and price change respectively returns boolean False if no change or removed
    '''
    page = requests.get(url)
    soup = BeautifulSoup(page.text, 'html.parser')
    if soup.find("title").text == 'This vehicle has sold! | Go Auto':
        return (str(date.today()), False)
    info_dict = eval(
        str(re.findall('window._vdp_event_data = (.+?)}|$', str(soup.text.replace('\n', "")))[0] + '}').replace('null',
                                                                                                                'None').replace(
            'false', 'False'))
    # price
    price = info_dict['list_price']
    # check if the current price is in the price_history
    if str(price) != str(prev_price):
        return (False, price)
    else:
        # if the link is not removed and the price is not changed then we return both parameters False
        return (False, False)


def scrape_url(url):
    
    page = requests.get(str(url))
    soup = BeautifulSoup(page.text, 'html.parser')
    
    info = [None] * 28
    info[0] = date.today().strftime('%Y/%m/%d')
    info[1] = None
    info[2] = soup.find("title").text
    info[3] = url
    info[4] = re.findall('"make_name":"(.+?)"', str(soup))[0]
    info[5] = re.findall('"model_name":"(.+?)"', str(soup))[0]
    info[6] = re.findall('"year":(.+?),', str(soup))[0]
    info[7] = re.findall('"odometer":(.+?),', str(soup))[0]
    try:
        info[8] = re.findall('"list_price":(.+?),', str(soup))[0]
    except:
        try:
            info[8] = re.findall('"special_price":(.+?),', str(soup))[0]
        except:
            pass
    info[9] = re.findall('"stock_type":"(.+?)"', str(soup))[0]
    info[10] = re.findall('"dealer_province_name":"(.+?)"', str(soup))[0]
    info[11] = re.findall('"dealer_city_name":"(.+?)"', str(soup))[0]
    try:
        info[12] = re.findall('"transmission_type":"(.+?)"', str(soup))[0]
    except:
        pass
    info[13] = re.findall('"drive_type_name":"(.+?)"', str(soup))[0]
    try:
        info[14] = re.findall('"body_type_category":"(.+?)"', str(soup))[0]
    except:
        pass
    try:
        info[15] = re.findall('"exterior_colour_name":"(.+?)"', str(soup))[0]
    except:
        pass
    try:
        info[16] = re.findall('"fuel_type_name":"(.+?)"', str(soup))[0]
    except:
        pass
    info[17] = re.findall('"trim":"(.+?)"', str(soup))[0].replace('",','')
    info[18] = re.findall('srcset="(.+?) ', str(soup))
    info[18] = [x for x in list(set(info[18])) if '1200' in x]
    info[19] = re.findall('"vin":"(.+?)"', str(soup))[0]
    
    price_dict = str({date.today().strftime('%Y/%m/%d'): info[8]})
    info[22] = str(price_dict)
    try:
        info[23] = re.findall('"Engine","value":"(.+?)"', str(soup))[0]
    except:
        pass
    info[24] = re.findall('"stock_number":"(.+?)"', str(soup))[0]
    
    info[26] = re.findall('"is_featured":(.+?),', str(soup))[0]
    
    #Gets rid of empty lists
    info = [x if x else None for x in info]
    
    new_info.append(info)



@handle_exceptions(logger_name, email_subject, email_toaddrs)
@retry_decorator.retry(requests.exceptions.ConnectionError, tries=3, delay=120)
def main():
    global new_info
    aws = AWSHandler()
    aws.download_from_aws(aws_bucket_file_path, local_file_path)

    scrape_df = pd.read_csv(local_file_path, low_memory=False)
    all_prev_links = scrape_df["url"].values
    df = pd.read_csv(os.path.join(scraper_dir, "all_links.csv"))
    new_urls = []

# =============================================================================
#     for link in df["links"].values:
#         if link in all_prev_links:
#             continue
#             # updater
#             # # until all the scraping is completed
#             # if not pd.isnull(scrape_df[scrape_df["url"] == link]["date_removed"]).values[0]:
#             #     # Check if link is previously removed if it is removed we continue
#             #     continue
#             # prev_price = scrape_df[scrape_df["url"] == link]["price"]
#             # index = scrape_df.index[scrape_df["url"] == link].tolist()[0]
#             # date_removed, price = check_removed_and_price_change(link, prev_price)
#             # if date_removed:
#             #     scrape_df.loc[index, 'date_removed'] = date_removed
#             # if price:
#             #     scrape_df.loc[index, 'price'] = price
#             #     prev_price_dict = eval(scrape_df[scrape_df["url"] == link]["price_history"].values[0])
#             #     # Adding the changed price to the price history
#             #     prev_price_dict[str(date.today())] = price
#             #     scrape_df.loc[index, 'price_history'] = str(prev_price_dict)
#         else:
#             new_urls.append(link)
# =============================================================================

    new_urls = [x for x in df['links'].values if x not in all_prev_links]

    logger.info(f"Number of new urls -> {len(new_urls)}")
    new_info = []

    with concurrent.futures.ThreadPoolExecutor() as executor:
        executor.map(scrape_url, new_urls)

    df_scraped = pd.DataFrame(new_info, columns=scrape_df.columns)
    scrape_df = pd.concat([scrape_df, df_scraped])
    #scrape_df = scrape_df.drop_duplicates(subset=['url'], keep='first')
    scrape_df.to_csv(local_file_path, index=False)

    aws.upload_to_aws(local_file_path, aws_bucket_folder_path)


if __name__ == "__main__":
    main()
