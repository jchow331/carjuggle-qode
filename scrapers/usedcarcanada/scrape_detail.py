import concurrent.futures
import logging
import os.path
import time
from datetime import date

import pandas as pd
import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util import Retry

from base import log_config, retry_decorator
from base.aws_handler import AWSHandler
from base.logging_decorator import handle_exceptions

logger_name = "Usedcarcanada_scrapper_bots_canada"
email_subject = "UsedCarCananda Bot Alert"
email_toaddrs = ["jordan@qodemedia.com", "karan@qodemedia.net", "bikin@nerdplatoon.com"]
logger = logging.getLogger(__name__)

scraper_dir = os.path.dirname(os.path.abspath(__file__))
local_url_path = os.path.join(os.path.join(scraper_dir, "results"), "all_links.csv")
local_file_path = os.path.join(
    os.path.join(scraper_dir, "results"), "usedcarcanada_result.csv"
)
aws_bucket_file_path = "MasterCode1/scraping/usedcarcanada/usedcarcanada_result.csv"
aws_bucket_folder_path = "MasterCode1/scraping/usedcarcanada"
new_info = []


def check_removed_and_price_change(URL, prev_price):
    """
    :param link: carlink
    :param price_list: list of price from price history
    :return: tuple containning the date removed and price change respectively returns boolean False if no change or removed
    """
    page = requests.get(URL)
    soup = BeautifulSoup(page.text, "html.parser")

    if soup.find("title").text == "Carsgone Error 404":
        print("links removed")
        return (str(date.today()), False)

    rows = soup.find("table", class_="table table-striped").find_all("tr")
    info_dict = {}
    for row in rows:
        cells = row.find_all("td")
        try:
            info_dict[cells[0].get_text()] = cells[1].get_text()
        except IndexError:
            pass
    price = info_dict["Price"]
    # check if the current price is in the price_history
    if str(price) != str(prev_price):
        return (False, price)
    else:
        # if the link is not removed and the price is not changed then we return both parameters False
        return (False, False)


def scrape_info(URL_CP):
    # print(new_urls.index(URL_CP))
    if new_urls.index(URL_CP) % 500 == 0:
        time.sleep(30)
    URL, city_province, car_condition = URL_CP
    # print(URL)
    # print(city_province, car_condition)
    # page = requests.get(URL)
    print("url = ", URL)
    session = requests.Session()
    retry = Retry(connect=5, backoff_factor=2.0)
    adaptor = HTTPAdapter(max_retries=retry)
    session.mount("http://", adaptor)
    session.mount("https://", adaptor)
    page = session.request("GET", url=URL)
    # print(str(page))
    # if str(page) == '<Response [404]>|<Response [200]>':
    #    time.sleep(30)
    if page.status_code == 404:
        time.sleep(2)
    elif page.status_code == 200:
        time.sleep(2)
    else:
        time.sleep(30)

    soup = BeautifulSoup(page.text, "html.parser")
    info = [None] * 25
    if page.status_code == 404:
        info[1] = date.today().strftime("%Y/%m/%d")
        # print("page not found")
        pass

    try:
        img_url = soup.find("div", class_="large-item-body").find("a").img["src"]
        title = soup.find("div", class_="large-item-head").find("h1").text
        rows = soup.find("table", class_="table table-striped").find_all("tr")
        info_dict = {}
        for row in rows:
            cells = row.find_all("td")
            try:
                info_dict[cells[0].get_text()] = cells[1].get_text()
            except IndexError as e:
                pass
        info[1] = None
        info[0] = date.today().strftime("%Y/%m/%d")
        info[2] = title
        info[3] = URL
        try:
            info[4] = info_dict["Make"]
        except KeyError as k_e:
            pass
        try:
            info[5] = info_dict["Model"]
        except KeyError as k_e:
            pass
        try:
            info[6] = info_dict["Year"]
        except KeyError as k_e:
            pass
        try:
            info[8] = info_dict["Price"]
        except KeyError as k_e:
            pass
        try:
            info[9] = car_condition
        except KeyError as k_e:
            pass
        city_province = city_province.split(",")
        info[10] = city_province[-1]
        info[11] = city_province[0]
        try:
            info[17] = info_dict["Trim"]
        except KeyError as k_e:
            # print("Error Occurred ->", k_e, " \tline_no: ", k_e.__traceback__)
            pass
        info[18] = img_url
        try:
            info[7] = info_dict["Mileage"]
        except KeyError as k_e:
            # print("Error Occurred ->", k_e, " \tline_no: ", k_e.__traceback__)
            pass
        info[16] = None
        try:
            info[15] = info_dict["Exterior Color"]
        except KeyError as k_e:
            # print("Error Occurred ->", k_e, " \tline_no: ", k_e.__traceback__)
            pass
        try:
            info[14] = info_dict["Body Style"]
        except KeyError as k_e:
            # print("Error Occurred ->", k_e, " \tline_no: ", k_e.__traceback__)
            pass
        try:
            info[12] = info_dict["Transmission"]
        except KeyError as k_e:
            # print("Error Occurred ->", k_e, " \tline_no: ", k_e.__traceback__)
            pass
        price_dict = {date.today().strftime("%Y/%m/%d"): info_dict["Price"]}
        info[22] = str(price_dict)
        # print(info)
        try:
            info[23] = info_dict
        except KeyError as k_e:
            # print("Error Occurred ->", k_e, " \tline_no: ", k_e.__traceback__)
            pass
        try:
            info[24] = info_dict["Stock"]
        except KeyError as k_e:
            # print("Error Occurred ->", k_e, " \tline_no: ", k_e.__traceback__)
            pass
        new_info.append(info)
    except Exception as exc:
        # print("Error Occurred ->", exc, " \tline_no: ", exc.__traceback__)
        pass
    # print(info)
    print("\u2713", " Completed")


@handle_exceptions(logger_name, email_subject, email_toaddrs)
@retry_decorator.retry(requests.exceptions.ConnectionError, tries=3, delay=120)
def main():

    global new_urls

    aws_handler = AWSHandler()

    if not os.path.exists(local_file_path):
        aws_handler.download_from_aws(aws_bucket_file_path, local_file_path)

    scrape_df = pd.read_csv(local_file_path, low_memory=False)
    num_previously_scraped_cars = len(scrape_df)
    all_prev_links = scrape_df["url"].values

    df = pd.read_csv(local_url_path)
    new_urls = []
    for link, city_province, car_condition in df[
        ["links", "city_province", "car_condition"]
    ].values:
        if link in all_prev_links:
            # until all the scraping is completed
            continue
            # if not pd.isnull(scrape_df[scrape_df["url"] == link]["date_removed"]).values[0]:
            #     # Check if link is previously removed if it is removed we continue
            #     continue
            # prev_price = scrape_df[scrape_df["url"] == link]["price"]
            # index = scrape_df.index[scrape_df["url"] == link].tolist()[0]
            # date_removed, price = check_removed_and_price_change(link, prev_price)
            # if date_removed:
            #     scrape_df.loc[index, 'date_removed'] = date_removed
            # if price:
            #     scrape_df.loc[index, 'price'] = price
            #     prev_price_dict = eval(scrape_df[scrape_df["url"] == link]["price_history"].values[0])
            #     # Adding the changed price to the price history
            #     prev_price_dict[str(date.today())] = price
            #     scrape_df.loc[index, 'price_history'] = str(prev_price_dict)
        else:
            new_urls.append((link, city_province, car_condition))

    print("new urls:", len(new_urls))

    for x in new_urls:
        scrape_info(x)

    # =============================================================================
    #     with concurrent.futures.ThreadPoolExecutor() as executor:
    #         executor.map(scrape_info, new_urls)
    # =============================================================================

    df_scraped = pd.DataFrame(
        new_info,
        columns=[
            "date_added",
            "date_removed",
            "title",
            "url",
            "make",
            "model",
            "year",
            "kilometers",
            "price",
            "condition",
            "province",
            "City",
            "Vehicle_information.transmission",
            "Vehicle_information.drivetrain",
            "Vehicle_information.body_style",
            "Vehicle_information.exterior_colour",
            "Vehicle_information.fuel_type",
            "Vehicle_information.trim",
            "img_url",
            "vin",
            "NumOwners",
            "PrevAccident",
            "price_history",
            "metadata",
            "_id",
        ],
    )
    # scrape_df = scrape_df.append(df_scraped)
    scrape_df = pd.concat([scrape_df, df_scraped])
    scrape_df.to_csv(local_file_path, index=False)

    aws_handler.upload_to_aws(local_file_path, "MasterCode1/scraping/usedcarcanada")


if __name__ == "__main__":
    main()
