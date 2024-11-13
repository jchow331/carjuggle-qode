import re
import os
import logging
from datetime import date, timedelta
import time

from decouple import config
import pandas as pd
import undetected_chromedriver as uc
from bs4 import BeautifulSoup

from selenium.webdriver import Chrome
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException

from decouple import config

from base import log_config
from base.aws_handler import AWSHandler
from base.logging_decorator import handle_exceptions

logger_name = "cardoor"
email_subject = "cardoor Bot Alert"
email_toaddrs = ["jordan@qodemedia.com", "karan@qodemedia.net", "bikin@nerdplatoon.com"]
# email_toaddrs = []
logger = logging.getLogger(__name__)
scraper_dir = os.path.dirname(os.path.abspath(__file__))

local_file_path = os.path.join(
    os.path.join(scraper_dir, "results"), "cardoor_result.csv"
)
aws_bucket_file_path = "MasterCode1/scraping/cardoor/cardoor_result.csv"
aws_bucket_folder_path = "MasterCode1/scraping/cardoor"
website_url = "https://www.cardoor.ca/used-vehicles/?_p={}"


def call_driver(url):

    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--no-sandbox")
    # Incase of the access denied while using headless use this
    chrome_options.add_argument(
        f"user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/79.0.3945.79 Safari/537.36"
    )
    # chrome_options.add_argument("--disable-dev-shm-usage")
    # chrome_options.add_argument("--disable-extensions")
    # chrome_options.add_argument("disable-infobars")
    # driver = uc.Chrome(
    #     options=chrome_options,
    # )
    driver = Chrome(
        executable_path=config("CHROME_DRIVER"),
        options=chrome_options,
    )

    driver.get(url)
    time.sleep(10)
    return driver


def get_urls(page_num):

    driver = call_driver(website_url.format(page_num))
    soup = BeautifulSoup(driver.page_source, "html.parser")
    driver.quit()

    listings = soup.find_all("div", {"class": "hit"})
    listings = [re.findall('href="(.+?)"', str(x))[0] for x in listings]
    return listings


def get_history(driver, carfax_url):

    info = [None, None]

    driver.get(carfax_url)
    try:
        WebDriverWait(driver, 20).until(
            EC.presence_of_element_located((By.CLASS_NAME, "details-wrapper"))
        )
    except TimeoutException:
        return info
    soup = BeautifulSoup(driver.page_source, "html.parser")

    info[0] = (
        len(
            re.findall(
                "New Owner Reported",
                str(
                    soup.find("section", {"id": "detailed-history-section"}).find_all(
                        "td", {"class": "item-detail"}
                    )
                ),
            )
        )
        + 1
    )
    info[1] = len(
        soup.find("section", {"id": "accident-damage-section"}).find_all("tbody")
    )
    return info


def check_status(index, listing_url, old_price):

    try:
        driver = call_driver(listing_url)

        # If redirect, the listing has gone down
        if driver.current_url == "https://www.cardoor.ca/used-vehicles/":
            return [index, None]
        # If no redirect, check for price change
        else:
            soup = BeautifulSoup(driver.page_source, "html.parser")
            new_price = next(iter(re.findall('"price": "(.+?)"', str(soup))), None)

            if float(new_price) != old_price:
                return [None, new_price]
    except:
        pass

    finally:
        try:
            driver.quit()
        except:
            pass


def scrape_url(listing_url):

    info = [None] * 26

    driver = call_driver(listing_url)
    if driver.current_url == "https://www.cardoor.ca/used-vehicles/":
        driver.quit()
        return info
    soup = BeautifulSoup(driver.page_source, "html.parser")

    try:
        basic_info = soup.find_all("li", {"class": "basic-info-item"})
        img_info = soup.find("div", {"class": "info-right"})
        fax_info = soup.find("div", {"class": "vdp-history-report__logo"})
        dealer_info = re.findall("dataLayer.push\((.+?)\)", str(soup))[-1]
    except:
        driver.quit()
        return info

    info[0] = str(
        date.today()
        - timedelta(days=int(re.findall('"days_in_stock":(.+?)\}', dealer_info)[0]))
    )
    info[2] = next(iter(re.findall('"stock":"(.+?)"', str(soup))), None)
    info[3] = next(iter(re.findall('"vin":"(.+?)"', str(soup))), None)
    info[5] = listing_url
    info[6] = next(iter(re.findall('"make":"(.+?)"', str(soup))), None)
    info[7] = next(iter(re.findall('"model":"(.+?)"', str(soup))), None)
    info[8] = next(iter(re.findall('"year":"(.+?)"', str(soup))), None)
    info[9] = next(iter(re.findall('"miles":"(.+?)"', str(soup))), None)
    info[10] = next(iter(re.findall('"price": "(.+?)"', str(soup))), None)
    info[11] = next(
        iter(re.findall('"inventory_type":"(.+?)"', str(dealer_info))), None
    )  # They are all listed as used
    info[12] = next(
        iter(re.findall("<br\\\/>(.+?)<br\\\/>", str(dealer_info))), None
    ).split(", ")[-1][0:2]
    info[13] = next(
        iter(re.findall("<br\\\/>(.+?)<br\\\/>", str(dealer_info))), None
    ).split(", ")[0]
    info[14] = next(
        iter([list(x.descendants)[-3] for x in basic_info if "Transmission" in x.text]),
        None,
    )
    info[15] = next(
        iter([list(x.descendants)[-3] for x in basic_info if "Drivetrain" in x.text]),
        None,
    )
    info[16] = next(iter(re.findall('"bodytype":"(.+?)"', str(soup))), None)
    info[17] = next(
        iter([list(x.descendants)[-3] for x in basic_info if "Exterior" in x.text]),
        None,
    )
    info[18] = next(
        iter([list(x.descendants)[-3] for x in basic_info if "Engine" in x.text]), None
    )
    info[19] = next(
        iter([list(x.descendants)[-3] for x in basic_info if "Trim" in x.text]), None
    )
    info[20] = next(iter(re.findall('src="(.+?)"', str(img_info))), None)
    info[21] = next(iter(re.findall('href="(.+?)"', str(fax_info))), None)
    if info[21]:
        fax_info = get_history(driver, info[21])
        info[22] = fax_info[0]
        info[23] = fax_info[1]
    info[24] = str({info[0]: info[10]}).replace("'", '"')
    info[25] = next(iter(re.findall('"vehicle":\{(.+?)\}', str(soup))), None)
    driver.quit()
    return info


@handle_exceptions(logger_name, email_subject, email_toaddrs)
def main():

    a = AWSHandler()
    a.download_from_aws(aws_bucket_file_path, local_file_path)
    df = pd.read_csv(local_file_path)

    driver = call_driver(website_url.replace("?_p={}", ""))

    # Get num pages
    num_pages = int(
        driver.find_element(By.CLASS_NAME, "pagination-state").text.replace(
            "Page 1 of ", ""
        )
    )

    driver.quit()

    # Iterate through page numbers and get urls
    all_listings = []
    for page in range(num_pages):
        logger.info(f"Scrape for listings...{page}/{num_pages}")
        all_listings.extend(get_urls(page))
    all_listings = list(set(all_listings))

    # Iterate through new listings and scrape
    all_info = []
    new_df = pd.DataFrame(columns=df.columns.tolist())
    for listing in [x for x in all_listings if x not in df["url"].unique()]:
        logger.info(
            f"Scraping listings...{all_listings.index(listing)}/{len(all_listings)}"
        )
        all_info.append(scrape_url(listing))
        if (
            all_listings.index(listing) % 100 == 0
        ):  # Saves every hundred listings, saves memory
            new_df = pd.concat(
                [new_df, pd.DataFrame(all_info, columns=new_df.columns.tolist())]
            )
            all_info = []

    # =============================================================================
    #     #Iterate through previous listings and check if the listing is available
    #     to_check = df[df['date_removed'].isnull()].index.values
    #     removed, price_changed = [], {}
    #     for index in to_check:
    #         print(f'Checking previous...{list(to_check).index(index)}/{len(to_check)}')
    #         changes = check_status(index, df.loc[index,'url'], df.loc[index,'price'])
    #         if changes:
    #             if changes[0]:
    #                 removed.append(changes[0])
    #             elif changes[1]:
    #                 price_changed[index] = changes[1]
    #
    #     #Add price history to the listings whose prices changed
    #     print(f'Updating dataframe...{len(price_changed)} changes to make.')
    #     for index in price_changed:
    #         new_price = price_changed[index]
    #         history = json.loads(df.loc[index,'price_history'])
    #         history[str(date.today())] = new_price
    #
    #         df.loc[index,'price'] = new_price
    #         df.loc[index,'price_history'] = str(history).replace("\'", "\"")
    # =============================================================================

    # Append to old df and save
    df = pd.concat([df, new_df])
    # df.loc[removed, 'date_removed'] = str(date.today())
    df.sort_values(by=["date_removed", "date_added"], inplace=True)
    df.to_csv(local_file_path, index=False)

    a.upload_to_aws(local_file_path, aws_bucket_folder_path)


if __name__ == "__main__":
    main()
