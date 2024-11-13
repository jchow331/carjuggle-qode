import datetime
import logging
import math
import os

import requests
import pandas as pd
from decouple import config
# uncomment if selenium is required
import undetected_chromedriver as uc
from selenium import webdriver
# from selenium.webdriver.common.by import By
# from selenium.webdriver.support.ui import WebDriverWait
# from selenium.webdriver.support import expected_conditions as EC
# from selenium.common.exceptions import TimeoutException

from base import log_config, retry_decorator
from base.aws_handler import AWSHandler
from base.logging_decorator import handle_exceptions

logger_name = 'cargurus_scrapper_bots_canada'
email_subject = 'CarGurus Bot Alert'
email_toaddrs=['ajay.qode@gmail.com', 'karan@qodemedia.net', 'jordan@qodemedia.com', 'bikin@nerdplatoon.com']
logger = logging.getLogger(__name__)
scraper_dir = os.path.dirname(os.path.abspath(__file__))
# logging.basicConfig(format='%(levelname)s:%(message)s', level=logging.INFO)


def get_makes_code():
    # driver = uc.Chrome(config('CHROME_VERSION'))
    driver = webdriver.Chrome(executable_path=config('CHROME_DRIVER'))

    all_makes = []
    driver.get(
        "https://www.cargurus.ca/Cars/inventorylisting/viewDetailsFilterViewInventoryListing.action?sourceContext=carGurusHomePageModel&entitySelectingHelper.selectedEntity=m4&zip=K2J")
    options = driver.find_element_by_xpath(
        "/html/body/main/div[2]/div[1]/div/div[2]/div[1]/div[1]/div/div/div[1]/div/div/div[1]/form/div[1]/select[1]/optgroup[2]")
    opts = options.find_elements_by_tag_name("option")
    for option in opts:
        # print(option.get_attribute("value"))
        all_makes.append(option.get_attribute("value"))
    return all_makes


def scrape_data(car_dict):
    info = [None] * 27
    try:
        info[0] = str(datetime.date.today() - datetime.timedelta(days=int(car_dict["daysOnMarket"])))
    except Exception:
        pass
    try:
        info[1] = None
    except KeyError:
        pass

    try:
        info[2] = car_dict["listingTitle"]
    except KeyError:
        pass
    # url
    try:
        info[3] = f"https://www.cargurus.ca/Cars/inventorylisting/viewDetailsFilterViewInventoryListing.action?zip=K2J&showNegotiable=true&sortDir=ASC&sourceContext=carGurusHomePageModel&distance=50000&sortType=DEAL_SCORE&entitySelectingHelper.selectedEntity={car_dict['makeId']}#listing={car_dict['id']}/NONE"
    except KeyError:
        pass
    # make
    try:
        info[4] = car_dict["makeName"]
    except KeyError:
        pass
    # model
    try:
        info[5] = car_dict["modelName"]
    except KeyError:
        pass
    # year
    try:
        info[6] = car_dict["carYear"]
    except KeyError:
        pass
    # mileage
    try:
        info[7] = car_dict["mileage"]
    except KeyError:
        pass
    # Price
    try:
        info[8] = car_dict["expectedPrice"]
    except KeyError:
        pass
    # state
    try:
        info[10] = car_dict["sellerRegion"]
    except KeyError:
        pass
    # city
    try:
        info[11] = car_dict["sellerCity"]
    except KeyError:
        pass
    # condition
    try:
        info[9] = "Used"
    except KeyError:
        pass

    try:
        info[12] = car_dict['localizedTransmission']
    except KeyError:
        pass

    try:
        if info[2][-2:]=="WD":
            info[13] = info[2][-3:]
    except KeyError:
        pass

    try:
        info[14] = car_dict['bodyTypeName']
    except KeyError:
        pass
    try:
        info[15] = car_dict['exteriorColorName']
    except KeyError:
        pass
    try:
        info[16] = car_dict['localizedFuelType']
    except KeyError:
        pass
    try:
        # trim
        info[17] = car_dict["trimName"]
    except KeyError:
        pass
    # try:
    #     info[19] = car_dict['VIN:']
    # except KeyError:
    #     pass

    # info[20] = car_dict['vehicle']['OwnerCount']
    # info[21] = info_dict['vehicle']['AccidentCount']

    try:
        info[18] = car_dict["mainPictureUrl"]
    except KeyError:
        pass

    price_history_dict = str({info[0]: info[8]}).replace("\'", "\"")
    info[22] = str(price_history_dict)
    try:
        info[23] = car_dict['Engine:']
    except KeyError:
        pass
    try:
        info[24] = car_dict['id']
    except KeyError:
        pass
    # info[25] = str(car_dict)
    # Carfax_url
    info[26] = None

    new_info.append(info)

@handle_exceptions(logger_name, email_subject, email_toaddrs)
@retry_decorator.retry(requests.exceptions.ConnectionError, tries=3, delay=120)
def main():
    #Uncomment this code to get the below all_makes_list
    # driver = uc.Chrome()
    # all_makes_list = get_makes_code()
    # print(all_makes_list)
    a = AWSHandler()
    local_file_path = os.path.join(scraper_dir, "results/cargurusCA_result.csv")
    aws_bucket_file_path = "MasterCode1/scraping/cargurusCA/cargurusCA_result.csv"
    aws_bucket_folder_path = "MasterCode1/scraping/cargurusCA"

    # list of codes of all the makes
    all_makes_list = ['m4', 'm124', 'm79', 'm110', 'm19', 'm20', 'm3', 'm21', 'm22', 'm1', 'm23', 'm96',
                      'm24', 'm25', 'm98', 'm183', 'm2', 'm203', 'm26', 'm6', 'm27', 'm28', 'm84', 'm31', 'm32', 'm233',
                      'm33', 'm34', 'm35', 'm37', 'm38', 'm39', 'm40', 'm41', 'm42', 'm141', 'm43', 'm44', 'm45', 'm46',
                      'm12', 'm85', 'm87', 'm47', 'm48', 'm191', 'm49', 'm50', 'm51', 'm52', 'm135', 'm111', 'm194',
                      'm53', 'm54', 'm112', 'm7', 'm55', 'm56', 'm205', 'm271']

    if not os.path.exists(local_file_path):
        a.download_from_aws(aws_bucket_file_path, local_file_path)

    scrape_df = pd.read_csv(local_file_path)

    for make in all_makes_list:
        logger.info(f"Scraping -> {make}")

        # base_make_link = f"https://www.cargurus.ca/Cars/inventorylisting/viewDetailsFilterViewInventoryListing.action?zip=K2J&showNegotiable=true&sortDir=ASC&sourceContext=carGurusHomePageModel&distance=50000&sortType=DEAL_SCORE&entitySelectingHelper.selectedEntity={make}"
        # driver.get(base_make_link)
        # try:
        #     num_cars = WebDriverWait(driver, 10).until(EC.presence_of_element_located(
        #         (By.XPATH,
        #          '//*[@id="cargurus-listing-search"]/div/div/div[2]/div[2]/div[3]/div[2]/span/strong[2]'))).text
        # except TimeoutException:
        #     try:
        #         num_cars = WebDriverWait(driver, 10).until(EC.presence_of_element_located(
        #             (By.XPATH,
        #              '//*[@id="cargurus-listing-search"]/div[1]/div/div[2]/div[2]/div[4]/div[2]/span/strong[2]'))).text
        #     except Exception:
        #         num_cars = WebDriverWait(driver, 10).until(EC.presence_of_element_located(
        #             (By.XPATH,
        #              '//*[@id="cargurus-listing-search"]/div/div/div[2]/div[2]/div[3]/div[2]/span/strong'))).text
        # print(f"Number of cars in the {make} ->",num_cars)
        # num_cars = int(num_cars.replace(",", ""))

        #MAX number of cars scrapable through API is 10000
        num_cars = 10000
        offset = 15
        global new_info
        new_info = []
        end = math.ceil(num_cars / offset) * offset
        all_previously_scraped_listing_id = scrape_df["_id"]

        # print("all previoulsy scraped listing",len(all_previously_scraped_listing_id))
        previous_data_found = False

        for i in range(offset, num_cars + offset, offset):
            logger.info(f"num cars scraped for {make} - {i}")
            url = f"https://www.cargurus.ca/Cars/searchResults.action?zip=K2J&inventorySearchWidgetType=AUTO&searchId={i}&shopByTypes=NEAR_BY&sortDir=ASC&sourceContext=carGurusHomePageModel&distance=50000&sortType=AGE_IN_DAYS&entitySelectingHelper.selectedEntity={make}&offset={i}&maxResults=15&filtersModified=true"
            # headers = {
            #     'Cookie': 'CarGurusUserT=vGDT-27.34.22.239.1641544425962; JSESSIONID=4AEDE5B62EE3EDBA7B4AB29320947A5F.www33; LSW=prod-bos2-www-33; MultivariateTest=H4sIAAAAAAAAAKtWcnb0c3RxjHf1U7KqVjIyV7JSMrA2UNJRMgQxTUyBLGMQyxIkZmQCZOkagsRMwAqNQIKWMEEjM7huI1Mw0xik1AjKrK0FAKiMtohuAAAADrR2uiSXlNHmCUoH17buJU1YVybol8lDsJV76y%2FXiT0%3D; ViewVersion=%7B%22ca%22%3A%7B%22includes%22%3A%5B%229b8aa60e-e6ec-452d-a30a-6cfd86574f47%22%5D%2C%22exclude%22%3A%7B%22ea62de2f-3194-43ba-a167-cbae72434925%22%3A25%7D%2C%22type%22%3A%22IN%22%7D%7D; cg-ssid=daaa6734bf723ab564760b6759f8d9f381862f68fb21b6643a6f9fd137f7226c; preferredContactInfo=Y2l0eT1OZXBlYW4qcG9zdGFsQ29kZT1LMkoqc3RhdGU9T04qY291bnRyeT1DQSpob21lUG9zdGFsQ29kZT1LMkoq'
            # }
            # & nonShippableBaseline = {num_cars}
            response = requests.request("GET", url)
            car_list = eval(
                response.text.replace("true", "True").replace("false", "False").replace(
                    "none", "None").replace("null","None").replace(r"\N", r"\n"))
            if car_list is not None:
                for car_dict in car_list:
                    scrape_data(car_dict)

                    #check if the listing id is present in the previous
                    if int(car_dict["id"]) in list(all_previously_scraped_listing_id):
                        logger.info(f"The previous link is found for the {make} , Stopping scraping for the model.")
                        previous_data_found = True
                        i = end
                        break
            else:
                break

            if i % 300 == 0 or i == end:
                # print("WRITING TO CSV..")
                new_df = pd.DataFrame(new_info,
                                      columns=['date_added', 'date_removed', 'title', 'url', 'make', 'model', 'year',
                                               'kilometers', 'price', 'condition', 'state', 'City',
                                               'Vehicle_information.transmission', 'Vehicle_information.drivetrain',
                                               'Vehicle_information.body_style',
                                               'Vehicle_information.exterior_colour', 'Vehicle_information.fuel_type',
                                               'Vehicle_information.trim', 'img_url', 'vin', 'NumOwners',
                                               'PrevAccident', 'price_history', 'Vehicle_information.engine', '_id',
                                               'metadata', 'car_fax_url'])
                scrape_df = pd.concat([scrape_df, new_df])
                scrape_df.sort_values(by=['date_removed', 'date_added'],inplace=True)
                scrape_df.to_csv(local_file_path, index=False)
                new_info = []

            #stop scraping for the make if listing id has been previously seen
            if previous_data_found:
                # print(f"end of {make}")
                break

    #For uploading to aws we need to provide only the bucket folder path instead of the full path as it tends to make unnecessary folders
    a.upload_to_aws(local_file_path, aws_bucket_folder_path)
    # a.download_from_aws("MasterCodeUS/scraping/autolist/try.txt", "try_download.txt")

if __name__ == "__main__":
    main()
