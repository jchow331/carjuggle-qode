import datetime
import logging
import os
import time
import json

import requests
import pandas as pd

from base import log_config
from base import retry_decorator
from base.aws_handler import AWSHandler
from base.logging_decorator import handle_exceptions

logger_name = 'clutch_scrapper_bots_canada'
email_subject = 'clutch Bot Alert'
email_toaddrs = ['ajay.qode@gmail.com', 'karan@qodemedia.net', 'jordan@qodemedia.com', 'bikin@nerdplatoon.com']
# email_toaddrs = []
logger = logging.getLogger(__name__)
scraper_dir = os.path.dirname(os.path.abspath(__file__))

def scrape_details(vehicles):
    for vehicle in vehicles:
        try:
            info = [None] * 27
            try:
                info[0] = str(datetime.date.today())
            except Exception:
                pass
            try:
                info[1] = None
            except KeyError:
                pass

            try:
                info[2] = vehicle["name"]
            except KeyError:
                pass
            # url
            try:
                info[3] = "https://www.clutch.ca/vehicles/" + str(vehicle["id"])
            except KeyError:
                pass
            # make
            try:
                info[4] = vehicle["make"]["name"]
            except KeyError:
                pass
            # model
            try:
                info[5] = vehicle["model"]["name"]
            except KeyError:
                pass
            # year
            try:
                info[6] = vehicle["year"]
            except KeyError:
                pass
            # mileage
            try:
                info[7] = vehicle["mileage"]
            except KeyError:
                pass
            # Price
            try:
                info[8] = vehicle["vehiclePrices"][0]["price"]
            except KeyError:
                info[8] = vehicle["vehiclePrice-NS"]["price"]
                # pass
            # state
            try:
                info[10] = vehicle["location"]["address"]["province"]
            except KeyError:
                pass
            # city
            try:
                info[11] = vehicle["location"]["address"]["city"]
            except :
                pass
            # condition
            try:
                info[9] = "Used"
            except KeyError:
                pass

            try:
                info[12] = vehicle['transmission']['name']
            except KeyError:
                pass

            try:
                info[13] = vehicle['drivetrain']['name']
            except KeyError:
                pass

            try:
                info[14] = vehicle['bodyStyle']['name']
            except KeyError:
                pass
            try:
                info[15] = vehicle['exteriorColor']['name']
            except KeyError:
                pass
            try:
                info[16] = vehicle['fuelType']['name']
            except KeyError:
                pass
            try:
                # trim
                if 'trim' in vehicle:
                    info[17] = vehicle["trim"]["name"]
            except KeyError:
                pass
            try:
                info[19] = vehicle['vin']
            except KeyError:
                pass
            try:
                info[18] = str(vehicle["cardPhotoUrl"])
            except KeyError:
                pass
            # info[20] = vehicle['vehicle']['OwnerCount']
            # info[21] = info_dict['vehicle']['AccidentCount']
            try:
                price_history_dict = vehicle["vehiclePrices"][0]['marketingPriceChange']
                info[22] = str(price_history_dict)
            except KeyError:
                pass
            try:
                info[23] = vehicle['vehicleDetail']['engineDisplacement']
            except KeyError:
                pass
            try:
                info[24] = vehicle['id']
            except KeyError:
                pass
            info[25] = str(vehicle['vehicleDetail'])
            # Carfax_url
            try:
                info[26] = vehicle['vehicleDetail']['carfaxReportUrl']
            except KeyError:
                pass
            new_info.append(info)
        except Exception as e:
            logger.info(f"error occured whicle getting vehicle details:{e}")


@handle_exceptions(logger_name, email_subject, email_toaddrs)
@retry_decorator.retry(requests.exceptions.ConnectionError, tries=3, delay=120)
def main():
    global new_info
    new_info = []
    aws_handler = AWSHandler()
    local_file_path = os.path.join(scraper_dir, "results/clutch_result.csv")
    aws_bucket_file_path = "MasterCode1/scraping/clutch/clutch_result.csv"
    aws_bucket_folder_path = "MasterCode1/scraping/clutch"
    if not os.path.exists(local_file_path):
        aws_handler.download_from_aws(aws_bucket_file_path, local_file_path)

    scrape_df = pd.read_csv(local_file_path)
    url = "https://api.clutch.ca/"
    url_to_get_locations = "https://api.clutch.ca/v1/locations"
    payload={}
    headers = {}
    response = requests.request("GET", url_to_get_locations, headers=headers, data=payload)
    response = json.loads(response.text)
    locations = []
    # Getting total location's id in website
    for result in response:
        locations.append(result["id"])

    # Get vehicle details with locations api
    for i in locations:
        url_with_location = url+"v1/vehicles/locations/"+str(i)
        response = requests.request("GET", url_with_location, headers=headers, data=payload)
        details = json.loads(response.text)
        total_count = details['totalCount']
        page_size = details["pageSize"]
        total_pages = total_count//page_size
        for j in range(total_pages):
            data_with_page_num_url = url+"v1/vehicles/locations/"+str(i)+"?page="+str(j)
            res = requests.request("GET", data_with_page_num_url, headers=headers, data=payload)
            vehicles = json.loads(res.text)
            vehicles = vehicles["vehicles"]
            scrape_details(vehicles)
            new_df = pd.DataFrame(new_info,
                                    columns=['date_added', 'date_removed', 'title', 'url', 'make', 'model', 'year',
                                            'kilometers', 'price', 'condition', 'province', 'City',
                                            'Vehicle_information.transmission', 'Vehicle_information.drivetrain',
                                            'Vehicle_information.body_style',
                                            'Vehicle_information.exterior_colour', 'Vehicle_information.fuel_type',
                                            'Vehicle_information.trim', 'img_url', 'vin', 'NumOwners',
                                            'PrevAccident', 'price_history', 'Vehicle_information.engine', '_id',
                                            'metadata', 'car_fax_url'])
            print("number:------------------------------",j)
            print(new_info)
            scrape_df = pd.concat([scrape_df, new_df])
            scrape_df = scrape_df.drop_duplicates(subset='url')
            scrape_df.to_csv(local_file_path, index=False)
            new_info = []
        time.sleep(2)

    #For uploading to aws we need to provide only the bucket folder path instead of the full path as it tends to make unnecessary folders
    aws_handler.upload_to_aws(local_file_path, aws_bucket_folder_path)
    # a.download_from_aws("MasterCodeUS/scraping/autolist/try.txt", "try_download.txt")


if __name__ == "__main__":
    main()
