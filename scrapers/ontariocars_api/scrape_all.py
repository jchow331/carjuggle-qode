import datetime
import logging
import os
from pprint import pprint
import time
import json
import re

import requests
import pandas as pd

from base import log_config
from base import retry_decorator
from base.aws_handler import AWSHandler
from base.logging_decorator import handle_exceptions

logger_name = 'Ontariocars_bots_canada'
email_subject = 'Ontariocars Bot Alert'
email_toaddrs = ['ajay.qode@gmail.com', 'bikin@nerdplatoon.com', 'karan@qodemedia.net', 'jordan@qodemedia.com']
email_toaddrs = ['']
logger = logging.getLogger(__name__)
scraper_dir = os.path.dirname(os.path.abspath(__file__))

def scrape_details(vehicles):
    for vehicle in vehicles:
        print(vehicle["Make"])
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
                info[2] = " ".join([str(vehicle["Year"]), vehicle["Make"], vehicle["Model"], vehicle["Trim"]])
            except KeyError:
                pass
            # url
            try:
                model = vehicle["Model"].replace(' ','-').replace('.','')
                trim = vehicle["Trim"].replace(' ','-').replace('.','')
                url = "https://www.ontariocars.ca/{}/{}/{}/{}/{}?VehicleTypeCategory=0&VehicleCondition=1&dealerId={}&page=1".format(vehicle["Make"].lower(),model.lower(),trim.lower(),vehicle["Year"],vehicle['Id'],vehicle['DealerId'])
                info[3] = url
            except KeyError:
                pass
            # make
            try:
                info[4] = vehicle["Make"]
            except KeyError:
                pass
            # model
            try:
                info[5] = vehicle["Model"]
            except KeyError:
                pass
            # year
            try:
                info[6] = vehicle["Year"]
            except KeyError:
                pass
            # mileage
            try:
                info[7] = vehicle["MileageInKm"]
            except KeyError:
                pass
            # Price
            try:
                info[8] = vehicle["RetailPrice"]
            except KeyError:
                pass
            # state
            try:
                info[10] = vehicle["Location"]["Province"]["Name"]
            except KeyError:
                pass
            # city
            try:
                info[11] = vehicle["Location"]["City"]["Name"]
            except :
                pass
            # condition
            try:
                info[9] = "Used"
            except KeyError:
                pass

            try:
                info[12] = vehicle['Transmission']
            except KeyError:
                pass

            try:
                info[13] = vehicle['DriveTrain']
            except KeyError:
                pass

            try:
                info[14] = vehicle['BodyStyle']
            except KeyError:
                pass
            try:
                info[15] = vehicle['ExteriorColor']
            except KeyError:
                pass
            try:
                info[16] = vehicle['FuelType']
            except KeyError:
                pass
            try:
                # trim
                if 'Trim' in vehicle:
                    info[17] = vehicle["Trim"]
            except KeyError:
                pass
            try:
                info[19] = vehicle['VehicleIdentificationNumber']
            except KeyError:
                pass
            try:
                info[18] = "https://dealer.ontariocars.ca" + str(vehicle["DefaultPhotoUrl"])
            except KeyError:
                pass
            # info[20] = vehicle['vehicle']['OwnerCount']
            # info[21] = info_dict['vehicle']['AccidentCount']
            try:
                price_history_dict = ''
                info[22] = str(price_history_dict)
            except KeyError:
                pass
            try:
                info[23] = ""
            except KeyError:
                pass
            try:
                info[24] = vehicle['Id']
            except KeyError:
                pass
            info[25] = ''
            # Carfax_url
            try:
                info[26] = vehicle['CarfaxLink']
            except KeyError:
                pass
            new_info.append(info)
        except Exception as e:
            logger.info(f"error occured on getting vehicle data------ {e}")


@handle_exceptions(logger_name, email_subject, email_toaddrs)
@retry_decorator.retry(requests.exceptions.ConnectionError, tries=3, delay=540)
def main():
    global new_info
    new_info = []
    aws_handler = AWSHandler()
    local_file_path = os.path.join(scraper_dir, "results/ontariocars_result.csv")
    aws_bucket_file_path = "MasterCode1/scraping/ontariocars/ontariocars_result.csv"
    aws_bucket_folder_path = "MasterCode1/scraping/ontariocars"
    if not os.path.exists(local_file_path):
        aws_handler.download_from_aws(aws_bucket_file_path, local_file_path)

    scrape_df = pd.read_csv(local_file_path)
    url = "https://www.ontariocars.ca/umbraco/api/vehicleapi/search"

    payload='Page=1&PageSize=50'
    headers = {
    'Content-Type': 'application/x-www-form-urlencoded'
    }

    response = requests.request("POST", url, headers=headers, data=payload)
    response = json.loads(response.text)
    total_vehicles = response["TotalCount"]
    count_per_page = 50
    total_pages = total_vehicles//count_per_page
    # exit()
    # Get vehicle details with locations api
    for i in range(1,total_pages+2):
        # print("page-----{}---------------------------------------------page number----{}",total_pages,i)
        payload="Page={pg_num}&PageSize={pg_count}".format(pg_num=i, pg_count=count_per_page)
        response = requests.request("POST", url, headers=headers, data=payload)
        details = json.loads(response.text)
        vehicles = details["Result"]
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
        scrape_df = pd.concat([scrape_df, new_df])
        scrape_df = scrape_df.drop_duplicates(subset='url')
        scrape_df.to_csv(local_file_path, index=False)
        new_info = []
        time.sleep(2)
        # exit()

    #For uploading to aws we need to provide only the bucket folder path instead of the full path as it tends to make unnecessary folders
    aws_handler.upload_to_aws(local_file_path, aws_bucket_folder_path)
    # a.download_from_aws("MasterCodeUS/scraping/autolist/try.txt", "try_download.txt")


if __name__ == "__main__":
    main()
