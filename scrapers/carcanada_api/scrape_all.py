import datetime
import logging
import os
import time
import json
import re

import requests
import pandas as pd

from base import log_config
from base import retry_decorator
from base.aws_handler import AWSHandler
from base.logging_decorator import handle_exceptions

logger_name = 'Carcanada_bots_canada'
email_subject = 'Carcanada Bot Alert'
email_toaddrs = ['ajay.qode@gmail.com', 'summit@qodemedia.com', 'karan@qodemedia.net', 'jordan@qodemedia.com']
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
                info[2] = " ".join([str(vehicle["year"]), vehicle["make"], vehicle["model"], vehicle["trim"]])
            except KeyError:
                pass
            # url
            try:
                url = vehicle["vdp_url"]
                info[3] = url
            except KeyError:
                pass
            # make
            try:
                info[4] = vehicle["make"]
            except KeyError:
                pass
            # model
            try:
                info[5] = vehicle["model"]
            except KeyError:
                pass
            # year
            try:
                info[6] = vehicle["year"]
            except KeyError:
                pass
            # mileage
            try:
                info[7] = vehicle["odometer"]
            except KeyError:
                pass
            # Price
            try:
                info[8] = vehicle["asking_price"]
            except KeyError:
                info[8] = vehicle["internet_price"]
            # state
            try:
                info[10] = vehicle["company_data"]["company_province"]
            except KeyError:
                pass
            # city
            try:
                info[11] = vehicle["company_data"]["company_city"]
            except :
                pass
            # condition
            try:
                info[9] = vehicle["sale_class"]
            except KeyError:
                pass

            try:
                info[12] = vehicle['transmission']
            except KeyError:
                pass

            try:
                info[13] = vehicle['drive_train']
            except KeyError:
                pass

            try:
                info[14] = vehicle['body_style']
            except KeyError:
                pass
            try:
                info[15] = vehicle['exterior_color']
            except KeyError:
                pass
            try:
                info[16] = vehicle['fuel_type']
            except KeyError:
                pass
            try:
                # trim
                if 'trim' in vehicle:
                    info[17] = vehicle["trim"]
            except KeyError:
                pass
            try:
                info[19] = vehicle['vin']
            except KeyError:
                pass
            try:
                info[18] = str(vehicle["image"])
            except KeyError:
                pass
            # info[20] = vehicle['vehicle']['OwnerCount']
            # info[21] = info_dict['vehicle']['AccidentCount']
            try:
                price_history_dict = vehicle["initial_price"]
                info[22] = str(price_history_dict)
            except KeyError:
                pass
            try:
                info[23] = vehicle['engine']
            except KeyError:
                pass
            try:
                info[24] = vehicle['ad_id']
            except KeyError:
                pass
            info[25] = ''
            # Carfax_url
            try:
                info[26] = ''
            except KeyError:
                pass
            new_info.append(info)
        except Exception as e:
            logger.info(f"error occured on getting vehicle data------ {e}")


@handle_exceptions(logger_name, email_subject, email_toaddrs)
@retry_decorator.retry(requests.exceptions.ConnectionError, tries=3, delay=120)
def main():
    global new_info
    new_info = []
    aws_handler = AWSHandler()
    local_file_path = os.path.join(scraper_dir, "results/carcanada_result.csv")
    aws_bucket_file_path = "MasterCode1/scraping/carcanada/carcanada_result.csv"
    aws_bucket_folder_path = "MasterCode1/scraping/carcanada"
    if not os.path.exists(local_file_path):
        aws_handler.download_from_aws(aws_bucket_file_path, local_file_path)

    scrape_df = pd.read_csv(local_file_path)
    url = "https://www.carcanada.com/wp-content/plugins/convertus-vms/include/php/ajax-vehicles.php?endpoint=https://vms.prod.convertus.rocks/api/filtering/?cp=342&ln=en&pg=1&pc=15&dc=false&qs=&im=&svs=&sc=&v1=&st=&ai=&oem=&view=grid&in_transit=true&in_stock=true&on_order=true&SrpTemplateParams=%5Bobject%20Object%5D&defaultParams=&action=vms_data"

    payload={}
    headers = {}
    response = requests.request("GET", url, headers=headers, data=payload)
    response = json.loads(response.text)
    total_vehicles = response["summary"]["total_vehicles"]
    count_per_page = 50
    total_pages = total_vehicles//count_per_page
    # Get vehicle details with locations api
    for i in range(1,total_pages+2):
        # url = "https://www.carcanada.com/wp-content/plugins/convertus-vms/include/php/ajax-vehicles.php?endpoint=https://vms.prod.convertus.rocks/api/filtering/?cp=342&ln=en&pg={pg_num}&pc={pg_count}&dc=false&qs=&im=&svs=&sc=&v1=&st=&ai=&oem=&view=grid&in_transit=true&in_stock=true&on_order=true&SrpTemplateParams=%5Bobject%20Object%5D&defaultParams=&action=vms_data".format(pg_num=i, pg_count=count_per_page)
        url_to_scrape = "https://vms.prod.convertus.rocks/api/filtering/?cp=342&ln=en&pg={pg_num}&pc={pg_count}&dc=false&qs=&im=&svs=&sc=all&v1=Passenger%20Vehicles&st=&ai=&oem=&mk=&md=&tr=&bs=&tm=&dt=&cy=&ec=&mc=&ic=&pa=&ft=&eg=&v2=&v3=&fp=&fc=&fn=&tg=".format(pg_num=i, pg_count=count_per_page)
        response = requests.request("GET", url_to_scrape, headers=headers, data=payload)
        details = json.loads(response.text)
        vehicles = details["results"]
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
        scrape_df = scrape_df.drop_duplicates(subset='_id')
        scrape_df.to_csv(local_file_path, index=False)
        new_info = []
        # time.sleep(2)

    #For uploading to aws we need to provide only the bucket folder path instead of the full path as it tends to make unnecessary folders
    aws_handler.upload_to_aws(local_file_path, aws_bucket_folder_path)
    # a.download_from_aws("MasterCodeUS/scraping/autolist/try.txt", "try_download.txt")


if __name__ == "__main__":
    main()
