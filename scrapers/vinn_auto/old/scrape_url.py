import logging
import urllib3
from base import log_config, retry_decorator
import math
import multiprocessing
import random
import requests
import time
import json

from base.db_connection import connect_database
from base.logging_decorator import handle_exceptions

logger_name = 'vinn_auto_scraper_bots_canada'
email_subject = 'Vinn Auto Bot Alert'
email_toaddrs = ['summit@qodemedia.com', 'prabin@qodemedia.net', 'karan@qodemedia.net']
logger = logging.getLogger(__name__)


class UrlScrapper:

    def __init__(self):
        self.api_url = "https://api.vinnauto.com/v1/pages/inventory/"

    # def insert_new_url(self, url):
    #     try:
    #         sql = "INSERT INTO vinn_auto_url(vehicle_id, url, is_created) VALUES(%s, %s, %s)"
    #
    #         self.cursor.execute(sql, (url, True))
    #         self.db_connection.commit()
    #     except Exception as e:
    #         logger.error(f"Error while inserting data to Url table {e}")
    #         raise

    def insert_multiple_urls(self, urls, db_conn_cursor):
        try:
            sql = "INSERT INTO vinn_auto_url(vehicle_id, url, is_created, meta) VALUES(%s, %s, %s, %s)"

            db_conn_cursor.executemany(sql, urls)
            print("inserted")
        except Exception as e:
            logger.error(f"Error while inserting data to Url table {e}")
            # raise

    def check_url_exist(self, vehicle_id, db_conn_cursor):
        try:
            sql = "SELECT * FROM vinn_auto_url WHERE vehicle_id = %s"
            db_conn_cursor.execute(sql, (vehicle_id,))
            data = db_conn_cursor.fetchone()
            if data is not None:
                return True

            return False
        except Exception as e:
            logger.error(f"Error while checking url exist in DB {e}")
            # raise

    def total_vehicle_count(self):
        try:
            response = requests.get(f"{self.api_url}/count")
            total_vehicles = response.json()['total_count']
            return total_vehicles
        except Exception as e:
            logger.error(f'Error in Vehicle Count API: {e}')
            raise

    @retry_decorator.retry(urllib3.connection.HTTPSConnection, tries=1, delay=60)
    def fetch_urls(self, total_processor, index, total_pages):
        db_conn, db_conn_cursor = connect_database(True)
        processor_page_list = math.floor(total_pages / total_processor)
        start_number = index * processor_page_list
        end_number = start_number + processor_page_list
        if index+1 == total_processor:
            end_number = total_pages
        f = open(f"scrapers/vinn_auto/trackers/page_{index}.txt", "a")
        for i in range(start_number, end_number):
            data = []
            res = requests.get(f"{self.api_url}?view=inventory&page={i}&country=CA").json()
            for vehicle in res['data']:
                vehicle_details = vehicle['vehicle']
                vehicle_id = vehicle_details['id']
                vehicle_url = f"https://www.vinnauto.com/car/{str(vehicle_details['year'])}/{str(vehicle_details['model']['name'])}/" \
                              f"{str(vehicle_details['model']['make']['name'])}/{str(vehicle['address']['city'])}/" \
                              f"{str(vehicle_id)}"
                if not self.check_url_exist(vehicle_id, db_conn_cursor):
                    main_image = ''
                    if vehicle_details['main_photo']:
                        main_image = vehicle_details['main_photo']['original_url'] if 'original_url' in vehicle_details['main_photo'] else ''
                    meta_data = {'year':vehicle_details['year'], 'make':vehicle_details['model']['make']['name'],
                                    'model':vehicle_details['model']['name'], 'price':vehicle['price'], 'kilometers':vehicle['kilometers'],
                                    'status':vehicle_details['condition'], 'img_url' : main_image,
                                    'seller_details': vehicle['seller']
                    }
                    data.append((vehicle_id, vehicle_url, False, json.dumps(meta_data)))
            f.write(str(i)+"\n")
            if len(data) > 0:
                self.insert_multiple_urls(data, db_conn_cursor)
            time.sleep(random.randrange(2, 10))
        f.close()

    def index(self):
        try:
            total_vehicles = self.total_vehicle_count()
            total_pages = math.ceil(total_vehicles / 10)

            total_processor = multiprocessing.cpu_count()
            # self.fetch_urls(total_pages)
            if total_processor > 2:
                total_processor -= 1

            for i in range(total_processor):
                vars()[f"process_{i}"] = multiprocessing.Process(target=self.fetch_urls, args=(total_processor, i,
                                                                                               total_pages))

            for i in range(total_processor):
                vars()[f"process_{i}"].start()

            for i in range(total_processor):
                vars()[f"process_{i}"].join()

            print("All processes finished execution!")

            return total_pages
        except Exception as e:
            logger.error(f'Error while fetching URL: {e}')
            raise


@handle_exceptions(logger_name, email_subject, email_toaddrs)
def main():
    url_scraper = UrlScrapper()
    url_scraper.index()
