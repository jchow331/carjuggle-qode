import datetime
import json
import logging
import math
import multiprocessing
import pandas as pd
import random
import requests
import time

from base import log_config, retry_decorator
from base.db_connection import connect_database
from base.logging_decorator import handle_exceptions

logger_name = 'vinn_auto_scraper_bots_canada'
email_subject = 'Vinn Auto Bot Alert'
email_toaddrs = ['summit@qodemedia.com', 'prabin@qodemedia.net', 'karan@qodemedia.net']
# email_toaddrs = []
logger = logging.getLogger(__name__)


class VehicleScrapper:

    def __init__(self):
        self.db_connection, self.cursor = connect_database()
        self.mapping_data = json.load(open("scrapers/vinn_auto/attributes.json"))

    def insert_new_vehicle(self, vehicle_data, url_id, vehicle_id, db_conn, db_conn_cursor):
        try:
            sql = "SELECT * FROM vinn_auto_vehicle WHERE vehicle_id = %s"
            self.cursor.execute(sql, (vehicle_id,))
            if self.cursor.fetchone() is not None:
                return
            sql = """INSERT INTO vinn_auto_vehicle(url_id,
                                                vehicle_id,
                                                year,
                                                make,
                                                model,
                                                price,
                                                kilometers,
                                                condition,
                                                trim,
                                                transmission,
                                                is_available,
                                                vin,
                                                body_type,
                                                province,
                                                city,
                                                img_url,
                                                drive_train,
                                                exterior_color,
                                                fuel_type,
                                                images,
                                                price_history,
                                                scraped_at                                                
                                                )
                    VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""

            db_conn_cursor.execute(sql, vehicle_data)
            # self.db_connection.commit()
            self.update_url_table(url_id, db_conn_cursor)
            db_conn.commit()
        except Exception as e:
            logger.error(f"Error while inserting data to Vehicle Detail table {e}")
            db_conn.rollback()
            raise

    def update_url_table(self, url_id, db_conn_cursor):
        sql_update_query = """Update vinn_auto_url set is_created = %s where id = %s"""
        db_conn_cursor.execute(sql_update_query, (True, url_id))

    def get_new_urls(self):
        try:
            sql = "SELECT id, vehicle_id, url, meta FROM vinn_auto_url WHERE is_created = %s and meta is not null"
            self.cursor.execute(sql, (False,))
            all_urls = self.cursor.fetchall()
            return all_urls
        except Exception as e:
            logger.error(f"Error while checking url exist in DB {e}")
            raise

    @retry_decorator.retry(requests.exceptions.ConnectionError, tries=3, delay=360)
    def fetch_vehicle_data(self, total_processor, index, total_vehicle_urls):
        try:
            db_conn, db_conn_cursor = connect_database()
            total_vehicle_urls_count = len(total_vehicle_urls)
            processor_page_list = math.floor(total_vehicle_urls_count / total_processor)
            start_number = index * processor_page_list
            end_number = start_number + processor_page_list
            if index + 1 == total_processor:
                end_number = total_vehicle_urls_count

            for i in range(start_number, end_number):
                url_id = total_vehicle_urls[i][0]
                vehicle_id = total_vehicle_urls[i][1]
                vehicle_details = total_vehicle_urls[i][3]
                if vehicle_details:
                    res = requests.get(f"https://www.vinnauto.com/api/v1/vehicles/{vehicle_id}").json()
                    img_url = vehicle_details['img_url']
                    photos = vehicle_details['img_url']
                    # img_url = None
                    # if 'photo_count' in vehicle and vehicle['photo_count'] > 0:
                    # if len(vehicle_details['img_url']) > 1:
                    #     # photos_df = pd.DataFrame(vehicle_details['img_url'])
                    #     photos = vehicle_details['img_url'].to_dict()
                    #     img_url = vehicle_details['img_url'][0]
                    dt = res['scraped_at'].replace(" UTC", "")
                    price_history = {dt: res['price']}
                    province, city ='', ''
                    if vehicle_details['seller_details']['address']:
                        province = vehicle_details['seller_details']['address']['province']
                        city = vehicle_details['seller_details']['address']['city']
                    extra_details = self.get_extra_details(res)
                    vehicle_data = (url_id,
                                    vehicle_id,
                                    vehicle_details['year'],
                                    vehicle_details['make'],
                                    vehicle_details['model'],
                                    vehicle_details['price'],
                                    vehicle_details['kilometers'],
                                    vehicle_details['status'],
                                    res['trim'],
                                    extra_details['transmission'],
                                    res['available'],
                                    res['vin'],
                                    extra_details['body_type'],
                                    province,
                                    city,
                                    img_url,
                                    extra_details['drive_type'],
                                    extra_details['exterior_colour'],
                                    extra_details['fuel_type'],
                                    json.dumps(photos),
                                    json.dumps(price_history),
                                    res['scraped_at'],
                                    )
                    self.insert_new_vehicle(vehicle_data, url_id, vehicle_id, db_conn, db_conn_cursor)
                    time.sleep(random.randrange(2, 10))
        except Exception as e:
            logger.error(f'Error while fetching Vehicle Data: {e}')
            raise

    def get_extra_details(self, vehicle):
        details = {}
        details['body_type'], details['transmission'], details['fuel_type'], details['exterior_colour'], details['drive_type'] = '', '', '', '', ''
        for data in self.mapping_data['body_types']:
            if vehicle['body_type_id'] is not None and vehicle['body_type_id'] == data['id']:
                details['body_type'] = data['name']
                break
        for data in self.mapping_data['transmissions']:
            if vehicle['transmission_id'] is not None and vehicle['transmission_id'] == data['id']:
                details['transmission'] = data['name']
                break
        for data in self.mapping_data['fuel_types']:
            if vehicle['fuel_type_id'] is not None and vehicle['fuel_type_id'] == data['id']:
                details['fuel_type'] = data['name']
                break
        for data in self.mapping_data['colours']:
            if vehicle['exterior_colour_id'] is not None and vehicle['exterior_colour_id'] == data['id']:
                details['exterior_colour'] = data['name']
                break
        for data in self.mapping_data['drive_types']:
            if vehicle['drive_type_id'] is not None and vehicle['drive_type_id'] == data['id']:
                details['drive_type'] = data['name']
                break
        # details['body_type'] = next(data['name'] for data in self.mapping_data['body_types'] if data['id'] == vehicle['body_type_id'])
        # details['transmission'] = next(data['name'] for data in self.mapping_data['transmissions'] if data['id'] == vehicle['transmission_id'])
        # details['fuel_type'] = next(data['name'] for data in self.mapping_data['fuel_types'] if data['id'] == vehicle['fuel_type_id'])
        # details['color'] = next(data['name'] for data in self.mapping_data['colours'] if data['id'] == vehicle['exterior_colour_id'])
        # details['drive_type'] = next(data['name'] for data in self.mapping_data['drive_types'] if data['id'] == vehicle['drive_type_id'])
        return details

    def index(self):
        try:
            total_vehicle_urls = self.get_new_urls()
            total_vehicle_urls_count = len(total_vehicle_urls)
            total_processor = multiprocessing.cpu_count()

            if total_processor > 2:
                total_processor -= 1

            if total_vehicle_urls_count < total_processor:
                total_processor = total_vehicle_urls_count

            for i in range(total_processor):
                vars()[f"process_{i}"] = multiprocessing.Process(target=self.fetch_vehicle_data,
                                                                 args=(total_processor, i, total_vehicle_urls))

            for i in range(total_processor):
                vars()[f"process_{i}"].start()

            for i in range(total_processor):
                vars()[f"process_{i}"].join()

            print("All processes finished execution!")

        except Exception as e:
            logger.error(f'Error while fetching URL: {e}')
            raise


@handle_exceptions(logger_name, email_subject, email_toaddrs)
def main():
    vehicle_scraper = VehicleScrapper()
    vehicle_scraper.index()
