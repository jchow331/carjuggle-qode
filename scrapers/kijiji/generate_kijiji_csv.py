import logging
import numpy as np
import pandas as pd

from datetime import date
from decouple import config

from base import log_config
from base.db_connection import connect_database
from base.logging_decorator import handle_exceptions
from base.aws_handler import AWSHandler

logger_name = 'kijiji_scraper_bots_canada'
email_subject = 'Kijiji Bot Alert'
email_toaddrs = ['karan@nerdplatoon.com', 'bikin@nerdplatoon.com']
logger = logging.getLogger("Kijiji")

db_user=config('SEARCH_ENGINE_DB_USER')
db_password=config('SEARCH_ENGINE_DB_PASSWORD')
db_host=config('SEARCH_ENGINE_DB_HOST')
db_port=config('SEARCH_ENGINE_DB_PORT', cast=int)
db_name=config('SEARCH_ENGINE_DB_NAME')


class CSVHandler:

    def __init__(self):
        self.db_connection, self.cursor = connect_database(user=db_user, password=db_password, host=db_host, port=db_port, database=db_name)

    def generate_df(self):
        try:
            print("Generating DF......")
            # columns_order = ['scraped_first_time', 'make', 'model', 'year', 'kilometers', 'price', 'province',
            #                  'transmission', 'drive_type', 'body_type', 'fuel_type', 'trim', 'scraped_month',
            #                  'scraped_year']
            sql_query = 'SELECT kijijiauto_kijijiautourl.url, kijijiauto_kijijiautovehicle.*,core_city.name as city, core_bodytype.name AS body_type, core_province.name as province,core_model.name as model, core_brand.name as make from kijijiauto_kijijiautovehicle JOIN kijijiauto_kijijiautourl ON ' \
                        'kijijiauto_kijijiautovehicle.link_id = kijijiauto_kijijiautourl.id join core_city ON core_city.id=kijijiauto_kijijiautovehicle.city_id join core_model ON core_model.id=kijijiauto_kijijiautovehicle.model_id join core_brand ON core_model.brand_id=core_brand.id join core_bodytype ON core_bodytype.id=kijijiauto_kijijiautovehicle.body_type_id join core_province on core_province.id=kijijiauto_kijijiautovehicle.province_id'

            df = pd.read_sql(sql_query, self.db_connection)
            # df['scraped_month'] = df['scraped_at'].dt.month
            # df['scraped_year'] = df['scraped_at'].dt.year
            # df['scraped_first_time'] = df['scraped_at'].dt.date
            df['date_added'] = df['posted_date']
            df['date_removed'] = df['deleted_at']
            df['City'] = df['city']
            df['img_url'] = df['thumbnail']
            df['metadata'] = np.nan
            df['NumOwners'] = np.nan
            df['PrevAccident'] = np.nan
            df['carfax_url'] = np.nan
            df['is_featured'] = np.nan
            df['price_history'] = df['price_track']
            df['Vehicle_information.engine'] = np.nan

            # print("Hereeeee ", type(str(df['scraped_at'].dt.date)))

            df.drop(['id', 'is_available', 'thumbnail', 'posted_date', 'created_at','city','link_id','body_type_id','model_id','province_id','city_id','engine','deleted_at'],
                    axis=1, inplace=True)
            column_dict = {'transmission': 'Vehicle_information.transmission',
                           'drivetrain': 'Vehicle_information.drivetrain',
                           'body_type': 'Vehicle_information.body_style',
                           'exterior_color': 'Vehicle_information.exterior_colour',
                           'fuel_type': 'Vehicle_information.fuel_type',
                           'trim': 'Vehicle_information.trim'}

            df.rename(columns=column_dict, inplace=True)

            print("DF Generated")
            return df
        except Exception as e:
            logger.error("Error while generating DF ", e)
            raise

    def generate_and_upload_csv(self):
        try:
            df = self.generate_df()
            print("Generating CSV........")
            today = date.today()
            df.to_csv(f'scrapers/kijiji/results/kijiji_{today}.csv', index=False)
            print("CSV Generated and uploading to AWS........")
            aws_handler = AWSHandler()
            aws_handler.upload_to_aws(f'scrapers/kijiji/results/kijiji_{today}.csv', 'MasterCode1/scraping/kijijiautos')
            # aws_handler.upload_to_aws(f'MasterCode1/scraping/vinnauto/vinn_auto_result_{today}.csv', 'scrapers/vinn_auto/results')
            print("CSV uploaded to AWS")
        except Exception as e:
            logger.error("Error while generating CSV ", e)
            raise


@handle_exceptions(logger_name, email_subject, email_toaddrs)
def main():
    csv_handler = CSVHandler()
    csv_handler.generate_and_upload_csv()