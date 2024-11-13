import logging
import numpy as np
import pandas as pd

from datetime import date

from base import log_config
from base.db_connection import connect_database
from base.logging_decorator import handle_exceptions
from base.aws_handler import AWSHandler

logger_name = 'vinn_auto_scraper_bots_canada'
email_subject = 'Vinn Auto Bot Alert'
email_toaddrs = ['summit@qodemedia.com', 'prabin@qodemedia.net', 'karan@nerdplatoon.com']
logger = logging.getLogger("Model")


class CSVHandler:

    def __init__(self):
        self.db_connection, self.cursor = connect_database()

    def generate_df(self):
        try:
            print("Generating DF......")
            # columns_order = ['scraped_first_time', 'make', 'model', 'year', 'kilometers', 'price', 'province',
            #                  'transmission', 'drive_type', 'body_type', 'fuel_type', 'trim', 'scraped_month',
            #                  'scraped_year']
            sql_query = 'SELECT vinn_auto_url.url, vinn_auto_vehicle.* from vinn_auto_vehicle JOIN vinn_auto_url ON ' \
                        'vinn_auto_vehicle.url_id = vinn_auto_url.id'

            df = pd.read_sql(sql_query, self.db_connection)
            # df['scraped_month'] = df['scraped_at'].dt.month
            # df['scraped_year'] = df['scraped_at'].dt.year
            # df['scraped_first_time'] = df['scraped_at'].dt.date
            df['date_added'] = df['scraped_at']
            df['date_removed'] = np.nan
            df['City'] = df['city']
            df['img_url'] = df['img_url']
            df['metadata'] = np.nan
            df['NumOwners'] = np.nan
            df['PrevAccident'] = np.nan
            df['carfax_url'] = np.nan
            df['is_featured'] = np.nan
            df['price_history'] = df['price_history']
            df['Vehicle_information.engine'] = np.nan

            # print("Hereeeee ", type(str(df['scraped_at'].dt.date)))

            df.drop(['id', 'url_id', 'vehicle_id', 'is_available', 'images', 'scraped_at', 'created_at',
                    'updated_at', 'deleted_at'],
                    axis=1, inplace=True)
            column_dict = {'transmission': 'Vehicle_information.transmission',
                           'drive_train': 'Vehicle_information.drivetrain',
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
            df.to_csv(f'scrapers/vinn_auto/results/vinn_auto_{today}.csv', index=False)
            print("CSV Generated and uploading to AWS........")
            aws_handler = AWSHandler()
            aws_handler.upload_to_aws(f'scrapers/vinn_auto/results/vinn_auto_{today}.csv', 'MasterCode1/scraping/vinnauto')
            # aws_handler.upload_to_aws(f'MasterCode1/scraping/vinnauto/vinn_auto_result_{today}.csv', 'scrapers/vinn_auto/results')
            print("CSV uploaded to AWS")
        except Exception as e:
            logger.error("Error while generating CSV ", e)
            raise


@handle_exceptions(logger_name, email_subject, email_toaddrs)
def main():
    csv_handler = CSVHandler()
    csv_handler.generate_and_upload_csv()