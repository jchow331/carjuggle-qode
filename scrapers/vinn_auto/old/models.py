import logging
import psycopg2
import os
import sys

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, BASE_DIR)

from base import log_config
from decouple import config
from psycopg2 import Error
from base.logging_decorator import handle_exceptions

logger_name = 'vinn_auto_scraper_bots_canada'
email_subject = 'Vinn Auto Bot Alert'
email_toaddrs = ['summit@qodemedia.com', 'prabin@qodemedia.net']
logger = logging.getLogger("Model")


class AbstractDBConnectionModel:

    def custom_query(self, db_connection, cursor):
        url_sql = '''CREATE TABLE VINN_AUTO_URL(
                ID uuid DEFAULT uuid_generate_v4 () UNIQUE,
                VEHICLE_ID INT NOT NULL UNIQUE,
                URL CHAR(255) NOT NULL,
                META JSON,
                IS_CREATED BOOLEAN NOT NULL,
                CREATED_AT TIMESTAMP WITH TIME ZONE DEFAULT (current_timestamp AT TIME ZONE 'UTC'),
                UPDATED_AT TIMESTAMP WITH TIME ZONE DEFAULT (current_timestamp AT TIME ZONE 'UTC'),
                DELETED_AT TIMESTAMP WITH TIME ZONE
            )'''
        cursor.execute(url_sql)
        print("VINN AUTO URL Table created successfully........")
        db_connection.commit()

        vehicle_sql = '''CREATE TABLE VINN_AUTO_VEHICLE(
                ID uuid DEFAULT uuid_generate_v4 () UNIQUE,
                URL_ID uuid,
                CONSTRAINT FK_VEHICLE_URL FOREIGN KEY(URL_ID) REFERENCES VINN_AUTO_URL(id) ON DELETE SET NULL,
                VEHICLE_ID INT NOT NULL UNIQUE,
                YEAR NUMERIC(4,0) NOT NULL,
                MAKE CHAR(255) NOT NULL,
                MODEL CHAR(255) NOT NULL,
                PRICE NUMERIC(10,2) DEFAULT 0,
                KILOMETERS INT DEFAULT 0,
                CONDITION CHAR(15),
                TRIM CHAR(255),
                TRANSMISSION CHAR(255),
                IS_AVAILABLE BOOLEAN DEFAULT TRUE,
                VIN CHAR(20),
                BODY_TYPE CHAR(50),
                PROVINCE CHAR(100) NOT NULL,
                CITY CHAR(255),
                IMG_URL TEXT,
                DRIVE_TRAIN CHAR(255),
                EXTERIOR_COLOR CHAR(50),
                FUEL_TYPE CHAR(20),
                IMAGES JSON,
                PRICE_HISTORY JSON,
                SCRAPED_AT TIMESTAMP WITH TIME ZONE DEFAULT (current_timestamp AT TIME ZONE 'UTC'),
                CREATED_AT TIMESTAMP WITH TIME ZONE DEFAULT (current_timestamp AT TIME ZONE 'UTC'),
                UPDATED_AT TIMESTAMP WITH TIME ZONE DEFAULT (current_timestamp AT TIME ZONE 'UTC'),
                DELETED_AT TIMESTAMP WITH TIME ZONE
            )'''
        cursor.execute(vehicle_sql)
        print("VINN AUTO VEHICLE Table created successfully........")

        db_connection.commit()
        db_connection.close()

    def connect_database(self):
        try:
            db_connection = psycopg2.connect(user=config('DB_USER'),
                                             password=config('DB_PASSWORD'),
                                             host=config('DB_HOST'),
                                             port=config('DB_PORT'),
                                             database=config('DB_NAME'))
            cursor = db_connection.cursor()
            self.custom_query(db_connection, cursor)

        except (Exception, Error) as error:
            logger.error(f'Error while connecting to PostgresSQL: {error}')
            raise


@handle_exceptions(logger_name, email_subject, email_toaddrs)
def main():
    db_conn = AbstractDBConnectionModel()
    db_conn.connect_database()


if __name__ == "__main__":
    main()
