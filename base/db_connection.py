import logging
import psycopg2

from base import log_config
from decouple import config
from psycopg2 import Error
logger = logging.getLogger(__name__)

db_user=config('DB_USER')
db_password=config('DB_PASSWORD')
db_host=config('DB_HOST')
db_port=config('DB_PORT', cast=int)
db_name=config('DB_NAME')


def connect_database(autocommit=False, user=db_user, password=db_password, host=db_host, port=db_port, database=db_name):
    try:
        db_connection = psycopg2.connect(user=user,
                                         password=password,
                                         host=host,
                                         port=port,
                                         database=database)
        db_connection.autocommit = autocommit
        cursor = db_connection.cursor()
        return db_connection, cursor
    except (Exception, Error) as error:
        logger.error(f"Error while connecting to PostgresSQL {error}")
        raise
