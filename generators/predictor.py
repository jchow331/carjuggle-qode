import pandas as pd
import numpy as np
from base.aws_handler import AWSHandler
from datetime import date
from dateutil.relativedelta import relativedelta

import warnings
warnings.filterwarnings('ignore')



# Necessary imports for logging
import logging
import os
from base import log_config     # DO NOT REMOVE: this might seem as if it is not used anywhere but still keep this import
from base.logging_decorator import handle_exceptions

# Set global variables for logger and emails (change this according to your needs)
logger_name = 'data_predictor_canada'
email_subject = 'Data Predicting Bot Alert'
email_toaddrs = ['sana@qodemedia.com']
logger = logging.getLogger(__name__)
scraper_dir = os.path.dirname(os.path.abspath(__file__))    # full path to the scraper dir where this script is placed



def get_data(file_destination):

    # downloading
    aws = AWSHandler()
    today = date.today()
    file_name ='price_prediction_graph.csv'
    file_path = file_destination + file_name
    df = aws.download_object_as_csv(file_path)

    logger.info(f'Downloaded {file_name} from {file_path}')
    logger.info(f'{df}')

    # formatting
    df['make']=df['make'].astype(str)
    df['model']=df['model'].astype(str)
    df['year']=df['year'].astype(int)
    df['date']=pd.to_datetime(df['date'])


    return df



def get_plot(year, make, model, kilometers, final_result):
    X=final_result[(final_result['year']==int(year))&(final_result['make']==str(make))&(final_result['model']==str(model))
         &(int(kilometers)<=final_result['mean']+1.25*final_result['std'])
         &(int(kilometers)>=final_result['mean']-1.25*final_result['std'])]

    for elt in X.index[::-1]:
        if str(X.loc[elt, 'B_MA'])=='nan':
            # logger.info(f'{X},{elt}')
            indx = list(X.index).index(elt)
            break
    
    if len(X)>0:
        logger.info(f'{X}')
        date = list(X.date[indx:])
        price = list(X.price[indx:])
        b_moving_average = list(X.B_MA[indx:])
        b_upper =  list(X.BU[indx:])
        b_lower =  list(X.BL[indx:])

        logger.info(f'{date},{price}, {b_moving_average}, {b_upper}, {b_lower}')
        
        return date, price, b_moving_average, b_upper, b_lower
    else:
        return 'Not enough data'




def main():
    global df

    df = get_data('MasterCode1/cleaning/forecasting/')
    # date, price, b_moving_average, b_upper, b_lower = get_plot(2018, 'Toyota', 'Rav4', 67000, df)
    date, price, b_moving_average, b_upper, b_lower = get_plot(2022, 'Dodge', 'Durango', 6000, df)

    

    


if __name__ == '__main__':

    main()