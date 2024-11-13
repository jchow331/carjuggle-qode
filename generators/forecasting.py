import logging
import os
import warnings
import swifter
from datetime import date
from dateutil.relativedelta import relativedelta

import pandas as pd
import numpy as np

# Necessary imports for logging
from base.aws_handler import AWSHandler
from base import log_config     # DO NOT REMOVE: this might seem as if it is not used anywhere but still keep this import
from base.logging_decorator import handle_exceptions

warnings.filterwarnings('ignore')

# Set global variables for logger and emails (change this according to your needs)
logger_name = 'data_forecasting_canada'
email_subject = 'Data Forecasting Bot Alert'
email_toaddrs = ['sana@qodemedia.com']
logger = logging.getLogger(__name__)
scraper_dir = os.path.dirname(os.path.abspath(__file__))    # full path to the scraper dir where this script is placed



def get_data(file_destination):

    # downloading
    aws = AWSHandler()
    today = date.today()
    file_name ='AllData_new_used'+'-'+str(today.month)+'-'+str(today.year)+'.csv'
    file_path = file_destination + file_name
    df = aws.download_object_as_csv(file_path)

    logger.info(f'Downloaded {file_name} from {file_path}')
    logger.info(f'{df}')

    # formatting
    df['Vehicle_information.trim']=df['Vehicle_information.trim'].astype(str)
    df['make']=df['make'].astype(str)
    df['model']=df['model'].astype(str)
    df['year']=df['year'].astype(int)
    df['kilometers']=df['kilometers'].astype(int)
    df['scraped_first_time']=pd.to_datetime(df['scraped_first_time'])
    df['scrape_month']=df['scraped_first_time'].dt.month
    df['scrape_year']=df['scraped_first_time'].dt.year

    return df



def data_groupping(nbre_months, df):

    group_year = df.groupby(['year'])['kilometers'].agg(['mean', 'std'])

    group_mean = df.groupby(['year' ,'make', 'model']).count()
    group_mean.reset_index(inplace=True, drop=False)
    group_mean.rename(columns={'scraped_first_time':'count'}, inplace=True)
    group_mean = group_mean[['year', 'make','model', 'count']]
    

    group_mean = pd.merge(group_mean, group_year, left_on='year', right_on= group_year.index)



    today = date.today()
    last = (pd.to_datetime(today, format='%Y-%m-%d') - relativedelta(months=nbre_months))

    listofmonths = pd.date_range(start=last, end=today, freq='MS') 
    time_data=pd.DataFrame(listofmonths, columns=['date'])
    time_data['scrape_month']=time_data['date'].dt.month
    time_data['scrape_year']=time_data['date'].dt.year


    group_mean['key'] = 1
    time_data['key'] = 1
    result = pd.merge(group_mean, time_data, on='key')

    return result



def calculate_average_price(result_row):

    result_year = result_row[0]
    result_make = result_row[1]
    result_scrape_month = result_row[2]
    result_scrape_year = result_row[3]
    result_kil_mean = result_row[4]
    result_kil_std = result_row[5]
    result_model = result_row[6]
    
    X=df[(df['year']==result_year)&(df['make']==result_make)&(df['model']==result_model)
        &(df['scrape_month']==result_scrape_month)&(df['scrape_year']==result_scrape_year)
        &(df['kilometers']<=result_kil_mean+1.25*result_kil_std)&(df['kilometers']>=result_kil_mean-1.25*result_kil_std)]
    mean_price = X['price'].median()
    
    if len(X)<=3:
        flag = 1
    else:
        flag = 0
    
    return mean_price, flag





def bollinger_bands(dataframe, n, m):
    # takes dataframe on input
    # n = smoothing length, size of the moving window
    # m = number of standard deviations away from MA
    
    #typical price
    data = dataframe['price']

    
    # takes one column from dataframe
    B_MA = pd.Series((data.rolling(n, min_periods=n-1).mean()), name='B_MA')
    sigma = data.rolling(n, min_periods=n-1).std() 
    
    BU = pd.Series((B_MA + m * sigma), name='BU')
    BL = pd.Series((B_MA - m * sigma), name='BL')
    
    dataframe = dataframe.join(B_MA)
    dataframe = dataframe.join(BU)
    dataframe = dataframe.join(BL)
    
    return dataframe



def generate_results(result, observations, mandatory_months):

    cycle_month_min=result.loc[0,'scrape_month']
    cycle_year_min=result.loc[0,'scrape_year']

    indices = [i for i, x in enumerate(list(result.loc[:, ['scrape_month', 'scrape_year']].values)) if ((x[0] == cycle_month_min) and (x[1] ==cycle_year_min))]
    
    final_result=pd.DataFrame()
    for i in indices:
        
        Y = pd.DataFrame(result.loc[i:i+observations-1, ['year','make','model', 'mean', 'std','price','flag', 'mandatory']].values, index=(result.loc[i:i+observations-1, ['date']]).date, columns=['year','make','model', 'mean', 'std','price','flag', 'mandatory'])
        Y.index = pd.to_datetime(Y.index)
        # Z=Y.copy().dropna()
        Z=Y.copy()
        # Y.drop('flag', axis=1, inplace=True)
        Y.drop(['flag', 'mandatory'], axis=1, inplace=True)
        
    #     print(Z['flag'].sum())
        # if len(Z)>=observations and Z['flag'].sum()==0 :
        if (Z['mandatory']==True).sum()==mandatory_months:
            if Z.loc[Z[Z['mandatory']==True].index, 'flag'].sum()==0:

                last_month = (pd.to_datetime(Y.index[-1], format='%Y-%m-%d') + relativedelta(months=1))
                Z.loc[pd.to_datetime(last_month), 'price']=np.nan
                Z.loc[pd.to_datetime(last_month), 'year'] = result.loc[i, 'year']
                Z.loc[pd.to_datetime(last_month), 'make'] = result.loc[i, 'make']
                Z.loc[pd.to_datetime(last_month), 'model'] = result.loc[i, 'model']
                Z.loc[pd.to_datetime(last_month), 'mean'] = result.loc[i, 'mean']
                Z.loc[pd.to_datetime(last_month), 'std'] = result.loc[i, 'std']
                Z.drop('flag', axis=1, inplace=True)
                
                Z = bollinger_bands(Z, 3, 1.25)
                final_result = pd.concat([final_result, Z])

    final_result['date'] = final_result.index
    final_result.index = range(len(final_result))

    return final_result


def upload_data(mydf, file_destination):

    aws = AWSHandler()

    # today = date.today()
    # file_name ='price_prediction_graph'+'-'+str(today.day)+'-'+str(today.month)+'-'+str(today.year)+'.csv'
    file_name ='price_prediction_graph.csv'
    file_path = file_destination + file_name
    aws.upload_csv_object(mydf,file_path)

    logger.info(f'Uploaded {file_name} to {file_path}')
    





def main():
    global df

    OBSERVATIONS = 16
    MANDATORY_MONTHS=6
    today = date.today()

    df = get_data('MasterCode1/cleaning/AllData/forecasting/')
    result = data_groupping(OBSERVATIONS , df)
    result[['price','flag']]=result[['year', 'make', 'scrape_month', 'scrape_year', 'mean', 'std', 'model' ]].copy().swifter.apply(lambda info : calculate_average_price(info), axis=1, result_type="expand")
    result['mandatory'] = result['date']>=(pd.to_datetime(today, format='%Y-%m-%d') - relativedelta(months=6))
    final_result = generate_results(result, OBSERVATIONS, MANDATORY_MONTHS)
    upload_data(final_result, 'MasterCode1/cleaning/forecasting/')
    
    

    


if __name__ == '__main__':

    main()