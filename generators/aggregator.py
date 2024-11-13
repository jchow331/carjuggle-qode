import logging
import os
from datetime import datetime, timedelta, date
from dateutil.relativedelta import relativedelta  

import pandas as pd
import numpy as np


# Necessary imports for logging
from base.aws_handler import AWSHandler
from base import log_config     # DO NOT REMOVE: this might seem as if it is not used anywhere but still keep this import
from base.logging_decorator import handle_exceptions, scrapper_error_notification

# Set global variables for logger and emails (change this according to your needs)
logger_name = 'data_aggregator_canada'
email_subject = 'Data Aggregator Bot Alert'
email_toaddrs = ['sana@qodemedia.com', 'karan@qodemedia.net','ajay.qode@gmail.com','jordan@qodemedia.com' ]
logger = logging.getLogger(__name__)
scraper_dir = os.path.dirname(os.path.abspath(__file__))    # full path to the scraper dir where this script is placed


def get_cleaned_files(folder_to_get_data_from ):

    aws = AWSHandler()

    files_paths=[]
    all_files = aws.get_folder_bucket_files(folder_to_get_data_from )
    
    for file in all_files:
        if '_timeless.csv' in file:
            files_paths.append(file)
    
    logger.info(files_paths)
    return files_paths



def aggregate_and_test(files_paths):


    aws = AWSHandler()
    today = date.today()
    three_weeks_ago=today-timedelta(weeks = 3)
    logger.info(f'Checking if we have cleaned data after {three_weeks_ago}')



    websites_with_corncerns=[]
    discontinued_websites=['autoshowwinnipeg', 'bmw', 'canadablackbook', 'crownautogroup', 'houseofcars', 
    'lexus','wheels', 'carmax']

    data=pd.DataFrame()
    for file in files_paths:
        mydf0=aws.download_object_as_csv(file)
        mydf0.drop([column for column in mydf0.columns if "Unnamed: 0" in column ], axis=1, inplace=True)
        logger.info(file)
        mydf0.rename(columns={'_id':"id", "province_to_keep":'province'}, inplace=True)
        logger.info(mydf0)
        website=file.split('/')[2]

        ### check if the file is getting new data
        if mydf0[mydf0['scraped_first_time']>=str(three_weeks_ago)].shape[0]>1:
            logger.info(f'{website} is getting_new_data')
        else:
            logger.info(f'{website} is NOT getting_new_data')
            websites_with_corncerns.append(website)
            
        data= pd.concat([data, mydf0], sort=False)
        

    data.reset_index(inplace=True, drop=True)
    logger.info(data)

    websites_with_corncerns = [website for website in websites_with_corncerns if website not in discontinued_websites]

    if len(websites_with_corncerns)>0:
        logger.info(f'websites that need to be checked: {websites_with_corncerns}')
        logger.info(f'raise error here and send emails')

        email_subject = str(websites_with_corncerns) + " Bot Status"
        message = str(websites_with_corncerns) + " bot(s) is/are not updating since three weeks"
        scrapper_error_notification(email_subject, email_toaddrs,message)



    
    return data


def formatting(data):

    data.drop(data[data['kilometers'].isnull()].index, inplace=True)
    data.reset_index(inplace=True, drop=True)
    data.drop(data[data['kilometers']=='Not Available'].index, inplace=True)
    data.reset_index(inplace=True, drop=True)
    data.drop(data[data['price']==0].index, inplace=True)
    data.reset_index(inplace=True, drop=True)

    data['model']=data['model'].astype(str)
    data['Vehicle_information.trim']=data['Vehicle_information.trim'].astype(str)
    data['price']=data['price'].astype(int)
    data['year']=data['year'].astype(int)

    data['kilometers']=data['kilometers'].astype(int)

    data.drop_duplicates(subset=['make', 'model', 'year', 'kilometers','new=0, certified=1, used=2',
        'price', 'province', 'City', 'Vehicle_information.transmission',
        'Vehicle_information.drivetrain', 'Vehicle_information.body_style', 'Vehicle_information.fuel_type',
        'Vehicle_information.trim'], keep='first',inplace = True)



    data=data[['id', 'scraped_first_time', 'make', 'model', 'year', 'kilometers',
       'price', 'province', 'City', 'Vehicle_information.transmission',
       'Vehicle_information.drivetrain', 'Vehicle_information.body_style',
       'Vehicle_information.exterior_colour', 'Vehicle_information.fuel_type',
       'Vehicle_information.trim', 'new=0, certified=1, used=2']]


    data.reset_index(inplace=True, drop=True)
    data.drop('id', axis=1, inplace=True)

    can_province_abbrev = {
  'Alberta': 'AB',
  'British Columbia': 'BC',
  'Manitoba': 'MB',
  'New Brunswick': 'NB',
  'Newfoundland And Labrador': 'NL',
  'Northwest Territories': 'NT',
  'Nova Scotia': 'NS',
  'Nunavut': 'NU',
  'Ontario': 'ON',
  'Prince Edward Island': 'PE',
  'Quebec': 'QC',
  'Saskatchewan': 'SK',
  'Yukon': 'YT'}
    data['province'] = data['province'].replace(can_province_abbrev )

    return data



def drop_unfrequent_makes_models_trims(mydf):

    # Makes
    # drop all makes with less or equal to only 350 cars to sell
    #recognizing the makes
    makes_to_drop=list(mydf.groupby('make').count()[mydf.groupby('make').count()['model']<=350].index)
    logger.info(f'Unfrequent makes that need to be dropped  {makes_to_drop}')

    # dropping the makes
    logger.info(f'The number of rows was {mydf.shape[0]}')
    mydf = mydf[~mydf['make'].isin(makes_to_drop)]
    mydf.reset_index(drop=True, inplace=True)
    logger.info(f'After removing the unfrequent makes, the number of rows become {mydf.shape[0]}')

    # Models
    # drop all models with less than 10 cars
    # drop very unfrequent models
    # recognizing the models to keep
    freq=mydf.groupby('model').count()['make']
    model_names=freq[freq>=10].index
    # Keeping only the frequent models
    mydf = mydf[mydf['model'].isin(model_names)]
    mydf.reset_index(drop=True, inplace=True)
    logger.info(f'After removing the unfrequent models, the number of rows become {mydf.shape[0]}')



    #drop very unfrequent trims
    # we need to find the unfrequent trims associated with the models
    # we can have the same trim name for different models so we need to distinguish them 
    # and remove the ones with less than 5 occurences
    trims_to_drop=list(mydf.groupby(['model','Vehicle_information.trim']).count()[mydf.groupby(['model','Vehicle_information.trim']).count()['make']<5].index)
    logger.info(len(trims_to_drop))
    logger.info(trims_to_drop)

    logger.info(f'The number of rows was {mydf.shape[0]}')

    trim_ind_to_drop=[]
    for i in range(len(mydf)):
        if (mydf.loc[i, 'model'],mydf.loc[i, 'Vehicle_information.trim'])  in trims_to_drop:
            trim_ind_to_drop.append(i)
    logger.info(mydf.iloc[trim_ind_to_drop, :])    
    mydf.drop(trim_ind_to_drop, inplace=True)
    mydf.reset_index(drop=True, inplace=True)

    logger.info(f'After removing the unfrequent trims, the number of rows become {mydf.shape[0]}')

    return mydf




def remove_kilometers_price_year_general_outliers(mydf, data):

    #remove price outliers
    rr_price=sorted(data["price"])
    quantile1_price, quantile3_price= np.round(np.percentile(rr_price,[0.2,99.8]),0)
    logger.info(f'Price data should very within {quantile1_price}, {quantile3_price}')
    mydf=mydf[(mydf.price <= quantile3_price) & (mydf.price >= quantile1_price )]
    mydf.reset_index(inplace=True, drop=True)
    

    # remove kilometer outliers
    perc1, perc2= np.round(np.percentile(sorted(data["kilometers"]),[0,99.5]))
    logger.info(f'Kilometer data should very within {perc1}, {perc2}')
    mydf=mydf[(mydf.kilometers <= perc2)]
    mydf=mydf[(mydf.kilometers >= perc1)]
    mydf.reset_index(inplace=True, drop=True)

    #remove year outliers
    year1, year2= np.round(np.percentile(sorted(data["year"]),[0.05,100]))
    logger.info(f'Year data should very within  {year1}, {year2}')
    mydf=mydf[(mydf.year >= year1)]
    mydf=mydf[(mydf.year <= year2)]
    mydf.reset_index(inplace=True, drop=True)

    logger.info(f'After removing the price, kilometer and year outliers, the number of rows become {mydf.shape[0]}')

    return mydf


def history_window(number_of_months, mydf):

    history_months = str(date.today() - relativedelta(months=+number_of_months))
    mydf=mydf[mydf['scraped_first_time']>history_months]
    mydf.reset_index(inplace=True, drop=True)
    logger.info(f'After keeeping data from the last {number_of_months} months, the number of rows become {mydf.shape[0]}')

    return mydf



def removing_specific_outliers(mydf):
    ## This step is added to further clean the data for training purposes

    to_check=mydf.groupby(['make', 'model', 'Vehicle_information.trim','year']).count()
    to_check.reset_index(inplace=True)

    ##REMOVE Price OUTLIERS  givin make, model year and trim
    outliers_ind=[]
    for i in to_check.index:

        X=mydf[(mydf['make']==to_check.loc[i,'make'])&(mydf['model']==to_check.loc[i,'model'])&(mydf['Vehicle_information.trim']==to_check.loc[i,'Vehicle_information.trim'])&(mydf['year']==to_check.loc[i,'year'])]
        
        data_median, data_std = np.median(X['price']), np.std(X['price'])
        #print(data_median, data_std)

    # identify outliers
        cut_off = data_std * 2.2
        lower, upper = data_median - cut_off, data_median + cut_off
        outliers_ind.extend(X[(X['price']<lower) | (X['price']>upper)].index)

        
        if i%500==0:
            logger.info(f'Removing outliers -- Progress: {round(i*100/len(to_check))}')

    logger.info(f'Length of the data to be removed: {len(outliers_ind)}')    
    logger.info(mydf.loc[outliers_ind, :])    
    mydf.drop(outliers_ind, inplace=True)
    mydf.reset_index(inplace=True, drop=True)

    return mydf



def upload_data(mydf, file_destination):

    aws = AWSHandler()

    today = date.today()
    file_name ='AllData_new_used'+'-'+str(today.month)+'-'+str(today.year)+'.csv'
    file_path = file_destination + file_name
    aws.upload_csv_object(mydf,file_path)

    logger.info(f'Uploaded {file_name} to {file_path}')










    


def main():


    folder_to_get_data_from = 'MasterCode1/cleaning/'
    files_paths = get_cleaned_files(folder_to_get_data_from )
    data = aggregate_and_test(files_paths)
    data = formatting(data)
    mydf = drop_unfrequent_makes_models_trims(data)
    mydf = remove_kilometers_price_year_general_outliers(mydf, data)
    mydf = history_window(24, mydf)
    upload_data(mydf, 'MasterCode1/cleaning/AllData/forecasting/')
    mydf = removing_specific_outliers(mydf)
    upload_data(mydf, 'MasterCode1/cleaning/AllData/')

    


if __name__ == '__main__':

    main()

