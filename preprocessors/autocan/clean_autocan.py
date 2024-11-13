# import sys
import re
# import json
# import os
# os.environ['KMP_DUPLICATE_LIB_OK']='True'
#from datetime import datetime
import io
import time
from random import randint
from collections import Counter
from collections import defaultdict
import math


from bs4 import BeautifulSoup as bs
import pandas as pd
import numpy as np
#from pandas.io.json import json_normalize
#from tqdm import tqdm, tqdm_notebook
#import concurrent.futures as futures
import requests
#from requests.exceptions import HTTPError
import concurrent.futures
import swifter
#from random import seed




from base.aws_handler import AWSHandler
# aws = AWSHandler()


# Necessary imports for logging
import logging
import os
from base import log_config     # DO NOT REMOVE: this might seem as if it is not used anywhere but still keep this import
from base.logging_decorator import handle_exceptions

# Set global variables for logger and emails (change this according to your needs)
logger_name = 'autocan_cleaner_bots_canada'
email_subject = 'Autocan Cleaner Bot Alert'
email_toaddrs = ['sana@qodemedia.com', 'karan@qodemedia.net']
logger = logging.getLogger(__name__)
scraper_dir = os.path.dirname(os.path.abspath(__file__))    # full path to the scraper dir where this script is placed







def match(info):

    i = info.name
    City = info[0]
    province = info[1]


    can_province_abbrev_code = {'AB':'Alberta',
                               'BC':'British Columbia',
                               'MB':'Manitoba',
                               'NB':'New Brunswick',
                               'NL':'Newfoundland And Labrador',
                               'NT':'Northwest Territories',
                               'NS':'Nova Scotia',
                               'NU':'Nunavut',
                               'ON':'Ontario',
                               'PE':'Prince Edward Island',
                               'QC':'Quebec',
                               'SK':'Saskatchewan',
                               'YT':'Yukon'}
    
    if (City == '' or str(City) == 'nan') or (province == '' or str(province) == 'nan'):

        City2 = ''
        is_match = i
        return City2, is_match
    
    else:
    
    
        df_match = locations.loc[locations['name'].str.title() == City]
        df_match['province.code'] = df_match['province.code'].replace(can_province_abbrev_code)
        df_match = df_match.loc[df_match['province.code'] == province]


        if len(df_match) == 1:
            City2 = df_match['name'].tolist()[0]
            is_match = 'Match'
            return City2, is_match
        else:
                #Try matching by 'contains'
                df_match = locations.loc[locations['name'].str.contains(City, regex=False)]
                #print(df_match)
                df_match['province.code'] = df_match['province.code'].replace(can_province_abbrev_code)
                df_match = df_match.loc[df_match['province.code'] == province]
                if len(df_match) == 1:
                    City2 = df_match['name'].tolist()[0]
                    is_match = 'Match'
                    return City2, is_match

                else:
                    df_match = extra_geo.loc[extra_geo['name'].str.title() == City]
                    df_match['province.code'] = df_match['province.code'].replace(can_province_abbrev_code)
                    df_match = df_match.loc[df_match['province.code'] == province]

                    if len(df_match) == 1:
                        City2 = df_match['name'].tolist()[0]
                        is_match = 'Match'
                        return City2, is_match

                    else:
                        #Try matching by 'contains'
                        df_match = extra_geo.loc[extra_geo['name'].str.contains(City, regex=False)]
                        df_match['province.code']=df_match['province.code'].replace(can_province_abbrev_code)
                        df_match = df_match.loc[df_match['province.code'] == province]

                        if len(df_match) == 1:
                            City2 = df_match['name'].tolist()[0]
                            is_match = 'Match'
                            return City2, is_match

                        else:
                            #Try geoloc (to avoid request spam)
                            df_match = geoloc.loc[geoloc['city_before'].str.title() == City]
                            df_match = df_match.loc[df_match['province'].str.title() == province]

                            if len(df_match) == 1:
                                City2 = df_match['city_after'].tolist()[0]
                                is_match = 'Match'
                                return City2, is_match

                            else:
                                City2 = ''
                                is_match = i
                                return City2, is_match
                
   
        
                                
            
def collect(alist):
    stri = ''
    for le in range(len(alist)):
        stri = stri + alist[le] + " "
    stri = stri.strip(' ')
    return stri




def check1(soup):
    try:
        L = soup.find_all("td",{"class":None})[3].find("small").text.split()
        ref = soup.find_all("td",{"class":None})[3].find("small").text.split().index('>')
        city_to_check1 = collect(L[:ref])
    except:
        city_to_check1 = collect(L)
    return city_to_check1




def check2(soup):
    L = soup.find_all("td",{"class":None})[3].find("small").text.split()
    ref = soup.find_all("td",{"class":None})[3].find("small").text.split().index('>')
    city_to_check2 = collect(L[ref+1:])
    return city_to_check2




def province(soup):
    try:
        L = str(soup.find_all("td", {"class": None})[3])
        province = re.search(r"</a>, (.*)<", L).group(1)
        if "<br/>" in province:
            province = re.search(r"(.*)<br/>", province).group(1)
    except:
        province = ""
    return province




# Function to convert a list to a string
def listToString(s):  
    str1 = " " 
    return (str1.join(s))
  
    
    
    
def name(soup):
    try:
        name = soup.find("table", {"class": "restable"}).find_all("td")[2].a.text
    except:
        name = ""
    return name




def get_cosine(vec1, vec2):
    intersection = set(vec1.keys()) & set(vec2.keys())
    numerator = sum([vec1[x] * vec2[x] for x in intersection])

    sum1 = sum([vec1[x] ** 2 for x in list(vec1.keys())])
    sum2 = sum([vec2[x] ** 2 for x in list(vec2.keys())])
    denominator = math.sqrt(sum1) * math.sqrt(sum2)

    if not denominator:
        return 0.0
    else:
        return float(numerator) / denominator


def text_to_vector(text):
    WORD = re.compile(r"\w+")
    words = WORD.findall(text)
    return Counter(words)



def get_best(text1, list_of_possibilities):
    if list_of_possibilities != []:
        output_scores = []
        vector1 = text_to_vector(str(text1).title())
        for i in range(len(list_of_possibilities)):
            vector = text_to_vector(str(list_of_possibilities[i]).title())
            output_scores.append(get_cosine(vector1, vector))

        max_value = max(output_scores)


        if max_value >= 0.71:
            max_index = [i for i, j in enumerate(output_scores) if j == max_value]
            if len(max_index) == 1:
                result = list_of_possibilities[max_index[0]]
            else:
                result = ''
                
        else:
            result = ''
    else:
            result = ''

    return result




def import_files(RESULT_PATH, KBB_PATH, LOCATIONS_PATH,GEOLOC_PATH , EXTRA_GEO_PATH, EXPORT_PATH_TIMELESS):

    logger.info(f"Importing files....")
    
    aws = AWSHandler()
    
#     df0 = aws.download_object_as_csv(PREVIOUS_RESULT_PATH)
#     df0.drop([column for column in df0.columns if "Unnamed: 0" in column ], axis = 1, inplace = True)
#     #print(df0)
#     #print('Display previous results')
#     pd.set_option('display.max_columns', None)
#     df0.rename(columns = {"_id": "id"}, inplace = True)
#     df0['id'] = np.where(df0['id'].isnull(), df0['url'], df0['id'])
#     df0['id'] = df0['id'].str.strip()
#     #print(df0)
#     #print(df0.columns)


    df = aws.download_object_as_csv(RESULT_PATH)
    df.drop([column for column in df.columns if "Unnamed: 0" in column ], axis = 1, inplace = True)
    df.rename(columns = {"_id": "id", 'scraped_first_time':'date_added'}, inplace = True)
    #print('Display recent results')
    pd.set_option('display.max_columns', None)
    df['id'] = np.where(df['id'].isnull(), df['url'], df['id'])
    df['id'] = df['id'].str.strip()
    df=df[['date_added', 'date_removed', 'id', 'url', 'make', 'model',
           'year', 'kilometers', 'price', 'condition', 'province', 'City',
           'Vehicle_information.transmission', 'Vehicle_information.drivetrain',
           'Vehicle_information.body_style', 'Vehicle_information.exterior_colour',
           'Vehicle_information.fuel_type', 'Vehicle_information.trim', 'img_url',
           'price_history', 'metadata']]
    #print(df)
    
    
#     df2 = aws.download_object_as_csv(RECOVERED_RESULT_PATH)
#     df2.drop([column for column in df2.columns if "Unnamed: 0" in column ], axis = 1, inplace = True)
#     df2.rename(columns = {"_id": "id"}, inplace = True)
#     #print('Display recovered results')
#     pd.set_option('display.max_columns', None)
#     #print(df2)


#     df = pd.concat([df0, df1, df2], axis=0, sort=False)
    df.drop_duplicates(subset = ['id'], keep = 'last', inplace = True)
    df.reset_index(inplace = True, drop=True)
    #print('Concatenate results')
    #print(df)


    ## !!!!!! If we want to be very precise and if we are updating frequently, we should download from EXPORT_PATH_TIME instead!!!!!
    dfcleaned=aws.download_object_as_csv(EXPORT_PATH_TIMELESS)
    dfcleaned.drop([column for column in dfcleaned.columns if "Unnamed: 0" in column ], axis=1, inplace=True)
    dfcleaned.rename(columns={'state':"province", "_id":"id"}, inplace=True)
    #print('Cleaned past results')
    #print(dfcleaned)




    trims_kbb=aws.download_object_as_csv(KBB_PATH)
    trims_kbb.drop([column for column in trims_kbb.columns if "Unnamed: 0" in column ], axis=1, inplace=True)
    #print(trims_kbb)
    trims_kbb.dropna( inplace = True)
    trims_kbb.reset_index(inplace = True, drop = True)




    locations = aws.download_object_as_csv(LOCATIONS_PATH)
    locations.drop([column for column in locations.columns if "Unnamed: 0" in column ], axis=1, inplace=True)
    #print('locations file:', locations)

    geoloc = aws.download_object_as_csv(GEOLOC_PATH)
    geoloc = geoloc[['province','city_before', 'city_after']]
    geoloc.dropna(inplace=True)
    geoloc.reset_index(inplace = True, drop = True)
    #print('geoloc file', geoloc )

    extra_geo = aws.download_object_as_csv(EXTRA_GEO_PATH)
    extra_geo.drop([column for column in extra_geo.columns if "Unnamed: 0" in column ], axis=1, inplace=True)
    #print('extra_geo file', extra_geo)


    #print(df.info())
    logger.info(f"Importing files....Done")
    logger.info(df)
    logger.info(f"size is  {df.shape}")
    
    return df, dfcleaned, trims_kbb, locations, geoloc, extra_geo



def initial_formatting(df, dfcleaned):

    
    # Initial formatting
    # =============================================================================
    #print('Initial fomatting and cleaning ids, prices, kilometers, years, condition .......')
    logger.info(f"Initial formatting....")

    df=df[["id", 'url','date_added',"date_removed","make","model", "year","kilometers", "price","condition","province", "City", "Vehicle_information.transmission",
       "Vehicle_information.drivetrain", "Vehicle_information.body_style", "Vehicle_information.exterior_colour",
       "Vehicle_information.fuel_type", "Vehicle_information.trim", 'price_history']]


    if dfcleaned.empty:
        pass
    else:
        df=df[~df['id'].isin(dfcleaned.id)]
        df.reset_index(drop=True, inplace=True)
        #print('Romoving cleaned data: ', df.shape)
 





    # #Gets rid of some null and duplicate values
    df.drop(df[(df["price"].isnull())|(df["price"]=="")].index,inplace=True)
    df.drop(df[df["price"]==0].index,inplace=True)

    #####
    df.drop(df[(df["City"].isnull())|(df["City"]=="")].index,inplace=True)
    #####
    df.drop(df[(df["year"].isnull())|(df["year"]=="")].index,inplace=True)
    ######
    df.drop(df[(df["province"].isnull())|(df["province"]=="")].index,inplace=True)
    ########
    df.drop(df[(df["Vehicle_information.exterior_colour"].isnull())|(df["Vehicle_information.exterior_colour"]=="")].index,inplace=True)
    ######
    df.drop(df[(df["date_added"].isnull())|(df["date_added"]=="")].index,inplace=True)

    df.drop_duplicates(subset=['id'],inplace=True)
    df.reset_index(drop=True, inplace=True)
    



    # #Corrects year, mileage and price column
    # # =============================================================================
    df['kilometers'].replace(to_replace=r',', value='', regex=True, inplace=True)
    df['kilometers'].replace(to_replace=r' kilometers', value='', regex=True, inplace=True)
    df['kilometers'].replace(to_replace=r' kilometres', value='', regex=True, inplace=True)
    df['kilometers'].replace(to_replace=r' km', value='', regex=True, inplace=True)
    df['kilometers'].replace(to_replace=r'-', value='', regex=True, inplace=True)
    df.drop(df[(df["kilometers"].isnull())|(df["kilometers"]=="")].index,inplace=True)
    df.drop(df[(df["kilometers"]=="---")].index,inplace=True)
    df['kilometers'] = df['kilometers'].astype(int)

    


    df.drop(df[(df["price"]=="---")].index,inplace=True)
    df['price'].replace(to_replace=r',', value='', regex=True, inplace=True)
    df['price'].replace(to_replace=r'\$', value='', regex=True, inplace=True)
    df.drop(df[(df["price"].isnull())|(df["price"]=="")|(df["price"]=="Contact for price")].index,inplace=True)
    df.reset_index(drop=True, inplace=True)
    
    
    df['price'] = pd.to_numeric(df['price']).astype(int)
    
    #### drop cars with kilometers=0 and condition =Used

    df.drop(df[(df['kilometers']==0)&(df['condition']=='Used')].index,inplace=True)
    df.reset_index(drop=True, inplace=True)
   


    #Corrects year
    # # =============================================================================
    
    df['year'] = df['year'].astype(float).astype(int)
    
    
    # Text formatting
    # title format
    df['make']=df['make'].str.title()
    df['model']=df['model'].str.title()
    df['province']=df['province'].str.upper()
    df['Vehicle_information.trim']=df['Vehicle_information.trim'].str.title()


    pd.set_option('display.max_columns', None)
    #print('Initial formatting')
    #print(df)

    #print(df.info())

    #print(df.shape)
    logger.info(f"Initial formatting....Done")
    logger.info(df)
    logger.info(f"size is  {df.shape}")

    return df


def fix_truncated_makes(x):
    #global allmakes 
    ##print(allmakes)
    
    if x in allmakes:
        return x
    else:
        for ed_make in allmakes:
            if x  in ed_make.split():
                #print(x, '=====>',ed_make.split())
                x=ed_make
                
                return x
            else:
                x=np.nan
                

def clean_makes(df,trims_kbb):
    # # Makes 
    # # =============================================================================
    #print ("Cleaning makes... ")
    logger.info(f"Cleaning makes....")


    df['make'] = df['make'].astype(str)
    df['make']=df['make'].str.strip()

    df['make']=df['make'].str.title()


    # #Uses the scraped makes and models from kbb to further sort makes
    # regex cleaning
    df['make'].replace(to_replace = '-', value = ' ', regex = True, inplace = True)
    trims_kbb['make'].replace(to_replace = '-', value = ' ', regex = True, inplace = True)


    df['make'].replace(to_replace = 'Mercedes Amg', value = 'Mercedes Benz', regex=True, inplace=True)
    df['make'].replace(to_replace = 'American Motors (Amc)', value = 'Am General', inplace=True) 
    


    #print ("Fix makes using fix_truncated_makes function: ... ")
    
    
   # Fix makes using fix_truncated_makes function

    global allmakes 
    allmakes = list(set(trims_kbb.make))

    df['make'] = df['make'].swifter.progress_bar(False).apply(lambda x : fix_truncated_makes(x))


    df.drop(df[df["make"].isnull()].index,inplace=True)
    df.reset_index(drop=True, inplace=True)

    #print ("Cleaning makes: Done.\n")
    #print ("size", df.shape)
    #print ("------------")
    #print(df)
    logger.info(f"Cleaning makes....Done")
    logger.info(df)
    logger.info(f"size is  {df.shape}")
    
    return df, trims_kbb


def upload_progress(df, PROGRESS_PATH):

    aws = AWSHandler()
    aws.upload_csv_object(df,PROGRESS_PATH)
    
    
    
def flatten(t):
    "flattens a list of list and convert it into a flat list"
    return [item for sublist in t for item in sublist]



def catch_exception_model(word, model, collection_words_temp_model):
    """if the word has a number like rav4, it will capture 'rav' and see it exist by itself in the list of models and it will also do the same for '4'
    if the word doesn't have a number, the code will go to the except part of the code and will do nothing"""
    try:
        res = re.findall(r'(\w+?)(\d+)', word)[0] 
        if res[0] in list(set(collection_words_temp_model)):
            model.append(res[0])
        if res[1] in list(set(collection_words_temp_model)):
            model.append(res[1])   
    except:
        pass
    return model



    
    
    
def remove_extra_words_from_models(info):
    df_Year=info[0]
    df_Make=info[1]
    df_Model=info[2]
    
    if (str(df_Year)!='nan') & (str(df_Make)!='nan') & (str(df_Model)!='nan'):
        
        X=trims_kbb[(trims_kbb['year']==df_Year) & (trims_kbb['make']==df_Make)]
        if X.empty:
            model_to_keep=''
        else:

        
#         collection_words_temp_model=[]
#         for j in range(len(list(set(X.Model)))):
#             collection_words_temp_model.extend(str(list(set(X.Model))[j]).split())

######## equivalent to:

            collection_words_temp_model=set()
            X['model'].str.split().swifter.progress_bar(False).apply(collection_words_temp_model.update)
            collection_words_temp_model=list(collection_words_temp_model)

            
                
            temp=str(df_Model).split()
            model=[]

            [model.append(word) if word in collection_words_temp_model else catch_exception_model(word, model, collection_words_temp_model) for word in temp]
            temp=re.split(r'(^[^\d]+)', df_Model)[1:]
            [model.append(word) if word in collection_words_temp_model else catch_exception_model(word, model, collection_words_temp_model) for word in temp]
            
            temp=str(df_Model).replace(" ",'').title().split()
            [model.append(word) if word in collection_words_temp_model else catch_exception_model(word, model, collection_words_temp_model) for word in temp]

            #remove duplicate words without messing up the order
            model = list(dict.fromkeys(model))

            model_to_keep=listToString(model)
            

    else:
        model_to_keep=''
        
    return model_to_keep


def model_index_to_review(info):

    i=info.name
    thismake=info[0]
    this_model_to_keep=info[1]
    
    
    if thismake in list(d.keys()):
        
        thesemodels=list(d[thismake])

        if this_model_to_keep not in (thesemodels):
            m=0
            up_model=sorted(set(this_model_to_keep.split()))
            for thismodel in thesemodels:
                m=m+1
                if up_model==sorted(set(thismodel.split())):
                    this_model_to_keep=thismodel
                    ind_to_drop='No'
                    break

                if m==len(thesemodels): 
                    this_model_to_keep=this_model_to_keep
                    ind_to_drop=i
        else:
            this_model_to_keep=this_model_to_keep
            ind_to_drop='No'
            
    else:
        this_model_to_keep=this_model_to_keep
        ind_to_drop=i
        
    return this_model_to_keep, ind_to_drop


def model_last_try(info):
    l=info.name
    thismake=info[0]
    new_model=info[1]
    new_trim=info[2]
    #print(l, thismake,new_model, new_trim)
    
    if "Hybrid" in new_model:
        new_model=new_model.replace("Hybrid",'').replace('Plug In Hybrid','').strip()
        if "Hybrid" not in new_trim:
            new_trim=new_trim+" Hybrid"
        else:
            new_trim=new_trim

    if thismake=='Ford':
        if str(new_model).replace(' ','').replace('-','').lower() in ['f250', 'f350', 'f450']:
#             print(' I entered')
            new_model='Super Duty '+ new_model
        
#     #print('Lets try those again',ind_to_drop)

# models_to_be_removed=ind_to_drop.copy()

# for l in ind_to_drop:
    
#     thismake=df.loc[l, 'make'] 
    
    if thismake in list(d.keys()) and thismake != 'Mazda':
        thesemodels=list(d[thismake])

        potential_match=[]
        list_model_words=[]
        for kbb_model in thesemodels:
            list_model_words=kbb_model.split()
            for word in list_model_words:
                try:
                    Az09= re.findall(r'(\w+?)(\d+)', word)[0] 
                    list_model_words.extend(Az09)
                    
                except:
                    pass
            ##print(list_model_words)
            
            if any(item in list_model_words for item in new_model.split()) and new_model!='':
                potential_match.append(kbb_model)
            
                
            
        #print(potential_match)
        #print(new_model)
        if get_best(new_model, potential_match)!='':
            model_to_keep=get_best(new_model, potential_match)
            ##print(potential_match)
            ##print(new_model,'====>',get_best(new_model, potential_match))
            models_to_be_removed='No'
        else:
            model_to_keep=new_model
            models_to_be_removed=l
            
    else:
        model_to_keep=new_model
        models_to_be_removed=l
        
    return model_to_keep, new_trim, models_to_be_removed
        
            
            
            
            
        


def clean_models(df, trims_kbb):


    # # Models
    # # =============================================================================
    #print ("Cleaning models: ...(4/11) ")
    logger.info(f"Cleaning models...")


    df.drop(df[df["model"].isnull()].index,inplace=True)
    df.reset_index(drop=True, inplace=True)

    df['model'].replace(to_replace='-', value=' ', regex=True, inplace=True)
    trims_kbb['model'].replace(to_replace='-', value=' ', regex=True, inplace=True)
    trims_kbb['model']=trims_kbb['model'].str.title()
    df['model']=df['model'].str.title()
    df['model']=df['model'].str.strip()
    
    #tqdm_notebook().pandas(desc="Model: Removing extra words")
    #print("Model: Removing extra words")
    df['model_to_keep']=df[['year','make', 'model' ]].copy().swifter.progress_bar(False).apply(lambda info : remove_extra_words_from_models(info), axis=1)
    
    #print(df)
    #print("df size:", df.shape)
    


    # initializing list
    make_model_list=list(trims_kbb.groupby(['make', 'model']).count().index)

#     # #printing original list
#     #print("The original list is : " + str(make_model_list))

    # Using defaultdict() + loop
    # Convert list of tuples to dictionary value lists

    global d
    d = defaultdict(list)
    #print(d)
    for i, j in make_model_list:
        d[i].append(j)
      # #printing result 
    d=dict(d)
    #print(dict(d))
    
    
    #tqdm_notebook().pandas(desc="Model: cheking indexes to try again")
    #print("Model: cheking indexes to try again")
    df[['model_to_keep','ind_to_drop']]=df[['make', 'model_to_keep' ]].copy().swifter.progress_bar(False).apply(lambda info : model_index_to_review(info), axis=1, result_type="expand")
    
    ind_to_drop=list(set(df['ind_to_drop']))
    try:
        ind_to_drop.remove('No')
    except:
        pass
#     models_to_be_removed=ind_to_drop.copy()
#     #print(models_to_be_removed)
    
    
    #print(ind_to_drop)
    #print(df.loc[ind_to_drop,:])
    
    
    
    #tqdm_notebook().pandas(desc="last step in cleaning models")
    #print("last step in cleaning models")
    new_df=pd.DataFrame()
    #generate the model to keep and the updated trims for that selections of rows
    #provide an updated index list
    new_df[['model_to_keep','Vehicle_information.trim','ind_to_drop']] = df.loc[ind_to_drop,('make','model_to_keep' ,'Vehicle_information.trim')].copy().swifter.progress_bar(False).apply(lambda info : model_last_try(info), axis=1, result_type="expand")
    # update the dataframe based on the result new_df
    df.update(new_df)

    ##print the updated portion of the dataframe
    #print(df.loc[ind_to_drop,('model_to_keep','ind_to_drop','Vehicle_information.trim')])
    ind_to_drop=list(set(df.loc[ind_to_drop,'ind_to_drop']))
    try:
        ind_to_drop.remove('No')
    except:
        pass

    #print(ind_to_drop)
    
    
    
    
    #print(df.loc[ind_to_drop,:])
    df.drop(ind_to_drop, inplace=True)
    df.reset_index(drop=True, inplace=True)

    df.drop(df[df["model_to_keep"].isnull()].index,inplace=True)
    df.drop(df[df["model_to_keep"]==''].index,inplace=True)
    df.reset_index(drop=True, inplace=True)

    #print ("Cleaning models: Done.\n")
    #print('df Size:', df.shape)
    #print ("------------")

    logger.info(f"Cleaning models....Done")
    logger.info(df)
    logger.info(f"size is  {df.shape}")
    

    return df, trims_kbb




    
def catch_exception_trim(word, trim, collection_words_temp):
    """try to capture numbers and packages W/"""
    try:
        res = re.findall(r'(\w+?)(\d+)', word)[0] 
        if res!='':
            if res[0] in list(set(collection_words_temp)):
                trim.append(res[0])
            if res[1] in list(set(collection_words_temp)):
                trim.append(res[1]) 
        else:
            word=word.replace('W/','').replace("'", '').replace('/','').title()
            if word in list(set(collection_words_temp)):
                trim.append(word)
            
    except:
        pass
    return trim
 
    
def remove_extra_words_from_trims(info):
    df_Year=info[0]
    df_Make=info[1]
    df_Model=info[2]
    df_Trim=info[3]
    
    if (str(df_Year)!='nan') & (str(df_Make)!='nan') & (str(df_Model)!='nan') & (str(df_Trim)!='nan'):

        
        X=trims_kbb[(trims_kbb['year']==df_Year) & (trims_kbb['make']==df_Make) & (trims_kbb['model']==df_Model)]
        if X.empty:
            trim_to_match=''
        else:


            collection_words_temp=set()
            X['trim_mod'].str.split().swifter.progress_bar(False).apply(collection_words_temp.update)
            X['trim1'].str.split().swifter.progress_bar(False).apply(collection_words_temp.update)
            collection_words_temp=list(collection_words_temp)
            
                
            temp=str(df_Trim).split()
            trim=[]
                    
            [trim.append(word) if word in list(set(collection_words_temp)) else catch_exception_trim(word, trim, collection_words_temp) for word in temp]
            temp=re.split(r'(^[^\d]+)', df_Trim)[1:]
            [trim.append(word.title()) if word in list(set(collection_words_temp)) else catch_exception_trim(word, trim, collection_words_temp) for word in temp]

            trim = list(dict.fromkeys(trim))

            trim_to_match=listToString(trim)

    else:
        trim_to_match=''
        
    return trim_to_match



def trim_index_to_review(info):

    k=info.name
    thisyear=info[0]
    thismake=info[1]
    this_model_to_keep=info[2]
    this_trim=info[3]
    this_trim_to_match=info[4]
    
    
    if (str(thisyear)!='nan') & (str(thismake)!='nan') & (str(this_model_to_keep)!='nan') & (str(this_trim)!='nan'):
        
        X=trims_kbb[(trims_kbb['year']==thisyear) & (trims_kbb['make']==thismake)& (trims_kbb['model']==this_model_to_keep)]
        X.reset_index(drop=True, inplace=True)

        if X.empty:
            this_trim_to_keep=this_trim
            ind_to_drop=k
        else:
     
            for l in X.index:
        
                if sorted(str(this_trim_to_match).split())==sorted(str(X['trim1'].values[l]).split()):
      
                    this_trim_to_keep=X.loc[l,'trim_mod']
                    ind_to_drop='No'
                    break

                if l==len(X)-1:

                    new_trim=this_trim_to_match
                    new_trim=new_trim.replace('4Dr','').replace('3Dr', '').replace('2Dr', '').replace('5Dr', '')
                    new_trim=new_trim.replace('Awd', '').replace('4Wd', '').replace('2Wd', '').replace('4X4', '').replace('Fwd', '').replace('Rwd', '').replace('4X2', '').replace('2X4', '')
                    new_trim=new_trim.replace('Cab Plus','').replace('Regular Cab', '').replace('Sdn', '').replace('Wgn', '').replace('Conv', '').replace('Cpe', '').replace('Suv', '')
                    new_trim=new_trim.replace('Pkg', '').replace('Ed', '').replace('V6', '').replace('I4', '')
                    new_trim=re.sub(r'(?<=\d)\.+(?=\d)', '', new_trim)
                    
                    if this_trim_to_match!='' and new_trim=='':
                        new_trim='Base'

                    for m in X.index: 
                        if sorted(str(new_trim).split())==sorted(str(X['trim_mod'].values[m]).split()):
                            this_trim_to_keep=X.loc[m,'trim_mod']
                            ind_to_drop='No'
                            break

                        if m==len(X)-1: 
                        
                            in_case_recurrent_word=[]
                            t=[ in_case_recurrent_word+str(list(X.trim_mod)[n]).split() for n in X.index]
                            in_case_recurrent_word=flatten(t)


                            z = in_case_recurrent_word

                            for p in range(len(list(Counter(z).keys()))):
                                if "Base" in X['trim_mod'].values:
                                    if Counter(z)[list(Counter(z).keys())[p]]==len(X.index)-1 and str(list(Counter(z).keys())[p]) not in new_trim :
                                        new_trim=new_trim+' '+list(Counter(z).keys())[p]
                                        

                                else:
                                    if Counter(z)[list(Counter(z).keys())[p]]==len(X.index) and str(list(Counter(z).keys())[p]) not in new_trim :
                                        new_trim=new_trim+' '+list(Counter(z).keys())[p]
        #                         #print('new trim 2',  new_trim)
        
                        
                            for q in X.index:        
                                if sorted(new_trim.split())==sorted(str(X['trim_mod'].values[q]).split()):
                                    this_trim_to_keep=X.loc[q,'trim_mod']
                                    ind_to_drop='No'
    #                                 #print('trim 2 success')
                                    break
                                    
                                
                            
                                elif q==len(X)-1:
                                    this_trim_to_keep=this_trim
                                    ind_to_drop=k
                                    return this_trim_to_keep, ind_to_drop  



           

            
    else:
        this_trim_to_keep=this_trim
        ind_to_drop=k
     
        
    return this_trim_to_keep, ind_to_drop


def trim_last_try(info):
    I=info.name
    thisyear=info[0]
    thismake=info[1]
    thismodel=info[2]
    thistrim=info[3]
    #print(I, thisyear,thismake,thismodel, thistrim)

    if (str(thisyear)!='nan') & (str(thismake)!='nan') & (str(thismodel)!='nan') & (str(thistrim)!='nan'):
        Y=trims_kbb[(trims_kbb['year']==thisyear) & (trims_kbb['make']==thismake)& (trims_kbb['model']==thismodel)]
        if Y.empty:
            this_trim_to_keep=thistrim
            ind_to_drop=I
        else:
            new_trim=thistrim
            new_trim=new_trim.replace('Used','').replace('New', '').replace('Certified', '')
            new_trim=new_trim.replace(str(thisyear),'').replace(str(thismake), '').replace(str(thismodel), '')
            new_trim=new_trim.replace('Awd', '').replace('4Wd', '').replace('2Wd', '').replace('4X4', '').replace('Fwd', '').replace('Rwd', '').replace('4X2', '').replace('2X4', '')
            new_trim2=new_trim.rstrip().lstrip().strip()

            new_trim=new_trim.replace('4Dr','').replace('3Dr', '').replace('2Dr', '').replace('5Dr', '')
            new_trim=new_trim.replace('Cab Plus','').replace('Regular Cab', '').replace('Sdn', '').replace('Wgn', '').replace('Conv', '').replace('Cpe', '').replace('Suv', '')
            new_trim=new_trim.replace('Cvt', '').replace('W/', '')
            new_trim=new_trim.strip()

            list_to_remove=['Conv','Wgn','Man','Auto','Suv' ,'Hb', 'Wagon', 'Cab', 'Crew' ]
            trim=[word for word in new_trim.split() if word not in list_to_remove]
            new_trim=listToString(trim)

            if new_trim2!='' and new_trim=='':
                new_trim='Base'

            thesetrims=list(set(Y['trim_mod'].values))

            potential_match=[]
#             for kbb_trim in thesetrims:
#                 ##print(I,new_trim,'######',df.loc[I,'trim_to_match'],'######',df.loc[I,'Vehicle_information.trim'],'######',kbb_trim )
#                 #if str(new_trim) in kbb_trim and new_trim!='':
#                 if any(item in re.split(r'[\s]', str(kbb_trim).replace('W/','').replace('/','')) for item in new_trim.split()) and new_trim!='':
#                     ##print('entered hrere', new_trim, kbb_trim)
#                     potential_match.append(kbb_trim)
                    
            potential_match=[kbb_trim for kbb_trim in thesetrims if any(item in re.split(r'[\s]', str(kbb_trim).replace('W/','').replace('/','')) for item in new_trim.split()) and new_trim!='']
            #print(I, new_trim , 'Versus', potential_match)
            if get_best(new_trim, potential_match)!='':
                this_trim_to_keep=get_best(new_trim, potential_match)
                ind_to_drop='No'
                #print(I, thistrim,'====>',new_trim,'====>',get_best(new_trim, potential_match))
       
                
                
            elif len(potential_match)==1:
                this_trim_to_keep=potential_match[0]
                #print(I, thistrim,'====>',new_trim,'====>',potential_match)
                ind_to_drop='No'

            else:
                this_trim_to_keep=thistrim
                ind_to_drop=I
    else:
        this_trim_to_keep=thistrim
        ind_to_drop=I  
        
          
    return this_trim_to_keep, ind_to_drop




def clean_trims(df, trims_kbb):
    
        # # Trims
    # # =============================================================================
    #print ("Cleaning trims... (5/11)")

    logger.info(f"Cleaning trims...")

    df['together']= df.Series.astype(str).str.cat(df.Trim.astype(str), sep=' ')
    df['together'] = df['together'].str.title()
    df['Vehicle_information.trim'] = df['Vehicle_information.trim'].astype(str) + ' '+  df['together'].astype(str) 



    df['Vehicle_information.trim']=df['Vehicle_information.trim'].str.title()
    df['Vehicle_information.trim']=df['Vehicle_information.trim'].str.strip()

    trims_kbb['trim_mod'].replace(to_replace='Edition', value='Ed', regex=True, inplace=True)
    trims_kbb['trim_mod'].replace(to_replace='Ed.', value='Ed', regex=True, inplace=True)
    df['Vehicle_information.trim'].replace(to_replace='Edition', value='Ed', regex=True, inplace=True)
    df['Vehicle_information.trim'].replace(to_replace='Ed.', value='Ed', regex=True, inplace=True)


    trims_kbb['trim_mod'].replace(to_replace='Packages', value='Pkg', regex=True, inplace=True)
    trims_kbb['trim_mod'].replace(to_replace='Package', value='Pkg', regex=True, inplace=True)

    df['Vehicle_information.trim'].replace(to_replace='Packages', value='Pkg', regex=True, inplace=True)
    df['Vehicle_information.trim'].replace(to_replace='Package', value='Pkg', regex=True, inplace=True)

    df['Vehicle_information.trim'].replace(to_replace='-', value=' ', regex=True, inplace=True)
    trims_kbb['trim_mod'].replace(to_replace='-', value=' ', regex=True, inplace=True)


    df['Vehicle_information.trim'].replace(to_replace='Limited', value='Ltd', regex=True, inplace=True)
    trims_kbb['trim_mod'].replace(to_replace='Limited', value='Ltd', regex=True, inplace=True)


    df['Vehicle_information.trim'].replace(to_replace='Navigation', value='Navi', regex=True, inplace=True)
    trims_kbb['trim_mod'].replace(to_replace='Navigation', value='Navi', regex=True, inplace=True)

    trims_kbb['trim_mod'].replace(to_replace='Technology', value='Tech', regex=True, inplace=True)
    df['Vehicle_information.trim'].replace(to_replace='Technology', value='Tech', regex=True, inplace=True) 




    trims_kbb['trim_mod']=trims_kbb['trim_mod'].str.lstrip().str.rstrip()

    df['Vehicle_information.trim']=df['Vehicle_information.trim'].str.lstrip().str.rstrip()


    # Part 1: Here we clean/standardize the scraped trims  from kbb
    df['Vehicle_information.trim'].replace(to_replace='Sedan', value='Sdn', regex=True, inplace=True)
    df['Vehicle_information.trim'].replace(to_replace='Wagon', value='Wgn', regex=True, inplace=True)
    df['Vehicle_information.trim'].replace(to_replace='Coupe', value='Cpe', regex=True, inplace=True)
    df['Vehicle_information.trim'].replace(to_replace='Hatchback', value='Hb', regex=True, inplace=True)
    df['Vehicle_information.trim'].replace(to_replace='Convertible', value='Conv', regex=True, inplace=True)
    df['Vehicle_information.trim'].replace(to_replace='Manual', value='Man', regex=True, inplace=True)
    df['Vehicle_information.trim'].replace(to_replace='Automatic', value='Auto', regex=True, inplace=True)



    trims_kbb['trim_mod'].replace(to_replace='-', value=' ', regex=True, inplace=True)
    df['Vehicle_information.trim'].replace(to_replace='-', value=' ', regex=True, inplace=True)

    df['Vehicle_information.trim'].replace(to_replace=r'\(', value=' ', regex=True,inplace=True)
    df['Vehicle_information.trim'].replace(to_replace=r'\)', value=' ', regex=True,inplace=True)
    df['Vehicle_information.trim'].replace(to_replace='\*', value=' ', regex=True, inplace=True)
    df['Vehicle_information.trim'].replace(to_replace=',', value=' ', regex=True, inplace=True)
    df['Vehicle_information.trim'].replace(to_replace='\+', value=' ', regex=True, inplace=True)



    df['Vehicle_information.trim'] = df['Vehicle_information.trim'].str.lstrip().str.rstrip()
    trims_kbb['trim_mod']=trims_kbb['trim_mod'].str.lstrip().str.rstrip()

    df['Vehicle_information.trim']=df['Vehicle_information.trim'].str.title()
    trims_kbb['trim_mod']=trims_kbb['trim_mod'].str.title()






    #tqdm_notebook().pandas(desc="Trim: Removing extra words")
    #print("Trim: Removing extra words")
    df['trim_to_match']=df[['year','make', 'model_to_keep', 'Vehicle_information.trim' ]].copy().swifter.progress_bar(False).apply(lambda info : remove_extra_words_from_trims(info), axis=1)

    #print(df)

    #print("Trim: cheking indexes to try again")
    #tqdm_notebook().pandas(desc="Trim: cheking indexes to try again")
    df[['trim_to_keep','ind_to_drop']]=df[['year','make', 'model_to_keep', 'Vehicle_information.trim' ,'trim_to_match']].copy().swifter.progress_bar(False).apply(lambda info : trim_index_to_review(info), axis=1, result_type="expand")
    ##print(df[['year','make', 'model_to_keep', 'Vehicle_information.trim' ,'trim_to_match']].copy().swifter.progress_bar(False).apply(lambda info : trim_index_to_review(info), axis=1, result_type="expand"))
    #print(df)

    ind_to_drop=list(set(df['ind_to_drop']))
    try:
        ind_to_drop.remove('No')
    except:
        pass
#     models_to_be_removed=ind_to_drop.copy()
#     #print(models_to_be_removed)
    
    
    #print(ind_to_drop)
    #print(df.loc[ind_to_drop,:])


    new_df=pd.DataFrame()
    new_df[['trim_to_keep','ind_to_drop']] = df.loc[ind_to_drop,('year','make','model_to_keep' ,'trim_to_match')].copy().swifter.progress_bar(False).apply(lambda info : trim_last_try(info), axis=1, result_type="expand")
    df.update(new_df)

    ind_to_drop=list(set(df['ind_to_drop']))
    try:
        ind_to_drop.remove('No')
    except:
        pass

    ##print the updated portion of the dataframe
    #print(df.loc[ind_to_drop,('model_to_keep','ind_to_drop','Vehicle_information.trim')])
    ind_to_drop=list(set(df.loc[ind_to_drop,'ind_to_drop']))
    try:
        ind_to_drop.remove('No')
    except:
        pass

    #print('We are going to remove the following')
    #print(ind_to_drop)
    #print(df.loc[ind_to_drop,:])


    df.drop(ind_to_drop, inplace=True)
    df.reset_index(drop=True, inplace=True)

    df.drop(df[df["trim_to_keep"].isnull()].index,inplace=True)
    df.drop(df[df["trim_to_keep"]==''].index,inplace=True)
    df.reset_index(drop=True, inplace=True)

    #print ("Cleaning trims: Done.\n")
    #print('df Size:', df.shape)
    #print ("------------")

    logger.info(f"Cleaning trims....Done")
    logger.info(df)
    logger.info(f"size is  {df.shape}")



    return df, trims_kbb



def is_province(info):
    index=info.name
    province=info[0]

    can_province_abbrev_code = {
   'AB':'Alberta',
   'BC':'British Columbia',
   'MB':'Manitoba',
   'NB':'New Brunswick',
   'NL':'Newfoundland And Labrador',
   'NT':'Northwest Territories',
   'NS':'Nova Scotia',
   'NU':'Nunavut',
   'ON':'Ontario',
   'PE':'Prince Edward Island',
   'QC':'Quebec',
   'SK':'Saskatchewan',
   'YT':'Yukon'
}


    try:
        province=can_province_abbrev_code[province.split(",")[1].split()[0]]
        ind_to_drop = 'keep'

    except:
        if province.strip().upper() in list(can_province_abbrev_code.keys()):
            province=can_province_abbrev_code[province.strip().upper()]
            ind_to_drop = 'keep'
        else:
            province=''
            ind_to_drop = index
                     
    return province, ind_to_drop
    
    

def clean_provinces(df):
# # provinces
# # =============================================================================
    #print ("Cleaning provinces... ")

    logger.info(f"Cleaning provinces...")


    #print("Initial Province set: ", set(df['province']))
    

    df['province'].replace(to_replace=r'.*BC\nUse.*', value='BC', regex=True, inplace=True)
    df['province'].replace(to_replace=r'.*BC\nKeyboard.*', value='BC', regex=True, inplace=True)
    df['province'].replace(to_replace=r'.*BC\nMap.*', value='BC', regex=True, inplace=True)
    df['province'].replace(to_replace=r'.*BC\nTerms.*', value='BC', regex=True, inplace=True)
    df['province'].replace(to_replace=r'.*BC\nRepor.*', value='BC', regex=True, inplace=True)
    df['province'].replace(to_replace=r'.*AB\nKeyboard.*', value='AB', regex=True, inplace=True)
    df['province'].replace(to_replace=r'.*AB\nMap.*', value='AB', regex=True, inplace=True)
    df['province'].replace(to_replace=r'.*AB\nTerms.*', value='AB', regex=True, inplace=True)
    df['province'].replace(to_replace=r'.*AB\nRepor.*', value='AB', regex=True, inplace=True)
    df['province'] = df['province'].str.strip()
    
    #print("Initial Province set: ", set(df['province']))
    
    


    #print("province: is this a real province?")
    df[['province','ind_to_drop']]=df[['province']].swifter.progress_bar(False).apply(lambda info : is_province(info), axis=1, result_type="expand")
    ind_to_drop=list(set(df['ind_to_drop']))
    try:
        ind_to_drop.remove('keep')
    except:
        pass
    
    #print(df.loc[ind_to_drop,:])
    df.drop(ind_to_drop, inplace=True)
    df.drop(df[df["province"].isnull()].index,inplace=True)
    df.drop(df[df["province"]==''].index,inplace=True)
    
    df.reset_index(drop=True, inplace=True)


    #print ("Cleaning provinces: Done.\n")
    #print('Final set: ', set(df['province']))
    #print('df Size:', df.shape)
    #print ("------------")
    logger.info(f"Cleaning provinces....Done")
    logger.info(df['province'].unique())
    logger.info(f"size is  {df.shape}")
    
    
    return df


def scrape_url_and_information(information):

    
    province, city, = information[0]
    indexes= information[1]


    link="https://www.geonames.org/search.html?q=" + city + "&country=CA"
    page = requests.get(link)
    soup = bs(page.text, 'html.parser')
    

    try:
        city_to_check1=check1(soup) 
    except:
        city_to_check1=''

    try:
        city_to_check2=check2(soup)
    except:
        city_to_check2=''

    try:
        province_to_check=province(soup)
    except:
        province_to_check=''

    try:
        city_name=name(soup)
    except:
        city_name=''
        
    return province, city, indexes, city_to_check1, city_to_check2, province_to_check, city_name



def scrape_url_geogratis(information):

    can_province_code={
    'Newfoundland And Labrador':10,
    'Prince Edward Island': 11,
    'Nova Scotia':12,
    'New Brunswick':13,
    'Quebec':24,
    'Ontario': 35,
    'Manitoba':46,
    'Saskatchewan':47,
    'Alberta':48,
    'British Columbia':59,
    'Yukon':60,
    'Northwest Territories':61,
    'Nunavut':72 }

    can_pro_code={
    10:"NL",
    11:"PE",
    12:"NS",
    13:"NB",
    24:"QC",
    35:"ON",
    46:"MB",
    47:"SK",
    48:"AB",
    59:"BC",
    60:"YT",
    61:"NT",
    72:"NU" }

    index = information.name
    City = information[0]
    province = information[1]

    page2=requests.get("http://geogratis.gc.ca/services/geoname/en/geonames.csv?&q="+ City).content
    c=pd.read_csv(io.StringIO(page2.decode('utf-8')), error_bad_lines=False)
    pd.set_option('display.max_columns', None)
    c.head(20)

    if c.empty:
        index_to_drop=index
        city2=''
    else:
        X=c[(c['name']==City)&(c['province.code']==can_province_code[province])&(c['concise.code'].isin(['CITY','TOWN','UNP']))&(c['status.code']=='official')]
        ##print(X)
        if X.empty:
            index_to_drop=index
            city2=''
        else:
            X=X[['name', 'location', 'concise.code','province.code' ,'latitude','longitude']]
            X.drop_duplicates(subset=['name', 'location', 'concise.code','province.code'],inplace=True)
            X.reset_index(inplace=True, drop=True)
            
            ##print(X)
            index_to_drop='keep'
            city2=City
            for i in X.index:
                extra_geo.loc[len(extra_geo)] = [X.loc[i,'name'],X.loc[i,'location'],X.loc[i,'concise.code'],can_pro_code[X.loc[i, 'province.code']],X.loc[i, 'latitude'],X.loc[i, 'longitude']]
                extra_geo.drop_duplicates(inplace=True)
                extra_geo.reset_index(inplace=True, drop=True)


    return city2, index_to_drop


def clean_cities(df):

    logger.info(f"Cleaning cities...")


    df['City'].replace(to_replace=r"Montr\u00e9al", value='Montréal', regex=False, inplace=True)
    
    df['City'] = df['City'].str.title()
    df['City'] = df['City'].str.strip()
    # some cities are presented as 629 Brant St Burlington (address instead of a city)
    # I am detecting if there is a street number in the city. If so I will take the last word
    df['City'] = [city.split()[-1] if str(city)!='nan' and re.search("\d{1,4}",city) else city for city in df.City]



    df[['City2','ind_to_drop']] = df[['City', 'province']].copy().swifter.progress_bar(False).apply(lambda info : match(info), axis=1, result_type="expand")
    #print(df[['province','City','City2','ind_to_drop']].head(50))

    ind_to_drop=list(set(df['ind_to_drop']))
    try:
        ind_to_drop.remove('Match')
    except:
        pass



    #print('Indexes of cities that we need to look up:',ind_to_drop)
    if ind_to_drop!=[]:
        province_city_df=df.loc[ind_to_drop,['province','City',"ind_to_drop"]]
        #### let's take a look at the dataframe
        #print(province_city_df)


        #### We need to transform the dataframe into a dictionnary with unique searches
        ### many province amd city combinations exist many times. we need to find the unique combination of province and city
        ### and just collect the indexes associated with them
        dict_cities = defaultdict(list)
        province_city_list=list(province_city_df.groupby(['province','City', 'ind_to_drop']).count().index)

        for i, j, k in province_city_list:
            dict_cities[(i,j)].append(k)

        # #printing result 
        dict_cities=dict(dict_cities)
        #print('Transformed dictionnary of provinces and cities combinations that we need to look up')
        #print(dict(dict_cities))


        # Converting into list of tuple
        list_tuples = [(i, j) for i, j in dict_cities.items()]

        # #printing list of tuple
        #print('#printing list of tuple')
        #print(list_tuples)

        if list_tuples==[]:
            cities_to_drop=ind_to_drop
        else:


            with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
                result=executor.map(scrape_url_and_information, list_tuples)


            # get results into a dataframe
            df_result= pd.DataFrame(list(result), columns =['province','City', 'Indexes', 'city_to_check1', 'city_to_check2', 'province_to_check', 'city_name'])
            #print('This is the dataframe with the irght format')
            ##print(df_result)


            df_result[['City2','match2']] = df_result[['city_to_check1', 'province']].copy().swifter.progress_bar(False).apply(lambda info : match(info), axis=1, result_type="expand")


            df_result[['City3','match3']] = df_result[['city_to_check2', 'province']].copy().swifter.progress_bar(False).apply(lambda info : match(info), axis=1, result_type="expand")


            df_result[['City4','match4']] = df_result[['city_name', 'province']].copy().swifter.progress_bar(False).apply(lambda info : match(info), axis=1, result_type="expand")


            #print(df_result)

            df_result['City2'] = df_result.swifter.progress_bar(False).apply(lambda x: x['City2'] if x['City3']=='' else x['City3'], axis=1)
            df_result['City2'] = df_result.swifter.progress_bar(False).apply(lambda x: x['City2'] if x['City4']=='' else x['City4'], axis=1) 



            index_to_correct=df_result[df_result['City2']!=''].index
            for i in index_to_correct:
                geoloc.loc[len(geoloc)] = [df_result.loc[i,'province'],df_result.loc[i,'City'], df_result.loc[i,'City2']]
                df.loc[df_result.loc[i,'Indexes'], 'City2']=df_result.loc[i,'City2']



            #### Last check: using geogratis.gc.ca

            #keep the indexes that we were not able to find a macth for
            df_result=df_result.loc[df_result[df_result['City2']==''].index, ['province', 'City', 'Indexes']]
            df_result.reset_index(inplace = True, drop = True)

            df_result[['City2', 'index_to_drop']]= df_result[['City', 'province']].copy().swifter.progress_bar(False).apply(lambda info : scrape_url_geogratis(info), axis=1, result_type="expand")

            index_to_correct=df_result[df_result['City2']!=''].index
            for i in index_to_correct:
                df.loc[df_result.loc[i,'Indexes'], 'City2']=df_result.loc[i,'City2']


            index_to_drop=df_result[df_result['City2']==''].index

            cities_to_drop=[]
            df_result.loc[index_to_drop,'Indexes'].apply(cities_to_drop.extend)

            #print('############')
            #print(df_result.loc[index_to_drop,:])
            #print('############')







        #print('Indexes of cities to drop:',cities_to_drop)
        #print(df.loc[cities_to_drop,['City', 'province']])
        df.loc[cities_to_drop,'City2']=''
        df.drop(cities_to_drop, inplace=True)
        df = df.reset_index(drop=True)
    



    else:
        pass

    #print(df['City2'])
    #print(df.shape)
    logger.info(f"Cleaning cities....Done")
    logger.info(df)
    logger.info(f"size is  {df.shape}")

    return df, geoloc, extra_geo

def get_decoder(df, DECODED_PATH):

    aws = AWSHandler()
    
    # only decode the observation with missing values in transmission, bodystyle, fueltype and drivetrain
    result = df[(df['Vehicle_information.transmission'].str.strip()=='')|(df['Vehicle_information.transmission'].str.strip()=='-')
                |(df['Vehicle_information.transmission'].isnull())|(df['Vehicle_information.transmission'].str.contains('Other|Unsure', na=False))
                |(df['Vehicle_information.drivetrain'].str.strip()=='')|(df['Vehicle_information.drivetrain'].str.strip()=='-')
                |(df['Vehicle_information.drivetrain'].isnull())|(df['Vehicle_information.drivetrain'].str.contains('Other|Unsure', na=False))
                |(df['Vehicle_information.fuel_type'].str.strip()=='')|(df['Vehicle_information.fuel_type'].str.strip()=='-')
                |(df['Vehicle_information.fuel_type'].isnull())|(df['Vehicle_information.fuel_type'].str.contains('Other|Unsure', na=False))
                |(df['Vehicle_information.body_style'].str.strip()=='')|(df['Vehicle_information.body_style'].str.strip()=='')
                |(df['Vehicle_information.body_style'].isnull())|(df['Vehicle_information.body_style'].str.contains('Other|Unsure', na=False))]
    

    # This vin decoder will block you after a certain number of attempts
    # This is why we can't apply // scraping and we should add. some waiting time between requests
    #print('This part of the code may take some time until it is done running')

    try:
        decoder=aws.download_object_as_csv(DECODED_PATH).drop(columns='Unnamed: 0')
        VIN_Missing=list(set(result['id'])-set(decoder['vin'])-set(result['url']))
        #print(len(VIN_Missing), ' VINS are missing')
    except:
        try:
            decoder=aws.download_object_as_csv(DECODED_PATH)
            VIN_Missing=list(set(result['id'])-set(decoder['vin'])-set(result['url']))
            #print(len(VIN_Missing), ' VINS are missing')
        except:
            VIN_Missing=list(set(result['id'])-set(result['url']))
            decoder=pd.DataFrame(columns=['vin', 'Series2', 'Series','Trim'])

    if len(VIN_Missing) == 0:
        df=pd.merge(df, decoder, left_on="id", right_on="vin", how="left", sort=False)
        return df, decoder

    else:
        cardict={}

        k=-1
        for vin in VIN_Missing:
            k=k+1
            if str(vin)=='nan' or ' ' in vin or 'http' in vin:
                pass
            else:
                try:
                    page = requests.get("https://vpic.nhtsa.dot.gov/api//vehicles/DecodeVinValues/" + vin + "?format=xml")
                    soup = bs(page.text, 'html.parser')

                    Dict={}
                    Dict['vin']=soup.vin.text
                    Dict['Trim']=soup.trim.text
                    Dict['Series']=soup.series.text
                    Dict['Series2']=soup.series2.text
                    Dict['Model Year']=soup.modelyear.text
                    Dict['Model']=soup.model.text
                    Dict['Make']=soup.make.text
                    Dict['Body Class']=soup.bodyclass.text
                    Dict['Drive Type']=soup.drivetype.text
                    Dict['Transmission Style']=soup.transmissionstyle.text
                    Dict['Fuel Type - Primary']=soup.fueltypeprimary.text
                    cardict[vin] = Dict



                    decoder=pd.concat([decoder, pd.DataFrame.from_dict(cardict, orient='index')], axis =0).reset_index().drop('index', axis=1)
                    cardict={}

                except:
                    time.sleep(randint(1500, 2000))


            if k%500==0 and k!=0:
                #print(decoder)
                aws.upload_csv_object(decoder,DECODED_PATH)
                #print('Number of decoded VINs', k)
                #print('Progress %',round(k*100/len(VIN_Missing)))
                time.sleep(randint(60, 80))


        aws.upload_csv_object(decoder,DECODED_PATH)
        #print(decoder)
        df=pd.merge(df, decoder, left_on="id", right_on="vin", how="left", sort=False)

        return df, decoder







def lookup_fueltype(info):
    i = info.name
    year = info[0]
    make = info[1]
    model_to_keep = info[2]
    trim_to_keep = info[3]
    fueltype = info[4]
    
    possible_fuel=list(set(trims_kbb[(trims_kbb['year']==year) & (trims_kbb['make']==make) & (trims_kbb['model']==model_to_keep) & (trims_kbb['trim_mod']==trim_to_keep)]['fuel_type']))
    if len(possible_fuel)==1:

        fueltype=possible_fuel[0]
        ind_to_drop='Found'


    elif fueltype in possible_fuel:

        fueltype=fueltype
        ind_to_drop='Found'

    else: 
        fueltype=''
        ind_to_drop=i
    
    return fueltype, ind_to_drop



def fueltype_last_try(info):
    
    fueltype = info
    
    if str(fueltype) in ['Diesel', 'Electric', 'Gas', 'Hybrid', 'Natural Gas']:
        fueltype = fueltype
    else:
        fueltype =''
        
        
    return fueltype


def clean_fueltype(df):

    # #Cleans up fuel type
    # # =============================================================================
    #print ("Cleaning fuel Type... ")
    logger.info(f"Cleaning fuel type....")

    
    
    #print("Fueltype: Target initial set: ", set(trims_kbb['fuel_type']))
    
    
    #print("Vin decoder initial set for fuel type: ", set(df['Vehicle_information.fuel_type']))
    
    
    df['Fuel Type - Primary'].replace(to_replace=r'Hybrid Gas/Electric', value='Hybrid', regex=True, inplace=True)
    df['Fuel Type - Primary'].replace(to_replace=r'.*Hybrid.*', value='Hybrid', regex=True, inplace=True)
    df['Fuel Type - Primary'].replace(to_replace=r'.*Diesel.*', value='Diesel', regex=True, inplace=True)
    df['Fuel Type - Primary'].replace(to_replace=r'.*Natural.*', value='Natural Gas', regex=True, inplace=True)
    df['Fuel Type - Primary'].replace(to_replace=r'.*Electric.*', value='Electric', regex=True, inplace=True)
    df['Fuel Type - Primary'].replace(to_replace=r'.*Gas Engine.*', value='Gas', regex=True, inplace=True)
    df['Fuel Type - Primary'].replace(to_replace=r'.*Ethanol.*', value='Gas', regex=True, inplace=True)
    df['Fuel Type - Primary'].replace(to_replace=r'.*Flexible Fuel.*', value='Gas', regex=True, inplace=True)
    df['Fuel Type - Primary'].replace(to_replace=r'.*Fuel Cell.*', value='Natural Gas', regex=True, inplace=True)
    df['Fuel Type - Primary'].replace(to_replace=r'.*Gasoline.*', value='Gas', regex=True, inplace=True)

    #print("Initial set: ", set(df['Vehicle_information.fuel_type']))
    df['Vehicle_information.fuel_type'] = df['Vehicle_information.fuel_type'].str.strip()
    df['Vehicle_information.fuel_type'] = df['Vehicle_information.fuel_type'].str.title()
    
    df['Vehicle_information.fuel_type'].replace(to_replace=r'.*Hybrid.*', value='Hybrid', regex=True, inplace=True)
    df['Vehicle_information.fuel_type'].replace(to_replace=r'.*Electric.*', value='Electric', regex=True, inplace=True)
    df['Vehicle_information.fuel_type'].replace(to_replace=r'.*Gasoline.*', value='Gas', regex=True, inplace=True)
    df['Vehicle_information.fuel_type'].replace(to_replace=r'.*Natural Gas.*', value='Natural Gas', regex=True, inplace=True)
    df['Vehicle_information.fuel_type'].replace(to_replace=r'.*Diesel.*', value='Diesel', regex=True, inplace=True)
    df['Vehicle_information.fuel_type'].replace(to_replace=r'.*Unleaded.*', value='Gas', regex=True, inplace=True)
    df['Vehicle_information.fuel_type'].replace(to_replace=r'.*Gasoline.*', value='Gas', regex=True, inplace=True)
    df['Vehicle_information.fuel_type'].replace(to_replace=r'.*Flex.*', value='Gas', regex=True, inplace=True)
    df['Vehicle_information.fuel_type'].replace(to_replace='Compressed Natural Gas', value='Natural Gas', inplace=True)

    #print("Initial Fuel Type set: ", set(df['Vehicle_information.fuel_type']))

    #print('Cleaning fuelstyle: Find indexes to drop')
    df[['Vehicle_information.fuel_type','ind_to_drop']]=df[['year','make', 'model_to_keep', 'trim_to_keep' ,'Vehicle_information.fuel_type' ]].copy().swifter.progress_bar(False).apply(lambda info : lookup_fueltype(info), axis=1, result_type="expand")
    #print(df)

    fuel_to_drop=list(set(df['ind_to_drop']))
    try:
        fuel_to_drop.remove('Found')
    except:
        pass
    
    new_df=pd.DataFrame()
    new_df['Vehicle_information.fuel_type'] = df.loc[fuel_to_drop,'Fuel Type - Primary' ].copy().swifter.progress_bar(False).apply(lambda info : fueltype_last_try(info), axis=1)
    #print(new_df)                                                                                                          
    df.update(new_df)


    
    
    fuel_to_drop=df[df['Vehicle_information.fuel_type']==''].index
    
    #print(df.loc[fuel_to_drop,:])  
    df.drop(fuel_to_drop, inplace=True)
    df.reset_index(inplace=True, drop=True)    


    df.drop(df[df['Vehicle_information.fuel_type'].isnull()].index, inplace=True)
    df.drop(df[df['Vehicle_information.fuel_type']=='Other'].index, inplace=True)
    df.drop(df[df['Vehicle_information.fuel_type']=='Other/Don’t Know'].index, inplace=True)
    df.reset_index(inplace=True, drop=True)
    #print ("Cleaning Fuel Style: Done.\n")  
    #print("Final fuel Type set: ", set(df['Vehicle_information.fuel_type']))

    df = df[df['Vehicle_information.fuel_type'].isin(['Diesel', 'Electric', 'Gas', 'Hybrid', 'Natural Gas'])]
    df.reset_index(inplace=True, drop=True)

    #print ("size", df.shape)

    logger.info(f"Cleaning fuel type....Done")
    logger.info(df['Vehicle_information.fuel_type'].unique())
    logger.info(f"size is  {df.shape}")


    return df
    

def lookup_bodystyle(info):
    i = info.name
    year = info[0]
    make = info[1]
    model_to_keep = info[2]
    trim_to_keep = info[3]
    bodystyle = info[4]
    
    possible_styles=list(set(trims_kbb[(trims_kbb['year']==year) & (trims_kbb['make']==make) & (trims_kbb['model']==model_to_keep) & (trims_kbb['trim_mod']==trim_to_keep)]['body_style']))
    adjustments=list(set(trims_kbb[(trims_kbb['year']==year) & (trims_kbb['make']==make) & (trims_kbb['model']==model_to_keep) & (trims_kbb['trim_mod']==trim_to_keep)]['trim1']))

    adjust_list = [trim1.split()[-1] for trim1 in adjustments]

    if len(possible_styles)==1:

        bodystyle_to_keep=possible_styles[0]
        ind_to_drop='Found'



    elif bodystyle in possible_styles:

        bodystyle_to_keep=bodystyle
        ind_to_drop='Found'


    elif (bodystyle in adjust_list) and (bodystyle in list(set(trims_kbb['body_style']))):
        
        bodystyle_to_keep=bodystyle
        ind_to_drop='Found'


    else: 
        bodystyle_to_keep=''
        ind_to_drop=i
    
    return bodystyle_to_keep, ind_to_drop



def bodystyle_last_try(info):
    
    bodyclass = info
    
    if str(bodyclass) in ['Convertible','Coupe','Hatchback','Pickup','Sedan','Suv','Van/Minivan','Wagon']:
        bodystyle_to_keep = bodyclass
    else:
        bodystyle_to_keep =''
        
        
    return bodystyle_to_keep
        
    
    

def clean_bodytype(df):

    #print ("Cleaning bodystyle... (6/11)")
    logger.info(f"Cleaning body style....")

    # #Look for information for body style
    ## check body style
    #print("Let's start by cleaning the scraped bodystyles from decoded data")
    #print('Inital bidystyles from decoded data is : ', set(df['Body Class']))

    df['Body Class'].replace(to_replace='Roadster', value='Convertible', regex=True, inplace=True)
    df['Body Class'].replace(to_replace='Convertible/Cabriolet', value='Convertible', regex=True, inplace=True)
    df['Body Class'].replace(to_replace='Station Wagon', value='Wagon', regex=True, inplace=True)
    #df['Vehicle_information.body_style'].replace(to_replace='Hatch', value='Hatchback', regex=True, inplace=True)

    df['Body Class'].replace(to_replace=r'.*Crossover Utility Vehicle.*', value='Suv', regex=True, inplace=True)
    df['Body Class'].replace(to_replace=r'Hatchback/Liftback/Notchback', value='Hatchback', regex=True, inplace=True)
    df['Body Class'].replace(to_replace=r'.*Sport Utility Vehicle.*', value='Suv', regex=True, inplace=True)
    df['Body Class'].replace(to_replace=r'Sedan/Saloon', value='Sedan', regex=True, inplace=True)

    df['Body Class'].replace(to_replace='Cargo Van', value='TBD', regex=True, inplace=True)
    df['Body Class'].replace(to_replace=r'Cargo Van', value='TBD', regex=True, inplace=True)
    df['Body Class'].replace(to_replace=r'Minivan', value='TBD', regex=True, inplace=True)
    df['Body Class'].replace(to_replace=r'Van', value='TBD', regex=True, inplace=True)
    df['Body Class'].replace(to_replace=r'Passenger Van', value='TBD', regex=True, inplace=True)
    df['Body Class'].replace(to_replace=r'TBD', value='Van/Minivan', regex=True, inplace=True)


    df['Body Class'].replace(to_replace=r'.*Chassis Cab.*', value='Pickup', regex=True, inplace=True)
    df['Body Class'].replace(to_replace=r'.*cab.*', value='Pickup', regex=True, inplace=True)
    df['Body Class'].replace(to_replace=r'.*crew.*', value='Pickup', regex=True, inplace=True)
    df['Body Class'].replace(to_replace=r'.*Sport Utility Truck.*', value='Pickup', regex=True, inplace=True)
    df['Body Class'].replace(to_replace=r'Pickup Truck', value='Pickup', regex=True, inplace=True)
    df['Body Class'].replace(to_replace=r'Truck', value='Pickup', regex=True, inplace=True)


    df['Body Class'].replace(to_replace=r'Limousine', value='Sedan', regex=True, inplace=True)
    #print('Final bodystyles set from the scpared data', set(df['Body Class']))

    #print('Target Body Style set', set(trims_kbb['body_style']))

    #print('Initial Body Style set', set(df['Vehicle_information.body_style']))

    df['Vehicle_information.body_style']=df['Vehicle_information.body_style'].str.title()
    df['body_style_to_keep']=df['Vehicle_information.body_style']


    df['Vehicle_information.body_style'] = df['Vehicle_information.body_style'].str.strip()
    df['Vehicle_information.body_style'].replace(to_replace='Avant', value='Wagon', regex=True, inplace=True)
    df['Vehicle_information.body_style'].replace(to_replace='Roadster', value='Convertible', regex=True, inplace=True)
    df['Vehicle_information.body_style'].replace(to_replace='Cabriolet', value='Convertible', regex=True, inplace=True)
    df['Vehicle_information.body_style'].replace(to_replace='Station Wagon', value='Wagon', regex=True, inplace=True)
    df['Vehicle_information.body_style'].replace(to_replace='Crew Cab', value='Truck', regex=True, inplace=True)
    df['Vehicle_information.body_style'].replace(to_replace=r'.*Cab.*', value='Truck', regex=True, inplace=True)
    df['Vehicle_information.body_style'].replace(to_replace=r'.*Crew.*', value='Truck', regex=True, inplace=True)
    df['Vehicle_information.body_style'].replace(to_replace=r'.*cab.*', value='Truck', regex=True, inplace=True)
    df['Vehicle_information.body_style'].replace(to_replace=r'.*crew.*', value='Truck', regex=True, inplace=True)
    df['Vehicle_information.body_style'].replace(to_replace=r'Koup', value='Coupe', regex=True, inplace=True)
    df['Vehicle_information.body_style'].replace(to_replace=r'Cargo Van', value='Van', regex=True, inplace=True)
    df['Vehicle_information.body_style'].replace(to_replace='Truck', value='Pickup', regex=True, inplace=True)
    df['Vehicle_information.body_style'].replace(to_replace='Van', value='TBD', regex=False, inplace=True)
    df['Vehicle_information.body_style'].replace(to_replace='Minivan', value='TBD', regex=False, inplace=True)
    df['Vehicle_information.body_style'].replace(to_replace='TBD', value='Van/Minivan', regex=False, inplace=True)
    df['Vehicle_information.body_style'].replace(to_replace=r'.*Hatchback.*', value='Hatchback', regex=True, inplace=True)
    
    
    #tqdm_notebook().pandas(desc="Cleaning bodystyle")
    #print('Cleaning bodystyle: Find indexes to drop')
    df[['body_style_to_keep','ind_to_drop']]=df[['year','make', 'model_to_keep', 'trim_to_keep' ,'Vehicle_information.body_style' ]].copy().swifter.progress_bar(False).apply(lambda info : lookup_bodystyle(info), axis=1, result_type="expand")
    #print(df)

    style_to_drop=list(set(df['ind_to_drop']))
    try:
        style_to_drop.remove('Found')
    except:
        pass
    
    new_df=pd.DataFrame()
    new_df['body_style_to_keep'] = df.loc[style_to_drop,'Body Class' ].copy().swifter.progress_bar(False).apply(lambda info : bodystyle_last_try(info), axis=1)
    #print('new_df, check indexes' )
    #print(new_df)
    df.update(new_df)


    
    
    style_to_drop=df[df['body_style_to_keep']==''].index
    
    #print(df.loc[style_to_drop,:])  
    df.drop(style_to_drop, inplace=True)
    df.drop(df[df['body_style_to_keep'].isnull()].index, inplace=True)
    df.reset_index(inplace=True, drop=True)

    df = df[df['body_style_to_keep'].isin(['Convertible','Coupe','Hatchback','Pickup','Sedan','Suv','Van/Minivan','Wagon'])]
    df.reset_index(inplace=True, drop=True)    

    #print ("Cleaning Body Style: Done.\n")  

    #print ("size", df.shape)

    #print("Final body style set:",set(df['body_style_to_keep']))
    logger.info(f"Cleaning body style....Done")
    logger.info(df['body_style_to_keep'].unique())
    logger.info(f"size is  {df.shape}")

    
    return df



def lookup_drivetrain(info):
    i = info.name
    year = info[0]
    make = info[1]
    model_to_keep = info[2]
    trim_to_keep = info[3]
    fueltype=info[4]
    bodystyle_to_keep = info[5]
    drivetrain = info[6]
    
    possible_drivetrain=list(set(trims_kbb[(trims_kbb['year']==year) & (trims_kbb['make']==make) & (trims_kbb['model']==model_to_keep) & (trims_kbb['trim_mod']==trim_to_keep)& (trims_kbb['fuel_type']==fueltype)& (trims_kbb['body_style']==bodystyle_to_keep)]['drive_train']))


    if len(possible_drivetrain)==1:

        drivetrain=possible_drivetrain[0]
        ind_to_drop='Found'



    elif drivetrain in possible_drivetrain:

        drivetrain=drivetrain
        ind_to_drop='Found'


    else: 
        drivetrain=''
        ind_to_drop=i
    
    return drivetrain, ind_to_drop

def drivetrain_last_try(info):
    
    drivetrain = info
    
    if str(drivetrain) in ['FWD', '4WD', 'AWD', 'RWD']:
        drivetrain = drivetrain
    else:
        drivetrain =''
        
        
    return drivetrain


def clean_drivetrain(df):
    # Drivetrain
    # =============================================================================
    #print ("Cleaning drivetrain... ")
    logger.info(f"Cleaning drivetrain...")



    #print('Target Drivetrain set', set(trims_kbb['drive_train']))
    
    #print ("Drivetrain initial set: ", set(df['Drive Type']))
    df['Drive Type'].replace(to_replace=r'2WD/4WD', value='', regex=True, inplace=True)
    df['Drive Type'].replace(to_replace=r'.*4WD.*', value='4WD', regex=True, inplace=True)
    df['Drive Type'].replace(to_replace=r'.*4x2.*', value='', regex=True, inplace=True)
    df['Drive Type'].replace(to_replace=r'.*FWD.*', value='FWD', regex=True, inplace=True)
    df['Drive Type'].replace(to_replace=r'.*RWD.*', value='RWD', regex=True, inplace=True)
    df['Drive Type'].replace(to_replace=r'.*AWD.*', value='AWD', regex=True, inplace=True)

    #print ("Drivetrain initial set: ", set(df['Vehicle_information.drivetrain']))
    
    df['Vehicle_information.drivetrain'] = df['Vehicle_information.drivetrain'].str.strip()
    df['Vehicle_information.drivetrain'] = df['Vehicle_information.drivetrain'].str.title()

    df['Vehicle_information.drivetrain'].replace(to_replace='4x4', value='4WD', regex=True, inplace=True)
    df['Vehicle_information.drivetrain'].replace(to_replace='4X4', value='4WD', regex=True, inplace=True)
    
    
    #print('Cleaning drivetrain: Find indexes to drop')
    df[['Vehicle_information.drivetrain','ind_to_drop']]=df[['year','make', 'model_to_keep', 'trim_to_keep' ,'Vehicle_information.fuel_type','body_style_to_keep','Vehicle_information.drivetrain' ]].copy().swifter.progress_bar(False).apply(lambda info : lookup_drivetrain(info), axis=1, result_type="expand")
    #print(df)

    drivetrain_to_drop=list(set(df['ind_to_drop']))
    try:
        drivetrain_to_drop.remove('Found')
    except:
        pass

    new_df=pd.DataFrame()
    new_df['Vehicle_information.drivetrain'] = df.loc[drivetrain_to_drop,'Drive Type' ].copy().swifter.progress_bar(False).apply(lambda info : drivetrain_last_try(info), axis=1)
    #print('new_df, check indexes' )
    #print(new_df)
    df.update(new_df)


    
    
    drivetrain_to_drop=df[df['Vehicle_information.drivetrain']==''].index


    #print(df.loc[drivetrain_to_drop,:])  
    df.drop(drivetrain_to_drop, inplace=True)
    df.reset_index(inplace=True, drop=True) 


    df = df.loc[df['Vehicle_information.drivetrain'] != '']
    df = df[~df['Vehicle_information.drivetrain'].isnull()]
    df = df.reset_index(drop=True)

    df = df[df['Vehicle_information.drivetrain'].isin(['FWD', '4WD', 'AWD', 'RWD'])]
    df.reset_index(inplace=True, drop=True)
    #print ("Cleaning Drivetrain: Done.\n") 

    #print ("size", df.shape)

    #print("Final set: ",set(df['Vehicle_information.drivetrain']))
    logger.info(f"Cleaning drivetrain....Done")
    logger.info(df['Vehicle_information.drivetrain'].unique())
    logger.info(f"size is  {df.shape}")
    
    return df


def lookup_transmission(info):
    i = info.name
    year = info[0]
    make = info[1]
    model_to_keep = info[2]
    trim_to_keep = info[3]
    fueltype=info[4]
    bodystyle_to_keep = info[5]
    drivetrain = info[6]
    transmission = info[7]
    
    possible_transmission=list(set(trims_kbb[(trims_kbb['year']==year) & (trims_kbb['make']==make) & (trims_kbb['model']==model_to_keep) & (trims_kbb['trim_mod']==trim_to_keep)& (trims_kbb['fuel_type']==fueltype)& (trims_kbb['body_style']==bodystyle_to_keep)& (trims_kbb['drive_train']==drivetrain)]['transmission']))


    if len(possible_transmission)==1:

        transmission=possible_transmission[0]
        ind_to_drop='Found'



    elif transmission in possible_transmission:

        transmission=transmission
        ind_to_drop='Found'


    else: 
        transmission=''
        ind_to_drop=i
    
    return transmission, ind_to_drop


def transmission_last_try(info):
    
    transmission = info
    
    if str(transmission) in ['Manual', 'Automatic']:
        transmission = transmission
    else:
        transmission =''
        
        
    return transmission



def clean_transmission(df):
        # Transmission
    # =============================================================================
    #print ("Cleaning transmission... )")
    logger.info(f"Cleaning transmission....")


    #print ("Transmission initial set: ", set(df['Vehicle_information.transmission']))

    df['Vehicle_information.transmission']=df['Vehicle_information.transmission'].str.title()

    df['Vehicle_information.transmission'].replace(to_replace=r'.*Automatic.*', value='Automatic', regex=True, inplace=True)
    df['Vehicle_information.transmission'].replace(to_replace=r'.*Manual.*', value='Manual', regex=True, inplace=True)
    df['Vehicle_information.transmission'].replace(to_replace=r'.*Cvt.*', value='Automatic', regex=True, inplace=True)

    
    #print("Transmission decoded initial set: " ,set(df['Transmission Style']))
    df['Transmission Style'].replace(to_replace=r'.*Automatic.*', value='Automatic', regex=True, inplace=True)
    df['Transmission Style'].replace(to_replace=r'.*Automatic (Automatic).*', value='Automatic', regex=True, inplace=True)
    df['Transmission Style'].replace(to_replace=r'.*Manual.*', value='Manual', regex=True, inplace=True)
    df['Transmission Style'].replace(to_replace='Continuously Variable Transmission', value='Automatic', regex=True, inplace=True)
    df['Transmission Style'].replace(to_replace='CVT', value='Automatic', regex=True, inplace=True)
    df['Transmission Style'].replace(to_replace=r'.*Dual Clutch.*', value='Automatic', regex=True, inplace=True)
    df['Transmission Style'].replace(to_replace=r'.*Dual Clutch.*', value='Automatic', regex=True, inplace=True)
    #print('cleaning again')
    df['Transmission Style'].replace(to_replace=r'.*Automatic \(Automatic\).*', value='Automatic', regex=True, inplace=True)
    df['Transmission Style'].replace(to_replace=r'.*Electronic Continuously Variable.*', value='Automatic', regex=True, inplace=True)
    df['Transmission Style'].replace(to_replace=r'.*Dual-Clutch Transmission.*', value='Automatic', regex=True, inplace=True)
    #print(set(df['Transmission Style']))


    #print('Cleaning transission: Find indexes to drop')
    df[['Vehicle_information.transmission','ind_to_drop']]=df[['year','make', 'model_to_keep', 'trim_to_keep' ,'Vehicle_information.fuel_type','body_style_to_keep','Vehicle_information.drivetrain', 'Vehicle_information.transmission' ]].copy().swifter.progress_bar(False).apply(lambda info : lookup_transmission(info), axis=1, result_type="expand")
    #print(df)

    transmission_to_drop=list(set(df['ind_to_drop']))
    try:
        transmission_to_drop.remove('Found')
    except:
        pass

    new_df=pd.DataFrame()
    new_df['Vehicle_information.transmission'] = df.loc[transmission_to_drop,'Transmission Style' ].copy().swifter.progress_bar(False).apply(lambda info : transmission_last_try(info), axis=1)
    #print('new_df, check indexes' )
    #print(new_df)
    df.update(new_df)


    
    
    transmission_to_drop=df[df['Vehicle_information.transmission']==''].index

    #print(df.loc[transmission_to_drop,:])  
    df.drop(transmission_to_drop, inplace=True)
    df.reset_index(inplace=True, drop=True) 



    df.drop(df[df['Vehicle_information.transmission']==''].index, inplace=True)
    df.drop(df[df['Vehicle_information.transmission'].isnull()].index, inplace=True)
    df.drop(df[df['Vehicle_information.transmission']=='Other'].index, inplace=True)
    df.drop(df[df['Vehicle_information.transmission']=='N/A'].index, inplace=True)
    df.drop(df[df['Vehicle_information.transmission']=='Not Available'].index, inplace=True)
    df.reset_index(inplace=True, drop=True)

    df = df[df['Vehicle_information.transmission'].isin(['Manual', 'Automatic'])]
    df.reset_index(inplace=True, drop=True) 
    #print ("Cleaning Transmission: Done.\n") 
    #print('Size: ', df.shape)
    #print("Final set: ",set(df['Vehicle_information.transmission']))
    logger.info(f"Cleaning transmission....Done")
    logger.info(df['Vehicle_information.transmission'].unique())
    logger.info(f"size is  {df.shape}")

    return df



def clean_condition_price(df):
        # Condition
    # =============================================================================
    ##print("Initial condition set: ", set(df['condition']))
    logger.info(f"Cleaning conditions and prices....")


    df['condition']=df['condition'].str.title()
    df['condition']=df['condition'].str.strip()
    #print("Current conditions:", set(df['condition']))


    # replace null values for used cars
#     today = datetime.today()
#     df.loc[df[(df['condition'].isnull()) & (df['kilometers'] >= 15000) & (df['year']< today.year)].index,'condition'] = 'Used'
    df['new=0, certified=1, used=2'] = df['condition'].replace({'New':0, 'Used':2, 'Certified Used':1})


    df = df.loc[df['new=0, certified=1, used=2'] != '']
    df = df[~df['new=0, certified=1, used=2'].isnull()]
    df = df.reset_index(drop=True)

    Q=df[df['new=0, certified=1, used=2']==0]['kilometers']

    df.iloc[Q[Q>=500].index,:]
    #print(Q.shape, df.iloc[Q[Q>=500].index,:])


    df.drop(df.iloc[Q[Q>=500].index,:].index, inplace=True)
    df.reset_index(inplace=True, drop=True)

    #print ("Cleaning condition: Done.\n")

    df.drop(df[df['price']==0].index, inplace=True)
    df.reset_index(inplace=True, drop=True)
    #print ("Cleaning prices: Done.\n") 

    #print(df.shape)
    logger.info(f"Cleaning conditions and prices....Done")
    logger.info(df['new=0, certified=1, used=2'].unique())
    logger.info(f"size is  {df.shape}")

    return df




def clean_colours(df):
    # Colours
    # =============================================================================

    #print ("Replacing and Cleaning Colours... ")
    logger.info(f"Cleaning colours....")


    #print('Initial color set: ', set(df['Vehicle_information.exterior_colour']))

    df = df[~df['Vehicle_information.exterior_colour'].isnull()]
    df = df.reset_index(drop=True)

    df['Vehicle_information.exterior_colour']=df['Vehicle_information.exterior_colour'].str.title()
    df['Vehicle_information.exterior_colour'].replace(to_replace=' Exterior', value='', regex=True, inplace=True)

    #print("Initial Exterior Colour set: ", set(df['Vehicle_information.exterior_colour']))
    df['Vehicle_information.exterior_colour'] = df['Vehicle_information.exterior_colour'].str.strip()

    ##### Lexus
    df['Vehicle_information.exterior_colour'].replace(to_replace='Claret Mica', value='Burgundy', regex=True, inplace=True)
    df['Vehicle_information.exterior_colour'].replace(to_replace='Nightfall Mica', value='Blue', regex=True, inplace=True)
    df['Vehicle_information.exterior_colour'].replace(to_replace='Redline', value='Red', regex=True, inplace=True)
    df['Vehicle_information.exterior_colour'].replace(to_replace='Obsidian', value='Black', regex=True, inplace=True)
    df['Vehicle_information.exterior_colour'].replace(to_replace='Satin Cashmere Metallic', value='Cream', regex=True, inplace=True)
    df['Vehicle_information.exterior_colour'].replace(to_replace='Autumn Shimmer', value='Brown', regex=True, inplace=True)
    df['Vehicle_information.exterior_colour'].replace(to_replace='Caviar', value='Black', regex=True, inplace=True)
    df['Vehicle_information.exterior_colour'].replace(to_replace='Nebula Grey Pearl', value='Grey', regex=True, inplace=True)
    df['Vehicle_information.exterior_colour'].replace(to_replace='Manganese Lustre', value='Silver', regex=True, inplace=True)
    df['Vehicle_information.exterior_colour'].replace(to_replace='Ultrasonic Blue Mica W/ Black', value='Blue', regex=True, inplace=True)



    ##### Mercedes
    df['Vehicle_information.exterior_colour'].replace(to_replace='Amg Solarbeam', value='Yellow', regex=True, inplace=True)



    df['Vehicle_information.exterior_colour'].replace(to_replace=r'.*Red.*', value='Red', regex=True, inplace=True)
    df['Vehicle_information.exterior_colour'].replace(to_replace=r'.*Rouge.*', value='Red', regex=True, inplace=True)
    df['Vehicle_information.exterior_colour'].replace(to_replace=r'.*Burgundy.*', value='Burgundy', regex=True, inplace=True)
    df['Vehicle_information.exterior_colour'].replace(to_replace=r'.*Bordeaux.*', value='Burgundy', regex=True, inplace=True)
    df['Vehicle_information.exterior_colour'].replace(to_replace=r'.*Burgandy.*', value='Burgundy', regex=True, inplace=True)
    df['Vehicle_information.exterior_colour'].replace(to_replace=r'.*Velvet.*', value='Red', regex=True, inplace=True)
    df['Vehicle_information.exterior_colour'].replace(to_replace=r'.*Rosso Corsa.*', value='Red', regex=True, inplace=True)
    df['Vehicle_information.exterior_colour'].replace(to_replace=r'.*Bourguogne.*', value='Burgundy', regex=True, inplace=True)
    df['Vehicle_information.exterior_colour'].replace(to_replace=r'.*Purple.*', value='Purple', regex=True, inplace=True)
    df['Vehicle_information.exterior_colour'].replace(to_replace=r'.*Violet.*', value='Purple', regex=True, inplace=True)
    df['Vehicle_information.exterior_colour'].replace(to_replace=r'.*Blue.*', value='Blue', regex=True, inplace=True)
    df['Vehicle_information.exterior_colour'].replace(to_replace=r'.*Bleu.*', value='Blue', regex=True, inplace=True)
    df['Vehicle_information.exterior_colour'].replace(to_replace=r'.*Indigo.*', value='Blue', regex=True, inplace=True)
    df['Vehicle_information.exterior_colour'].replace(to_replace=r'.*Navy.*', value='Blue', regex=True, inplace=True)
    df['Vehicle_information.exterior_colour'].replace(to_replace=r'.*Darkb Lue.*', value='Blue', regex=True, inplace=True)
    df['Vehicle_information.exterior_colour'].replace(to_replace=r'.*Aqua.*', value='Blue', regex=True, inplace=True)
    df['Vehicle_information.exterior_colour'].replace(to_replace=r'.*Pearl.*', value='Pearl', regex=True, inplace=True)
    df['Vehicle_information.exterior_colour'].replace(to_replace=r'.*Bianco.*', value='White', regex=True, inplace=True)
    df['Vehicle_information.exterior_colour'].replace(to_replace=r'.*White.*', value='White', regex=True, inplace=True)
    df['Vehicle_information.exterior_colour'].replace(to_replace=r'.*Beige.*', value='Cream', regex=True, inplace=True)
    df['Vehicle_information.exterior_colour'].replace(to_replace=r'.*Cream.*', value='Cream', regex=True, inplace=True)
    df['Vehicle_information.exterior_colour'].replace(to_replace=r'.*Blanc.*', value='White', regex=True, inplace=True)
    df['Vehicle_information.exterior_colour'].replace(to_replace=r'.*Blanc.*', value='White', regex=True, inplace=True)
    df['Vehicle_information.exterior_colour'].replace(to_replace=r'.*Black.*', value='Black', regex=True, inplace=True)
    df['Vehicle_information.exterior_colour'].replace(to_replace=r'.*Noir.*', value='Black', regex=True, inplace=True)
    df['Vehicle_information.exterior_colour'].replace(to_replace=r'.*Ebony.*', value='Black', regex=True, inplace=True)
    df['Vehicle_information.exterior_colour'].replace(to_replace=r'.*Ébène.*', value='Black', regex=True, inplace=True)
    df['Vehicle_information.exterior_colour'].replace(to_replace=r'.*Argent.*', value='Silver', regex=True, inplace=True)
    df['Vehicle_information.exterior_colour'].replace(to_replace=r'.*Silver.*', value='Silver', regex=True, inplace=True)
    df['Vehicle_information.exterior_colour'].replace(to_replace=r'.*Grey.*', value='Grey', regex=True, inplace=True)
    df['Vehicle_information.exterior_colour'].replace(to_replace=r'.*Gris.*', value='Grey', regex=True, inplace=True)
    df['Vehicle_information.exterior_colour'].replace(to_replace=r'.*Gray.*', value='Grey', regex=True, inplace=True)
    df['Vehicle_information.exterior_colour'].replace(to_replace=r'.*Granite.*', value='Grey', regex=True, inplace=True)
    df['Vehicle_information.exterior_colour'].replace(to_replace=r'.*Charcoal.*', value='Grey', regex=True, inplace=True)
    df['Vehicle_information.exterior_colour'].replace(to_replace=r'.*Graphite.*', value='Grey', regex=True, inplace=True)
    df['Vehicle_information.exterior_colour'].replace(to_replace=r'.*Metal.*', value='Metallic', regex=True, inplace=True)
    #df['Vehicle_information.exterior_colour'].replace(to_replace=r'.*Met.*', value='Metallic', regex=True, inplace=True)
    df['Vehicle_information.exterior_colour'].replace(to_replace=r'.*Steel.*', value='Metallic', regex=True, inplace=True)
    df['Vehicle_information.exterior_colour'].replace(to_replace=r'.*Platinum.*', value='Platinum', regex=True, inplace=True)
    df['Vehicle_information.exterior_colour'].replace(to_replace=r'.*Taupe.*', value='Taupe', regex=True, inplace=True)
    df['Vehicle_information.exterior_colour'].replace(to_replace=r'.*Tan.*', value='Tan', regex=True, inplace=True)
    df['Vehicle_information.exterior_colour'].replace(to_replace=r'.*Teal.*', value='Green', regex=True, inplace=True)
    df['Vehicle_information.exterior_colour'].replace(to_replace=r'.*Yellow.*', value='Yellow', regex=True, inplace=True)
    df['Vehicle_information.exterior_colour'].replace(to_replace=r'.*Orange.*', value='Orange', regex=True, inplace=True)
    df['Vehicle_information.exterior_colour'].replace(to_replace=r'.*Rose.*', value='Pink', regex=True, inplace=True)
    df['Vehicle_information.exterior_colour'].replace(to_replace=r'.*Pink.*', value='Pink', regex=True, inplace=True)
    df['Vehicle_information.exterior_colour'].replace(to_replace=r'.*Green.*', value='Green', regex=True, inplace=True)
    df['Vehicle_information.exterior_colour'].replace(to_replace=r'.*Sand.*', value='Sand', regex=True, inplace=True)
    df['Vehicle_information.exterior_colour'].replace(to_replace=r'.*Bronze.*', value='Bronze', regex=True, inplace=True)
    df['Vehicle_information.exterior_colour'].replace(to_replace=r'.*Copper.*', value='Copper', regex=True, inplace=True)
    df['Vehicle_information.exterior_colour'].replace(to_replace=r'.*Gold.*', value='Gold', regex=True, inplace=True)
    df['Vehicle_information.exterior_colour'].replace(to_replace=r'.*Brown.*', value='Brown', regex=True, inplace=True)
    df['Vehicle_information.exterior_colour'].replace(to_replace=r'.*Maroon.*', value='Brown', regex=True, inplace=True)
    df['Vehicle_information.exterior_colour'].replace(to_replace=r'.*Olive.*', value='Green', regex=True, inplace=True)

    list_of_colors=['Red','Burgundy', 'Purple',  'Blue', 'Pearl', 'White', 'Cream', 'Silver', 
                    'Black', 'Grey', 'Metallic','Platinum',  'Yellow', 'Orange', 'Pink', 'Green', 'Sand',
                'Bronze', 'Copper', 'Gold', 'Brown', 'Tan', 'Taupe']
 

    list_ind_other_colors=list(df[~df['Vehicle_information.exterior_colour'].isin(list_of_colors)].index)
            
    #print(df.loc[list_ind_other_colors, :])        
    df=df.drop(list_ind_other_colors)
    df.reset_index(inplace=True, drop=True)


    #print("Final Exterior Colour set: ",set(df['Vehicle_information.exterior_colour']))
    #print ("Cleaning Colors: Done.\n")
    #print ("size", df.shape)

    logger.info(f"Cleaning colours....Done")
    logger.info(df['Vehicle_information.exterior_colour'].unique())
    logger.info(f"size is  {df.shape}")



    return df


def clean_numowners_previousaccident(df):
    
    logger.info(f"Cleaning owners and accidents....")

    #print('Cleaning number of Owners....')
    #print('New cars have 0 owners')
    df.loc[df[df['new=0, certified=1, used=2'] == 0].index, 'NumOwners'] = 0

    ################################################

    #print('Cleaning number of Previous Accidents....')
    #print('if yes return 1 , if no return 0')

    prev_acc={}
    for i in list(set(df['PrevAccident'])):
        if str(i)!='nan':
            if i==0 :
                prev_acc[0]=0
            else:
                prev_acc[i]=1

    #print(prev_acc)

    df['PrevAccident'] = df['PrevAccident'].replace(prev_acc)

    df = df.loc[df['PrevAccident'] != '']
    df = df[~df['PrevAccident'].isnull()]
    df = df.reset_index(drop=True)

    df['PrevAccident']=df['PrevAccident'].astype(int)

    #print ("Cleaning NumOwners: Done.\n") 
    #print(df.shape)
    logger.info(f"Cleaning owners and accidents....Done")
    logger.info(f"size is  {df.shape}")

    return df



def last_formatting(df):
    
    logger.info(f"Last formatting...")


    df['City']=df['City2'].copy()

    #print(df[df['date_added'] == ''].index)

    df.reset_index(inplace=True, drop=True)
    df.drop(df[df['date_added'].isnull()].index, inplace=True)
    df.reset_index(inplace=True, drop=True)


    ##print (dfW1[dfW1['date_removed'] == pd.NaT].index)

    #transform to datetime
    df['date_added'] =pd.to_datetime(df['date_added'], utc=True)
    df['date_added'] = df['date_added'].dt.date
    

     #Exporting data before dealing with time columns
    dfWO=df.copy()

    dfWO=dfWO[['id',"date_added", 'make', 'model_to_keep', 'year', 'kilometers', 'new=0, certified=1, used=2', 'price', 'province', 'City', 
         'Vehicle_information.transmission','Vehicle_information.drivetrain','body_style_to_keep',
         'Vehicle_information.exterior_colour','Vehicle_information.fuel_type','trim_to_keep']]
    dfWO.rename(columns={'date_added':"scraped_first_time",'model_to_keep':'model','trim_to_keep':"Vehicle_information.trim", "body_style_to_keep":"Vehicle_information.body_style"}, inplace=True)

    #print(dfWO)


    #print ("Last formatting steps and exporting file with time (duration column)...")

    #Exporting data with time column
    # # =============================================================================
    #print('Extra step')

    dfW1=df.copy()
    dfW1=df[['id',"date_added",'date_removed', 'make', 'model_to_keep', 'year', 'kilometers', 'new=0, certified=1, used=2', 'price', 'province', 'City', 
         'Vehicle_information.transmission','Vehicle_information.drivetrain','body_style_to_keep',
         'Vehicle_information.exterior_colour','Vehicle_information.fuel_type','trim_to_keep','price_history']]

    dfW1.rename(columns={'model_to_keep':'model','trim_to_keep':"Vehicle_information.trim", "body_style_to_keep":"Vehicle_information.body_style"}, inplace=True)
    

    #print (dfW1[dfW1['date_removed'] == ''].index)
    dfW1.drop((dfW1[dfW1['date_removed'] == ''].index), inplace=True)
    dfW1.reset_index(inplace=True, drop=True)
    dfW1.drop(dfW1[dfW1['date_added'].isnull()].index, inplace=True)
    dfW1.reset_index(inplace=True, drop=True)
    #print(dfW1[dfW1['date_removed'].isnull()].index)
    dfW1.drop(dfW1[dfW1['date_removed'].isnull()].index, inplace=True)
    dfW1.reset_index(inplace=True, drop=True)

    ##print (dfW1[dfW1['date_removed'] == pd.NaT].index)
    if dfW1.empty:
        #print('empty dataset')
        dfW1 = pd.DataFrame()
        
    else:

    #transform to datetime
        dfW1['date_added'] = dfW1['date_added'].apply(pd.to_datetime)
        dfW1['date_removed'] = dfW1['date_removed'].apply(pd.to_datetime)
        #calculate duration
        dfW1['duration'] = dfW1['date_removed']-dfW1['date_added']
        # keep the number of days in an integer format
        dfW1['duration'] = dfW1['duration'].dt.days.astype(int)



        dfW1=dfW1[['id',"date_added",'date_removed', 'make', 'model', 'year', 'kilometers', 'new=0, certified=1, used=2', 'price', 'province', 'City', 
                'Vehicle_information.transmission','Vehicle_information.drivetrain',"Vehicle_information.body_style",
                'Vehicle_information.exterior_colour','Vehicle_information.fuel_type','Vehicle_information.trim', 'duration','price_history']]

        dfW1.rename(columns={'date_added':"scraped_first_time",'date_removed':"scraped_last_time"}, inplace=True)

        #print(dfW1)

    return dfWO, dfW1


# def export_files(dfWO, dfW1, EXPORT_PATH_TIMELESS, EXPORT_PATH_TIME,):

#     sys.path.insert(1, MODULE_FULL_PATH)
#     aws = AwsHandler()
#     try:
#         dfcleaned_timeless = aws.download_object_as_csv(EXPORT_PATH_TIMELESS)
#         dfcleaned_timeless.rename(columns = {'_id': "id"}, inplace = True)

#         try:
#             dfcleaned_timeless['scraped_first_time'] =pd.to_datetime(dfcleaned_timeless['scraped_first_time'], utc=True)
#             dfcleaned_timeless['scraped_first_time'] = dfcleaned_timeless['scraped_first_time'].dt.date
#         except:
#             pass


#         df = pd.concat([dfcleaned_timeless, dfWO], axis = 0, sort = False)
#         df.drop([column for column in df.columns if "Unnamed: 0" in column ], axis = 1, inplace = True)
        
#         list_subset=list(df.columns)
#         try:
#             list_subset.remove('id')
#             list_subset.remove('scraped_first_time')    
#         except:
#             pass

#         df.drop_duplicates(subset=list_subset, keep='last', inplace=True)
#         df.reset_index(inplace = True, drop = True)
        
#         aws.upload_csv_object(df, EXPORT_PATH_TIMELESS)
#         #print(df.shape)
#     except:
#         aws.upload_csv_object(dfWO, EXPORT_PATH_TIMELESS)
#         #print(dfWO.shape)

#     try:
#         dfcleaned_time = aws.download_object_as_csv(EXPORT_PATH_TIME)
#         dfcleaned_time.rename(columns = {'_id':"id"}, inplace = True)

#         try:
#             dfcleaned_time['scraped_first_time'] = pd.to_datetime(dfcleaned_time['scraped_first_time'], utc=True)
#             dfcleaned_time['scraped_first_time'] = dfcleaned_time['scraped_first_time'].dt.date
#         except:
#             pass

#         df_time = pd.concat([dfcleaned_time, dfW1], axis = 0, sort = False)
#         df_time.drop([column for column in df_time.columns if "Unnamed: 0" in column ], axis = 1, inplace=True)
        
#         list_subset=list(df_time.columns)
#         try:
#             list_subset.remove('id')
#             list_subset.remove('scraped_first_time')    
#         except:
#             pass
            
#         df_time.drop_duplicates(subset=list_subset, keep='last', inplace=True)
#         df_time.reset_index(inplace = True, drop = True)
        
#         aws.upload_csv_object(df_time, EXPORT_PATH_TIME)
#         #print(df_time.shape)
#     except:
#         aws.upload_csv_object(dfW1, EXPORT_PATH_TIME)
#         #print(dfW1.shape)



def export_files(DF1, link1):

    logger.info(f"Exporting files...")


    aws = AWSHandler()
    try:
        dfcleaned_timeless = aws.download_object_as_csv(link1)
        dfcleaned_timeless.rename(columns = {'_id': "id"}, inplace = True)
        #print(dfcleaned_timeless.shape)

        try:
            dfcleaned_timeless['scraped_first_time'] =pd.to_datetime(dfcleaned_timeless['scraped_first_time'], utc=True)
            dfcleaned_timeless['scraped_first_time'] = dfcleaned_timeless['scraped_first_time'].dt.date
        except:
            pass


        df = pd.concat([dfcleaned_timeless, DF1], axis = 0, sort = False)
        df.drop([column for column in df.columns if "Unnamed: 0" in column ], axis = 1, inplace = True)
        
        list_subset=list(df.columns)
        try:
            list_subset.remove('id')
            list_subset.remove('scraped_first_time')    
        except:
            pass

        df.drop_duplicates(subset=list_subset, keep='last', inplace=True)
        df.reset_index(inplace = True, drop = True)
        
        aws.upload_csv_object(df, link1)
        #print(df.shape)
        logger.info(f"size is  {df.shape}")
    except:
        aws.upload_csv_object(DF1, link1)
        #print(DF1.shape)
        logger.info(f"size is  {DF1.shape}")




    #print('DONE.')
    logger.info(f"DONE.")


    
    

 
    


    


    


    
@handle_exceptions(logger_name, email_subject, email_toaddrs)     
def main():

    RESULT_PATH = 'MasterCode1/scraping/autocan/autocan_result.csv'
    DECODED_PATH = "MasterCode1/cleaning/autocan/decoded.csv"
    KBB_PATH = "MasterCode1/cleaning/kbb_trims/more_formatting_kbb.csv"
    LOCATIONS_PATH = "MasterCode1/cleaning/data/all_geo.csv"
    GEOLOC_PATH = "MasterCode1/cleaning/data/geoloc.csv"
    EXTRA_GEO_PATH = "MasterCode1/cleaning/data/extra_geo.csv"
    # PROGRESS_PATH = 'MasterCode1/cleaning/autocan/cleaned_autocan_progress.csv'
    EXPORT_PATH_TIMELESS = "MasterCode1/cleaning/autocan/cleaned_autocan_timeless.csv"
    EXPORT_PATH_TIME = 'MasterCode1/cleaning/autocan/cleaned_autocan_time.csv'

    
    
    global trims_kbb
    global locations
    global extra_geo
    global geoloc
    df, dfcleaned, trims_kbb, locations, geoloc, extra_geo = import_files(RESULT_PATH, KBB_PATH, LOCATIONS_PATH, GEOLOC_PATH , EXTRA_GEO_PATH, EXPORT_PATH_TIMELESS)
    start_time = time.time()
    df = initial_formatting(df, dfcleaned)
    df, trims_kbb = clean_makes(df, trims_kbb)
    # upload_progress(df,  PROGRESS_PATH)
    
    df,  trims_kbb = clean_models(df, trims_kbb)
    # upload_progress(df,  PROGRESS_PATH)
    df, decoder = get_decoder(df,  DECODED_PATH)
    df, trims_kbb = clean_trims(df, trims_kbb)
    # upload_progress(df,  PROGRESS_PATH)

    df = clean_provinces(df)
    df, geoloc, extra_geo = clean_cities(df )
    export_files(geoloc, GEOLOC_PATH)
    export_files(extra_geo, EXTRA_GEO_PATH)
    


    df = clean_bodytype(df )

    df = clean_fueltype(df )

    # upload_progress(df,  PROGRESS_PATH)

    df = clean_drivetrain(df )
    df = clean_transmission(df)
    df = clean_colours(df)

    df = clean_condition_price(df)
    
    ##df = clean_numowners_previousaccident(df)
    dfWO, dfW1 = last_formatting(df)

    export_files(dfWO, EXPORT_PATH_TIMELESS)
    export_files(dfW1, EXPORT_PATH_TIME)
    
    #print("--- %s seconds ---" % (time.time() - start_time))
    logger.info(f"--- {time.time() - start_time} seconds ---")
    
    
    

if __name__ == '__main__':



    main()
