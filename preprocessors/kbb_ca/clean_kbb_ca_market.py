import os
import logging
import time
import re

import pandas as pd
import numpy as np


from base.aws_handler import AWSHandler
# aws = AWSHandler()
# Necessary imports for logging
from base import log_config     # DO NOT REMOVE: this might seem as if it is not used anywhere but still keep this import
from base.logging_decorator import handle_exceptions

# Set global variables for logger and emails (change this according to your needs)
logger_name = 'kbb_market_cleaner_bots_canada'
email_subject = 'Kbb Market Cleaner Bot Alert'
email_toaddrs = ['sana@qodemedia.com']
logger = logging.getLogger(__name__)
scraper_dir = os.path.dirname(os.path.abspath(__file__))    # full path to the scraper dir where this script is placed


# Function to convert a list to a string
def listToString(s):  
    str1 = " " 
    return (str1.join(s))

def import_file( RESULT_PATH):
    #print('Importing file')
    logger.info(f"Initial importing...")
    aws = AWSHandler()
    df=aws.download_object_as_csv(RESULT_PATH)

    logger.info(f"Initial importing...Done")
    logger.info(df)
    logger.info(f"size is  {df.shape}")
    return df

def initial_formatting(df):
    logger.info(f"Initial formatting...")
    # General Formatting
    df['make']=df['make'].str.title()
    df['model']=df['model'].str.title()
    df['trim1']=df['trim1'].str.title()
    df['trim2']=df['trim2'].str.title()
    df['year']=df['year'].astype(int)
    df['trim_mod']=df['trim1'].copy()

    df['trim_mod'].replace(to_replace='Manual', value='',regex=True, inplace=True)
    df['trim_mod'].replace(to_replace='Automatic', value='',regex=True, inplace=True)
    df['trim_mod'].replace(to_replace='Cpe', value='', regex=True, inplace=True)
    df['trim_mod'].replace(to_replace='Sdn', value='',regex=True, inplace=True)
    #trims_autotrader['trim'].replace(to_replace='Man', value='',regex=True, inplace=True)
    #trims_autotrader['trim'].replace(to_replace='Auto', value='', regex=True,inplace=True)
    #trims_autotrader['trim'].replace(to_replace='Hb', value='', regex=True,inplace=True)
    #trims_autotrader['trim'].replace(to_replace='Wgn', value='', regex=True,inplace=True)
    df['trim_mod'].replace(to_replace='Suv', value='', regex=True,inplace=True)
    #trims_autotrader['trim'].replace(to_replace='Awd', value='', regex=True,inplace=True)
    df['trim_mod'].replace(to_replace='Fwd', value='', regex=True,inplace=True)
    df['trim_mod'].replace(to_replace='4Wd', value='', regex=True,inplace=True)
    df['trim_mod'].replace(to_replace='2Wd', value='', regex=True,inplace=True)
    df['trim_mod'].replace(to_replace='4x4', value='', regex=True,inplace=True)
    df['trim_mod'].replace(to_replace='4X4', value='', regex=True,inplace=True)
    df['trim_mod'].replace(to_replace='4X2', value='', regex=True,inplace=True)
    df['trim_mod'].replace(to_replace='2Wd', value='', regex=True,inplace=True)
    df['trim_mod'].replace(to_replace='Rwd', value='', regex=True,inplace=True)
    df['trim_mod'].replace(to_replace='Sedan', value='', regex=True,inplace=True)
    df['trim_mod'].replace(to_replace='Coupe', value='', regex=True,inplace=True)
    df['trim_mod'].replace(to_replace='Cabriolet', value='', regex=True,inplace=True)
    df['trim_mod'].replace(to_replace='Truck', value='', regex=True,inplace=True)

    #trims_autotrader['trim'].replace(to_replace='Quad', value='', regex=True,inplace=True)
    df['trim_mod'].replace(to_replace='Package', value='Pkg', regex=True,inplace=True)
    df['trim_mod'].replace(to_replace='Packages', value='Pkg', regex=True,inplace=True)
    df['trim_mod'].replace(to_replace='Technology', value='Tech', regex=True,inplace=True)


    df['trim_mod'].replace(to_replace='2Dr', value='', regex=True,inplace=True)
    df['trim_mod'].replace(to_replace='3Dr', value='', regex=True,inplace=True)
    df['trim_mod'].replace(to_replace='4Dr', value='', regex=True,inplace=True)
    df['trim_mod'].replace(to_replace='5Dr', value='', regex=True,inplace=True)
    df['trim_mod'].replace(to_replace='Cvt', value='', regex=True,inplace=True)

    df['trim_mod'].replace(to_replace='\*Ltd Avail\*', value='Ltd Avail', regex=True, inplace=True)
    df['trim_mod'].replace(to_replace='\*Ltd Avai', value='Ltd Avail',regex=True, inplace=True)



    df['trim_mod'].replace(to_replace='Limited', value='Ltd', regex=True, inplace=True)
    df['trim_mod'].replace(to_replace='Navigation', value='Navi',regex=True, inplace=True)
    df['trim_mod'].replace(to_replace=' Ed\.', value=' Ed', regex=True, inplace=True)
    df['trim_mod'].replace(to_replace='Edition', value='Ed', regex=True, inplace=True)


    df['trim_mod'].replace('&Amp;','&', regex=True,inplace=True)

    #### remove those words with truncating them from words
    list_to_remove=['Conv','Wgn','Man','Auto','Suv' ,'Hb', 'Wagon' ]

    for k in df.index:

        temp=df.loc[k,'trim_mod'].split()
        trim=[]
        for word in temp:
            if word not in list_to_remove:
                trim.append(word)

        df.loc[k,'trim_mod']=listToString(trim)

        if k%1000==0:
            logger.info(f"Removing abreviations from trims  {np.round(k*100/len(df))}") 


    return df


def remove_years_from_trims(df):

    logger.info(f"removing years from trims...")
    for i in df.index:
        temp=df.loc[i, "trim_mod"].split()
        for word in temp:
            if word==str(df.loc[i, "year"]) or word==str(df.loc[i, "year"])+' 5' or word==str(df.loc[i, "year"])+'.5':
                df.loc[i, 'trim_mod']=df.loc[i, 'trim_mod'].replace(word, '')
                df.loc[i, "trim_mod"] = df.loc[i,"trim_mod"].lstrip().rstrip()
                if df.loc[i, "trim_mod"]=='':
                    df.loc[i, "trim_mod"]='Base'

    return df


def removes_drivetrain_from_trims(df):

    logger.info(f"Removing drivetrain from trims...")
    # if the make is not acura, we should remove awd from the trim
    # the reason we are exclusing acura is that acura has trims called Sh-Awd

    # if the trims_autotrader[i,'trim_mod'] is blank after all these operations, we should keep Base as a trim

    for i in df.index:
        if df.loc[i,'make']!='Acura':
            df.loc[i,'trim_mod']=df.loc[i,'trim_mod'].replace('Awd', '')
        
        if df.loc[i, 'trim_mod']=='':
            df.loc[i, 'trim_mod']='Base'
        
        # in case of Acura and the trim is not Sh Awd
        if 'Awd' == df.loc[i,'trim_mod'] and len(df.loc[i,'trim_mod'].split('-'))==1:
            df.loc[i, 'trim_mod']='Base'
        if 'Awd' in df.loc[i,'trim_mod'] and 'Sh-Awd' not in df.loc[i,'trim_mod']:
            df.loc[i,'trim_mod']=df.loc[i,'trim_mod'].replace('Awd', '')

    return df



def standard_trims_acura(df):

    logger.info(f"Standardizing trims for Acura...")

    for i in df[(df['make']=='Acura')].index:
        df.loc[i,'trim_mod']=df.loc[i,'trim_mod'].replace('Type S', 'Type-S')
        df.loc[i,'trim_mod']=df.loc[i,'trim_mod'].strip()
    return df


def get_bodystyle(df):

    logger.info(f"Isolating bodystyle...")
    df['body_style']=[x[0].title() if str(x[0])!='nan' else np.nan for x in df['trim2'].str.split()]
    #print(df['body_style'].unique())
    return df



def get_transmission(df):

    logger.info(f"Isolating transmission...")
    df['transmission']=[x[-1].title() if str(x[-1])!='nan' else np.nan for x in df['trim2'].str.split()]
    #print(df['transmission'].unique())
    return df


def get_drivetrain(df):

    logger.info(f"Isolating drivetrain...")
    df['drive_train']=[listToString(x[-4:-1]) if str(x[-4:-1])!='nan' else np.nan for x in df['trim2'].str.split()]
    df['drive_train'].replace(to_replace='All Wheel Drive', value='AWD', regex=True, inplace=True)
    df['drive_train'].replace(to_replace='Four Wheel Drive', value='4WD', regex=True, inplace=True)
    df['drive_train'].replace(to_replace='Front Wheel Drive', value='FWD', regex=True, inplace=True)
    df['drive_train'].replace(to_replace='Rear Wheel Drive', value='RWD', regex=True, inplace=True)
    #print(df['drive_train'].unique())
    return df



def get_fueltype(df):
    
    logger.info(f"Isolating fueltype...")
    df['fuel_type']=[listToString(x[-5:-4]) if str(x[-5:-4])!='nan' else np.nan for x in df['trim2'].str.split()]
    df['fuel_type'].replace(to_replace=r'.*Cng.*', value='Natural Gas', regex=True, inplace=True)
    df['fuel_type'].replace(to_replace='Flex', value='Gas', regex=True, inplace=True)
    df['fuel_type'].replace(to_replace='Gasoline', value='Gas', regex=True, inplace=True)
    #print(df['fuel_type'].unique())
    return df


def clean_up_pickup_van_data(df):


    logger.info(f"Cleaning up pickup and van data...")
    Pickup_data=df[df['body_style']=='Pickup']

    Listcodes=["Double Cab Standard Box","Reg Cab Standard Box","Reg Cab Long Box","Crew Cab Short Box",
           "Crew Cab Standard Box","Crew Cab Short Box", "Long Bed", "Standard Bed", "Single Cab",
           "W/1Lt",'W/2Lt',"W/1Lz", "W/1Wt", "W/2Wt","W/1Lz","W/2Lz","W/1Sa","W/1Sd","W/1Sc",
           'W/1Sb','W/1Sh', 'W/1Sj',"1Sb","1Sc","1Se","1Sf","1Sh","1Sj","1Sw",'C6P',
           "Crewmax","Doublecab","Access Cab","Mega Cab","Double Cab","Ext Cab","Reg Cab","Crew Cab","Club Cab",
           "Quad Cab","Mega Cab", "Supercab","Supercrew","Supercab Flareside", "Cab Plus", "Cab Plus4", "King Cab"]
    # remove things like 
    #8' Box
    #5.5' Box
    #6.75' Box
    #5'7&Quot; Box
    df['trim_mod'].replace(to_replace=r"\d(\.\d{1,3})?(\d)?\'(\d\&Quot\;)?\sBox", value='', regex=True, inplace=True)

    for ind in Pickup_data.index:
        df.loc[ind,"trim_mod"]=df.loc[ind,"trim_mod"].strip()
        trim_list=df.loc[ind,"trim_mod"].split('"')
        
        if len(trim_list)==2 :
            df.loc[ind,"trim_mod"]=trim_list[1].strip()
        elif len(trim_list)==3:
            df.loc[ind,"trim_mod"]=df.loc[ind,"trim_mod"].split()[0]
        elif '"' in df.loc[ind,"trim_mod"] and len(trim_list)==1:
            df.loc[ind,"trim_mod"]='Base'
        
        for words in Listcodes:
            if words in df.loc[ind,"trim_mod"]:
                df.loc[ind,"trim_mod"]=df.loc[ind,"trim_mod"].replace(words, '').replace("  "," ").strip()
                df.loc[ind,"trim_mod"]=df.loc[ind,"trim_mod"].replace(words, '').replace("  "," ").strip()
        
        
        if df.loc[ind,"trim_mod"]=="" or df.loc[ind,"trim_mod"]==" ":
            df.loc[ind,"trim_mod"]='Base'


        #print(ind, df.loc[ind,"trim1"],"==========>",df.loc[ind,"trim_mod"])


    van_data=df[df['body_style']=='Van/Minivan']

    # for ind in van_data.index:
    #     #print(ind,'===',df.loc[ind,"make"],'===',df.loc[ind,"model"],'===',df.loc[ind,"year"],'===' ,df.loc[ind,"trim_mod"])


    # stripping patters such as 
    #135"
    #111.2"
    df['trim_mod'].replace(to_replace=r'\d{1,3}(\.\d{0,3})?\"', value='', regex=True, inplace=True)
    # remove the double space after stripping patterns such as 135"
    df['trim_mod']=df['trim_mod'].str.replace("  ", " ")
    # for ind in van_data.index:
    #     #print(ind,'===',df.loc[ind,"make"],'===',df.loc[ind,"model"],'===',df.loc[ind,"year"],'===' ,df.loc[ind,"trim_mod"])
    #     pass

    return df



def remove_code_patterns_duplicates(df):
    logger.info(f"Removing duplicates...")
    # remove all the codes with the following patterns
    #"W/1Lt",'W/2Lt',"W/1Lz", "W/1Wt", "W/2Wt","W/1Lz","W/2Lz","W/1Sa","W/1Sd","W/1Sc",'W/1Sb','W/1Sh', 'W/1Sj
    # we also strip the strings from extra white spaces and if the trim becomes empty, we replace it with Base
    forb=['1St', '2Nd', '3Rd']
    for i in df.index:
        if re.search('W[/]\d[A-z]{2}',df.loc[i,"trim_mod"]) and not(any(ordinal in df.loc[i,"trim_mod"] for ordinal in forb)):
            df.loc[i,"trim_mod"]=re.sub(r'W[/]\d[A-z]{2}', "", df.loc[i,"trim_mod"]).strip().replace("  "," ")
            if df.loc[i,"trim_mod"]=="":
                df.loc[i,"trim_mod"]='Base'
            #print(i,'===',df.loc[i,"make"],'===',df.loc[i,"model"],'===',df.loc[i,"year"],'===',df.loc[i,"trim1"],'===' ,df.loc[i,"trim_mod"])

    df.drop_duplicates(inplace=True)
    df.reset_index(drop=True, inplace=True)
    logger.info(f"size is  {df.shape}")

    return df


def fix_saab_models(df):
    logger.info(f"Fix Saab models...")
    ## Fix that some Saab models are turning into dates rather than strings 03-Sep ==> 3-Sep
    df_Saab=df[(df['make']=='Saab')&((df['model']=='03-Sep')|(df['model']=='05-Sep'))]
    df_Saab

    for ind in df_Saab.index:
        df.loc[ind,'model']=df.loc[ind,'model'].replace('03-Sep', '3-Sep')
        df.loc[ind,'model']=df.loc[ind,'model'].replace('05-Sep', '5-Sep')

    return df



def final_formatting(df):
    # remove "Wb" and "Ltd Avail"
    logger.info(f"Final formatting...")
    df['trim_mod'].replace(to_replace='Ltd Avail', value='', regex=True, inplace=True)
    for i in df.index:
        if df.loc[i,'trim_mod']=="":
            df.loc[i,'trim_mod']='Base'


    for i in df.index:
        df.loc[i,'trim_mod']=df.loc[i,'trim_mod'].replace("  ", " ").strip()
        listtrim=df.loc[i,'trim_mod'].split()
        #print(listtrim)
        list_trim_to_keep=listtrim
        if "Wb" in listtrim:
            list_trim_to_keep.remove("Wb")
            #print(list_trim_to_keep)
            if list_trim_to_keep==[]:
                df.loc[i,'trim_mod']='Base'
            else:
                df.loc[i,'trim_mod']=listToString(list_trim_to_keep).strip()

                
            
            #print(i,'===',df.loc[i,"make"],'===',df.loc[i,"model"],'===',df.loc[i,"year"],'===',df.loc[i,"trim1"],'===' ,df.loc[i,"trim_mod"])

    #print(df)
    #print(df.shape)
    logger.info(f"size is  {df.shape}")

    return df



def export_files(dataframe, link):


    logger.info(f"Exporting files...")      
    aws = AWSHandler()

    dataframe.drop_duplicates( inplace=True)
    dataframe.reset_index(inplace = True, drop = True)
        
    aws.upload_csv_object(dataframe, link)
    logger.info(f"size is  {dataframe.shape}")
    #print(dataframe.shape)


@handle_exceptions(logger_name, email_subject, email_toaddrs)
def main():
    start_time = time.time()
    RESULT_PATH = 'MasterCode1/cleaning/kbb_trims/new_kbb_mm.csv'
    EXPORT_PATH = 'MasterCode1/cleaning/kbb_trims/more_formatting_kbb.csv'
    df=import_file( RESULT_PATH)
    df=initial_formatting(df)
    df=remove_years_from_trims(df)
    df=removes_drivetrain_from_trims(df)
    df=standard_trims_acura(df)
    df=get_bodystyle(df)
    df=get_transmission(df)
    df=get_drivetrain(df)
    df=get_fueltype(df)
    df=clean_up_pickup_van_data(df)
    df=remove_code_patterns_duplicates(df)
    df=fix_saab_models(df)
    df=final_formatting(df)
    export_files(df, EXPORT_PATH)

    #print("--- %s seconds ---" % (time.time() - start_time))
    logger.info(f"--- {time.time() - start_time} seconds ---")

if __name__ == '__main__':
    main()




            
        
       
        
        
    
    


    



                
















