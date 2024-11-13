import logging
import time
import sys
import os
import re

import pandas as pd
from decouple import config
from bs4 import BeautifulSoup
from decouple import config
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import Select
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support import expected_conditions as EC

from base import log_config
from base.aws_handler import AWSHandler
from base.logging_decorator import handle_exceptions

logger_name = 'kbb_ca_mm'
email_subject = 'kbb_ca_mm Bot Alert'
email_toaddrs = ['jordan@qodemedia.com', 'karan@qodemedia.net', 'bikin@nerdplatoon.com']
logger = logging.getLogger(__name__)
scraper_dir = os.path.dirname(os.path.abspath(__file__))

local_file_path = os.path.join(os.path.join(scraper_dir, "results"), "new_kbb_mm.csv")
aws_bucket_file_path = "MasterCode1/cleaning/kbb_trims/new_kbb_mm.csv"
aws_bucket_folder_path = "MasterCode1/cleaning/kbb_trims"
website_url = 'https://www.kbb.ca/en-ca/trade-in-value/'

@handle_exceptions(logger_name, email_subject, email_toaddrs)
def main():
    
    aws_handler = AWSHandler()
    #if not os.path.exists(local_file_path):
    aws_handler.download_from_aws(aws_bucket_file_path, local_file_path)
    
    df_old = pd.read_csv(local_file_path)
    #df_old = df_old.iloc[:, 1:]
    
    list_of_dicts=[]
    
    options = Options()
    options.add_argument('--headless')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-gpu')
    driver = webdriver.Chrome(chrome_options=options, executable_path=config('CHROME_DRIVER'))
    driver.get(website_url)
    
    #Scrapes all info
    soup = BeautifulSoup(driver.page_source, 'html.parser')
    makes = soup.find_all('select', {'id':'selectBrand'})
    time.sleep(5)
    for make in makes:
        make = re.findall('"(.+?)">(.+?)</option',str(make))
    make = make[1:]
    make_nums, makes = zip(*make)
    
    for x in makes:
        WebDriverWait(driver, 5).until(EC.presence_of_element_located((By.ID,f"selectBrand")))
        select = Select(driver.find_element(By.ID, 'selectBrand'))
        select.select_by_visible_text(x)
    
        #Get models
        time.sleep(2)
    
        soup = BeautifulSoup(driver.page_source, 'html.parser')
        models = soup.find_all('select', {'id':'selectModel'})
        for model in models:
            model = re.findall('<option id="(.+?)">(.+?)</option',str(model))
    
        model_list =[]
        for mod in model:
            model_list.append(mod[-1])
    
        for model in model_list[1:]:
            try:
                select = Select(driver.find_element(By.ID, 'selectModel'))
                model=str(model).strip('"').strip("'").replace('&amp;', '&')
                select.select_by_visible_text(model)
    
                #Get years
                time.sleep(2)
                soup = BeautifulSoup(driver.page_source, 'html.parser')
                years = soup.find_all('select', {'id':'selectYear'})
                # print(years)
    
                for year in years:
                    year = re.findall('">(.+?)</option',str(year))
                year = year[1:]
                # print(year)
                for z in year:
                    try:
                        select = Select(driver.find_element(By.ID, 'selectYear'))
                        select.select_by_visible_text(z)
    
                        #Get trims
                        time.sleep(2)
                        soup = BeautifulSoup(driver.page_source, 'html.parser')
                        trims = soup.find_all('select', {'id':'selectVersion'})
                        # print(trims)
    
                        for trim in trims:
                            try:
                                trim = re.findall('label=(.+?)><option(.+?)/option>',str(trim))
                                trim1, trim2 = zip(*trim)
                                trim2 = re.findall('value="(.+?)">(.+?)<', str(trim2))
                                trim3, trim2 = zip(*trim2)
    
                                for a in trim1:
                                    #Put it all together
                                    list_of_dicts.append({'make':x, 'model':model, 'year': z, \
                                                    'trim1': a, 'trim2': (trim2[trim1.index(a)])})
                                    # print({'make':x, 'model':model, 'year': z, \
                                    #                 'trim1': a, 'trim2': (trim2[trim1.index(a)])})
                            except Exception:
                                pass
                    except Exception:
                        pass
            except Exception:
                pass
        df=pd.DataFrame(list_of_dicts)
    
    driver.quit()

    df['trim1'] = df['trim1'].str.strip('"')
    df['trim1'] = df['trim1'].str.strip("'")
    df['model'] = df['model'].str.replace('&amp;', '&')
    df['trim1'] = df['trim1'].str.replace('&amp;', '&')
    df['trim2'] = df['trim2'].str.replace('&amp;', '&')

    #Appending and cleaning up
    df = pd.concat([df_old, df]).drop_duplicates()
    
    df['new'] = df['make'].astype(str) + df['model'].astype(str) + df['year'].astype(str) + df['trim1'].astype(str) + df['trim2'].astype(str)
    df['new'] = df['new'].str.upper()
    df = df.drop_duplicates(subset=['new'],keep='first')
    df = df.drop(columns=['new'])

    #Get rid of French rows
    #df = df[df['trim2'].str.lower().str.contains('berline|vus|hayon|cabriolet|familiale|fourgonnette|camion')==False]
    df = df[df['trim2'].str.lower().str.contains('automatique|manuelle')==False]
    
    df['year'] = df['year'].astype(str)
    df = df.sort_values(by=['make','model','year','trim1','trim2'], key=lambda col: col.str.lower())
    
    df.to_csv(local_file_path, index=False)
    aws_handler.upload_to_aws(local_file_path, aws_bucket_folder_path)



if __name__ == "__main__":
    main()
