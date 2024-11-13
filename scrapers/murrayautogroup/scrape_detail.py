import os
import logging
from datetime import date
from concurrent.futures import ThreadPoolExecutor

import pandas as pd
from bs4 import BeautifulSoup
import undetected_chromedriver as uc
from decouple import config

options = uc.ChromeOptions()
options.headless=True
options.add_argument('--headless')
driver = uc.Chrome(options=options, version_main=config('CHROME_VERSION'))


# Necessary imports for logging
from .scrape_url import collect_url
from base import log_config
from base.logging_decorator import handle_exceptions


# Set global variables for logger and emails (change this according to your needs)
logger_name = 'murrayautogroup_scraper_bots_canada'
email_subject = 'murrayautogroup Details Scraper Bot Alert'
email_toaddrs = ['bikin@nerdplatoon.com', 'prabin@qodemedia.net', 'ajay.qode@gmail.com', 'jordan@qodemedia.com', 'karan@qodemedia.net']
logger = logging.getLogger(__name__)

from base.aws_handler import AWSHandler
aws_handler = AWSHandler()

scraper_dir = os.path.dirname(os.path.abspath(__file__))
local_file_path = os.path.join(os.path.join(scraper_dir, "results"), "murrayautogroup_info.csv")
aws_bucket_file_path = "MasterCode1/scraping/murrayautogroup/murrayautogroup.csv"
aws_bucket_folder_path = "MasterCode1/scraping/murrayautogroup"

columns = ['Vehicle_information.body_style','Vehicle_information.drivetrain','Vehicle_information.engine',
'Vehicle_information.exterior_colour','Vehicle_information.fuel_type','Vehicle_information.transmission','Vehicle_information.trim',
'_id','city','condition','date_added','date_removed','image_url','is_featured','kilometers','make','metadata','model',
'price','price_history','province','title','url','vin','year']


#converting list to dict
def Convert(lis):
    res_dct = {lis[i].split(' ')[0] : lis[i].split(' ')[-1] for i in range(0, len(lis))}
    return res_dct


def Convert_list(liss):
    li = {liss[i] : liss[i+1] for i in range(0, len(liss), 2)}
    return li


def url_feature():
    list_urls_fea = ['https://www.murrayautogroup.ca/featured-new-vehicles/',
                     'https://www.murrayautogroup.ca/featured-used-vehicles/']
    list_featured = []
    for url1 in list_urls_fea:
        driver.get(url1)
        list_url = driver.find_elements_by_xpath("//a[contains(@class,'g-vehicle-card__image gtm_vehicle_tile_cta')]")
        for l in list_url:
            list_featured.append(l.get_attribute('href'))
    return list_featured


def url_is_feature(url):
    if url in list_feature:
        is_featured = True
    else:
        is_featured = False
    return is_featured


#scraping all details of url 
def scrape_info(tup):
    global df_scrapee, list_feature, new_info 
    info = [None] * 25
    try:
        url = tup
    except:
        _, url = tup
    try:
        driver.get(str(url))
        soup = BeautifulSoup(driver.page_source, 'html.parser')
        Go = True
        try:
            title = soup.find('h1').text
        except:
            title = ''
        if title == 'New & Used vehicles for sale across Canada':
            Go = False

        if Go == True:
            try:
                stock = soup.find(class_="overview-group__content").text.replace(' #:', ':').strip().split(' ')
                ss = Convert_list(stock)
            except:
                pass
            try:
                deta = [i.text for i in soup.find_all(class_="detailed-specs__single-content")]
                tab = Convert(deta)

            except:
                pass
            try:
                loc = soup.find(class_="far fa-map-marker-alt icon").parent.text
            except:
                loc = ''
            
            try:
                price = soup.find(class_="convertus-dollar-sign").parent.text.replace('$', '').replace(',', '').strip()
            except:
                price = ''
            list_img = []
            try:
                l_img = driver.find_elements_by_class_name('thumbnails__image')
                for i in l_img:
                    url_img = i.get_attribute('style').replace('background-image: url("', '').replace('");', '').replace('133x100', '1024x786')
                    list_img.append(url_img)
            except:
                pass
            try:
                info[0]= tab.get('Body')
            except:
                info[0] = ''
            try:
                info[1]= tab.get('Drive')
            except:
                info[1] = ''
            try:
                info[2]= tab.get('Engine')
            except:
                info[2] = ''
            try:
                info[3]= tab.get('Exterior')
            except:
                info[3] = ''
            try:
                info[4]= tab.get('Fuel')
            except:
                info[4] = ''
            try:
                info[5]= tab.get('Transmission')
            except:
                info[5] = ''
            try:
                info[6]= tab.get('Trim')
            except:
                info[6] = ''
            try:
                info[7]= ss.get('Stock:')
            except:
                info[7] = ''
        
            info[8]= 'Brandon'
            try:
                info[9]= tab.get('Condition')
            except:
                info[9] = ''
            info[10]= str(date.today())
            try:
                info[12] = url_img
            except:
                info[12] = ''
            info[13] = url_is_feature(url)
            try:
                info[14]= tab.get('Kilometers')
            except:
                info[14] = ''
            try:
                info[15]= title.split(' ')[1]
            except:
                info[15] = ''
            metadata = {}
            try:
                metadata['Interior Colour'] = tab.get('Interior')
                metadata['Door'] = tab.get('Doors')
                metadata['Hwy Fuel Economy'] = tab.get('Hwy Fuel Economy')
                metadata['City Fuel Economy'] = tab.get('City Fuel Economy')
            except:
                pass
            info[16]= str(metadata)
            try:
                info[17]= title.split(' ')[2]
            except:
                info[17] = ''
            info[18]= price
            try:
                info[19]= str({str(date.today()) : price})
            except:
                info[19] = ''
            info[20]= 'MB'
            info[21]= title
            info[22]= url
            try:
                info[23] = ss.get('VIN:')
            except:
                info[23] = ''
            try:
                info[24]= title.split(' ')[0]
            except:
                info[24] = ''
            # print(info)
            new_info.append(info)
            i = len(new_info)
            if i%10 == 0:                                   
                df_scrapee = pd.DataFrame(new_info,columns= columns)
                df = pd.read_csv(local_file_path)
                try:
                    df = df.loc[:,~df.columns.str.match("Unnamed")]
                except:
                    pass
                df_i = pd.concat([df, df_scrapee], ignore_index=True)

                try:
                    df_i = df_i.loc[:,~df_i.columns.str.match("Unnamed")]
                except:
                    pass
                df_i.to_csv(local_file_path)
                logger.info(f'No_of_items_download: {len(df)} New_append_data:  {len(df_i)}')
    except Exception as e:
        logger.info(f'{e}')

def main():
    url1="https://www.murrayautogroup.ca/vehicles"
    if not os.path.exists(local_file_path):
        try:
            aws_handler.download_from_aws(aws_bucket_file_path, local_file_path)
            df = pd.read_csv(local_file_path)
            try:
                df.drop('Unnamed: 0', inplace=True, axis=1)
            except:
                pass            
        except:
            df = pd.DataFrame()
    else:
        df = pd.read_csv(local_file_path)
        try:
            df.drop('Unnamed: 0', inplace=True, axis=1)
        except:
            pass 
    collect_url(url1)
    # df_current = pd.read_csv('scrapers/murrayautogroup/results/murrayautogroup_info.csv')
    url_df = pd.read_csv('scrapers/murrayautogroup/results/urls.csv')
    # urls = url_df[~url_df.url.isin(df_current.url)]
    urls = list(url_df.url)
    with ThreadPoolExecutor(max_workers = 1) as executor:
        executor.map(scrape_info, urls)
    
    df.to_csv(local_file_path, index=False)
    aws_handler.upload_to_aws(local_file_path, aws_bucket_folder_path)


if __name__ == "__main__":
    new_info = []
    list_feature = url_feature()
    main()
    
   


