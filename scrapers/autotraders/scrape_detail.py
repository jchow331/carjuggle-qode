from datetime import date
import re
import concurrent.futures
import os
import logging

import pandas as pd
import requests
from bs4 import BeautifulSoup

from base import retry_decorator
# from base import log_config
from base.logging_decorator import handle_exceptions
from base.aws_handler import AWSHandler

new_info = []
scraper_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "results")
local_file_path = os.path.join(scraper_dir, "autotraders_result.csv")
# print(local_file_path)
logger_name = 'autotraders_scrapper_bots_canada'
email_subject = 'Autotraders Bot Alert'
email_toaddrs = ['karan@qodemedia.net', 'sana@qodemedia.com', 'jordan@qodemedia.com']
logger = logging.getLogger(__name__)
aws_bucket_file_path = "MasterCode1/scraping/autotraders/autotraders_result.csv"
aws_bucket_folder_path = "MasterCode1/scraping/autotraders"

def scrape_detail(info):
    url,featured=info
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/98.0.4758.81 Safari/537.36'}
    page = requests.get(url, headers=headers)
    # print(dir(page))
    # print(page.text)
    soup = BeautifulSoup(page.text, 'html.parser')
    info = [None] * 27
    info[0] = date.today().strftime('%Y/%m/%d')
    info[1] = None
    # title
    info[2] = soup.find("title").text
    info[3] = url

    # Metadata dictionary
    try:
        script=str(soup.find_all("script",type="text/javascript")[5])
        json_obj=script.split("window['ngVdpModel'] = ")[1].split(";\r\n")[0].replace("true","True").replace("false","False").replace("null","None")
        meta_data_dict=eval(json_obj)
        info[25] = str(meta_data_dict)
    except IndexError:
        try:
            script = str(soup.find_all("script", type="text/javascript")[5])
            json_obj = script.split("window['ngVdpModel'] = ")[1].split(";\r\n")[0].replace("true", "True").replace(
                "false", "False").replace("null", "None")
            meta_data_dict = eval(json_obj)
            info[25] = str(meta_data_dict)
        except Exception:
            pass
    try:
        # Numowners
        if "One Owner" in script:
            info[20] = 1
        elif "ONE OWNER" in script:
            info[20] = 1
        if "Accident Free" in script:
            info[21] = 0
        elif "ACCIDENT FREE" in script:
            # num_accidents
            info[21] = None
    except Exception:
        pass
    #featured
    info[26] = featured

    try:
        info[4] = re.findall('"make":(.+?),|$', str(soup))[0].replace("\"","")
    except KeyError:
        pass

    try:
        info[5] = re.findall('"model":(.+?),|$', str(soup))[0].replace("\"","")
    except KeyError:
        pass

    try:
        # year
        info[6] = soup.find("title").text.split()[0]
    except AttributeError:
        pass

    try:
        info[7] = re.findall('"mileage":(.+?)",|$', str(soup))[0].replace("\"","")

    except Exception:
        pass

    try:
        # price
        info[8] = re.findall('"price":(.+?)",|$', str(soup))[0].replace("\"","")
    except Exception:
        pass

    # condition
    try:
        info[9] = re.findall('"condition":(.+?),|$', str(soup))[0].replace("\"","")
    except KeyError:
        pass

    # city
    try:
        info[11] = re.findall('"city":(.+?)},|$', str(soup))[0].replace("\"","")
    except Exception:
        pass

    try:
        info[10] = re.findall('"provinceAbbreviation":(.+?),|$', str(soup))[0].replace("\"","")
    except Exception:
        pass

    # drivetrain
    try:
        info[13] = re.findall('"drivetrain":(.+?),|$', str(soup))[0].replace("\"","")
    except KeyError:
        pass

    #trim
    try:
        info[17] = re.findall('"trim":(.+?),|$', str(soup))[0].replace("\"","")
    except KeyError:
        pass

    # img_url
    try:
        info[18] = re.findall('"galleryUrl":(.+?),|$', str(soup))[0].replace("\"","")
    except Exception:
        pass

    # fuel_type
    try:
        info[16] = re.findall('"Fuel Type","value":(.+?)}|$', str(soup))[0].replace("\"","")
    except KeyError:
        pass

    # 'Body Style'
    try:
        info[14] = re.findall('"Body Type","value":(.+?)},|$', str(soup))[0].replace("\"","")
    except KeyError:
        pass

    # transmission
    try:
        info[12] = re.findall('"transmission":(.+?),|$', str(soup))[0].replace("\"","")
    except KeyError as e:
        pass

    try:
        info[15] = re.findall('"Exterior Colour","value":(.+?)}|$', str(soup))[0].replace("\"","")
        # print(type(info_dict['enh_vehic_variant']))
    except KeyError as e:
        pass

    # try:
    #     info[19] = info_dict['vin']
    # except KeyError as e:
    #     pass

    try:
        # engine
        info[23] = re.findall('"Engine","value":"(.+?)},|$', str(soup))[0].replace("\"","")
    except KeyError as e:
        pass
    # num_owners
    # try:
    #     info[20] = None
    # except Exception:
    #     pass
    # # num_accidents
    # try:
    #     info[21] = None
    # except Exception:
    #     pass

    try:
        price_dict = {date.today().strftime('%Y/%m/%d'): info[8]}
        info[22] = str(price_dict)
        # print(info)
    except Exception:
        pass

    try:
        info[24] = None
    except KeyError as e:
        pass


    new_info.append(info)


@handle_exceptions(logger_name, email_subject, email_toaddrs)
@retry_decorator.retry(requests.exceptions.ConnectionError, tries=3, delay=360)
def main():
    aws_handler =AWSHandler()
    links = pd.read_csv(os.path.join(scraper_dir,"all_links.csv"))
    new_urls= links[["links","featured"]]
    new_urls=new_urls.values.tolist()


    NUM_WORKERS=32

    #Doesn't need to download the df again since it does it when scraping for urls
    #aws_handler.download_from_aws(aws_bucket_file_path, local_file_path)
    logger.info('Reading old csv and comparing urls, this might take a few minutes.')
    scrape_df = pd.read_csv(local_file_path)
    #new_urls = [x for x in new_urls if x[0] not in scrape_df['url'].values.tolist()]
    logger.info('Done.')

    # for info in new_urls:
    #     print(info)
    #     scrape_detail(info)
    #     print(new_info)
    #     break
    try:
        for i in range(0, len(new_urls) - NUM_WORKERS, NUM_WORKERS):
            print(f'{i}/{len(new_urls)}')
            urls_chunk = new_urls[i:i + NUM_WORKERS]
            # print(urls_chunk)
            with concurrent.futures.ThreadPoolExecutor(max_workers=NUM_WORKERS) as executor:
                executor.map(scrape_detail, urls_chunk)
            # print(urls_chunk)
            # for link in urls_chunk:
            #     scrape_url(link)
            # print(new_info)
            
            #if i% (NUM_WORKERS* 10) ==0 or i % ((len(new_urls)//NUM_WORKERS)*NUM_WORKERS):
    except Exception as e:
        logger.info(f"Exception occured:{e}")
        #aws_handler.upload_to_aws(local_file_path, aws_bucket_folder_path)
    finally:
        df_scraped = pd.DataFrame(new_info,
                          columns=['date_added', 'date_removed', 'title', 'url', 'make', 'model', 'year',
                                   'kilometers', 'price', 'condition', 'province', 'City',
                                   'Vehicle_information.transmission', 'Vehicle_information.drivetrain',
                                   'Vehicle_information.body_style',
                                   'Vehicle_information.exterior_colour', 'Vehicle_information.fuel_type',
                                   'Vehicle_information.trim', 'img_url', 'vin', 'NumOwners',
                                   'PrevAccident', 'price_history', 'Vehicle_information.engine', '_id',
                                   'metadata','featured'])
        scrape_df = pd.concat([scrape_df, df_scraped])
        
        scrape_df.drop_duplicates(inplace=True)
        scrape_df.to_csv(local_file_path, index=False)
        aws_handler.upload_to_aws(local_file_path, aws_bucket_folder_path)


if __name__ == '__main__':
    main()

#Metadata dictionary
# script=str(soup.find_all("script",type="text/javascript")[5])
# json_obj=script.split("window['ngVdpModel'] = ")[1].split(";\r\n")[0].replace("true","True").replace("false","False").replace("null","None")
# eval(json_obj)