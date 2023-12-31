import os
from datetime import datetime
from time import sleep

import pandas as pd
from pymongo.mongo_client import MongoClient
from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.common.exceptions import NoSuchElementException
from selenium.webdriver.chrome.options import Options

import undetected_chromedriver as uc

import logging

from fnmatch import fnmatch
import shutil

ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
DOWNLOAD_FOLDER = ROOT_DIR + '/downloads/'
FILE_NAME_FORMAT = datetime.now().strftime("%Y-%m-%d-%H%M%S-westernpower-outage")
DOWNLOAD_WESTERNPOWER_FILE_DESTINATION = DOWNLOAD_FOLDER + FILE_NAME_FORMAT + '.csv'
WESTERNPOWER_URL = 'https://www.westernpower.com.au/faults-outages/power-outages/'

API_DOWNLOADS_DESINATION = os.path.dirname(os.path.dirname(ROOT_DIR)) + '/outages/westernpower/'
API_DOWNLOADS_DESINATION_FILE = API_DOWNLOADS_DESINATION + FILE_NAME_FORMAT + '.csv'

LOGS_FILE = os.path.dirname(os.path.dirname(ROOT_DIR)) + '/logs/westernpower.log'
SCREEN_SHORT = '/home/webstring-tushar/Documents/work/outage/outages/westernpower/screenshort/'+ FILE_NAME_FORMAT + '.png'

error = error_msg = ''

logging.basicConfig(filename=LOGS_FILE,
                    filemode='a',
                    format='%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s',
                    datefmt='%Y-%m-%d-%H:%M:%S',
                    level=logging.INFO)

logger = logging.getLogger('westernpowerlogs')

logger.info("1. ====={0} STARTED =====".format(FILE_NAME_FORMAT))
logger.info("1a. ROOT DIR: {0}".format(ROOT_DIR))
logger.info("1b. DOWNLOAD FOLDER: {0}".format(DOWNLOAD_FOLDER))
logger.info("1c. FILE NAME FORMAT: {0}".format(FILE_NAME_FORMAT))
logger.info("1e. API_DOWNLOADS_DESINATION: {0}".format(API_DOWNLOADS_DESINATION))
logger.info("1f. API_DOWNLOADS_DESINATION_FILE: {0}".format(API_DOWNLOADS_DESINATION_FILE))
logger.info("1g. UNITEDENERGY URL: {0}".format(WESTERNPOWER_URL))

if not os.path.exists(DOWNLOAD_FOLDER):
    try:
        logger.info("2a. Download folder path was not existed, so creating...")
        os.makedirs(DOWNLOAD_FOLDER)
        logger.info("2b. Download folder path was not existed, so created...")
    except Exception as e:
        error = True
        error_msg += 'Unable to create directory on server: {0}'.format(DOWNLOAD_FOLDER)
        logger.error("2ERR. Error creating directory: {0}: {1}".format(DOWNLOAD_FOLDER, e))

if not os.path.exists(API_DOWNLOADS_DESINATION):
    try:
        logger.info("3a. API Download folder path was not existed, so creating...")
        os.makedirs(API_DOWNLOADS_DESINATION)
        logger.info("3b. API Download folder path was not existed, so created...")
    except Exception as e:
        error = True
        error_msg += 'Unable to create API directory on server: {0}'.format(API_DOWNLOADS_DESINATION)
        logger.error("2ERR. Error creating API directory: {0}: {1}".format(API_DOWNLOADS_DESINATION, e))


def check_if_element_exists(data, byxpath=None, byclass=None):
    if byxpath:
        try:
            txt = data.find_element(By.XPATH, byxpath).text
        except NoSuchElementException as e:
            return None
        return txt
    else:
        try:
            txt = data.find_element(By.CLASS_NAME, byclass).text
        except NoSuchElementException as e:
            return None
        return txt
    
try:
    options = uc.ChromeOptions()
    # options.add_argument("--headless")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-extensions")
    options.add_argument('--disable-application-cache')
    options.add_argument('--disable-gpu')
    options.add_argument("--disable-setuid-sandbox")
    options.add_argument("--disable-dev-shm-usage")

    driver = uc.Chrome(ouse_subprocess=True,options=options)
  
    driver.get(WESTERNPOWER_URL)
    delay = 12
    elem = WebDriverWait(driver, delay).until(EC.presence_of_element_located((By.XPATH, '//div[contains(@class,"outage-list")]')))
    print(elem)


except:
    pass


    

#     driver = uc.Chrome(use_subprocess=True)
#     driver.get(WESTERNPOWER_URL)
#     driver.implicitly_wait(8)
#     delay =10
#     # elem = driver.find_element(By.XPATH,'/html/body/div[1]/main/div/div/div/div/div/div/div/div/div/div[1]/div/div[2]/div[1]').text
#     # elem.find_elements(By.XPATH,'/html/body/div[1]/main/div/div/div/div/div/div/div/div/div/div[1]/div/div[2]/div[1]')
#     elem = WebDriverWait(driver, delay).until(EC.presence_of_element_located((By.XPATH,'/html/body')))
#     print("element{0}".format(elem.text))
#     data_table = driver.find_elements(By.XPATH, '//div[@class="outage-list__outages"]/child::div')
#     print(data_table)
#     # srcreen_short = driver.find_element(By.XPATH,'//*[@id="main"]/div/div/div/div/div/div/div/div/div/div[1]/div/div[2]/div[1]')
#     # srcreen_short.screenshot(SCREEN_SHORT)
#     if len(data_table) > 0:
#         outages = []
#         columns = ['outage_type', 'affected_suburbs', 'estimated_restoration_time', 'customers_affected', 'extra_information']
#         for outage in data_table:
#             print(outage)
#             out = []
#             outage_type_text = check_if_element_exists(outage, byclass='outage-title').text
#             affected_suburbs_text = check_if_element_exists(outage, byclass='outage-suburb-list')
#             customers_affected_text = check_if_element_exists(outage, byclass='outage__customers')
#             extra_information_text = check_if_element_exists(outage, byclass='outage__actions')
#             if extra_information_text:
#                 extra_information_text = extra_information_text.replace(',', '-')
#             check_time = check_if_element_exists(outage, byclass='restoration__time')
#             check_time_description = check_if_element_exists(outage, byclass='restoration__delays')
#             if check_time != None:
#                 estimated_restoration_time_text = check_time
#             else:
#                 estimated_restoration_time_text = check_time_description     
#             out.extend([outage_type_text, affected_suburbs_text, estimated_restoration_time_text, customers_affected_text, extra_information_text])
#             outages.append(out)
            
#         try:
        
#             df = pd.DataFrame(outages, columns = columns)
#             print("df{0}".format(df))
#             df2 = df.rename(columns={'outage_type':'otype','affected_suburbs':'suburb','estimated_restoration_time':'o_res_time','customers_affected':'affected_cust','extra_information':'status'},inplace=False)
#             df2.head()
#             df2.to_csv(DOWNLOAD_WESTERNPOWER_FILE_DESTINATION, encoding='utf-8')
#             westernpower_data = df2.to_dict(orient='records')
          
#             logger.info("Successfully written extracted data to CSV: {0}".format(DOWNLOAD_WESTERNPOWER_FILE_DESTINATION))
#         except Exception as e:
#             error = True
#             error_msg += 'Unable to write extracted data to CSV: {0}: {1}'.format(DOWNLOAD_WESTERNPOWER_FILE_DESTINATION, e)
#             logger.error("Unable to write extracted data to CSV: {0}: {1}".format(DOWNLOAD_WESTERNPOWER_FILE_DESTINATION, e))
#         sleep(3)
#         for r, d, f in os.walk(DOWNLOAD_FOLDER):
#             if len(f) > 0:
#                 for fr in f:
#                     print(fr)
#                     if fnmatch(fr, FILE_NAME_FORMAT + '*'):
#                         shutil.copy(os.path.join(r, fr),API_DOWNLOADS_DESINATION_FILE)
#         logger.info("Successfully written file for API delivery: {0}".format(API_DOWNLOADS_DESINATION_FILE))
#     else:
#         logger.info("No data found in extaction: {0}".format(FILE_NAME_FORMAT))

# except Exception as e:
#     error = True
#     error_msg += 'Unable to extract data from the URL: {0}: {1}'.format(WESTERNPOWER_URL, e)
#     logger.error("Unable to extract data from the URL: {0}: {1}".format(WESTERNPOWER_URL, e))




# import sys
# sys.path.append(r'/home/webstring-tushar/Documents/work/outage/outage-owl/helpers')

# import notifications 
# import connection


# if error == True:
#     notifications.send_email_notification_of_failure(source_name='westernpower', source_url=WESTERNPOWER_URL, extraction_date=datetime.today().strftime('%Y-%m-%d'), error_msg=error_msg)
# else :
#     connection.extract_data(data = westernpower_data ,name="Westernpower")
# logger.info("9a. ====={0} DONE=====\n".format(FILE_NAME_FORMAT))




