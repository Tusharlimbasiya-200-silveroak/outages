import os
from datetime import datetime
from time import sleep

import pandas as pd

from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
import csv
from selenium.webdriver.chrome.options import Options



from webdriver_manager.chrome import ChromeDriverManager 
from selenium.webdriver.chrome.service import Service as ChromeService

import logging

from fnmatch import fnmatch
import shutil

ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
DOWNLOAD_FOLDER = ROOT_DIR + '/downloads/'
FILE_NAME_FORMAT = datetime.now().strftime("%Y-%m-%d-%H%M%S-ausnet-outage")
DOWNLOAD_AUSNET_FILE_DESTINATION = DOWNLOAD_FOLDER + FILE_NAME_FORMAT + '.csv'
AUSNET_URL = 'https://www.outagetracker.com.au/outage-list'

API_DOWNLOADS_DESINATION = os.path.dirname(os.path.dirname(ROOT_DIR)) + '/outages/ausnet/'
API_DOWNLOADS_DESINATION_FILE = API_DOWNLOADS_DESINATION + FILE_NAME_FORMAT + '.csv'

LOGS_FILE = os.path.dirname(os.path.dirname(ROOT_DIR)) + '/logs/ausnet.log'

error = error_msg = ''

logging.basicConfig(filename=LOGS_FILE,
                    filemode='a',
                    format='%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s',
                    datefmt='%Y-%m-%d-%H:%M:%S',
                    level=logging.DEBUG)

logger = logging.getLogger('energexlogs')

logger.info("1. ====={0} STARTED =====".format(FILE_NAME_FORMAT))
logger.info("1a. ROOT DIR: {0}".format(ROOT_DIR))
logger.info("1b. DOWNLOAD FOLDER: {0}".format(DOWNLOAD_FOLDER))
logger.info("1c. FILE NAME FORMAT: {0}".format(FILE_NAME_FORMAT))
logger.info("1e. API_DOWNLOADS_DESINATION: {0}".format(API_DOWNLOADS_DESINATION))
logger.info("1f. API_DOWNLOADS_DESINATION_FILE: {0}".format(API_DOWNLOADS_DESINATION_FILE))
logger.info("1g. Energex URL: {0}".format(AUSNET_URL))

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

csv_headers = ['Types','status','Towns impacted','Outage time','Estimated time of restoration','Reason','Customers impacted','Incident number']

options = Options()
options.add_argument("--headless")
options.add_argument("--window-size=1920,1200")
try:
    data_frames_list = [] 
    driver = webdriver.Chrome(service=ChromeService(ChromeDriverManager().install()), options=options)
    driver.get(AUSNET_URL)
    logger.info("Fetched Energex url: {0}".format(AUSNET_URL))
    delay = 5
    
    elem = WebDriverWait(driver, delay).until(EC.presence_of_element_located((By.CLASS_NAME, 'w-full')))
    data_table = driver.find_element(By.XPATH,'//*[@id="__next"]/main/div[2]/div[2]/table')
    parsed_data = data_table.get_attribute('outerHTML')
    page_source_dfs = pd.read_html(parsed_data)
    data_frames_list.extend(page_source_dfs)



    for i in range(2, 100):
        button_xpath = f'//button[contains(@aria-label, "Page {i}")]'

        try:
            button = WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.XPATH, button_xpath)))
            button.click()
            data_table = driver.find_element(By.XPATH, '//*[@id="__next"]/main/div[2]/div[2]/table')
            parsed_data = data_table.get_attribute('outerHTML')
            page_source_dfs_data = pd.read_html(parsed_data)
            data_frames_list.extend(page_source_dfs_data)
        except Exception as e:
            break
            # print(f"Error occurred while clicking button {i}: {e}")
        
    try:
        concatenated_data = pd.concat(data_frames_list, ignore_index=True)
        df=concatenated_data.rename(columns={'Type':'otype','Towns impacted':'suburb','Outage time':'ostart_time','Estimated time of restoration':'o_res_time','Reason':'reason','Customers impacted':'affected_cust','Incident number':'oid'})
        df.to_csv(DOWNLOAD_AUSNET_FILE_DESTINATION, encoding='utf-8')
        ausnet_data = df.to_dict(orient="records")

        logger.info("Successfully written extracted data to CSV file: {0}".format(DOWNLOAD_AUSNET_FILE_DESTINATION))
    except Exception as e:
        error = True
        error_msg += 'Error while writing extracted data to CSV: {0}: {1}'.format(DOWNLOAD_AUSNET_FILE_DESTINATION, e)
        logger.error("Error while writing extracted data to CSV: {0}: {1}".format(DOWNLOAD_AUSNET_FILE_DESTINATION, e))
    
    sleep(3)
    for r, d, f in os.walk(DOWNLOAD_FOLDER):
        if len(f) > 0:
            for fr in f:
                if fnmatch(fr, FILE_NAME_FORMAT + '*'):
                    shutil.copy(os.path.join(r, fr),API_DOWNLOADS_DESINATION_FILE)
    logger.info("Successfull written file for API Delivery: {0}".format(API_DOWNLOADS_DESINATION_FILE))
except Exception as e:
    error = True
    error_msg += 'Unable to fetch data from the URL: {0}: {1}'.format(AUSNET_URL, e)
    logger.error("2ERR. Unable to fetch data from the URL: {0}: {1}".format(AUSNET_URL, e))


import sys
sys.path.append(r'/home/webstring-tushar/Documents/work/outage/outage-owl/helpers')

import notifications 
import connection

if error == True:
    notifications.send_email_notification_of_failure(source_name='ausnet', source_url=AUSNET_URL, extraction_date=datetime.today().strftime('%Y-%m-%d'), error_msg=error_msg)
else :
    connection.extract_data(data=ausnet_data,name="ausnet")
logger.info("9a. ====={0} DONE=====\n".format(FILE_NAME_FORMAT))


   