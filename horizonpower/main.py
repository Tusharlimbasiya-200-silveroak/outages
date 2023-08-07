import os
from datetime import datetime
from time import sleep

import pandas as pd

from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager 
from selenium.webdriver.chrome.service import Service as ChromeService

import logging

from fnmatch import fnmatch
import shutil

ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
DOWNLOAD_FOLDER = ROOT_DIR + '/downloads/'
FILE_NAME_FORMAT = datetime.now().strftime("%Y-%m-%d-%H%M%S-horizonpower-outage")
DOWNLOAD_HORIZONPOWER_FILE_DESTINATION = DOWNLOAD_FOLDER + FILE_NAME_FORMAT + '.csv'
HORIZONPOWER_URL = 'https://www.horizonpower.com.au/faults-outages/?q='

API_DOWNLOADS_DESINATION = os.path.dirname(os.path.dirname(ROOT_DIR)) + '/outages/horizonpower/'
API_DOWNLOADS_DESINATION_FILE = API_DOWNLOADS_DESINATION + FILE_NAME_FORMAT + '.csv'

SCREEN_SHORT = '/home/webstring-tushar/Documents/work/outage/outages/horizonpower/screenshort/'+ FILE_NAME_FORMAT + '.png'


LOGS_FILE = os.path.dirname(os.path.dirname(ROOT_DIR)) + '/logs/horizonpower.log'

error = error_msg = ''

logging.basicConfig(filename=LOGS_FILE,
                    filemode='a',
                    format='%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s',
                    datefmt='%Y-%m-%d-%H:%M:%S',
                    level=logging.DEBUG)

logger = logging.getLogger('horizonpowerlogs')

logger.info("1. ====={0} STARTED =====".format(FILE_NAME_FORMAT))
logger.info("1a. ROOT DIR: {0}".format(ROOT_DIR))
logger.info("1b. DOWNLOAD FOLDER: {0}".format(DOWNLOAD_FOLDER))
logger.info("1c. FILE NAME FORMAT: {0}".format(FILE_NAME_FORMAT))
logger.info("1e. API_DOWNLOADS_DESINATION: {0}".format(API_DOWNLOADS_DESINATION))
logger.info("1f. API_DOWNLOADS_DESINATION_FILE: {0}".format(API_DOWNLOADS_DESINATION_FILE))
logger.info("1g. Horizonpower URL: {0}".format(HORIZONPOWER_URL))

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

csv_headers = ['status', 'affected_area', 'start_time', 'estimated_restoration_time', 'affected_customers']

options = Options()
options.add_argument("--headless")
options.add_argument("--window-size=1920,1200")
try:
    driver = webdriver.Chrome(service=ChromeService(ChromeDriverManager().install()), options=options)
    driver.get(HORIZONPOWER_URL)
    delay = 5
    screen_short = driver.find_element(By.XPATH,'//*[@id="outages-list"]/table')
    screen_short.screenshot(SCREEN_SHORT)

    elem = WebDriverWait(driver, delay).until(EC.presence_of_element_located((By.CLASS_NAME, 'list-outage-item')))
    data_table = driver.find_element(By.CLASS_NAME, 'outage-list__desktop')
    parsed_data = data_table.get_attribute('outerHTML')
    page_source_dfs = pd.read_html(parsed_data)
    

    try:    
        for table in page_source_dfs:
            table.to_csv(DOWNLOAD_HORIZONPOWER_FILE_DESTINATION, encoding='utf-8')
            df2 = table.rename(columns={'Status':'status','Affected area':'suburb','Start time':'ostart_time','Estimated restoration time':'o_res_time','Affected customers':'affected_cust','View on map':"street"},inplace=False)
            df2.head()
            horizonpower_data = df2.to_dict(orient='records')
        logger.info("Successfully written extracted data to CSV file: {0}".format(DOWNLOAD_HORIZONPOWER_FILE_DESTINATION))
    except Exception as e:
        error = True
        error_msg += 'Unable to write extracted data to CSV file: {0}: {1}'.format(DOWNLOAD_HORIZONPOWER_FILE_DESTINATION, e)
        logger.error("Unable to write extracted data to CSV file: {0}: {1}".format(DOWNLOAD_HORIZONPOWER_FILE_DESTINATION, e))
    sleep(3)
    for r, d, f in os.walk(DOWNLOAD_FOLDER):
        if len(f) > 0:
            for fr in f:
                if fnmatch(fr, FILE_NAME_FORMAT + '*'):
                    shutil.copy(os.path.join(r, fr),API_DOWNLOADS_DESINATION_FILE)
    logger.info("Successfully written file for API Delivery: {0}".format(API_DOWNLOADS_DESINATION_FILE))
except Exception as e:
    error = True
    error_msg += 'Error while extracting data from url: {0}: {1}'.format(HORIZONPOWER_URL, e)
    logger.error("Error while extracting data from url: {0}: {1}".format(HORIZONPOWER_URL, e))



import sys
sys.path.append(r'/home/webstring-tushar/Documents/work/outage/outage-owl/helpers')

import notifications 
import connection


if error == True:
    notifications.send_email_notification_of_failure(source_name='horizonpower', source_url=HORIZONPOWER_URL, extraction_date=datetime.today().strftime('%Y-%m-%d'), error_msg=error_msg)
else :
    connection.extract_data(data=horizonpower_data,name="horizonpower")
logger.info("9a. ====={0} DONE=====\n".format(FILE_NAME_FORMAT))