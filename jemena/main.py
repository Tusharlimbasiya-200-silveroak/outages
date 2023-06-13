import os
from datetime import datetime
from time import sleep

import pandas as pd

from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.keys import Keys
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
FILE_NAME_FORMAT = datetime.now().strftime("%Y-%m-%d-%H%M%S-jemena-outage")
DOWNLOAD_JEMENA_FILE_DESTINATION = DOWNLOAD_FOLDER + FILE_NAME_FORMAT + '.csv'
JEMENA_URL = 'https://jemena.com.au/outages-and-faults/electricity/planned'

API_DOWNLOADS_DESINATION = os.path.dirname(os.path.dirname(ROOT_DIR)) + '/outages/jemena/'
API_DOWNLOADS_DESINATION_FILE = API_DOWNLOADS_DESINATION + FILE_NAME_FORMAT + '.csv'

LOGS_FILE = os.path.dirname(os.path.dirname(ROOT_DIR)) + '/logs/jemena.log'

error = error_msg = None

logging.basicConfig(filename=LOGS_FILE,
                    filemode='a',
                    format='%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s',
                    datefmt='%Y-%m-%d-%H:%M:%S',
                    level=logging.INFO)

logger = logging.getLogger('jemenalogs')

logger.info("1. ====={0} STARTED =====".format(FILE_NAME_FORMAT))
logger.info("1a. ROOT DIR: {0}".format(ROOT_DIR))
logger.info("1b. DOWNLOAD FOLDER: {0}".format(DOWNLOAD_FOLDER))
logger.info("1c. FILE NAME FORMAT: {0}".format(FILE_NAME_FORMAT))
logger.info("1e. API_DOWNLOADS_DESINATION: {0}".format(API_DOWNLOADS_DESINATION))
logger.info("1f. API_DOWNLOADS_DESINATION_FILE: {0}".format(API_DOWNLOADS_DESINATION_FILE))
logger.info("1g. JEMENA URL: {0}".format(JEMENA_URL))

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

options = Options()
options.add_argument("--headless")
options.add_argument("--window-size=1920,1200")

try:
    driver = webdriver.Chrome(service=ChromeService(ChromeDriverManager().install()), options=options)
    driver.get(JEMENA_URL)
    delay = 10

    elem = WebDriverWait(driver, delay).until(EC.presence_of_element_located((By.ID, 'table')))
    sleep(5)
    data_elements = driver.find_elements(By.CLASS_NAME, 'grouprow')
    if len(data_elements) > 0:
        outages = []
        columns = ['type','suburb','street','date', 'day', 'time', 'status']
        for data in data_elements:
            suburb_name = data.get_attribute('data-target').replace('.','')
            suburb_name = "collapse " + suburb_name
            suburb_data = driver.find_elements(By.XPATH,'//tr[contains(@class, "{0}")]'.format(suburb_name))
            if len(suburb_data) > 0:
                for suburb in suburb_data:
                    out = []
                    suburb_details = suburb.find_elements(By.TAG_NAME, 'td')
                    sub = suburb_details[0].get_attribute('innerHTML')
                    street = suburb_details[1].get_attribute('innerHTML')
                    suburb_date = suburb_details[2].get_attribute('innerHTML')
                    suburb_day = suburb_details[3].get_attribute('innerHTML')
                    suburb_time = suburb_details[4].get_attribute('innerHTML')
                    suburb_status = suburb_details[5].get_attribute('innerHTML')
                    out.extend(["planned",sub,street,suburb_date,suburb_day,suburb_time,suburb_status])
                    outages.append(out)
        try:
            df = pd.DataFrame(outages, columns = columns)
            df.to_csv(DOWNLOAD_JEMENA_FILE_DESTINATION, header=columns, encoding='utf-8')
            driver.close()
            logger.info("Successfully written extracted data to CSV: {0}".format(DOWNLOAD_JEMENA_FILE_DESTINATION))
        except Exception as e:
            error = True
            error_msg += 'Unable to write extracted data to CSV: {0}: {1}'.format(DOWNLOAD_JEMENA_FILE_DESTINATION, e)
            logger.error("Unable to write extracted data to CSV: {0}: {1}".format(DOWNLOAD_JEMENA_FILE_DESTINATION, e))

        sleep(3)
        for r, d, f in os.walk(DOWNLOAD_FOLDER):
            if len(f) > 0:
                for fr in f:
                    if fnmatch(fr, FILE_NAME_FORMAT + '*'):
                        shutil.copy(os.path.join(r, fr),API_DOWNLOADS_DESINATION_FILE)
        logger.info("Successfully written file for API Delivery: {0}".format(API_DOWNLOADS_DESINATION_FILE))
    else:
        logger.info("Looks like no data found for this source at the moment: {0}".format(JEMENA_URL))
        driver.close()
except Exception as e:
    error = True
    error_msg += 'Problem extracting data from URL: {0}: {1}'.format(JEMENA_URL, e)
    logger.error("Problem extracting data from URL: {0}: {1}".format(JEMENA_URL, e))
    driver.close()

from helpers.notifications import send_email_notification_of_failure as notify
from helpers.connection import add_extraction_source_details as conn

if error == True:
    notify(source_name='jemena', source_url=JEMENA_URL, extraction_date=datetime.today().strftime('%Y-%m-%d'), error_msg=error_msg)
else:
    conn(source_name='jemena', source_url=JEMENA_URL, extraction_date=datetime.today().strftime('%Y-%m-%d'), success=True)
logger.info("9a. ====={0} DONE=====\n".format(FILE_NAME_FORMAT))