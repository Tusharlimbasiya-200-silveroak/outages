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
FILE_NAME_FORMAT = datetime.now().strftime("%Y-%m-%d-%H%M%S-ergon-outage")
DOWNLOAD_ERGON_FILE_DESTINATION = DOWNLOAD_FOLDER + FILE_NAME_FORMAT + '.csv'
ERGON_URL = 'https://www.ergon.com.au/network/outages-and-disruptions/power-outages/outage-finder-text-view'

API_DOWNLOADS_DESINATION = os.path.dirname(os.path.dirname(ROOT_DIR)) + '/outages/ergon/'
API_DOWNLOADS_DESINATION_FILE = API_DOWNLOADS_DESINATION + FILE_NAME_FORMAT + '.csv'

LOGS_FILE = os.path.dirname(os.path.dirname(ROOT_DIR)) + '/logs/ergon.log'

error = error_msg = ''

logging.basicConfig(filename=LOGS_FILE,
                    filemode='a',
                    format='%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s',
                    datefmt='%Y-%m-%d-%H:%M:%S',
                    level=logging.INFO)

logger = logging.getLogger('ergonlogs')

logger.info("1. ====={0} STARTED =====".format(FILE_NAME_FORMAT))
logger.info("1a. ROOT DIR: {0}".format(ROOT_DIR))
logger.info("1b. DOWNLOAD FOLDER: {0}".format(DOWNLOAD_FOLDER))
logger.info("1c. FILE NAME FORMAT: {0}".format(FILE_NAME_FORMAT))
logger.info("1e. API_DOWNLOADS_DESINATION: {0}".format(API_DOWNLOADS_DESINATION))
logger.info("1f. API_DOWNLOADS_DESINATION_FILE: {0}".format(API_DOWNLOADS_DESINATION_FILE))
logger.info("1g. Ergon URL: {0}".format(ERGON_URL))

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
    driver.get(ERGON_URL)
    driver.implicitly_wait(15)

    data_table = driver.find_elements(By.XPATH, '//ul[@class="results"]/child::li')
    if len(data_table) > 0:
        outages = []
        columns = ['event_id','outage_type','area', 'num_of_customers_affected', 'reason', 'suburb(s)', 'street(s)', 'start_time','estimated_fix_time','status']
        for data in data_table:
            out = []
            try:
                check_if_arrow_closed = data.find_element(By.CLASS_NAME, 'arrow-closed')
                logger.info("hhhhhhhhhhhhhhhhh {0}".format(check_if_arrow_closed))
                if check_if_arrow_closed:
                    check_if_arrow_closed.click()
            except Exception as e:
                error = True
                error_msg += 'Seems div is already open: {0}'.format(e)
                logger.error("2ERR. Error because of div open: {0}".format(e))
                
            area_selector = data.find_element(By.CLASS_NAME, 'lga-heading-text')
            affected_customer_selector = data.find_element(By.CLASS_NAME, 'lga-customers-label')
            suburbs_and_streets_selector = data.find_elements(By.CLASS_NAME, 'suburb-and-street')
            outage_planned_selector = data.find_element(By.CLASS_NAME, 'outage-planned')
            outage_id_selector = data.find_element(By.CLASS_NAME, 'event-id-text')
            outage_start_time_selector = data.find_element(By.CLASS_NAME, 'start-text')
            outage_end_time_selector = data.find_element(By.CLASS_NAME, 'est-fix-time-text')
            outage_status_selector = data.find_element(By.CLASS_NAME, 'outage-status')
            outage_reason_selector = data.find_element(By.CLASS_NAME, 'reason-text')

            area_text = area_selector.text if area_selector else None
            affected_customers_text = affected_customer_selector.text if affected_customer_selector else None
            outage_planned_text = outage_planned_selector.text if outage_planned_selector else None
            outage_id_text = outage_id_selector.text if outage_id_selector else None
            outage_start_time_text = outage_start_time_selector.text if outage_start_time_selector else None
            outage_end_time_text = outage_end_time_selector.text if outage_end_time_selector else None
            outage_status_text = outage_status_selector.text.replace('Status', '').strip() if outage_status_selector else None
            outage_reason_text = outage_reason_selector.text if outage_reason_selector else None

            suburbs_list = []
            streets_list = []
            if len(suburbs_and_streets_selector) > 0:
                for sands in suburbs_and_streets_selector:
                    suburbs_list.append(sands.find_element(By.CLASS_NAME, 'suburb-data').text)
                    streets_list.append(sands.find_element(By.CLASS_NAME, 'street-data').text)
                out.extend([outage_id_text,outage_planned_text,area_text, affected_customers_text, outage_reason_text, suburbs_list[0],streets_list[0],outage_start_time_text,outage_end_time_text,outage_status_text])
            else:
                suburbs_list = None
                streets_list = None
                out.extend([outage_id_text,outage_planned_text,area_text, affected_customers_text, outage_reason_text, suburbs_list,streets_list,outage_start_time_text,outage_end_time_text,outage_status_text])
            outages.append(out)
        try:
            df = pd.DataFrame(outages, columns = columns)
            df.to_csv(DOWNLOAD_ERGON_FILE_DESTINATION, encoding='utf-8')
            logger.info("Successfully written CSV file at destination: {0}".format(DOWNLOAD_ERGON_FILE_DESTINATION))
        except Exception as e:
            error = True
            error_msg += 'Unable to write CSV from extracted data: {0}: {1}'.format(DOWNLOAD_ERGON_FILE_DESTINATION, e)
            logger.error("Unable to write CSV from extracted data: {0}: {1}".format(DOWNLOAD_ERGON_FILE_DESTINATION, e))
        sleep(3)
        for r, d, f in os.walk(DOWNLOAD_FOLDER):
            if len(f) > 0:
                for fr in f:
                    if fnmatch(fr, FILE_NAME_FORMAT + '*'):
                        shutil.copy(os.path.join(r, fr),API_DOWNLOADS_DESINATION_FILE)
        logger.info("successfully written file for API delivery: {0}".format(API_DOWNLOADS_DESINATION_FILE))
    else:
        logger.info("Looks like no data found for today: {0}".format(FILE_NAME_FORMAT))

except Exception as e:
    error = True
    error_msg += 'Couldn\'t scrape the data: {0}: {1}'.format(ERGON_URL, e)
    logger.error("Couldn't scrape the data: {0}: {1}".format(ERGON_URL, e))

from helpers.notifications import send_email_notification_of_failure as notify
from helpers.connection import add_extraction_source_details as conn

if error == True:
    notify(source_name='engon', source_url=ERGON_URL, extraction_date=datetime.today().strftime('%Y-%m-%d'), error_msg=error_msg)
else:
    conn(source_name='ergon', source_url=ERGON_URL, extraction_date=datetime.today().strftime('%Y-%m-%d'), success=True)
logger.info("9a. ====={0} DONE=====\n".format(FILE_NAME_FORMAT))
