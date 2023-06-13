import os
from datetime import datetime
from time import sleep

import pandas as pd

from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.common.exceptions import NoSuchElementException
from selenium.webdriver.chrome.options import Options

from webdriver_manager.chrome import ChromeDriverManager 
from selenium.webdriver.chrome.service import Service as ChromeService

import undetected_chromedriver as uc

import logging

from fnmatch import fnmatch
import shutil

ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
DOWNLOAD_FOLDER = ROOT_DIR + '/downloads/'
FILE_NAME_FORMAT = datetime.now().strftime("%Y-%m-%d-%H%M%S-unitedenergy-outage")
DOWNLOAD_UNITEDENERGY_FILE_DESTINATION = DOWNLOAD_FOLDER + FILE_NAME_FORMAT + '.csv'
UNITEDENERGY_URL = 'https://www.unitedenergy.com.au/outage-map/'

API_DOWNLOADS_DESINATION = os.path.dirname(os.path.dirname(ROOT_DIR)) + '/outages/unitedenergy/'
API_DOWNLOADS_DESINATION_FILE = API_DOWNLOADS_DESINATION + FILE_NAME_FORMAT + '.csv'

LOGS_FILE = os.path.dirname(os.path.dirname(ROOT_DIR)) + '/logs/unitedenergy.log'

error = error_msg = ''

logging.basicConfig(filename=LOGS_FILE,
                    filemode='a',
                    format='%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s',
                    datefmt='%Y-%m-%d-%H:%M:%S',
                    level=logging.DEBUG)

logger = logging.getLogger('unitedenergylogs')

logger.info("1. ====={0} STARTED =====".format(FILE_NAME_FORMAT))
logger.info("1a. ROOT DIR: {0}".format(ROOT_DIR))
logger.info("1b. DOWNLOAD FOLDER: {0}".format(DOWNLOAD_FOLDER))
logger.info("1c. FILE NAME FORMAT: {0}".format(FILE_NAME_FORMAT))
logger.info("1e. API_DOWNLOADS_DESINATION: {0}".format(API_DOWNLOADS_DESINATION))
logger.info("1f. API_DOWNLOADS_DESINATION_FILE: {0}".format(API_DOWNLOADS_DESINATION_FILE))
logger.info("1g. UNITEDENERGY URL: {0}".format(UNITEDENERGY_URL))

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

def check_if_element_exists(data=None, byxpath=None, byclass=None, check_parent=False):
    if byxpath:
        txt = None
        try:
            if check_parent:
                try:
                    txt = data.find_element(By.XPATH, byxpath)
                    if txt:
                        parent_txt = txt.find_element(By.XPATH, "..").text
                        txt = parent_txt.split(':')[1].strip()
                    else:
                        txt = None
                except NoSuchElementException as e:
                    txt = None
            else:
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

    options = Options()
    options.add_argument("--headless")
    options.add_argument("--window-size=1920,1200")

    driver = webdriver.Chrome(service=ChromeService(ChromeDriverManager().install()), options=options)
    # driver = uc.Chrome(options=options) # need to use this special one to bypass the clouflare robot check
    driver.get(UNITEDENERGY_URL)
    delay = 15

    elem = WebDriverWait(driver, delay).until(EC.presence_of_element_located((By.XPATH, '//span[contains(@class,"OutageListItem-module--suburb")]')))
    if elem:
        data_table = driver.find_elements(By.XPATH, '//div[contains(@class,"OutageListItem-module--root")]')
        if len(data_table) > 0:
            outages = []
            columns = ['outage_type', 'affected_suburbs', 'location', 'estimated_restoration_time', 'customers_affected', 'cause']
            for outage in data_table:
                outage_type_text = affected_suburbs_text = location_text = estimated_restoration_time_text = customers_affected_text = customers_affected_text = cause_text = None
                out = []
                outage_type_text = check_if_element_exists(outage, byxpath='//span[contains(@class,"OutageListItem-module--unplannedLabel")]')    
                affected_suburbs_text = check_if_element_exists(outage, byxpath='//span[contains(@class,"OutageListItem-module--suburb")]')
                location_text = check_if_element_exists(outage, byxpath='//span[contains(text(),"Fault location")]', check_parent=True)
                estimated_restoration_time_text = check_if_element_exists(outage, byxpath='//span[contains(text(),"restoration")]', check_parent= True)
                customers_affected_text = check_if_element_exists(outage, byxpath='//span[contains(text(),"affected")]', check_parent= True)
                cause_text = check_if_element_exists(outage, byxpath='//span[contains(text(),"Cause")]', check_parent= True)
                out.extend([outage_type_text,affected_suburbs_text,location_text,estimated_restoration_time_text, customers_affected_text,cause_text])
                outages.append(out)
            try:
                df = pd.DataFrame(outages, columns = columns)
                df.to_csv(DOWNLOAD_UNITEDENERGY_FILE_DESTINATION, encoding='utf-8')
                logger.info("Successfully written extracted data to CSV file: {0}".format(DOWNLOAD_UNITEDENERGY_FILE_DESTINATION))
            except Exception as e:
                error = True
                error_msg += 'Error while storing extracted data to CSV: {0}: {1}'.format(DOWNLOAD_UNITEDENERGY_FILE_DESTINATION, e)
                logger.error("Error while storing extracted data to CSV: {0}: {1}".format(DOWNLOAD_UNITEDENERGY_FILE_DESTINATION, e))
            sleep(3)
            for r, d, f in os.walk(DOWNLOAD_FOLDER):
                if len(f) > 0:
                    for fr in f:
                        if fnmatch(fr, FILE_NAME_FORMAT + '*'):
                            shutil.copy(os.path.join(r, fr),API_DOWNLOADS_DESINATION_FILE)
            logger.info("Successfully written file for API delivery: {0}".format(API_DOWNLOADS_DESINATION_FILE))
        else:
            logger.info("Looks like no outages found: {0}".format(FILE_NAME_FORMAT))
            
    else:
        logger.info("Looks like no outages found: {0}".format(FILE_NAME_FORMAT))
except Exception as e:
    error = True
    error_msg += 'Unable to extract data from url: {0}: {1}'.format(UNITEDENERGY_URL, e)
    logger.error("Unable to extract data from url: {0}: {1}".format(UNITEDENERGY_URL, e))

from helpers.notifications import send_email_notification_of_failure as notify
from helpers.connection import add_extraction_source_details as conn

if error == True:
    notify(source_name='unitedenergy', source_url=UNITEDENERGY_URL, extraction_date=datetime.today().strftime('%Y-%m-%d'), error_msg=error_msg)
else:
    conn(source_name='unitedenergy', source_url=UNITEDENERGY_URL, extraction_date=datetime.today().strftime('%Y-%m-%d'), success=True)
logger.info("9a. ====={0} DONE=====\n".format(FILE_NAME_FORMAT))