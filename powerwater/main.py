import os
from datetime import datetime
import re
from time import sleep
import requests
from bs4 import BeautifulSoup
from selenium import webdriver
import pandas as pd

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
FILE_NAME_FORMAT = datetime.now().strftime("%Y-%m-%d-%H%M%S-powerwater-outage")
DOWNLOAD_POWERWATER_FILE_DESTINATION = DOWNLOAD_FOLDER + FILE_NAME_FORMAT + '.csv'
POWERWATER_URL = 'https://www.powerwater.com.au/customers/outages'

API_DOWNLOADS_DESINATION = os.path.dirname(os.path.dirname(ROOT_DIR)) + '/outages/powerwater/'
API_DOWNLOADS_DESINATION_FILE = API_DOWNLOADS_DESINATION + FILE_NAME_FORMAT + '.csv'

LOGS_FILE = os.path.dirname(os.path.dirname(ROOT_DIR)) + '/logs/powerwater.log'

error = error_msg = ''

logging.basicConfig(filename=LOGS_FILE,
                    filemode='a',
                    format='%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s',
                    datefmt='%Y-%m-%d-%H:%M:%S',
                    level=logging.DEBUG)

logger = logging.getLogger('powerwaterlogs')

logger.info("1. ====={0} STARTED =====".format(FILE_NAME_FORMAT))
logger.info("1a. ROOT DIR: {0}".format(ROOT_DIR))
logger.info("1b. DOWNLOAD FOLDER: {0}".format(DOWNLOAD_FOLDER))
logger.info("1c. FILE NAME FORMAT: {0}".format(FILE_NAME_FORMAT))
logger.info("1e. API_DOWNLOADS_DESINATION: {0}".format(API_DOWNLOADS_DESINATION))
logger.info("1f. API_DOWNLOADS_DESINATION_FILE: {0}".format(API_DOWNLOADS_DESINATION_FILE))
logger.info("1g. Horizonpower URL: {0}".format(POWERWATER_URL))

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

columns = ['title', 'outage_origin_detail', 'outage_origin_suburb', 'outage_details', 'outage_order_no', 'outage_order_start_date', 'outage_order_start_time','outage_order_end_date', 'outage_order_end_time']
try:
    parsed_data = requests.get(POWERWATER_URL).text
    parsed_data = BeautifulSoup(parsed_data, 'html.parser')
    useful_data_divs = parsed_data.find_all('div', attrs={'class': 'outages-list__item'})
    outages = []
    for outage in useful_data_divs:
        out = []
        outage_type = outage.find('span', attrs={'class': 'outage-detail-supply-type'}).text
        if outage_type and outage_type.lower() == 'power':   
            outage_title = outage.find('title').text
            outage_region_detail = outage.find('span', attrs={'class': 'outage-detail-region'}).text
            outage_region_suburb = outage.find('span', attrs={'class': 'outage-detail-suburb'}).text
            outage_details = outage.find('span', attrs={'class': 'outage-detail-details'}).text
            outage_order_no = outage.find('span', attrs={'class': 'outage-detail-order-no'}).text
            outage_start_date = outage.find('div', attrs={'outages-list__cell-start'}).find('span', attrs={'class': 'outage-detail-date'}).text
            outage_start_time = outage.find('div', attrs={'outages-list__cell-start'}).find('span', attrs={'class': 'outage-detail-time'}).text
            outage_end_date = outage.find('div', attrs={'outages-list__cell-estimated-end'}).find('span', attrs={'class': 'outage-detail-date'}).text
            outage_end_time = outage.find('div', attrs={'outages-list__cell-estimated-end'}).find('span', attrs={'class': 'outage-detail-time'}).text
            out.extend([outage_title, outage_region_detail, outage_region_suburb, outage_details, outage_order_no, outage_start_date, outage_start_time,outage_end_date, outage_end_time])
            outages.append(out)
    try:
        df = pd.DataFrame(outages, columns = columns)
        df.to_csv(DOWNLOAD_POWERWATER_FILE_DESTINATION, encoding='utf-8')
        logger.info("Successfully written the extracted data to CSV: {0}".format(DOWNLOAD_POWERWATER_FILE_DESTINATION))
    except Exception as e:
        error = True
        error_msg += 'Unable to write extracted data to CSV: {0}: {1}'.format(DOWNLOAD_POWERWATER_FILE_DESTINATION, e)
        logger.error("Unable to write extracted data to CSV: {0}: {1}".format(DOWNLOAD_POWERWATER_FILE_DESTINATION, e))
    sleep(3)
    for r, d, f in os.walk(DOWNLOAD_FOLDER):
        if len(f) > 0:
            for fr in f:
                if fnmatch(fr, FILE_NAME_FORMAT + '*'):
                    shutil.copy(os.path.join(r, fr),API_DOWNLOADS_DESINATION_FILE)
    logger.info("Successfully written the file for API Delivery: {0}".format(API_DOWNLOADS_DESINATION_FILE))
except Exception as e:
    error = True
    error_msg += 'Error extracting data from the URL: {0}: {1}'.format(POWERWATER_URL, e)
    logger.error("Error extracting data from the URL: {0}: {1}".format(POWERWATER_URL, e))
    print("Table not loaded yet")

from helpers.notifications import send_email_notification_of_failure as notify
from helpers.connection import add_extraction_source_details as conn

if error == True:
    notify(source_name='powerwater', source_url=POWERWATER_URL, extraction_date=datetime.today().strftime('%Y-%m-%d'), error_msg=error_msg)
else:
    conn(source_name='powerwater', source_url=POWERWATER_URL, extraction_date=datetime.today().strftime('%Y-%m-%d'), success=True)
logger.info("9a. ====={0} DONE=====\n".format(FILE_NAME_FORMAT))

