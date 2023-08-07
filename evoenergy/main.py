import os
from time import sleep
import urllib.request
from datetime import datetime
import csv
import pandas as pd
import logging

from fnmatch import fnmatch
import shutil

ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
DOWNLOAD_FOLDER = ROOT_DIR + '/downloads/'
FILE_NAME_FORMAT = datetime.now().strftime("%Y-%m-%d-%H%M%S-evoenergy-outage")
DOWNLOAD_EVOENERGY_FILE_DESTINATION = DOWNLOAD_FOLDER + FILE_NAME_FORMAT + '.csv'

API_DOWNLOADS_DESINATION = os.path.dirname(os.path.dirname(ROOT_DIR)) + '/outages/evoenergy/'
API_DOWNLOADS_DESINATION_FILE = API_DOWNLOADS_DESINATION + FILE_NAME_FORMAT + '.csv'

LOGS_FILE = os.path.dirname(os.path.dirname(ROOT_DIR)) + '/logs/evoenergy.log'

EVOENEGY_URL = 'https://www.evoenergy.com.au/api/sitecore/Outage/ExportOutages'

error = error_msg = None

logging.basicConfig(filename=LOGS_FILE,
                    filemode='a',
                    format='%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s',
                    datefmt='%Y-%m-%d-%H:%M:%S',
                    level=logging.DEBUG)

logger = logging.getLogger('evoenergylogs')

logger.info("1. ====={0} STARTED =====".format(FILE_NAME_FORMAT))
logger.info("1a. ROOT DIR: {0}".format(ROOT_DIR))
logger.info("1b. DOWNLOAD FOLDER: {0}".format(DOWNLOAD_FOLDER))
logger.info("1c. FILE NAME FORMAT: {0}".format(FILE_NAME_FORMAT))
logger.info("1e. API_DOWNLOADS_DESINATION: {0}".format(API_DOWNLOADS_DESINATION))
logger.info("1f. API_DOWNLOADS_DESINATION_FILE: {0}".format(API_DOWNLOADS_DESINATION_FILE))
logger.info("1g. Energex URL: {0}".format(EVOENEGY_URL))

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


if not os.path.isfile(DOWNLOAD_EVOENERGY_FILE_DESTINATION):
    try:
        opener = urllib.request.URLopener()
        opener.addheader('User-Agent', 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) ')
        downloaded_file,headers = opener.retrieve('https://www.evoenergy.com.au/api/sitecore/Outage/ExportOutages', DOWNLOAD_EVOENERGY_FILE_DESTINATION)
        logger.info("Successfully extracted data from source: {0}".format(EVOENEGY_URL))
        sleep(3)
        for r, d, f in os.walk(DOWNLOAD_FOLDER):
            if len(f) > 0:
                for fr in f:
                    if fnmatch(fr, FILE_NAME_FORMAT + '*'):
                        shutil.copy(os.path.join(r, fr),API_DOWNLOADS_DESINATION_FILE)
        logger.info("Successfully written data for API delivery: {0}".format(API_DOWNLOADS_DESINATION_FILE))
    except Exception as e:
        error = True
        error_msg += 'Unable to extract data from URL: {0}: {1}'.format(EVOENEGY_URL, e)
        logger.error("Unable to extract data from URL: {0}: {1}".format(EVOENEGY_URL, e))
else:
    print("Extract is already done for this date: {0}".format(FILE_NAME_FORMAT))


with open(API_DOWNLOADS_DESINATION_FILE,'r') as f:
    csv_reader = csv.DictReader(f)
    df = pd.DataFrame(csv_reader)
    df2 = df.rename(columns={'Outage Id':'oid','Type':'otype','Status Description':'status','Reason':'reason','Affected Suburbs':'suburb','Affected Customer Count':'affected_cust','Planned Start':'ostart_time','Planned Restoration':'o_res_time'})
    evoenegy_data = df2.to_dict(orient='records')
   

import sys
sys.path.append(r'/home/webstring-tushar/Documents/work/outage/outage-owl/helpers')

import notifications
import connection


if error == True:
    notifications.send_email_notification_of_failure(source_name='evoenergy', source_url=EVOENEGY_URL, extraction_date=datetime.today().strftime('%Y-%m-%d'), error_msg=error_msg)
else :
    connection.extract_csv_data(data=evoenegy_data,name='evoenergy')
logger.info("9a. ====={0} DONE=====\n".format(FILE_NAME_FORMAT))
