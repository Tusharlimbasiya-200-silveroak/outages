from time import sleep
import requests
from datetime import datetime
import os 
import shutil
from fnmatch import fnmatch
import logging

ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
DOWNLOAD_FOLDER = ROOT_DIR + '/downloads/'
FILE_NAME_FORMAT = datetime.now().strftime("%Y-%m-%d-%H%M%S-ausgrid-outage")

FILE_PATH = DOWNLOAD_FOLDER + datetime.now().strftime("%Y-%m-%d-%H%M%S-ausgrid-outage.csv")

API_DOWNLOADS_DESINATION = os.path.dirname(os.path.dirname(ROOT_DIR)) + '/outages/ausgrid/'
API_DOWNLOADS_DESINATION_FILE = API_DOWNLOADS_DESINATION + FILE_NAME_FORMAT + '.csv'

# data_table_url = 'https://www.ausgrid.com.au/services/Outage/Outage.asmx/GetDetailedPlannedOutages'
# data_table_url = 'https://www.ausgrid.com.au/Outages/Outage-List-View'
data_table_url = "https://www.ausgrid.com.au/webapi/OutageListData/GetDetailedPlannedOutages"

LOGS_FILE = os.path.dirname(os.path.dirname(ROOT_DIR)) + '/logs/ausgrid.log'

error = error_msg = None

logging.basicConfig(filename=LOGS_FILE,
                    filemode='a',
                    format='%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s',
                    datefmt='%Y-%m-%d-%H:%M:%S',
                    level=logging.DEBUG)

logger = logging.getLogger('ausgridlogs')

logger.info("1. ====={0} STARTED =====".format(FILE_NAME_FORMAT))
logger.info("1a. ROOT DIR: {0}".format(ROOT_DIR))
logger.info("1b. DOWNLOAD FOLDER: {0}".format(DOWNLOAD_FOLDER))
logger.info("1c. FILE NAME FORMAT: {0}".format(FILE_NAME_FORMAT))
logger.info("1d. FILE PATH: {0}".format(FILE_PATH))
logger.info("1e. API_DOWNLOADS_DESINATION: {0}".format(API_DOWNLOADS_DESINATION))
logger.info("1f. API_DOWNLOADS_DESINATION_FILE: {0}".format(API_DOWNLOADS_DESINATION_FILE))
logger.info("1g. Extraction URL: {0}".format(data_table_url))

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
        logger.info("2a. API Download folder path was not existed, so creating...")
        os.makedirs(API_DOWNLOADS_DESINATION)
        logger.info("2a. Download folder path was not existed, so created...")
    except Exception as e:
        error = True
        error_msg += 'Unable to create directory on server: {0}'.format(API_DOWNLOADS_DESINATION)
        logger.error("2ERR. Error creating directory: {0}: {1}".format(API_DOWNLOADS_DESINATION, e))

def str_to_date_time(str_timestamp):
    var_dt = datetime.fromisoformat(str_timestamp)
    return var_dt

def date_to_date_string(dt):
    return dt.strftime("%a %d %b")

def date_to_formatted_date(dt):
    return dt.strftime("%I:%M %p (%a %d %b)")

def add_csv_escape_quotes(st):
    return st.replace('"', '""')

def array_to_csv_line(arr):
    arre = ['"' + add_csv_escape_quotes(i) + '"' for i in arr]
    return ','.join(arre)

headers_dict = {
    'Connection': 'keep-alive', 'Pragma': 'no-cache', 'Cache-Control': 'no-cache',
    'Accept': 'application/json, text/plain, */*',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) '
                  'Chrome/87.0.4280.88 Safari/537.36',
    'Content-Type': 'application/json;charset=UTF-8', 'Origin': 'https://www.ausgrid.com.au',
    'Sec-Fetch-Site': 'same-origin', 'Sec-Fetch-Mode': 'cors'}
post_data = ""
try:
    response = requests.post(data_table_url, headers=headers_dict, data=str(post_data))

    if response.status_code != 200:
        exit(0)

    response_json = response.json()
    # response_data = response_json['d']['Data']
    response_data = response_json

    f_headers_arr = ['Date', 'Start / Finish', 'Affecting parts of', 'Reference', 'Streets', 'Reason', 'Customers',
                    'Status']

    f_content = array_to_csv_line(f_headers_arr)

    for d in response_data:
        tStartDate = str_to_date_time(d['StartDateTime'])
        tEndDate = str_to_date_time(d['EndDateTime'])
        oDate = date_to_date_string(tStartDate)
        oStEt = date_to_formatted_date(tStartDate) + " - " + date_to_formatted_date(tEndDate)
        oApf = d['Area']
        oRef = d['JobId']
        oStreets = d['Streets']
        oReason = d['Cause']
        oCustomers = str(d['Customers'])
        oStatus = d['Status']
        oArr = [oDate, oStEt, oApf, oRef, oStreets, oReason, oCustomers, oStatus]
        f_content += '\n'
        f_content += array_to_csv_line(oArr)
    try: 
        f = open(FILE_PATH, "w")
        f.write(f_content)
        f.close()
        logger.info("written extracted content to downloads folder: {0}".format(FILE_PATH))
    except Exception as e:
        error = True
        error_msg += 'Can\'t write file from extracted source, something went wrong on server: {0}'.format(FILE_PATH)
        logger.error("Error: {0}".format(e))

    sleep(3)
    for r, d, f in os.walk(DOWNLOAD_FOLDER):
        if len(f) > 0:
            for fr in f:
                if fnmatch(fr, FILE_NAME_FORMAT + '*'):
                    shutil.copy(os.path.join(r, fr),API_DOWNLOADS_DESINATION_FILE)

    logger.info("File successfully written for API service: {0}".format(API_DOWNLOADS_DESINATION_FILE))
except Exception as e:
    error = True
    error_msg += 'Unable to download file from source url: {0}'.format(data_table_url)
    logger.error('Error: {0}'.format(e))

from helpers.notifications import send_email_notification_of_failure as notify
from helpers.connection import add_extraction_source_details as conn

if error == True:
    notify(source_name='ausgrid', source_url=data_table_url, extraction_date=datetime.today().strftime('%Y-%m-%d'), error_msg=error_msg)
else:
    conn(source_name='ausgrid', source_url=data_table_url, extraction_date=datetime.today().strftime('%Y-%m-%d'), success=True)
logger.info("9a. ====={0} DONE=====\n".format(FILE_NAME_FORMAT))