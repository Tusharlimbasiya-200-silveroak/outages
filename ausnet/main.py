from time import sleep
import requests
import os
from datetime import datetime
from fnmatch import fnmatch
import shutil
import logging

ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
DOWNLOAD_FOLDER = ROOT_DIR + '/downloads/'
FILE_NAME_FORMAT = datetime.now().strftime("%Y-%m-%d-%H%M%S-ausnet-outage")

FILE_PATH = DOWNLOAD_FOLDER + datetime.now().strftime("%Y-%m-%d-%H%M%S-ausnet-outage.csv")

API_DOWNLOADS_DESINATION = os.path.dirname(os.path.dirname(ROOT_DIR)) + '/outages/ausnet/'
API_DOWNLOADS_DESINATION_FILE = API_DOWNLOADS_DESINATION + FILE_NAME_FORMAT + '.csv'

data_table_url = 'https://www.outagetracker.com.au/home/GetOutageListData'

LOGS_FILE = os.path.dirname(os.path.dirname(ROOT_DIR)) + '/logs/ausnet.log'

error = error_msg = None

logging.basicConfig(filename=LOGS_FILE,
                    filemode='a',
                    format='%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s',
                    datefmt='%Y-%m-%d-%H:%M:%S',
                    level=logging.DEBUG)

logger = logging.getLogger('ausnetlogs')

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
        logger.info("2b. Download folder path was not existed, so created...")
    except Exception as e:
        error = True
        error_msg += 'Unable to create directory on server: {0}'.format(API_DOWNLOADS_DESINATION)
        logger.error("2ERR. Error creating directory: {0}: {1}".format(API_DOWNLOADS_DESINATION, e))

def strToDatetime(str_timestamp):
    int_timestamp = int(str_timestamp[6:-2])
    var_dt = datetime.fromtimestamp(int_timestamp / 1000.0)
    return var_dt


def dtToDate(dt):
    return dt.strftime("%a %-d %b")


def dtToFdt(dt):
    return dt.strftime("%I:%M %p (%a %-d %b)")


def addEscapeQuote(st):
    return st.replace('"', '""')


def arrToLine(arr):
    arre = ['"' + addEscapeQuote(i) + '"' for i in arr]
    return ','.join(arre)



headers_dict = {
    'Connection': 'keep-alive',
    'Pragma': 'no-cache',
    'Cache-Control': 'no-cache',
    'Accept': 'application/json, text/javascript, */*; q=0.01',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/87.0.4280.88 Safari/537.36',
    'Content-Type': 'application/json;charset=UTF-8',
    'Referer': 'https://www.outagetracker.com.au/outagelist',
    'Sec-Fetch-Site': 'same-origin',
    'Sec-Fetch-Mode': 'cors',
    'Sec-Fetch-Dest': 'empty'
}

try:
    response = requests.get(data_table_url, headers=headers_dict)

    if response.status_code != 200:
        error = True
        error_msg += "Couldn't fetch datatable from url: {0}".format(data_table_url)
        logger.error("Couldn't fetch datatable from url: {0}".format(data_table_url))
        exit(0)

    response_json = response.json()
    response_data = response_json

    f_headers_arr = [
        'Reference',
        'Type',
        'Suburb',
        'PostCode',
        'Affected Customers',
        'Status',
        'Cause/Reason',
        'Outage Time',
        'Outage Date',
        'Est Time To Restore',
        'Notes'
    ]

    f_content = arrToLine(f_headers_arr)
    '''
    {'startDate': '18-JAN-2021', 'startTime': '12:00PM', 'cause': 'Critical Maintenance',
    'status': 'Pre-Arranged', 'suburb': 'Yarra Glen', 'postCode': 3775, 'numberOffSupply': 1,
    'estimatedTimeToRestoration': '02:00PM 18-JAN-2021', 'notes': '', 'type': 'Planned', 'incident': 'INCD-8692-c'}
    '''
    for d in response_data:
        o_reference = d['incident']
        o_type = d['type']
        o_suburb = d['suburb']
        o_postcode = str(d['postCode'])
        o_aff_customers = str(d['numberOffSupply'])
        o_status = d['status']
        o_cause = d['cause']
        o_outage_time = d['startTime']
        o_outage_date = d['startDate']
        o_est_to_restore = d['estimatedTimeToRestoration']
        o_notes = d['notes']
        oArr = [o_reference, o_type, o_suburb, o_postcode, o_aff_customers, o_status, o_cause, o_outage_time,
                o_outage_date, o_est_to_restore, o_notes]
        f_content += '\n'
        f_content += arrToLine(oArr)

    try:
        f = open(FILE_PATH, "w")
        f.write(f_content)
        f.close()
        logger.info("File written successfully: {0}".format(FILE_PATH))
    except Exception as e:
        error = True
        error_msg += "Error while writing extracted content to file: {0}".format(FILE_PATH)
        logger.error("Error while writing extracted content to file: {0}".format(FILE_PATH))
    sleep(3)
    for r, d, f in os.walk(DOWNLOAD_FOLDER):
        if len(f) > 0:
            for fr in f:
                if fnmatch(fr, FILE_NAME_FORMAT + '*'):
                    shutil.copy(os.path.join(r, fr),API_DOWNLOADS_DESINATION_FILE)
    logger.info("File has been written successfully for API Delivery: {0}".format(API_DOWNLOADS_DESINATION_FILE))
except Exception as e:
    error = True
    error_msg += "Error while extracting content from url: {0}: {1}".format(data_table_url, e)
    logger.error("Error while extracting content from url: {0}: {1}".format(data_table_url, e))

from helpers.notifications import send_email_notification_of_failure as notify
from helpers.connection import add_extraction_source_details as conn

if error == True:
    notify(source_name='ausnet', source_url=data_table_url, extraction_date=datetime.today().strftime('%Y-%m-%d'), error_msg=error_msg)
else:
    conn(source_name='ausnet', source_url=data_table_url, extraction_date=datetime.today().strftime('%Y-%m-%d'), success=True)
logger.info("9a. ====={0} DONE=====\n".format(FILE_NAME_FORMAT))