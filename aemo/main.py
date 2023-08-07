import os
import urllib.request
import shutil
import zipfile
from datetime import datetime
from fnmatch import fnmatch
import pandas as pd
import logging
import csv

ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
DOWNLOAD_FOLDER = ROOT_DIR + '/downloads/'
FILE_NAME_FORMAT = datetime.now().strftime("%Y-%m-%d-%H%M%S-aemo-outage")
ZIP_FILE_DOWNLOAD_FOLDER = DOWNLOAD_FOLDER + 'zips/'
EXTRACT_FOLDER = DOWNLOAD_FOLDER + 'extracts/' + FILE_NAME_FORMAT
ZIP_FILE_PATH = ZIP_FILE_DOWNLOAD_FOLDER + FILE_NAME_FORMAT + '.zip'
EXTRACT_AEMO_FILE_DESTINATION = EXTRACT_FOLDER

AEMO_URL = 'https://www.aemo.com.au/aemo/data/NOS/PUBLIC_NOSDAILY.zip'
API_DOWNLOADS_DESINATION = os.path.dirname(os.path.dirname(ROOT_DIR)) + '/outages/aemo/'
API_DOWNLOADS_DESINATION_FILE = API_DOWNLOADS_DESINATION + FILE_NAME_FORMAT + '.csv'

LOGS_FILE = os.path.dirname(os.path.dirname(ROOT_DIR)) + '/logs/aemo.log'

error = error_msg = ''

logging.basicConfig(filename=LOGS_FILE,
                    filemode='a',
                    format='%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s',
                    datefmt='%Y-%m-%d-%H:%M:%S',
                    level=logging.DEBUG)

logger = logging.getLogger('aemologs')

logger.info("1. ====={0} STARTED =====".format(FILE_NAME_FORMAT))
logger.info("1a. ROOT DIR: {0}".format(ROOT_DIR))
logger.info("1b. DOWNLOAD FOLDER: {0}".format(DOWNLOAD_FOLDER))
logger.info("1c. FILE NAME FORMAT: {0}".format(FILE_NAME_FORMAT))
logger.info("1d. ZIP FILE DOWNLOAD FOLDER: {0}".format(ZIP_FILE_DOWNLOAD_FOLDER))
logger.info("1e. EXTRACT FOLDER: {0}".format(EXTRACT_FOLDER))
logger.info("1f. ZIP FILE PATH: {0}".format(ZIP_FILE_PATH))
logger.info("1g. EXTRACT AEMO FILE DESTINATION: {0}".format(EXTRACT_AEMO_FILE_DESTINATION))
logger.info("1h. AEMO URL: {0}".format(AEMO_URL))

if not os.path.exists(ZIP_FILE_DOWNLOAD_FOLDER):
    logger.info("2a. Zip file folder path was not existed, so creating...")
    try:
        os.makedirs(ZIP_FILE_DOWNLOAD_FOLDER)
        logger.info("2b. Zip file folder path was not existed, so created...")
    except Exception as e:
        error = True
        error_msg += 'Unable to create directory on server: {0}'.format(ZIP_FILE_DOWNLOAD_FOLDER)
        logger.error("2ERR. Error creating directory: {0}: {1}".format(ZIP_FILE_DOWNLOAD_FOLDER, e))

if not os.path.exists(EXTRACT_FOLDER):
    logger.info("3a. Extract folder path was not existed, so creating...")
    try:
        os.makedirs(EXTRACT_FOLDER)
        logger.info("3b. Extract folder path was not existed, so created...")
    except Exception as e:
        error = True
        error_msg += 'Unable to create directory on server: {0}'.format(EXTRACT_FOLDER)
        logger.error("3ERR. Error creating directory: {0}: {1}".format(EXTRACT_FOLDER, e))

if not os.path.exists(API_DOWNLOADS_DESINATION):
    logger.info("4a. API Download folder path was not existed, so creating...")
    try:
        os.makedirs(API_DOWNLOADS_DESINATION)
        logger.info("4b. API Download folder path was not existed, so created...")
    except Exception as e:
        error = True
        error_msg += 'Unable to create directory on server: {0}'.format(API_DOWNLOADS_DESINATION)
        logger.error("4ERR. Error creating directory: {0}: {1}".format(API_DOWNLOADS_DESINATION, e))

if len(os.listdir(EXTRACT_AEMO_FILE_DESTINATION)) == 0:
    logger.info("5a. Seems like, Data extract for this date is not there so starting one...")
    try:
        opener = urllib.request.URLopener()
        opener.addheader('User-Agent', 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) ')
        downloaded_file,headers = opener.retrieve('https://www.aemo.com.au/aemo/data/NOS/PUBLIC_NOSDAILY.zip', ZIP_FILE_PATH)
        if downloaded_file and headers:
            logger.info("6a. Yay! Got the file...")
            with zipfile.ZipFile(ZIP_FILE_PATH, 'r') as zip_ref:
                logger.info("6b. Extracting the download file")
                try:
                    zip_ref.extractall(EXTRACT_AEMO_FILE_DESTINATION)
                    logger.info("6b. Files extracted")
                except Exception as e:
                    error = True
                    error_msg += 'Unable to extract downloaded files on server: {0}'.format(ZIP_FILE_PATH)
                    logger.error("6ERR. Unable to extract zip folder: {0}: {1}".format(ZIP_FILE_PATH, e))

            for r, d, f in os.walk(EXTRACT_AEMO_FILE_DESTINATION):
                logger.info("7a. Checking the files downloaded")
                if len(f) > 0:
                    for fr in f:
                        if fnmatch(fr, "PUBLIC_NOSDAILY*"):
                            logger.info("7b. Checking if it has a file name starts with PUBLIC_NOSDAILY which we're interested in...")
                            with open(os.path.join(r, fr), 'r') as f:
                                with open(API_DOWNLOADS_DESINATION_FILE, 'w') as f1:
                                    next(f)
                                    for line in f:
                                        f1.write(line)
                            logger.info("7c. Yay! we found the file and wrote it to the API downloads directory: File: {0}".format(fr))
                            # shutil.copy(os.path.join(r, fr),API_DOWNLOADS_DESINATION_FILE)
                else:
                    error = True
                    error_msg += 'No file found, something went wrong on server: {0}'.format(EXTRACT_AEMO_FILE_DESTINATION)
                    logger.error("7ERR: Weird, No file found, something is wrong {0}: {1}".format(EXTRACT_AEMO_FILE_DESTINATION, e))
        else:
            logger.info("5b. Either program failed to get the data file or there isn't really any data...")
    except Exception as e:
        error = True
        error_msg += 'Can\'t download file from source, something went wrong on server: {0}'.format(FILE_NAME_FORMAT)
        logger.error("5ERR. Can't download file, something is wrong: {0}: {1}".format(FILE_NAME_FORMAT, e))
else:
    logger.info("8a. Yikes! Extract is already done for this date: {0}".format(FILE_NAME_FORMAT))

with open(API_DOWNLOADS_DESINATION_FILE,'r') as f:
    csv_reader = csv.DictReader(f)
    df = pd.DataFrame(csv_reader)
    df['full_column'] = df['Equipment Name'] + df['TNSP Submitted the Outage'] + df['Start Time']
    df2 = df.rename(columns={'Region':'region','Status':'status','Start Time':'ostart_time','End Time':'o_res_time','full_column':'oid'})
    aemo_data = df2.to_dict(orient='records')
   
            

import sys
sys.path.append(r'/home/webstring-tushar/Documents/work/outage/outage-owl/helpers')

import notifications 
import connection


if error == True:
    notifications.send_email_notification_of_failure(source_name='aemo', source_url=AEMO_URL, extraction_date=datetime.today().strftime('%Y-%m-%d'), error_msg=error_msg)
else :
    connection.extract_csv_data(data=aemo_data,name='aemo')
logger.info("9a. ====={0} DONE=====\n".format(FILE_NAME_FORMAT))