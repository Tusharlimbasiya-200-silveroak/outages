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
FILE_NAME_FORMAT = datetime.now().strftime("%Y-%m-%d-%H%M%S-ausnet-outage")
DOWNLOAD_ENERGEX_FILE_DESTINATION = DOWNLOAD_FOLDER + FILE_NAME_FORMAT + '.csv'
ENERGEX_URL = 'https://www.outagetracker.com.au/outage-list'

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
logger.info("1g. Energex URL: {0}".format(ENERGEX_URL))

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
# options.add_argument("--headless")
options.add_argument("--window-size=1920,1200")
try:
    driver = webdriver.Chrome(service=ChromeService(ChromeDriverManager().install()), options=options)
    driver.get(ENERGEX_URL)
    logger.info("Fetched Energex url: {0}".format(ENERGEX_URL))
    delay = 5
    
    elem = WebDriverWait(driver, delay).until(EC.presence_of_element_located((By.CLASS_NAME, 'w-full')))
    data_table = driver.find_element(By.XPATH,'//*[@id="__next"]/main/div[2]/div[2]/table')
    parsed_data = data_table.get_attribute('outerHTML')
    page_source_dfs = pd.read_html(parsed_data)
    print(page_source_dfs)
   
   
    all_list_elements = driver.find_element(By.XPATH,'//*[@id="__next"]/main/div[2]/div[4]/div[2]')
    print(all_list_elements.text,"llllllll")
    all_buttons = all_list_elements.find_elements(By.TAG_NAME, 'button')
    print(len(all_buttons))
  
    for button in all_buttons:
        # print(button.get_attribute('aria-label') if button.get_attribute('aria-label') else None)
        check_aria_label = button.get_attribute('aria-label')
        if check_aria_label != 'Previous' and check_aria_label != 'Next' and check_aria_label != None and check_aria_label != 'Page 1':
            print(check_aria_label,"kkkkkkkkkkk")
            paginated_page_url = button.click()
            print(button.text,"llllllllllllllllllllllllllll")
            sleep(5)
            paginated_page_table = driver.find_element(By.XPATH,'/html/body/div[1]/main/div[2]/div[2]/table')
            print(paginated_page_table,"kkkkkkkkkkkkkkkkk")
            parsed_button_data= paginated_page_table.get_attribute('outerHTML')
            page_source_button_dfs = pd.read_html(parsed_button_data)
            page_source_dfs.append(page_source_button_dfs)
            print(page_source_button_dfs)


    print("hello")        
    # last_page = all_list_elements[-1]
    # print(int(last_page.text))
    # # Iterate through the pages
    # for page_num in range(last_page):
    #     # Extract the data from the current page
    #     page_output = page_source_dfs
    #     # Add the data to the final_out list as a dictionary with the page number as the key
    #     page_source_dfs.append({
    #         page_num: page_output
    #     })
    #     # Locate the "Next" button on the webpage and click it to navigate to the next page
    #     next_page = driver.find_element(By.XPATH, "//a[text()='›']").click()

    

    try:
        for table in page_source_dfs:
            table.to_csv(DOWNLOAD_ENERGEX_FILE_DESTINATION, header=csv_headers, encoding='utf-8')
        logger.info("Successfully written extracted data to CSV fle: {0}".format(DOWNLOAD_ENERGEX_FILE_DESTINATION))
    except Exception as e:
        error = True
        error_msg += 'Error while writing extracted data to CSV: {0}: {1}'.format(DOWNLOAD_ENERGEX_FILE_DESTINATION, e)
        logger.error("Error while writing extracted data to CSV: {0}: {1}".format(DOWNLOAD_ENERGEX_FILE_DESTINATION, e))
    
    sleep(3)
    for r, d, f in os.walk(DOWNLOAD_FOLDER):
        if len(f) > 0:
            for fr in f:
                if fnmatch(fr, FILE_NAME_FORMAT + '*'):
                    shutil.copy(os.path.join(r, fr),API_DOWNLOADS_DESINATION_FILE)
    logger.info("Successfull written file for API Delivery: {0}".format(API_DOWNLOADS_DESINATION_FILE))
except Exception as e:
    error = True
    error_msg += 'Unable to fetch data from the URL: {0}: {1}'.format(ENERGEX_URL, e)
    logger.error("2ERR. Unable to fetch data from the URL: {0}: {1}".format(ENERGEX_URL, e))

# from helpers.notifications import send_email_notification_of_failure as notify
# from helpers.connection import add_extraction_source_details as conn

# if error == True:
#     notify(source_name='energex', source_url=ENERGEX_URL, extraction_date=datetime.today().strftime('%Y-%m-%d'), error_msg=error_msg)
# else:
#     conn(source_name='energex', source_url=ENERGEX_URL, extraction_date=datetime.today().strftime('%Y-%m-%d'), success=True)
# logger.info("9a. ====={0} DONE=====\n".format(FILE_NAME_FORMAT))



# from selenium import webdriver
# from selenium import webdriver
# from selenium.webdriver.support.ui import WebDriverWait
# from selenium.webdriver.support import expected_conditions as EC
# from selenium.webdriver.common.by import By
# from selenium.common.exceptions import TimeoutException
# from selenium.webdriver.chrome.options import Options

# from webdriver_manager.chrome import ChromeDriverManager 
# from selenium.webdriver.chrome.service import Service as ChromeService
# import pandas as pd


# ENERGEX_URL = 'https://www.outagetracker.com.au/outage-list'

# table_list =[]
# driver = webdriver.Chrome(service=ChromeService(ChromeDriverManager().install()))
# driver.get(ENERGEX_URL)

# Types= driver.find_elements(By.XPATH,'//table/tbody/tr/td[1]')
# for i in Types:
#     print(i.text)

