import os
import mysql.connector
from mysql.connector import Error
import logging

ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
LOGS_FILE = os.path.dirname(os.path.dirname(ROOT_DIR)) + '/logs/connection.log'

logging.basicConfig(filename=LOGS_FILE,
                    filemode='a',
                    format='%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s',
                    datefmt='%Y-%m-%d-%H:%M:%S',
                    level=logging.DEBUG)

logger = logging.getLogger('connectionlogs')


def connect_mysql():
    connection = None
    try:
        logger.info("1. connecting to MySQL DB")
        connection = mysql.connector.connect(host='127.0.0.1',
                                            database='senstra_api',
                                            user='senstra_user',
                                            password='B3*fz8D3CWzHI1%K')
        if connection.is_connected():
            db_Info = connection.get_server_info()
            logger.info("1a. Connected to mysql DB: {0}".format(db_Info))
            return connection,True
    except Error as e:
        return connection, False
        logger.error("Error while connecting to MySQL: {0}".format(e))


def add_extraction_source_details(source_name=None, source_url=None, extraction_date=None, no_of_records=None, success=None):
    conn,status = connect_mysql()
    if status:
        try:
            db_cursor = conn.cursor()
            sql = """INSERT INTO extraction_coverage (source_name,source_url, extraction_date, no_of_records) VALUES (%s, %s, %s, %s)"""
            record = (source_name,source_url,extraction_date,no_of_records)
            db_cursor.execute(sql, record)
            conn.commit()
            conn.close()
            logger.info("Coverage added: source_name: {0}, source_url: {1}, extraction_date: {2}, no_of_records: {3}".format(source_name, source_url, extraction_date, no_of_records, success))
            return True
        except Exception as error:
            logger.error("Error while inserting into table: {0}".format(error))
            return False