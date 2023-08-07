import os
import csv
import mysql.connector
from datetime import datetime
from mysql.connector import Error
import logging

import pandas as pd
import json
from pymongo.mongo_client import MongoClient


ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
LOGS_FILE = os.path.dirname(os.path.dirname(ROOT_DIR)) + '/logs/connection.log'

logging.basicConfig(filename=LOGS_FILE,
                    filemode='a',
                    format='%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s',
                    datefmt='%Y-%m-%d-%H:%M:%S',
                    level=logging.DEBUG)

logger = logging.getLogger('connectionlogs')


def extract_data(data,name):
    uri = "mongodb+srv://admin:admin123@cluster0.v6tpmcf.mongodb.net/"
    client = MongoClient(uri)
    db = client['outage_owl']
    coll = db['outages']
     
    for index,row in enumerate(data): 
                  
                doc = {
                    "pid": name ,
                    "oid": row.get('oid') or None,
                    "otype": row.get('otype') or None,
                    "addr": {
                        "street": row.get('street') or None,
                        "suburb": row.get('suburb') or None,
                        "area": row.get('area') or None
                    },
                    "ohist": [
                        {
                            "ostart_time": row.get('ostart_time') or None,
                            "o_res_time": row.get('o_res_time') or None,
                            "status": row.get('status') or None,
                            "reason": row.get('reason') or None,
                            "affected_cust": row.get('affected_cust') or None
                        }
                        # for row in eval(data['ohist'])
                    ],
                    "apiv": row.get('apiv') or None,
                    "ctime": datetime.now(),
                    "utime": datetime.now()
                }

                if doc['pid'] == "ausnet" or  doc['pid'] == 'powerwater' or doc['pid'] =='ergon' or doc['pid'] == 'ausgrid':
                    for rows in data[index + 1:]:
                            if rows.get('oid') == doc['oid']:
                                print('hello')
                                data_to_append =  {
                                "ostart_time": rows.get('ostart_time') or None,
                                "o_res_time": rows.get('o_res_time') or None,
                                "status": rows.get('status') or None,
                                "reason": rows.get('reason') or None,
                                "affected_cust": rows.get('affected_cust') or None
                            }
                                doc['ohist'].append(data_to_append)

                if doc['pid'] == 'energex':
                    for rows in data[index + 1:]:
                            if rows.get('suburb') == doc['addr']['suburb'] and rows.get('street') == doc['addr']['street']:
                                print('hello')
                                data_to_append =  {
                                "ostart_time": rows.get('ostart_time') or None,
                                "o_res_time": rows.get('o_res_time') or None,
                                "status": rows.get('status') or None,
                                "reason": rows.get('reason') or None,
                                "affected_cust": rows.get('affected_cust') or None
                            }
                                doc['ohist'].append(data_to_append)

                if doc['pid'] == 'horizonpower':
                    print('jj')
                    for rows in data[index + 1:]:
                        if rows.get('suburb') == doc['addr']['suburb']:
                                print('hello')
                                data_to_append =  {
                                "ostart_time": rows.get('ostart_time') or None,
                                "o_res_time": rows.get('o_res_time') or None,
                                "status": rows.get('status') or None,
                                "reason": rows.get('reason') or None,
                                "affected_cust": rows.get('affected_cust') or None
                            }
                                doc['ohist'].append(data_to_append)


                if doc['pid'] == 'jemena':
                    print('jj')
                    for rows in data[index + 1:]:
                        if rows.get('suburb') == doc['addr']['suburb'] and rows.get('street') == doc['addr']['street']:
                                print('hello')
                                data_to_append =  {
                                "ostart_time": rows.get('ostart_time') or None,
                                "o_res_time": rows.get('o_res_time') or None,
                                "status": rows.get('status') or None,
                                "reason": rows.get('reason') or None,
                                "affected_cust": rows.get('affected_cust') or None
                            }
                                doc['ohist'].append(data_to_append)
                    

                if doc['pid'] == 'unitedenergy':
                    print('jj')
                    for rows in data[index + 1:]:
                        if rows.get('suburb') == doc['addr']['suburb'] and rows.get('area') == doc['addr']['area']:
                                print('hello')
                                data_to_append =  {
                                "ostart_time": rows.get('ostart_time') or None,
                                "o_res_time": rows.get('o_res_time') or None,
                                "status": rows.get('status') or None,
                                "reason": rows.get('reason') or None,
                                "affected_cust": rows.get('affected_cust') or None
                            }
                                doc['ohist'].append(data_to_append)    
                coll.insert_one(doc)

                

def extract_csv_data(data,name):
       uri = "mongodb+srv://admin:admin123@cluster0.v6tpmcf.mongodb.net/"
       client = MongoClient(uri)
       db = client['outage_owl']
       coll = db['outages']
      
       for index,row in enumerate(data):      
                doc = {
                    "pid": name ,
                    "oid": row.get('oid') or None,
                    "otype": row.get('otype') or None,
                    "addr": {
                        "street": row.get('street') or None,
                        "suburb": row.get('suburb') or None,
                        "area": row.get('area') or None
                    },
                    "ohist": [
                        {
                            "ostart_time": row.get('ostart_time'),
                            "o_res_time": row.get('o_res_time') or None,
                            "status": row.get('status') or None,
                            "reason": row.get('reason') or None,
                            "affected_cust": row.get('affected_cust') or None
                        }
                        # for row in eval(data['ohist'])
                    ],
                    "apiv": row.get('apiv') or None,
                    "ctime": datetime.now(),
                    "utime": datetime.now()
                }
                
                if doc['pid'] == 'evoenergy':
                    print('hh')
                    for rows in data[index + 1:]:
                        if rows.get('oid') == doc['oid']:
                            print("heelo")
                            data_to_append =  {
                                "ostart_time": rows.get('ostart_time') or None,
                                "o_res_time": rows.get('o_res_time') or None,
                                "status": rows.get('status') or None,
                                "reason": rows.get('reason') or None,
                                "affected_cust": rows.get('affected_cust') or None
                            }
                            doc['ohist'].append(data_to_append)

                coll.insert_one(doc)
                

