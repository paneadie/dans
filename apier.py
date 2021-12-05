#!/usr/bin/env python
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import Select
from selenium.webdriver.common.by import By
import pickle
import requests
import numpy as np
import pandas as pd
import time
import csv
import re
import json
import xlrd
import os
from selenium.common.exceptions import NoSuchElementException
from datetime import datetime, date, timedelta
import nltk
from selenium.webdriver.firefox.options import Options
#from flatten_json import flatten
import time

import glob
from sqlalchemy import create_engine
import pymysql


# Here we'll loop through all files and blend them into one.

path = r'/home/stu/code/dans/files' # use your path
all_files = glob.glob(path + "/new.wines")

li = []

for filename in all_files:
    df = pd.read_csv(filename, index_col=None, header=0)
    li.append(df)

frame = pd.concat(li, axis=0, ignore_index=True)

# And now to cleanup results, and remove duplicates

wines = frame

wines.columns = ['URL']

# And now I'll filter out the product linkes
wines = wines[wines.URL.str.contains('https://www.danmurphys.com.au/product',case=False, na=False)]

wines = wines.replace(r'.*https://www.danmurphys.com.au/product/DM_', r'', regex=True)
wines = wines.replace(r'/.*$', r'', regex=True)
wines = wines.drop_duplicates()
wines = wines.dropna()

#Get the existing mysteries and call again
#### RUNNING HERE
user = 'root'
passw = 'MYSQLl0g1n!'
host =  '127.0.0.1'
port = 3306
database = 'dans_dev'
sqlEngine = create_engine('mysql+mysqlconnector://' + user + ':' + passw + '@' + host + ':' + str(port) + '/' + database , echo=False)
dbConnection    = sqlEngine.connect()

stockcode_sql = """\
--  
select distinct(Stockcode) from raw_dans_raw_main where Mystery=1;
"""

stockcode = pd.read_sql(stockcode_sql, dbConnection)

stockcode = stockcode.rename(columns={'Stockcode': 'URL'})
wines = wines.append(stockcode)


#And now to call the API

mylist = wines['URL'].tolist()


#ylist = mylist[0:100]
result = []

for wine in mylist:
    url = "https://api.danmurphys.com.au/apis/ui/Product/" + str(wine)
    try:
        r = requests.get(url).json()
        rtoo = pd.json_normalize(r["Products"])
        result.append(rtoo)
    except:
        print("No product found" + url)

my_df = pd.concat(result)
file_res = path + "/API_results_" + time.strftime("%Y%m%d") + ".csv"

my_df.to_csv(file_res)

