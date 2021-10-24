#!/usr/bin/env python
import numpy as np
import pandas as pd
import time
import csv
import re
import json
import xlrd
import os
from datetime import datetime, date, timedelta
import nltk
#from flatten_json import flatten
regex = re.compile(r"\[|\]|<", re.IGNORECASE)
import time
import copy


def load_n_explode(file_res="API_results_" + time.strftime("%Y%m%d") + ".csv"):
    # file_res = "API_results_20210823.csv"
    my_df = pd.read_csv(file_res, error_bad_lines=False)

    ## Categories
    # my_df["Categories"]
    # Willnjust keep 2 levels.
    my_df["Categories"] = my_df["Categories"].map(eval, na_action='ignore')
    new_df = my_df["Categories"].apply(pd.Series)
    my_df["Categories"] = new_df[0].apply(pd.Series).UrlFriendlyName
    my_df["Sub_Categories"] = new_df[1].apply(pd.Series).UrlFriendlyName


    ###********NEW> LIMIT TO WINES ONLY
    my_df = my_df[my_df['Categories'].isin(['red-wine', 'white-wine'])]
    
    ## WORK AROUND> NOT SURE WHY. TODO
    my_df = my_df[my_df['Stockcode'] != 'ER_2000003422_RX2386']
    my_df = my_df[my_df['Stockcode'] != 'ER_1000004375_CALSG16']
    my_df = my_df[my_df['AdditionalDetails'] != '[]']


    ## Reviews
    my_df["Reviews"] = my_df["Reviews"].map(eval, na_action='ignore')
    # Try with first 2 reviews
    new_df = my_df["Reviews"].apply(pd.Series)
    # First
    my_df["Review1_auth"] = new_df[0].apply(pd.Series).author.apply(pd.Series).Value
    my_df["Review1_authorcontent"] = new_df[0].apply(pd.Series).authorcontent.apply(pd.Series).Value
    my_df["Review1_points"] = new_df[0].apply(pd.Series).points.apply(pd.Series).Value
    try:
        my_df["Review1_source"] = new_df[0].apply(pd.Series).source.apply(pd.Series).Value
    except:
        my_df["Review1_source"] = ""
    try:
        my_df["Review1_text"] = new_df[0].apply(pd.Series).text.apply(pd.Series).Value
    except:
        my_df["Review1_text"] = ""
    try:
        my_df["Review1_vintage"] = new_df[0].apply(pd.Series).vintage.apply(pd.Series).Value
    except:
        my_df["Review1_vintage"] = ""
        
    # Second
    my_df["Review2_auth"] = new_df[1].apply(pd.Series).author.apply(pd.Series).Value
    my_df["Review2_authorcontent"] = new_df[1].apply(pd.Series).authorcontent.apply(pd.Series).Value
    my_df["Review2_points"] = new_df[1].apply(pd.Series).points.apply(pd.Series).Value
    try:
        my_df["Review2_source"] = new_df[1].apply(pd.Series).source.apply(pd.Series).Value
    except:
        my_df["Review2_source"] = ""
    try:
        my_df["Review2_text"] = new_df[1].apply(pd.Series).text.apply(pd.Series).Value
    except:
        my_df["Review2_text"] =""
    try:
        my_df["Review2_vintage"] = new_df[1].apply(pd.Series).vintage.apply(pd.Series).Value
    except:
        my_df["Review2_vintage"] = ""
    
    # Drop reviews
    my_df = my_df.drop('Reviews', 1)

    # Illl make a deep copy for later
    full_df = copy.deepcopy(my_df)
    # full_df = full_df

    # Additional details
    my_df["AdditionalDetails"] = my_df["AdditionalDetails"].map(eval, na_action='ignore')
    # Can't use nested lists of JSON objects in pd.json_normalize
    my_df = my_df.explode(column="AdditionalDetails").reset_index(drop=True)

    # Hacky, but it works... so we wont be touching this stuff!
    add_df = pd.DataFrame(pd.json_normalize(my_df["AdditionalDetails"]))
    del add_df["DisplayName"]
    df = pd.concat([my_df, add_df], axis=1)
    df = df.pivot(index='Stockcode', columns='Name', values='Value').reset_index().drop_duplicates(subset=['Stockcode'],
                                                                                                   keep=False)

      
    # Check point, and also a way to get rid of headers
    newdf = pd.merge(full_df, df, on='Stockcode')
    newdf["Mystery"] = newdf["Description"].str.contains("Wraps")
    # This is an old secret seleciton one. Only two, so will drop them
    newdf = newdf[~newdf["Description"].str.contains("Secret Selection")]
    newdf = newdf[~newdf["Stockcode"].str.contains("672366")]
    # newdf = newdf.drop_duplicates(subset=['Stockcode'], keep=False)
    
    # Dropping a few columns we dont want 
    newdf = newdf.drop('ProductTags', 1)
    newdf = newdf.drop('ProductSashes', 1)
    newdf = newdf.drop('UniqueSellingProposition', 1)
    newdf = newdf.drop('ImageVariants', 1)
    newdf = newdf.drop('AvailablePackTypes', 1)
    try:
        newdf = newdf.drop('GroupedDetails.image', 1)
    except:
        print("Nothing to drop. All good.")
    try:
        newdf = newdf.drop('GroupedDetails.video', 1)
    except:
        print("Nothing to drop. All good.")
    newdf = newdf.drop('GroupedDetails.workflow', 1)
    newdf = newdf.drop('categoryleafnodeid', 1)
    # Now to drop the additional details, we no need no more.
    newdf = newdf.drop('AdditionalDetails', 1)
    
    return newdf



os.chdir('/home/stu/code/dans/files')
#input_file = "API_results_20210903.csv"
input_file = "API_results_" + time.strftime("%Y%m%d") + ".csv"
wide = load_n_explode(input_file)

user = 'root'
passw = 'MYSQLl0g1n!'
host =  '127.0.0.1'
port = 3306
database = 'dans_dev'


from sqlalchemy import create_engine
import pymysql
import pandas as pd

#sqlEngine       = create_engine('mysql+pymysql://root:@127.0.0.1/test', pool_recycle=3600)
sqlEngine = create_engine('mysql+mysqlconnector://' + user + ':' + passw + '@' + host + ':' + str(port) + '/' + database , echo=False)
dbConnection    = sqlEngine.connect()
tableName = "raw_dans_raw_main"

try:
    frame           = wide.to_sql(tableName, dbConnection, if_exists='append', method='multi', chunksize = 1000 );
except ValueError as vx:
    print(vx)
except Exception as ex:   
    print(ex)
else:
    print("Table %s created/updated successfully."%tableName);   
finally:
    dbConnection.close()


