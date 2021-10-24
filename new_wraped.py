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
from sklearn.neighbors import NearestNeighbors
from sklearn.feature_extraction.text import TfidfVectorizer
import nltk
#from flatten_json import flatten
regex = re.compile(r"\[|\]|<", re.IGNORECASE)
import time
import copy
from fuzzywuzzy import process, fuzz
from sqlalchemy import create_engine
import pymysql
import pandas as pd


def giveaway(df):
    gives = df.copy(deep=True)
    gives = gives[['Stockcode','Description','webproductname','Prices.singleprice.Value','Prices.promoprice.Value','Prices.promoprice.BeforePromotion','Prices.promoprice.AfterPromotion','IsForDelivery']]
    gives = gives[gives.webproductname.notnull()]
    gives = gives[gives["Description"].str.contains("Wraps")]
    gives = gives[~gives["webproductname"].str.contains("Wraps")]
    gives = gives[gives["IsForDelivery"]]
    gives["METHOD"] = "giveaway"
    return gives



def ohe(df):
    #### ONE HOT ENCODED
    ##### First I split into numeric and nominal. OHE the nominal
    known=df
    exclude_col = known.select_dtypes(include=np.number).columns.tolist() + ["Stockcode"]
    my_df_num = known[exclude_col]
    my_df_cat = known.drop(exclude_col, axis=1)
    # my_df_cat.to_csv("FIN.csv")
    my_df_cat_ohe = pd.get_dummies(my_df_cat)
    my_df_ohe = pd.concat([my_df_num, my_df_cat_ohe], axis=1)
    my_df_ohe = my_df_ohe.fillna(0)
    my_df_ohe = my_df_ohe.replace(np.nan, 0)

    # Drop duplicates #TODO check whats better to keep
    my_df_ohe = my_df_ohe.loc[:, ~my_df_ohe.columns.duplicated()]
    # Clean up names
    my_df_ohe.columns = [regex.sub("_", col) if any(x in str(col) for x in set(('[', ']', '<'))) else col for col in
                         my_df_ohe.columns.values]

    return my_df_ohe

def get_knn(df, thresh):

    myst  = df[df["Mystery"]]
    known  = df[~df["Mystery"]]

    myst_nn = myst.drop("Stockcode", axis=1)
    myst_nn = myst_nn.drop("Mystery", axis=1)
    known_nn = known.drop("Stockcode", axis=1)
    known_nn = known_nn.drop("Mystery", axis=1)

    # Create the k-NN model using k=5
    nn_abs = NearestNeighbors(n_neighbors=1, algorithm='auto')

    # Fit it
    nn_abs.fit(known_nn)

    results_wine = []

    for index in range(len(myst.index)):
        distance, matches = nn_abs.kneighbors(myst_nn.iloc[[index]], 1, return_distance=True)
        results_wine.append(
            {
                #'Mystery': "https://www.danmurphys.com.au/product/" + str(myst['Stockcode'].iloc[[index][0]]),
                'Stockcode_x': str(myst['Stockcode'].iloc[[index][0]]),
                #'Matched': "https://www.danmurphys.com.au/product/" + str(known["Stockcode"].iloc[matches[0][0]]),
                'Stockcode_y': str(known["Stockcode"].iloc[matches[0][0]]),
                'Distance': str(distance[0][0])

            }
        )

    matched = pd.DataFrame(results_wine)
    matched["MatchLevel"] = np.where(matched['Distance'].astype(float) < float(thresh), "Good", "Poor")
    #matched = matched.sort_values(['MatchLevel', 'Savings'], ascending=[True, False])
    matched["METHOD"] = "KNN"
    return matched


def lazy_desc(df, keep1):
    #df = wide
    #keep1 = keep_nlp
    kept = df[keep1]

    kept.reset_index(drop=True, inplace=True)
    myst = kept[kept["Mystery"]]
    known = kept[~kept["Mystery"]]
    
    known = known[known["Review1_text"] != "[...]"]
    known = known[known["Review2_text"] != "[...]"]
    
    known = known[known["Review1_text"] != "[..]"]
    known = known[known["Review2_text"] != "[..]"]
    
    known = known[known["Review1_text"] != "[....]"]
    known = known[known["Review2_text"] != "[....]"]
 

    # Description match
    desc_match = pd.merge(myst[myst['RichDescription'].notna()], known, on=['RichDescription'], how='inner')
    desc_match["METHOD"] = "desc_match"
    # Web match
    webdesc_match = pd.merge(myst[myst['webdescriptionshort'].notna()], known, on=['webdescriptionshort'], how='inner')
    webdesc_match["METHOD"] = "webdesc_match"
    # Review match (TODO more than 2 deep)
    rev_match = pd.merge(myst[myst['Review1_text'].notna()], known, on=['Review1_text'], how='inner')
    rev_match["METHOD"] = "rev_match"
    #rev_match = rev_match[~[rev_match['Review1_text'] == [...]]]
    rev1_match = pd.merge(myst[myst['Review2_text'].notna()], known, on=['Review2_text'], how='inner')
    rev1_match["METHOD"] = "rev2_match" # I know. Using normal index for people...
    #rev_match = rev_match[~[rev_match.['Review1_text'].str.contains("[...]")]]
    text_match = desc_match.append(webdesc_match).append(rev_match).append(rev1_match).reset_index()
    text_match = text_match[['Stockcode_x', 'Stockcode_y',"METHOD"]]
    text_match["MatchLevel"] = "Good"
    return text_match


## Try Fuzzywuzzy

def fuzzy_merge(df_1, df_2, key1, key2, threshold=90, limit=1):
    """
    :param df_1: the left table to join
    :param df_2: the right table to join
    :param key1: key column of the left table
    :param key2: key column of the right table
    :param threshold: how close the matches should be to return a match, based on Levenshtein distance
    :param limit: the amount of matches that will get returned, these are sorted high to low
    :return: dataframe with boths keys and matches
    """
    s = df_2[key2].tolist()

    m = df_1[key1].apply(lambda x: process.extract(x, s, limit=limit))
    df_1['matches'] = m

    m2 = df_1['matches'].apply(lambda x: ', '.join([i[0] for i in x if i[1] >= threshold]))
    df_1['matches'] = m2

    return df_1

#fuzzy_merge(myst, known, 'webdescriptionshort', 'webdescriptionshort', threshold=80)


def make_clickable(val):
    # target _blank to open new window
    val1 = 'https://www.danmurphys.com.au/product/' + str(val)
    return '<a target="_blank" href="{}">{}</a>'.format(val1, val)


## CONFIG

keep_nlptdf =['Stockcode',
 'webdescriptionshort',
'RichDescription']

keep_ohe =['Categories',
 'Mystery',
 'Stockcode',
 'PackageSize',
 'Prices.inanysixprice.Message',
 'Prices.inanysixprice.Value',
 'Sub_Categories',
 'Review1_auth',
 'Review1_points',
 'Review1_source',
 'awardwinner',
 'glutenfree',
 'preservativefree',
 'varietal',
 'webalcoholpercentage',
 'webbottleclosure',
 'webcountryoforigin',
 'webfoodmatch',
 'webisorganic',
 'webisvegan',
 'webliquorsize',
 'webmaincategory',
 'webregionoforigin',
 'webstateoforigin',
 'webtotalreviewcount',
 'webwinebody',
 'webwinestyle',
 'IsForDelivery']


keep_nlp =['Categories',
 'Stockcode',
 'Mystery',
 'PackageSize',
 'RichDescription',
 'Review1_text',
 'Review2_text',
 'Prices.inanysixprice.Message',
 'Prices.inanysixprice.Value',
 'Sub_Categories',
 'Review1_auth',
 'Review1_points',
 'Review1_source',
 'awardwinner',
 'glutenfree',
 'preservativefree',
 'varietal',
 'webalcoholpercentage',
 'webbottleclosure',
 'webcountryoforigin',
 'webdescriptionshort',
 'webfoodmatch',
 'webisorganic',
 'webisvegan',
 'webliquorsize',
 'webmaincategory',
 'webregionoforigin',
 'webstateoforigin',
 'webtotalreviewcount',
 'webwinebody',
 'webwinestyle',
 'IsForDelivery']


#### RUNNING HERE
user = 'root'
passw = 'MYSQLl0g1n!'
host =  '127.0.0.1'
port = 3306
database = 'dans_dev'
sqlEngine = create_engine('mysql+mysqlconnector://' + user + ':' + passw + '@' + host + ':' + str(port) + '/' + database , echo=False)
dbConnection    = sqlEngine.connect()

sql = "With latest as (select Stockcode, max(ts_activity) ts_activity from raw_dans_raw_main group by Stockcode) select rdrm.* from raw_dans_raw_main rdrm, latest l where 1=1 and rdrm.Stockcode=l.Stockcode and rdrm.ts_activity=l.ts_activity ;"

wide = pd.read_sql(sql, dbConnection)
wide = wide.iloc[: , 2:]

wide["IsForDelivery"] = wide["IsForDelivery"].astype(bool)
wide["Mystery"] = wide["Mystery"].astype(bool)



## Get last runs
#input_file = "/home/stu/code/dans/files/previous.csv"
#prev = load_n_explode(input_file)

## 
#wide = prev.append(wide)
 

## Get the easy matches
gives = giveaway(wide)
lazy = lazy_desc(wide,keep_nlp)

#now the OHE and KNN
ohe_file = ohe(wide[keep_ohe])
knns = get_knn(ohe_file,1.8)

# Cleanup and save it
matches = knns.append(lazy)

# Check if its in stock

matches = matches[matches.MatchLevel.str.contains("Good")] # Get ride of bad matches first
avail = wide[wide["IsForDelivery"]][["Stockcode","Mystery",'varietal','webbottleclosure','Prices.singleprice.Value','Prices.promoprice.Value','Prices.promoprice.BeforePromotion','Prices.promoprice.AfterPromotion']]
matches = pd.merge(avail,matches, left_on='Stockcode', right_on="Stockcode_x")


matches["Savings"] = matches['Prices.promoprice.BeforePromotion'] - matches['Prices.promoprice.AfterPromotion']

matches = pd.merge(matches, wide[["Description","Stockcode"]], left_on='Stockcode_y', right_on="Stockcode",how='left')

matches = matches[['Stockcode_x', 'Stockcode_y', 'Description', 'varietal','webbottleclosure','Prices.promoprice.BeforePromotion', 'Prices.promoprice.AfterPromotion', 'Savings']]
matches = matches.sort_values(['Savings'], ascending=[False])

matches = matches.loc[:,~matches.columns.duplicated()]

matches = matches.drop_duplicates()

## General available
myst = wide[wide["Mystery"]]
myst["Savings"] = myst['Prices.promoprice.BeforePromotion'] - myst['Prices.promoprice.AfterPromotion']
myst =  pd.merge(avail,myst, left_on='Stockcode', right_on="Stockcode")
myst = myst[["Stockcode",'Description','varietal_x','webbottleclosure_x','Prices.promoprice.BeforePromotion_x', 'Prices.promoprice.AfterPromotion_x', 'Savings' ]]
myst = myst[["Stockcode",'Description','varietal_x','webbottleclosure_x','Prices.promoprice.BeforePromotion_x', 'Prices.promoprice.AfterPromotion_x', 'Savings' ]]
myst = myst.sort_values(['Savings'], ascending=[False])


#csv_file = "Match_results" + time.strftime("%Y%m%d") + ".csv"
#matches.to_csv(csv_file)

from datetime import date, timedelta  
## Taken out 24.10.2021
#yesterday = date.today() - timedelta(days=1) 
#today = date.today()   
#file_yes = "Match_results" + yesterday.strftime("%Y%m%d") + ".csv" 
#file_tod = "Match_results" + today.strftime("%Y%m%d") + ".csv"
#df1 = pd.read_csv(file_yes).iloc[:, 1:]
#df2 = pd.read_csv(file_tod).iloc[:, 1:]
#df_diff = pd.concat([df1,df2]).drop_duplicates(keep=False)

#matches = df_diff
#matches.reset_index(drop=True, inplace=True)

tableName = "match_results"

try:
    frame           = matches.to_sql(tableName, dbConnection, if_exists='append', method='multi', chunksize = 1000 );
except ValueError as vx:
    print(vx)
except Exception as ex:   
    print(ex)
else:
    print("Table %s created/updated successfully."%tableName);   
#finally:
#    dbConnection.close()



final = matches.style.format({'Stockcode_x': make_clickable, 'Stockcode_y': make_clickable, }) \
    .bar(subset=['Savings'], align='mid', color=['#5fba7d']) \
    .bar(subset=['Savings'], align='mid', color=['#5fba7d']) \
    .hide_index()


# Now complete list
myst.reset_index(drop=True, inplace=True)

tableName = "all_deals"

try:
    frame           = myst.to_sql(tableName, dbConnection, if_exists='append', method='multi', chunksize = 1000 );
except ValueError as vx:
    print(vx)
except Exception as ex:   
    print(ex)
else:
    print("Table %s created/updated successfully."%tableName);   
    
    
## Formatting   # Commented out 24/10/2021

#final_myst = myst.style.format({'Stockcode': make_clickable }) \
#    .bar(subset=['Savings'], align='mid', color=['#5fba7d']) \
#    .bar(subset=['Savings'], align='mid', color=['#5fba7d']) \
#    .hide_index()


##writing HTML Content
#heading = '<h1> Mystery Wines</h1>'
#subheading = '<h3> Matched </h3>'
## Using .now() from datetime library to add Time stamp
#now = datetime.now()
#current_time = now.strftime("%m/%d/%Y %H:%M:%S")
#header = '<div class="top">' + heading + subheading +'</div>'
#footer = '<div class="bottom"> <h3> This Report has been Generated on'+ current_time +'</h3> </div>'
#content = final
#content_2 = final_myst
## Concating everything to a single string

#html = header + content.render() + footer
#html_file = "Match_new.html"
#with open(html_file,'w+') as file:
#    file.write(html)


#html = header + content_2.render()  + footer
#html_file = "All_deals.html"
#with open(html_file,'w+') as file:
#    file.write(html)    
