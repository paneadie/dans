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

## Loading now as I'll need this at the end
master_cols = ['age',
'AgeRestricted',
'awardwinner',
'BackorderMessage',
'BackorderStockOnHand',
'brewery',
'Categories',
'cidertype',
'corkscoreeligible',
'countryoforigin',
'DeliveryOptionsInfo',
'Description',
'DisplayQuantity',
'dm_stockcode',
'extbeerid',
'glutenfree',
'ibu',
'image1',
'image2',
'image3',
'image4',
'index',
'Inventory',
'Inventory.availableinventoryqty',
'Inventory.backorderavailableinventoryqty',
'Inventory.backorderinventoryflag',
'Inventory.clickandcollect2hrsinventoryqty',
'Inventory.clickandcollect2hrsinventoryqtymessage',
'Inventory.clickandcollect7daysinventoryqty',
'Inventory.clickandcollect7daysinventoryqtymessage',
'Inventory.findinstoreinventoryqty',
'Inventory.findinstoreinventoryqtymessage',
'Inventory.isfindme2hrsinventoryavailable',
'Inventory.isfindme7daysinventoryavailable'
,'Inventory.isfindmedeliveryinventoryavailable'
,'Inventory.nextdaydeliveryinventoryqty'
,'Inventory.nextdaydeliveryinventoryqtymessage'
,'Inventory.onlineinventoryqty'
,'Inventory.pickadaydeliveryinventoryqty'
,'Inventory.pickadaydeliveryinventoryqtymessage'
,'Inventory.samedaydeliveryinventoryqty'
,'Inventory.samedaydeliveryinventoryqtymessage'
,'IsComingSoon'
,'IsDeliveryOnly'
,'IsEdrSpecial'
,'IsFeaturedTag'
,'IsFindMeAvailable'
,'IsForCollection'
,'IsForDelivery'
,'IsInDefaultList'
,'IsInTrolley'
,'IsInWishList'
,'IsMemberSpecial'
,'IsNew'
,'IsOnSpecial'
,'IsPreSale'
,'IsPurchasable'
,'isvintagerollingorstatic'
,'LargeImageFile'
,'MediumImageFile'
,'MinimumQuantity'
,'Mystery'
,'NumberOfReviews'
,'OverallRating'
,'PackageSize'
,'ParentStockCode'
,'PickupStoreInventoryInfo.Available'
,'PickupStoreInventoryInfo.DistanceFromDeliveryLocation'
,'PickupStoreInventoryInfo.StoreName'
,'PickupStoreInventoryInfo.StoreNo'
,'preservativefree'
,'Prices.caseprice.AfterPromotion'
,'Prices.caseprice.BeforePromotion'
,'Prices.caseprice.IsMemberOffer'
,'Prices.caseprice.Message'
,'Prices.caseprice.Value'
,'Prices.inanysixprice.AfterPromotion'
,'Prices.inanysixprice.BeforePromotion'
,'Prices.inanysixprice.IsMemberOffer'
,'Prices.inanysixprice.Message'
,'Prices.inanysixprice.Value'
,'Prices.promoprice.AfterPromotion'
,'Prices.promoprice.BeforePromotion'
,'Prices.promoprice.IsMemberOffer'
,'Prices.promoprice.Message'
,'Prices.promoprice.Value'
,'Prices.singleprice.AfterPromotion'
,'Prices.singleprice.BeforePromotion'
,'Prices.singleprice.IsMemberOffer'
,'Prices.singleprice.Message'
,'Prices.singleprice.Value'
,'Prices.webminqty.AfterPromotion'
,'Prices.webminqty.BeforePromotion'
,'Prices.webminqty.IsMemberOffer'
,'Prices.webminqty.Message'
,'Prices.webminqty.Value'
,'product_short_name'
,'ProductsInSameOffer'
,'producttitle'
,'QuantityInTrolley'
,'RecommendedProducts'
,'Review1_auth'
,'Review1_authorcontent'
,'Review1_points'
,'Review1_source'
,'Review1_text'
,'Review1_vintage'
,'Review2_auth'
,'Review2_authorcontent'
,'Review2_points'
,'Review2_source'
,'Review2_text'
,'Review2_vintage'
,'RichDescription'
,'SavedLists'
,'ShouldShowFindMeCta'
,'SmallImageFile'
,'Source'
,'standarddrinks'
,'Stockcode'
,'StockOnHand'
,'Sub_Categories'
,'SupplyLimit'
,'ts_activity'
,'Unit'
,'Unnamed: 0'
,'UrlFriendlyName'
,'varietal'
,'webalcoholpercentage'
,'webaverageproductrating'
,'webbadgescollection'
,'webbadgescollection2'
,'webbeerstyle'
,'webbottleclosure'
,'webbrandname'
,'webbrewedunderlicense'
,'webbundleenddate'
,'webbundleexpandincart'
,'webbundleskucomponent'
,'webbundleskuquantity'
,'webbundlestartdate'
,'webciderstyle'
,'webcountryoforigin'
,'webdescriptionshort'
,'webdsvflag'
,'webdsvname'
,'webdsvvendorid'
,'webfoodmatch'
,'webisorganic'
,'webisvegan'
,'weblangtonsclassification'
,'webliquorsize'
,'webmaincategory'
,'webmaxquantity'
,'webminquantity'
,'webpacksizecase'
,'webpacksizeinner'
,'webpacktype'
,'webpresaledmmaxqtylimit'
,'webpresaleenddate'
,'webpresaleflag'
,'webpresalemarketlaunchdate'
,'webpresalemdmmaxqtylimit'
,'webpresaleorderreleasedate'
,'webpresalestartdate'
,'webpresaleusermaxqtylimit'
,'webproductcanbechilled'
,'webproductname'
,'webproductsale'
,'webproducttype'
,'webpromomdmmessage'
,'webpromomessage'
,'webpromomessageenddt'
,'webpromomessagestartdt'
,'webpromosecondmesgenddt'
,'webpromosecondmesgstartdt'
,'webpromosecondmessage'
,'webpromotionalbundle'
,'webregionoforigin'
,'webreviewrating'
,'webspiritstyle'
,'webstateoforigin'
,'webtitle'
,'webtotalreviewcount'
,'webvideourl'
,'webvintagecurrent'
,'webvintagenote'
,'webwhiskystyle'
,'webwinebody'
,'webwinemaker'
,'webwinestyle']


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
    #my_df = my_df[my_df['Categories'].isin(['red-wine', 'white-wine'])]
    
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
    try:
        my_df["Review2_auth"] = new_df[1].apply(pd.Series).author.apply(pd.Series).Value
    except:
        my_df["Review2_auth"] = ""
    try:
        my_df["Review2_authorcontent"] = new_df[1].apply(pd.Series).authorcontent.apply(pd.Series).Value
    except:
        my_df["Review2_authorcontent"] = ""
    try:
        my_df["Review2_points"] = new_df[1].apply(pd.Series).points.apply(pd.Series).Value
    except:
        my_df["Review2_points"] = ""  
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
    my_df = my_df.drop_duplicates(subset='Stockcode', keep="last")
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

## FINALLY FIXED THIS SILLY BUG... ?!
## Get columns, use existing table columns, and then drop all that aren't in our target table.
run_cols = list(wide.columns)
cleaned_cols = [x for x in run_cols if x in master_cols]
cleaned = wide[cleaned_cols]


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
    frame           = cleaned.to_sql(tableName, dbConnection, if_exists='append', method='multi', chunksize = 1000 );
except ValueError as vx:
    print(vx)
except Exception as ex:   
    print(ex)
else:
    print("Table %s created/updated successfully."%tableName);   
finally:
    dbConnection.close()


