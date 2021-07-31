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
from flatten_json import flatten
regex = re.compile(r"\[|\]|<", re.IGNORECASE)

import time

### Dans
driver = webdriver.Firefox(executable_path=r'/usr/local/bin/geckodriver')
path = "https://www.danmurphys.com.au/red-wine/all"
driver.get(path)


with open("big_run.csv", "w") as csvFile:
    fieldnames = ['URL']
    writer = csv.DictWriter(csvFile, fieldnames=fieldnames)
    writer.writeheader()
    for item in range(1,416):
        path = "https://www.danmurphys.com.au/red-wine/all?page=" + str(item)
        driver.implicitly_wait(2)
        driver.get(path)
        elems = driver.find_elements_by_xpath("//a[@href]")
        for elem in elems:
            writer.writerow({'URL': elem.get_attribute("href")})


# with open("Cleaned_wines.csv", "w") as wines:

# response = requests.get("https://api.danmurphys.com.au/apis/ui/Product/796067").json()
# j=response.json()
# df = pd.DataFrame.from_dict(j)
# df.to_csv()
#Lets re-load the wines
wines = pd.read_csv("big_run.csv")
# And now I'll filter out the product linkes
wines = wines[wines.URL.str.contains('https://www.danmurphys.com.au/product')]

wines = wines.replace(r'https://www.danmurphys.com.au/product/DM_', r'', regex=True)
wines = wines.replace(r'/.*$', r'', regex=True)
wines = wines.drop_duplicates()

wines.describe()
#df = pd.read_csv('Cleaned_wines.csv') # can also index sheet by name or fetch all sheets
#df = pd.read_csv('Cleaned_wines_copy.csv') # can also index sheet by name or fetch all sheets

#wines
#mylist = df['Cleaned'].tolist()
mylist = wines['URL'].tolist()

#mylist = mylist[0:100]
result = []

for wine in mylist:
    url = "https://api.danmurphys.com.au/apis/ui/Product/" + str(wine)
    r = requests.get(url).json()
    try:
        rtoo = pd.json_normalize(r["Products"])
        result.append(rtoo)
    except:
        print("No product found")

my_df = pd.concat(result)
my_df.to_csv("thurs_run0729.csv")
#pd.json_normalize(flatten(r))





my_df = pd.read_csv("thurs_run0729.csv")


## Categories
#my_df["Categories"]
# Willnjust keep 2 levels.
my_df["Categories"] = my_df["Categories"].map(eval, na_action='ignore')
new_df = my_df["Categories"].apply(pd.Series)
my_df["Categories"] = new_df[0].apply(pd.Series).UrlFriendlyName
my_df["Sub_Categories"] = new_df[1].apply(pd.Series).UrlFriendlyName


## Reviews
my_df["Reviews"] = my_df["Reviews"].map(eval,  na_action='ignore')
# Try with first 2 reviews
new_df = my_df["Reviews"].apply(pd.Series)
# First
my_df["Review1_auth"] = new_df[0].apply(pd.Series).author.apply(pd.Series).Value
my_df["Review1_authorcontent"] = new_df[0].apply(pd.Series).authorcontent.apply(pd.Series).Value
my_df["Review1_points"] = new_df[0].apply(pd.Series).points.apply(pd.Series).Value
my_df["Review1_source"] = new_df[0].apply(pd.Series).source.apply(pd.Series).Value
my_df["Review1_text"] = new_df[0].apply(pd.Series).text.apply(pd.Series).Value
my_df["Review1_vintage"] = new_df[0].apply(pd.Series).vintage.apply(pd.Series).Value
# Second
my_df["Review1_auth"] = new_df[1].apply(pd.Series).author.apply(pd.Series).Value
my_df["Review1_authorcontent"] = new_df[1].apply(pd.Series).authorcontent.apply(pd.Series).Value
my_df["Review1_points"] = new_df[1].apply(pd.Series).points.apply(pd.Series).Value
my_df["Review1_source"] = new_df[1].apply(pd.Series).source.apply(pd.Series).Value
my_df["Review1_text"] = new_df[1].apply(pd.Series).text.apply(pd.Series).Value
my_df["Review1_vintage"] = new_df[1].apply(pd.Series).vintage.apply(pd.Series).Value


# Additional details
my_df["AdditionalDetails"] = my_df["AdditionalDetails"].map(eval,  na_action='ignore')
my_df["AdditionalDetails"] = my_df["AdditionalDetails"].apply(pd.Series.explode).explode("NewAdd")
df = pd.concat([my_df,pd.json_normalize(my_df["AdditionalDetails"]).pivot(columns='Name')['Value']], axis=1)

#check = pd.json_normalize(my_df["AdditionalDetails"]).pivot(columns='Name')

#check.to_csv("check.csv")

#check['Value']
#my_df["AdditionalDetails"].apply(pd.Series.explode)

########### Would likely want ot save to DB at this tome/.

#In excel: =TEXTJOIN("','",TRUE,A1:CF1)
#keep = ['Categories','BackorderMessage','varietal','DeliveryOptionsInfo','SavedLists','Stockcode','PackageSize','RichDescription','StockOnHand','UrlFriendlyName','IsDeliveryOnly','Prices.inanysixprice.Value','Prices.caseprice.Value','Prices.singleprice.Value','Inventory.availableinventoryqty','Prices.promoprice.Message','Prices.promoprice.Value','Prices.promoprice.PreText','Prices.promoprice.BeforePromotion','Prices.promoprice.AfterPromotion','Sub_Categories','Review1_auth','Review1_authorcontent','Review1_source','Review1_points','Review1_text','Review1_vintage','awardwinner','brewery','corkscoreeligible','countryoforigin','dm_stockcode','image1','image2','standarddrinks','webalcoholpercentage','webaverageproductrating','webbadgescollection','webbottleclosure','webcountryoforigin','webdescriptionshort','webdsvflag','webfoodmatch','webisvegan','weblangtonsclassification','webliquorsize','webmaincategory','webmaxquantity','webminquantity','webpacksizecase','webpacktype','webpresaleenddate','webpresaleflag','webpresalemarketlaunchdate','webpresalemdmmaxqtylimit','webpresaleorderreleasedate','webpresalestartdate','webpresaleusermaxqtylimit','webproductcanbechilled','webproductname','webproductsale','webproducttype','webpromomdmmessage','webpromomessage','webpromomessageenddt','webpromomessagestartdt','webpromosecondmesgenddt','webpromosecondmesgstartdt','webpromosecondmessage','webpromotionalbundle','webregionoforigin','webstateoforigin','webtotalreviewcount','webvideourl','webvintagecurrent','webvintagenote','webwinebody','webwinemaker','webwinestyle']
#keep = ['Categories','BackorderMessage','Description','DeliveryOptionsInfo','SavedLists','Stockcode','PackageSize','StockOnHand','UrlFriendlyName','IsDeliveryOnly','Prices.inanysixprice.Value','Prices.caseprice.Value','Prices.singleprice.Value','Inventory.availableinventoryqty','Prices.promoprice.Message','Prices.promoprice.Value','Prices.promoprice.PreText','Prices.promoprice.BeforePromotion','Prices.promoprice.AfterPromotion','Sub_Categories','Review1_auth','Review1_authorcontent','Review1_source','Review1_points','Review1_text','Review1_vintage','awardwinner','brewery','corkscoreeligible','countryoforigin','dm_stockcode','image1','image2','standarddrinks','webalcoholpercentage','webaverageproductrating','webbadgescollection','webbottleclosure','webcountryoforigin','webdescriptionshort','webdsvflag','webfoodmatch','webisvegan','weblangtonsclassification','webliquorsize','webmaincategory','webmaxquantity','webminquantity','webpacksizecase','webpacktype','webpresaleenddate','webpresaleflag','webpresalemarketlaunchdate','webpresalemdmmaxqtylimit','webpresaleorderreleasedate','webpresalestartdate','webpresaleusermaxqtylimit','webproductcanbechilled','webproductname','webproductsale','webproducttype','webpromomdmmessage','webpromomessage','webpromomessageenddt','webpromomessagestartdt','webpromosecondmesgenddt','webpromosecondmesgstartdt','webpromosecondmessage','webpromotionalbundle','webregionoforigin','webstateoforigin','webtotalreviewcount','webvideourl','webvintagecurrent','webvintagenote','webwinebody','webwinemaker','webwinestyle']
# Timmed version
keep = ['Description','Stockcode','PackageSize','Prices.inanysixprice.Value','Prices.caseprice.Value','Prices.singleprice.Value','Prices.promoprice.Message','Prices.promoprice.Value','Prices.promoprice.PreText','Sub_Categories','Review1_auth','Review1_points','Review1_vintage','awardwinner','countryoforigin','standarddrinks','webalcoholpercentage','webaverageproductrating','webbadgescollection','webbottleclosure','webcountryoforigin','webdescriptionshort','webdsvflag','webfoodmatch','webisvegan','weblangtonsclassification','webliquorsize','webmaincategory','webpacksizecase','webpacktype','webregionoforigin','webstateoforigin','webvintagecurrent','webvintagenote','webwinebody','webwinestyle']

# Now Keep them.
kept = df[keep]
#kept.to_csv('fri_big_run_kept.csv')
kept.reset_index(drop=True, inplace=True)

#myst.to_csv('fri_big_run_kept_myst.csv')
#known.to_csv('fri_big_run_kept_known.csv')
#myst = pd.read_csv("fri_big_run_kept_myst.csv")
#known = pd.read_csv("fri_big_run_kept_known.csv")

## Adding in tfidf for description, and then dropping the columns

from sklearn.feature_extraction.text import TfidfVectorizer

v = TfidfVectorizer(ngram_range=(2, 3))
x = v.fit_transform(kept['Description'])
# Convert to datafram
df1 = pd.DataFrame(x.toarray(), columns=v.get_feature_names())

res = pd.concat([kept, df1], axis=1)

y = v.fit_transform(kept['webdescriptionshort'].values.astype('U'))
df2 = pd.DataFrame(y.toarray(), columns=v.get_feature_names())

new_kept = pd.concat([res, df2], axis=1)

new_kept = new_kept.drop("Description", axis=1)

new_kept = new_kept.drop("webdescriptionshort", axis=1)

known = new_kept
known.reset_index(drop=True, inplace=True)
#### ONE HOT ENCODED
##### First i split into numeric and nominal. OHE the nominal
exclude_col = known.select_dtypes(include=np.number).columns.tolist() + ["Stockcode"]
my_df_num = known[exclude_col]
my_df_cat = known.drop(exclude_col, axis=1)
#my_df_cat.to_csv("FIN.csv")
my_df_cat_ohe = pd.get_dummies(my_df_cat)
my_df_ohe = pd.concat([my_df_num,my_df_cat_ohe], axis=1)
my_df_ohe = my_df_ohe.fillna(0)
my_df_ohe = my_df_ohe.replace(np.nan, 0)

# Drop duplicates #TODO check whats better to keep
my_df_ohe = my_df_ohe.loc[:,~my_df_ohe.columns.duplicated()]
# Clean up names
my_df_ohe.columns = [regex.sub("_", col) if any(x in str(col) for x in set(('[', ']', '<'))) else col for col in my_df_ohe.columns.values]


myst  = my_df_ohe[my_df_ohe.Stockcode.str.contains("MYSTERY")]
known  = my_df_ohe[~my_df_ohe.Stockcode.str.contains("MYSTERY")]

myst_nn = myst.drop("Stockcode", axis=1)
known_nn = known.drop("Stockcode", axis=1)


#my_df_ohe.to_csv("OHE.csv")
#myst_nn = myst_nn.reset_index()
#myst = myst.reset_index()

#known_nn = known_nn.reset_index()
#known = known.reset_index()

##### Now myst
#my_df_num_m = myst[exclude_col]
#my_df_cat_m = myst.drop(exclude_col, axis=1)
#my_df_cat.to_csv("FIN.csv")
#my_df_cat_ohe_m = pd.get_dummies(my_df_cat_m)
#my_df_ohe_m = pd.concat([my_df_num_m,my_df_cat_ohe_m], axis=1)
#my_df_ohe_m = my_df_ohe_m.drop("Stockcode", axis=1)
#my_df_ohe_m = my_df_ohe_m.fillna(0)
#my_df_ohe_m = my_df_ohe_m.replace(np.nan, 0)


from sklearn.neighbors import NearestNeighbors
# Create the k-NN model using k=5
nn_abs = NearestNeighbors(n_neighbors=5, algorithm='auto')



# Checkif we've got NsNS

#
#wines_corr =  my_df_ohe_d.corr(method = "pearson")
#
nn_abs.fit(known_nn)

distance, matches = nn_abs.kneighbors(myst_nn.iloc[[9]], 2, return_distance=True)
#matches

known["Stockcode"].iloc[matches[0]]

myst['Stockcode'].iloc[[9]]
#Now, tokenise each descition.
#MAGIC TIME

myst.iloc[[9]].to_csv("myst.csv")
myst_nn.iloc[[9]].to_csv("myst_nn.csv")



known.iloc[matches[0]].to_csv("known.csv")
known_nn.iloc[matches[0]].to_csv("known_nn.csv")

results_wine = []

for index in range(len(myst.index)):
    distance, matches = nn_abs.kneighbors(myst_nn.iloc[[index]], 1, return_distance=True)
    results_wine.append(
        {
            'Mystery': "https://www.danmurphys.com.au/product/" + str(myst['Stockcode'].iloc[[index][0]]),
            'Matched': "https://www.danmurphys.com.au/product/" + str(known["Stockcode"].iloc[matches[0][0]]),
            'Distance': str(distance[0][0])
        }
    )


matched = pd.DataFrame(results_wine)

#def create_clickable_id(id):
#    url_template= '''<a href="../../link/to/{id}" target="_blank">{id}</a>'''.format(id=id)
#    return url_template

#matched['Mystery'] = matched['Mystery'].apply(create_clickable_id)
#matched['Matched'] = matched['Matched'].apply(create_clickable_id)

matched.to_html("matched.html")

matched.to_csv("matched_sat_1.csv")

matches = nn_abs.kneighbors(myst_nn.iloc[[3]], 2, return_distance=False)
known["Stockcode"].iloc[matches[0]]

myst['Stockcode'].iloc[[3]]




https://www.danmurphys.com.au/product/DM_MYSTERY352/under-wraps-beechworth-sangiovese-2018

https://www.danmurphys.com.au/product/7252


## xgboost

from numpy import loadtxt
from xgboost import XGBClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score

X = known_nn
Y = known['Stockcode']

# Dont need this if already cleaned ohe
X.columns = [regex.sub("_", col) if any(x in str(col) for x in set(('[', ']', '<'))) else col for col in X.columns.values]


#
seed = 7
test_size = 0.33
X_train, X_test, y_train, y_test = train_test_split(X, Y, test_size=test_size, random_state=seed)


# fit model no training data
model = XGBClassifier(verbosity=2)
model.fit(X, Y)

# Check it out now
print(model)