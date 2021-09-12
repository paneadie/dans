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
regex = re.compile(r"\[|\]|<", re.IGNORECASE)

import time

### Dans
#driver = webdriver.Firefox(executable_path=r'/usr/local/bin/geckodriver')
#path = "https://www.danmurphys.com.au/red-wine/all"
#driver.get(path)   
# Make headless
options = Options()
options.headless = True

file = "scraped_" + time.strftime("%Y%m%d") + ".csv"


with open(file, "w") as csvFile:
    #path = "https://www.danmurphys.com.au/red-wine/all"
    fieldnames = ['URL']
    writer = csv.DictWriter(csvFile, fieldnames=fieldnames)
    writer.writeheader()
    for item in range(1,230):
        driver = webdriver.Firefox(options=options,executable_path=r'/usr/local/bin/geckodriver')
        #driver = webdriver.Firefox(executable_path=r'/usr/local/bin/geckodriver')
        path = "https://www.danmurphys.com.au/red-wine/all?page=" + str(item)
 #       driver.implicitly_wait(1)
        driver.get(path)
        elems = driver.find_elements_by_xpath("//a[@href]")
        for elem in elems:
            writer.writerow({'URL': elem.get_attribute("href")})
        driver.close()

    for item in range(1,6):
        driver = webdriver.Firefox(options=options,executable_path=r'/usr/local/bin/geckodriver')
        #driver = webdriver.Firefox(executable_path=r'/usr/local/bin/geckodriver')
        #path = "https://www.danmurphys.com.au/red-wine/all?page=" + str(item)
        path = "https://www.danmurphys.com.au/list/under-wraps?page=" + str(item)
 #       driver.implicitly_wait(1)
        driver.get(path)
        elems = driver.find_elements_by_xpath("//a[@href]")
        for elem in elems:
            writer.writerow({'URL': elem.get_attribute("href")})
        driver.close()


    for item in range(1,20):
        driver = webdriver.Firefox(options=options,executable_path=r'/usr/local/bin/geckodriver')
        #driver = webdriver.Firefox(executable_path=r'/usr/local/bin/geckodriver')
        #path = "https://www.danmurphys.com.au/red-wine/all?page=" + str(item)
        #path = "https://www.danmurphys.com.au/list/under-wraps?page=" + str(item)
        path = "https://www.danmurphys.com.au/search?searchTerm=wraps&page=" + str(item)
 #       driver.implicitly_wait(1)
        driver.get(path)
        elems = driver.find_elements_by_xpath("//a[@href]")
        for elem in elems:
            writer.writerow({'URL': elem.get_attribute("href")})
        driver.close()


driver.quit()

