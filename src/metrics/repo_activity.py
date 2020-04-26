import os
import sys
import logging
import pandas as pd
from pathlib import Path
from ipdb import set_trace
from selenium import webdriver
from datetime import datetime, timedelta
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys

# -- Update paths --
# - Logging Config -
logging.basicConfig(format='[+] %(message)s', level=logging.INFO)

# - Add project source to path -
root = Path(os.path.abspath(os.path.join(
    os.getcwd().split('frisky-frog')[0], 'frisky-frog')))

# - Add data dir to path - 
data = root.joinpath('data', 'measures')


if root.joinpath('src') not in sys.path:
    sys.path.append(str(root.joinpath('src')))


def get_date_range(year: int):
    """
    Generates date ranges from 
    """
    dt_start = datetime(year, 1, 1)
    dt_end =  datetime.today()
    dt_step = timedelta(days=1)

    date_range = []

    while dt_start < dt_end:
        date_range.append(dt_start.strftime("%Y-%m-%d"))
        dt_start += dt_step
    
    return date_range

def extract_counts_from_webpage():
    driver = webdriver.Chrome()
    for year in range(2015, 2021):
        dates = get_date_range(years)
        for date in dates:
            search_postfix = date
            search_str = "https://github.com/search?q=created%3A"+search_postfix
            driver.get(search_str)
            elem = driver.find_element(
                By.XPATH, '/html/body/div[4]/main/div/div[3]/div/div[2]/h3')
            set_trace()


extract_counts_from_webpage()


"covid OR coronavirus OR 2019-ncov OR corona OR covid19 OR SARS-CoV-2 created:2020-01..2020-04"
