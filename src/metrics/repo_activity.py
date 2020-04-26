import os
import sys
import logging
import pandas as pd
from pathlib import Path
from datetime import timedelta
from datetime import datetime
from ipdb import set_trace
import pycurl


# -- Update paths --
# - Logging Config -
logging.basicConfig(format='[+] %(message)s', level=logging.INFO)

# - Add project source to path -
root = Path(os.path.abspath(os.path.join(
    os.getcwd().split('frisky-frog')[0], 'frisky-frog')))

# - Add data dir to path - 
data = root.joinpath('data')


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
    for year in range(2015, 2021):
        dates = get_date_range(year)
        for date in dates:
            search_postfix = date
            search_str = "https://github.com/search?q=created%3A{}&type=Repositories".format(search_postfix)
            filename = root.joinpath('data', '{}.html'.format(date))
            curl_limit_rate(search_str, filename, rate_limit=1024)
            for line in content: 
                if 'repository results' in line:                     
                    set_trace()


def curl_progress(total, existing, upload_t, upload_d):
    try:
        frac = float(existing) / float(total)
    except:
        frac = 0
    print("Downloaded %d/%d (%0.2f%%)" % (existing, total, frac))


def curl_limit_rate(url, filename, rate_limit):
    """Rate limit in bytes"""
    c = pycurl.Curl()
    c.setopt(c.URL, url)
    c.setopt(c.MAX_RECV_SPEED_LARGE, rate_limit)
    if os.path.exists(filename):
        file_id = open(filename, "ab")
        c.setopt(c.RESUME_FROM, os.path.getsize(filename))
    else:
        file_id = open(filename, "wb")

    c.setopt(c.WRITEDATA, file_id)
    c.setopt(c.NOPROGRESS, 0)
    c.setopt(c.PROGRESSFUNCTION, curl_progress)
    c.perform()
    file_id.close()


extract_counts_from_webpage()


"covid OR coronavirus OR 2019-ncov OR corona OR covid19 OR SARS-CoV-2 created:2020-01..2020-04"
