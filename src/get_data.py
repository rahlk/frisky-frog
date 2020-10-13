import os
import sys
import numpy
import pandas as pd
from pathlib import Path
from ipdb import set_trace
from functools import partial
from pathos.multiprocessing import cpu_count, ProcessingPool

import logging
logging.basicConfig(format='[+] %(message)s', level=logging.INFO)

# Add project source to path
root = Path(os.path.abspath(os.path.join(
    os.getcwd().split("frisky-frog")[0], 'frisky-frog')))

if root.joinpath('src') not in sys.path:
    sys.path.append(str(root.joinpath('src')))

if root.joinpath('src') not in sys.path:
    sys.path.append(str(root.joinpath('src')))

from metrics import MetricsGetter
from crawler import Crawler, Agglomerate
from utils import json2repos


def _crawl_and_agglomerate(arg1, crawler_obj, agg_obj):
    """
    Crawl GH Archive to get the list events.
    """
    chosen_day = "{year}-{month:02d}-{date:02d}".format(**arg1)
    
    crawler_obj.set_date_range(**arg1)
    # Save events as hourly csv files in root/data/hourly
    saved = crawler_obj.save_events_as_csv()

    if saved:
        # Convert hourly csv files in root/data/hourly to daily data
        agg_obj.set_match_string(match_string=chosen_day)
        agg_obj.hourly2daily()

    return saved

def _just_agglomerate(arg1, agg_obj):
    """
    Crawl GH Archive to get the list events.
    """
    chosen_month = "{year}-{month:02d}".format(**arg1)
    
    # Convert hourly csv files in root/data/hourly to monthly data
    agg_obj.set_match_string(match_string=chosen_month)
    agg_obj.hourly2monthly()


def generate_date_time_range():
    date_ranges = list()
    for year in [2020, 2019]:
        for month in range(1, 13):
            for date in [(1, 31)]:
                date_ranges.append(
                    {"date": date, "month": month, "year": year})
    return date_ranges


if __name__ == "__main__":
    num_cpu = max(16, cpu_count())
    kwarg_list = generate_date_time_range()

    crawler = Crawler()
    agg = Agglomerate()

    par_deploy_func = partial(_just_agglomerate, agg_obj=agg)

    with ProcessingPool(num_cpu) as p:
        res = p.map(par_deploy_func, kwarg_list)
