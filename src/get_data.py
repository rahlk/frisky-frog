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


def _crawler(arg1, crawler_obj):
    """
    Crawl GH Archive to get the list events.
    """
    crawler_obj.set_date_range(**arg1)
    # Save events as hourly csv files in root/data/hourly
    crawler_obj.save_events_as_csv()
    return chosen_month


def _agglomerator(arg1, agg_obj):
    """
    Agglomerate data to find the most active projects in a month
    """
    chosen_month = "{year}-{month:02d}".format(**arg1)
    agg_obj.set_match_string(match_string=chosen_month)
    agg_obj.hourly2monthly()


def _metrics_getter(arg1, crawler_obj, metrics_obj):
    """
    Get Metrics.
    """
    crawler_obj.set_date_range(**arg1)
    chosen_month = "{year}-{month:02d}".format(**arg1)

    # metrics_obj.set_top_K_repos(
    #     K=1000, meta_data_file_name="{}.csv".format(chosen_month))
    metrics_obj.populate(crawler_obj, save_name="{}.json".format(chosen_month))

    return chosen_month


def generate_date_time_range():
    date_ranges = list()
    for year in [2019, 2020]:
        for month in range(1, 13):
            for dates in [(1, 31)]:
                if year == 2020 and month > 4:
                    continue
                else:
                    date_ranges.append(
                        {"date": dates, "month": month, "year": year})
    return date_ranges


if __name__ == "__main__":
    num_cpu = min(16, cpu_count())
    kwarg_list = generate_date_time_range()

    crawler = Crawler()
    agg = Agglomerate()
    metrics = MetricsGetter()
    metrics.set_top_K_repos()

    deploy_crawer = partial(_crawler, crawler_obj=crawler)

    deploy_agglomerator = partial(_agglomerator, agg_obj=agg)

    deploy_metrics_getter = partial(
        _metrics_getter, crawler_obj=crawler, metrics_obj=metrics)

    with ProcessingPool(num_cpu) as p:
        res = p.map(deploy_metrics_getter, kwarg_list)

    logging.info("Done computing metrics...")

    json2repos(json_dir=root.joinpath('data', 'metrics'),
               save_dir=root.joinpath('data', 'repositories'))
