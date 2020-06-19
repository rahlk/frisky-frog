import os
import re
import sys
import json
import requests
import pandas as pd
from tqdm import tqdm
from pathlib import Path
from ipdb import set_trace
from time import time, sleep
from datetime import datetime
from datetime import timedelta
from collections import defaultdict
from contextlib import ContextDecorator
# - Add project source to path -
root = Path(os.path.abspath(os.path.join(
    os.getcwd().split('frisky-frog')[0], 'frisky-frog')))

data_path = root.joinpath('data')

if root.joinpath('src') not in sys.path:
    sys.path.append(str(root.joinpath('src')))


if __name__ == "__main__":
    top_repos = pd.read_csv('active_repos.csv')
    trend_dir = data_path.joinpath('trends')
    selected  = set(top_repos['Repository'].iloc[-50:])
    daily_metrics_path = data_path.joinpath("daily")
    # -- Save daily stats -- 
    for file in daily_metrics_path.glob('*.csv'):
        date = file.stem
        metrics = pd.read_csv(file)
        metrics = metrics.loc[metrics['Repositories'].isin(selected)]
        stats = metrics.describe()
        stats.to_csv(trend_dir.joinpath("{}.csv".format(date)), index_label="Stats")
    
    # set_trace()
        
    # dframe = pd.concat((pd.read_csv(f, na_values=0) for f in repo_path.glob("*.csv") if re.sub('-', '/', f.stem) in selected), ignore_index=True)
    # dframe = dframe.fillna(0)
    # grouped = dframe.groupby(['Date'])
    # for date, metrics in grouped:
    #     stats = metrics.describe()
    #     set_trace()
    #     stats.to_csv(trend_dir.joinpath("{}.csv".format(date)))

    

        


