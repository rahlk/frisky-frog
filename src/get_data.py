import os
import sys
import numpy
from pathlib import Path
from pdb import set_trace

# Add project source to path
root = Path(os.path.abspath(os.path.join(
    os.getcwd().split("frisky-frog")[0], 'frisky-frog')))

if root.joinpath('src') not in sys.path:
    sys.path.append(str(root.joinpath('src')))

from metrics import MetricsGetter
from crawler import Crawler, Agglomerate

if __name__ == "__main__":
    crawl = Crawler(hour=(0, 23), date=(1, 31), month=(8, 12), year=2019)
    crawl.save_events_as_csv()
