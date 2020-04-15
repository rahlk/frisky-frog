import os
import sys
import numpy
import unittest
from pathlib import Path
from pdb import set_trace

# Add project source to path
root = Path(os.path.abspath(os.path.join(
    os.getcwd().split("frisky-frog")[0], 'frisky-frog')))

if root.joinpath('src') not in sys.path:
    sys.path.append(str(root.joinpath('src')))

from crawler import Agglomerate


class TestAgglomerate(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super(TestCrawler, self).__init__(*args, **kwargs)
        self.test_agg = Agglomerate()

    def test_hourly2daily(self):
        "Test hourly data to daily data commits."
        self.test_agg.hourly2daily()

    def test_hourly2weekly(self):
        "Test hourly data to weekly data commits."

    def test_hourly2monthly(self):
        "Test hourly data to monthly data commits."
