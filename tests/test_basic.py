import os
import sys
import numpy
import unittest
from pathlib import Path
from pdb import set_trace

# Add project source to path
root = Path(os.path.abspath(os.path.join(
    os.getcwd().split("frisky-frog")[0], 'frisky-frog/src')))

if root not in sys.path:
    sys.path.append(str(root))

from crawler import Crawler


class TestCrawler(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super(TestCrawler, self).__init__(*args, **kwargs)
        self.crawl = Crawler(
            hour=(0, 23), date=(1, 31), month=(3), year=2020)

    def test_daterange2url(self):
        count = 0
        for url in self.crawl._daterange2url():
            count += 1
        self.assertEqual(count, 24)

    def test_save_events_as_json(self):
        dataframe = self.crawl.save_events_as_json()

    def test_save_events_as_csv(self):
        dataframe = self.crawl.save_events_as_csv()

    def test_get_events_as_dataframe(self):
        dataframe = self.crawl.get_events_as_dataframe()
