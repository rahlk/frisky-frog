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

    def test_daterange2url(self):
        crawl = Crawler(hour=(14, 15), date=(1, 3), month=(1, 2), year=2018)
        count = 0
        for url in crawl._daterange2url():
            count += 1
        self.assertEqual(count, 12)

    def test_save_events_as_json(self):
        crawl = Crawler(hour=(0, 23), date=(1, 31), month=(1), year=2020)
        dataframe = crawl.save_events_as_json()

    def test_save_events_as_csv(self):
        crawl = Crawler(hour=(0, 23), date=(1, 31), month=(1), year=2020)
        dataframe = crawl.save_events_as_csv()

    def test_get_events_as_dataframe(self):
        crawl = Crawler(hour=(0, 23), date=(1, 31), month=(1), year=2020)
        dataframe = crawl.get_events_as_dataframe()
        set_trace()
