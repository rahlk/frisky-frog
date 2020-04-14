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

from crawler import Crawler


class TestCrawler(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super(TestCrawler, self).__init__(*args, **kwargs)
        self.save_path = Path(os.path.abspath(os.getcwd()))
        self.crawl = Crawler(
            hour=(16), date=(13), month=(3), year=2020)

    def test_daterange2url(self):
        count = 0
        for url in self.crawl._daterange2url():
            count += 1
        self.assertEqual(count, 6)

    def test_save_events_as_json(self):
        dataframe = self.crawl.save_events_as_json(
            save_path=self.save_path, file_name='test.json')

    def test_save_events_as_csv(self):
        dataframe = self.crawl.save_events_as_csv(
            save_path=self.save_path, file_name='test.csv')

    def test_get_events_as_dataframe(self):
        dataframe = self.crawl.get_events_as_dataframe()
