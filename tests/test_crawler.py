# import os
# import sys
# import numpy
# import unittest
# from pathlib import Path
# from ipdb import set_trace

# # Add project source to path
# root = Path(os.path.abspath(os.path.join(
#     os.getcwd().split("frisky-frog")[0], 'frisky-frog')))

# if root.joinpath('src') not in sys.path:
#     sys.path.append(str(root.joinpath('src')))

# from crawler import Crawler


# class TestCrawler(unittest.TestCase):
#     def __init__(self, *args, **kwargs):
#         super(TestCrawler, self).__init__(*args, **kwargs)
#         self.save_path = Path(os.path.abspath(os.getcwd())).joinpath('tests')
#         self.test_crawl = Crawler(
#             hour=(16), date=(12, 13), month=(3), year=2020)

#     def test_daterange2url(self):
#         count = 0
#         for url in self.test_crawl._daterange2url():
#             count += 1
#         self.assertEqual(count, 2)

#     def test_get_events_as_dataframe(self):
#         dataframe = self.test_crawl.get_events_as_dataframe()

#     def test_save_events_as_json(self):
#         dataframe = self.test_crawl.save_events_as_json(
#             save_path=self.save_path)

#     def test_save_events_as_csv(self):
#         dataframe = self.test_crawl.save_events_as_csv(
#             save_path=self.save_path)
