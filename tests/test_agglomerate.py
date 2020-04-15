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
