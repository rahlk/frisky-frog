import os
import sys
import json
import logging
import pandas as pd
from pdb import set_trace
from pathlib import Path, PosixPath
from typing import Dict, Tuple, List, Union, NewType

# Logging Config
logging.basicConfig(format='[+] %(message)s', level=logging.INFO)

# Add project source to path
root = Path(os.path.abspath(os.path.join(
    os.getcwd().split('frisky-frog')[0], 'frisky-frog')))

if root.joinpath('src') not in sys.path:
    sys.path.append(str(root.joinpath('src')))

# Common types used here.
URL = NewType('URL', str)
DateRange = Tuple[int, int]
Iterable = Union[set, list, tuple]
PandasDataFrame = NewType('PandasDataFrame', pd.core.frame.DataFrame)
PathType = NewType('Path', PosixPath)


class Agglomerate:
    """
    Agglomerates hourly GH Archive data into daily, weekly, and monthly data.
    """

    def __init__(self):
        pass

    def hourly2daily(self):
        """
        Agglomerates hourly to daily.

        Returns
        -------
        PandasDataFrame: 
        """

    def agglomerate_all(self, also_save=False, save_path=root.joinpath('data')):
        """
        Agglomerates hourly to all other ranges.  

        """
