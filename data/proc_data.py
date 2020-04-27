import os
import sys
import json
import pandas
import logging
from pathlib import Path
from pdb import set_trace

# -- Update paths --
# - Logging Config -
logging.basicConfig(format='[+] %(message)s', level=logging.INFO)

# - Add project source to path -
root = Path(os.path.abspath(os.path.join(
    os.getcwd().split('frisky-frog')[0], 'frisky-frog')))

# - Add data dir to path -
data = root.joinpath('data')


if root.joinpath('src') not in sys.path:
    sys.path.append(str(root.joinpath('src')))


def measure_json2table():
    for file in data.joinpath('measures', 'repositories', 'all').glob('*.json'):
        with open(file, "r") as json_file:ßß
            set_trace()


if __name__ == "__main__":
    measure_json2table()