# # __init__.py

import os
from .psql_db import test

import warnings
warnings.filterwarnings("ignore")

def __set_dir__():
    curpath = os.path.abspath(__file__)
    dirpath = os.path.dirname(os.path.dirname(os.path.dirname(curpath)))
    os.chdir(dirpath)
    # print(curpath, dirpath)
    return 

__set_dir__()