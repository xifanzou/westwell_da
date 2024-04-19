# # __init__.py

import os
from .get_path import get
from .read_path import read
from .export_file import export_to_csv
from modules.preprocessing import preprocess, task_process

import warnings
warnings.filterwarnings("ignore")

def __set_dir__():
    curpath = os.path.abspath(__file__)
    dirpath = os.path.dirname(os.path.dirname(os.path.dirname(curpath)))
    os.chdir(dirpath)
    # print(curpath, dirpath)
    return 

__set_dir__()