# # __init__.py

import os
from .get_path import get
from .process_data import read
from .export_file import export_to_csv
from modules.preprocessing import preprocess
from modules.task import task_process
from modules.error import error_process
from modules.IGV import igv_process

import warnings
warnings.filterwarnings("ignore")

def __set_dir__():
    curpath = os.path.abspath(__file__)
    dirpath = os.path.dirname(os.path.dirname(os.path.dirname(curpath)))
    os.chdir(dirpath)
    # print(curpath, dirpath)
    return 

__set_dir__()