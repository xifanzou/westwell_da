
import os
import pandas as pd
import numpy as np
from sqlalchemy import create_engine


def run():
    folder_path = os.path.abspath(os.path.join("data","processed"))
    print(f'folder path is {folder_path}')
    print(os.listdir())