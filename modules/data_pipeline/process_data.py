import pandas as pd 
import os
import re

from modules.data_pipeline import get_path
from modules.preprocessing import preprocess
from modules.error import error_process
from modules.task import task_process
from modules.IGV import igv_process

def read(week_num=int, data_src=str) -> dict:
    '''
    Read all relevant csvs into dataframes, 
    Each dataframe will be processed with corresponding data_src (IGV/TAS/ERROR),
    All dataframes are exported in a strctured dictionary.

    Paras:
    week_num (str): Week Number Indicator.
    data_src (str):Data Source Indicator (IGV/TASK/ERROR).

    Returns:
    A dictionary contains folder_name/vessel_name and processed dfs.
    '''
    in_dict  = get_path.get(week_num, data_src)
    out_dict = {}

    for project, path_dict in in_dict.items():
        vessel_name = None
        groupped_df = {}

        for folder_name in path_dict.keys():
            if __is_chinese__(folder_name) == True: 
                vessel_name = folder_name
            
            # process data
            for file_path in path_dict[folder_name]:
                groupped_df = process(week_num=week_num, data_src=data_src, project=project,
                        vessel_name=vessel_name, folder_name=folder_name,
                        file_path=file_path, groupped_df=groupped_df)

        groupped_df = __concat_groupped_df__(groupped_df)
        out_dict[project] = groupped_df
    
    return out_dict

def process(week_num=int, data_src=str, project=str, vessel_name=None, folder_name=str, file_path=str, groupped_df=None):
    df = pd.read_csv(file_path, encoding='utf-8-sig', on_bad_lines='skip')
    df['project'], df['week'] = '', ''
    df.loc[df.index, 'project'], df.loc[df.index, 'week'] = project, f"W{week_num}"

    cleaned_df = preprocess.run(project=project, data_src=data_src, df=df, vessel_name=vessel_name)
    
    if len(cleaned_df)>=100:     
        print(f'Now processing ---> {project, folder_name, os.path.basename(file_path)}')   
        processed = __process_by_data_src__(project=project, data_src=data_src, df=cleaned_df, vessel_name=vessel_name)
        
        # IGV dfs with at least 10 mins records (3s/record)
        if (len(processed) >= 200) | (data_src.upper() != 'IGV'):                     
            if folder_name not in groupped_df:
                groupped_df[folder_name] = [processed]
            else:
                groupped_df[folder_name].append(processed)
        else: print(f'Too litte info (time range < 10 mins) to extract from {folder_name, os.path.basename(file_path)}.')

    else:
        print(f'No info or empty file: {folder_name, os.path.basename(file_path)}')

    return groupped_df

        
def __process_by_data_src__(project=str, data_src=str, df=pd.DataFrame, vessel_name=None):
    if data_src.upper() == 'TASK':
        processed_df = task_process.run(df=df)
    elif data_src.upper() in 'ERRORHISTORY':
        processed_df = error_process.run(df=df)
    elif data_src.upper() == 'IGV':
        processed_df = igv_process.run(project=project, df=df)
    return processed_df


def __is_chinese__(char=str):
    """
    Check if a character is a Chinese character.
    """
    return bool(re.match('[\u4e00-\u9fff]', char))    

def __concat_groupped_df__(df_dict=dict) -> dict:
    out_dict = {}
    for k, v in df_dict.items():
        for df in v:
            if not df.index.is_unique:
                print(f"Duplicate index values found in DataFrame: {k}")
    
        v_reset = [df.reset_index(drop=True) for df in v]
        concatenated_df = pd.concat(v_reset, axis=0, ignore_index=True)
        out_dict[k] = concatenated_df
    return out_dict
