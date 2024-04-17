import pandas as pd 
import os
import re
from src.processer_file import get_path
from src.processer_data import preprocess, task_process, error_process, igv_process

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
            # print(f'\n{folder_name}')
            if __is_chinese__(folder_name) == True: 
                vessel_name = folder_name
            
            for file_path in path_dict[folder_name]:
                # try:
                df = pd.read_csv(file_path, encoding='utf-8-sig', on_bad_lines='skip')        
                processed = __process_by_data_src_(project=project, data_src=data_src, df=df, vessel_name=vessel_name)
                
                # IGV dfs with at least 5 mins records (3s/record)
                if (processed.shape[0] >= 100) | (data_src.upper() != 'IGV'): 
                    print(f'{project, folder_name, file_path}')
                    if folder_name not in groupped_df:
                        groupped_df[folder_name] = [processed]
                    else:
                        groupped_df[folder_name].append(processed)
                else: print(f'Too litte info to extract from {folder_name, os.path.basename(file_path)}.')
                # except Exception as e:
                #     print(f'An error occured in ')
                #     pass

        groupped_df = __concat_groupped_df__(groupped_df)
        out_dict[project] = groupped_df
    
    return out_dict
        
def __process_by_data_src_(project=str, data_src=str, df=pd.DataFrame, vessel_name=None):
    processed_df = preprocess.run(project=project, data_src=data_src, df=df, vessel_name=vessel_name)
    if data_src.upper() == 'TASK':
        processed_df = task_process.run(df=processed_df)
    elif data_src.upper() in 'ERRORHISTORY':
        processed_df = error_process.run(df=processed_df)
    elif data_src.upper() == 'IGV':
        processed_df = igv_process.run(projrect=project, df=processed_df)
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
