import os
import re
from os import walk

def get(week_num=int, data_src=str) -> dict:
    '''
    Get all needed paths.

    Paras:
    week_num (str): Week Number Indicator.
    data_src (str):Data Source Indicator (IGV/TASK/ERROR)

    Returns:
     nested dictionary with ordered paths.
    -{
        'project':
            {'folder1': [list of paths],
            'folder2': [list of paths]...}
        'project2':
            { 'folder1': [list of paths]}
        ...
    }

    '''
    path_dict = __get_all_path__(week_num=week_num, data_src=data_src)

    grouped_paths, out_dict = {}, {}
    for project, file_path in path_dict.items():
        grouped_paths = {}
        for path in file_path:
            parent_folder = os.path.basename(os.path.dirname(path))
            if parent_folder not in grouped_paths:
                grouped_paths[parent_folder] = [path]
            else:
                grouped_paths[parent_folder].append(path)

        out_dict[project] = grouped_paths

    return out_dict

def __get_all_path__(week_num=int, data_src=str) -> dict:    
    # set defaults
    file_folder_p = os.path.join('data\\raw', f'W{week_num}')
    file_folder_p = os.path.abspath(file_folder_p)
    file_paths, out_dict = [], {}

    for insider in os.listdir(file_folder_p):
        inside_folder_p = os.path.join(file_folder_p, insider)        
        project = re.findall(r'^([a-zA-Z]+)', insider)[0].upper()

        # empty file path if multiple projects are detected
        if project not in out_dict.keys():
            file_paths = []
        # get file paths
        for item in os.listdir(inside_folder_p):
            item_path = os.path.join(inside_folder_p, item)
            if os.path.isfile(item_path):
                file_paths.append(item_path)
            else:
                for nested_item in os.listdir(item_path):
                    nested_path = os.path.join(item_path, nested_item)
                    if os.path.isfile(nested_path):
                        file_paths.append(nested_path)
        # wrt data_src
        if (data_src).upper() == 'IGV':     file_paths = __igv_src__(project, file_paths=file_paths)
        elif (data_src).upper() == 'TASK':  file_paths = __task_src__(file_paths=file_paths)
        elif (data_src).upper() == 'ERROR': file_paths = __error_src__(file_paths=file_paths)
        else: print('Data source should be IGV, TASK, or ERROR.')

        out_dict[project] = file_paths

    return out_dict

def __igv_src__(project=str, file_paths=list) -> list:
    igv_file_paths = []
    if project not in ['ICA', 'YH']:
        igv_file_paths = [path for path in file_paths if 'igv' in path.lower()]
    else:
        igv_file_paths = [path for path in file_paths if len(re.findall(r'IGVData_KPI|Data.csv|_igv_|Data', path))>0]
    return igv_file_paths

def __task_src__(file_paths=list) -> list:
    task_file_paths = [path for path in file_paths if 'task' in path.lower()]
    return task_file_paths

def __error_src__(file_paths=list) -> list:
    error_file_paths = [path for path in file_paths if 'error' in path.lower()]
    return error_file_paths


# # Example
# project, paths = get(13, 'igv')

