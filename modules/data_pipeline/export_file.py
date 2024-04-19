import os
import re
import pandas as pd
from modules.data_pipeline import get_path 
from modules.data_pipeline import read_path 
from modules.preprocessing.task_process import sheet_calculation # For excel file export only, a specific process required

def export_to_csv(week_num=int, data_src=str):
    '''
    Get prepared dataframes stored in dictionary,
    Create file export folders,
    Export each dataframe into single file, and concat and export them into weekly files.

    Paras:
    week_num (str): Week Number Indicator.
    data_src (str):Data Source Indicator (IGV/TASK/ERROR).

    Returns:
    None
    '''
    if data_src.upper() == 'TASK': 
        __task_export_rule__(week_num=week_num, data_src=data_src)
    else:
        path_dict = get_path.get(week_num=week_num, data_src=data_src)
        df_dict = read_path.read(week_num=week_num, data_src=data_src)
        for project in path_dict.keys():
            __export_init__(week_num=week_num, data_src=data_src,
                                project=project, df_dict=df_dict)
            
    return 

def __task_export_rule__(week_num=int, data_src='TASK'):
    path_dict = get_path.get(week_num=week_num, data_src=data_src)
    df_dict = read_path.read(week_num=week_num, data_src=data_src)
    for project in path_dict.keys():
        if project.upper() == 'TJ': 
            weekly_df = __export_init__(week_num=week_num, 
                                        data_src=data_src, 
                                        project=project, 
                                        df_dict=df_dict)
            
            sheet1, sheet2, sheet3, weekly_df_excel = sheet_calculation(weekly_df)
            weekly_path_excel = __create_weekly_path_excel__(week_num=week_num, 
                                                            data_src=data_src,
                                                            project=project)
            __task_excel_writer__(output_path=weekly_path_excel,
                                  main_output=weekly_df_excel,
                                  sheet1=sheet1, sheet2=sheet2, sheet3=sheet3,
                                  weekly_df=weekly_df)
    return

def __export_init__(week_num=int, data_src=str, project=str, df_dict=dict):
    file_folder_p = __create_folder__(week_num=week_num, 
                                data_src=data_src, 
                                project=project)

    # export daily/vessel file
    weekly_df = __export_single_file__(data_src=data_src, df_dict=df_dict[project], file_folder_p=file_folder_p)
    weekly_path = __create_weekly_path__(week_num=week_num, 
                                            data_src=data_src,
                                            project=project)
    # export weekly file
    # try:
    if weekly_df.shape[0] >= 10:
        weekly_df.to_csv(weekly_path, encoding='utf-8-sig')
        if data_src.upper() == 'ERROR':
            weekly_path_excel = __create_weekly_path_excel__(week_num=week_num, 
                                                            data_src=data_src,
                                                            project=project)
            weekly_df.to_excel(weekly_path_excel, sheet_name='ErrorHistory')
            print(f'Weekly error file exported to {weekly_path_excel}')
        print(f'Weekly file exported to: {weekly_path}\n')
    else:
        print(f'Too litte info to extract and form a weekly file.')
    # except:
    #     print('No weekly data to export.\n')

    if project.upper() == 'TJ':
        return weekly_df
    else:
        return

def __export_single_file__(data_src=str, df_dict=dict, file_folder_p=str):
    weekly_df = pd.DataFrame()
    weekly_df_list = []
    for folder_name, df in df_dict.items():

        export_path = os.path.join(file_folder_p, 
                                   f'{data_src.capitalize()}Data_{folder_name}.csv')
        
        if not os.path.exists(export_path):
            if df.shape[0]>=1:
                df.to_csv(export_path, encoding='utf-8-sig')
                weekly_df_list.append(df)
                print(f"File exported to: {export_path}")
            else:
                print(f'File too small {df.shape}, {folder_name} has no info to extract.')
                
        else:
            print(f"Already exists: {export_path}. Skipping export.")
            pass
    try:
        weekly_df = pd.concat(weekly_df_list)
    except:
        pass

    return weekly_df

def __create_folder__(week_num=int, data_src=str, project=str) -> str:
    """
    Create folder for file exporting.
    """
    file_folder_p = os.path.join('data\\processed', 
                                f'W{week_num}', 
                                f'{project}')
    
    file_folder_p = os.path.abspath(file_folder_p)
    if not os.path.exists(file_folder_p): 
        os.makedirs(file_folder_p)
        print(f"Created directory: {file_folder_p}")
    return file_folder_p

def __create_weekly_path__(week_num=int, data_src=str, project=str):
    """
    Generate the export path for the merged dataframes.
    """
    merged_csv_path = os.path.join('data\\processed',
                                   f'W{week_num}',
                                   f"{data_src.capitalize()}_W{week_num}_{project}.csv")
    merged_csv_path = os.path.abspath(merged_csv_path)

    return merged_csv_path

def __create_weekly_path_excel__(week_num=int, data_src=str, project=str):
    """
    Generate the export path for the merged dataframes.
    """
    merged_xlsx_path = os.path.join('data\\processed',
                                   f'W{week_num}',
                                   f"{data_src.capitalize()}_W{week_num}_{project}.xlsx")
    merged_xlsx_path = os.path.abspath(merged_xlsx_path)

    return merged_xlsx_path

def __task_excel_writer__(output_path=str, main_output=pd.DataFrame, sheet1=pd.DataFrame, sheet2=pd.DataFrame, sheet3=pd.DataFrame, weekly_df=pd.DataFrame):
    writer = pd.ExcelWriter(output_path,engine='xlsxwriter')
    main_output.to_excel(writer, sheet_name='Task')
    sheet1.to_excel(writer, sheet_name="Sheet1")
    sheet2.to_excel(writer, sheet_name="Sheet2")
    sheet3.to_excel(writer, sheet_name="Sheet3")
    weekly_df.to_excel(writer, sheet_name='DictData')
    writer.close()
    print(f'Task excel file exported to {output_path}.\n')
