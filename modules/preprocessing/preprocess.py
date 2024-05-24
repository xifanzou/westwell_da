import pandas as pd
import numpy as np
import re


def run(project=str, data_src=str, df=pd.DataFrame, vessel_name=str) -> pd.DataFrame:
    '''
    Preporcess based on project and data_src. 

    Paras:
    project (str): Project Indicatoer.
    data_src (str):Data Source Indicator (IGV/TASK/ERROR)
    df (pd.DataFrame): Input DataFrame.
    vessel_name (str): Input Vessel Name.

    Returns:
    df (pd.DataFrame): DataFrame preprocessed and(/or) filtered based on project requirements.
    '''
    stripped_df = __strip_str__(df=df)

    # try:
    data_src = data_src.upper()
    if data_src == 'ERROR':
        preprocessed = __error_filter__(df=stripped_df)
    elif data_src == 'TASK':
        preprocessed = __task_filter__(df=stripped_df)
    elif data_src == 'IGV':
        project = project.upper()
        # print(f'Before filter: {stripped_df.shape}')
        preprocessed = __igv_filter__(project=project, vessel_name=vessel_name, df=stripped_df)
        # print(f'After filter {preprocessed.shape}')

    # except Exception as e:
    #     print("An error occurred:", e)
        # pass
    # return

    preprocessed = preprocessed.reset_index(drop=True)

    return preprocessed

def __igv_filter__(project=str, vessel_name=None, df=pd.DataFrame):
    '''
    Filters based on different projects.

    Paras:
    project (str): Project Indicatoer.
    df (pd.DataFrame): Input DataFrame.

    Returns:
    df (pd.DataFrame): DataFrame preprocessed or filtered based on project requirements.
    '''
    project = project.upper()

    # Datatime
    df['local_time'] = pd.to_datetime(df['local_time'], format='%Y-%m-%d %H:%M:%S')
    
    # Target_location
    if project.upper() in ['WH', 'DL', 'TS', 'YH', 'TPY']:
        df['target_location'] = df['target_location'].fillna('na')
        df['target_location'] = df['target_location'].apply(lambda x: x if ((len(x)>2)&(type(x)==str)) else np.NAN)
        df = df[df['target_location'].isna()==False]
    
    # vesselVisitID
    if project in ['DL', 'TS', 'YH']:
        df['vesselVisitID'] = df['local_time'].dt.month + df['local_time'].dt.day
    elif project in ['WH', 'TPY']:
        df['vesselVisitID'] = [vessel_name] * df.shape[0]
    elif project== 'TJ': # not working check why
        val_to_drop = ['None', '']
        df = df[df['vesselVisitID'].isin(val_to_drop)==False]

    # Current task
    if project.upper() in ['DL', 'TS', 'YH', 'TPY']:
        status = ['DSCH', 'LOAD', 'YARDMOVE', 'MANUALMOVE']
        df = df[df['current_task'].isin(status)]
    elif project.upper() in ['TJ', 'WH']:
        status = ['DSCH', 'LOAD', 'YARDMOVE']
        df = df[df['current_task'].isin(status)]    
        

    # Project specific
    if project in ['CK']:
        df = df[df['task_state_running']=='True']
        _useless_col = ['target_location', 'task_state_lock',
                        'vesselVisitID', 'mission_type', 'missionID',
                        'container1_type', 'container2_type',]
        df = df.drop(_useless_col, axis=1)
    if project in ['ICA']:
        df = __igv_ica_filter__(df=df)

    # print(project, df)
    return df.reset_index(drop=True)

def  __igv_ica_filter__(df=pd.DataFrame):
    df['local_time'] = pd.to_datetime(df['local_time'], format='%Y-%m-%d %H:%M:%S')
    df['mission_type_org'] = df['mission_type'].copy()
    df['vesselVisitID'] = df['local_time'].dt.month + df['local_time'].dt.day
    df = df[(df['mission_type'].str.contains('VSDS')) | 
            (df['mission_type'].str.contains('VSLD')) |
            (df['mission_type'].str.contains('XRAY'))]
    
    # Deal with XRAY 
    # print(f'before transform {df.mission_type.value_counts()}')
    df['mission_type'] = df['mission_type'].apply(lambda x: np.NaN if 'XRAY' in x.upper() else x)
    # df['mission_type'] = df['mission_type'].ffill()
    # print(f'After transform {df.mission_type.value_counts()}')
    
    # Filter on target location
    df = df[(df['target_location'].apply(lambda x: len(x)) >= 25) |
            df['target_location'].str.contains('ts') |
            df['target_location'].str.contains('tp')
            ]
    ## Deal with tp
    # print(f'before transform {df.target_location.value_counts()}')
    # df['target_location'] = df['target_location'].apply(lambda x: np.NaN if 'tp' in x else x)
    # df['target_location'] = df['target_location'].ffill()
    # print(f'After transform {df.target_location.value_counts()}')
    
    return df

def __error_filter__(df=pd.DataFrame):
    """
    Filter DataFrame to include only rows with error levels 5 and 6.

    Parameters:
    df (pd.DataFrame): Input DataFrame. 

    Returns:
    pd.DataFrame: DataFrame containing rows with error levels 5 and 6 and filtered on error messages.
    """
    if not isinstance(df, pd.DataFrame):
        raise ValueError("Input must be a pandas DataFrame.")

    if 'level' not in df.columns:
        raise ValueError("Input DataFrame must contain a 'level' column.")

    df = df[df['level'].isin([5, 6])]

    filtered_df = df[df['message'].apply(lambda x: __error_msg_filter__(x))==True].reset_index(drop=True)

    return filtered_df

def __error_msg_filter__(message):
    return len(re.findall(r'紧停|手柄|IMU|手动', message))==0

def __task_filter__(df=pd.DataFrame):
    """
    Preprocesses task DataFrame by converting 'task_time_datetime' column to datetime format.

    Parameters:
    df (pd.DataFrame): Input DataFrame. 

    Returns:
    pd.DataFrame: DataFrame with 'task_time_datetime' column converted to datetime format.
    """
    if not isinstance(df, pd.DataFrame):
        raise ValueError("Input must be a pandas DataFrame.")

    if 'task_time_datetime' not in df.columns:
        raise ValueError("Input DataFrame must contain a 'task_time_datetime' column.")

    df['task_time_datetime'] = pd.to_datetime(df['task_time_datetime'], format='%Y-%m-%d %H:%M:%S')

    status = ['LOAD', 'DSCH', 'YARDMOVE', 'Drive', 'PICKUP','GROUND','Lock']
    df = df[df['current_task'].isin(status)].reset_index(drop=True)

    return df

def __strip_str__(df=pd.DataFrame):
    """
    Strip leading and trailing whitespaces from column names and string columns.

    Parameters:
    DataFrame (pd.DataFrame): Input DataFrame. 

    Returns:
    pd.DataFrame: DataFrame with leading and trailing whitespaces stripped from column names and string columns.
    """
    df.columns = df.columns.str.strip()
    
    # Select columns of type 'object'
    obj_cols = df.select_dtypes(['object']).columns
    
    # Strip leading and trailing whitespaces from string columns
    df[obj_cols] = df[obj_cols].apply(lambda x: x.str.strip())

    return df

