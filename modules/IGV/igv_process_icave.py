import pandas as pd
from modules.IGV import igv_process

def ica_processer(df=pd.DataFrame) -> pd.DataFrame:
    '''
    IGV data cleaning and processing for ICAVE project.
    All functions should be executed in sequence.

    Parameters:
    df (pd.Dataframe): df preprocessed with filter

    Returns:
    df (pd.Dataframe): prepared df for general igv processing or for data analysis
    '''
    df = df.sort_values('local_time').reset_index(drop=True)
    df = get_location2(df=df)
    df = get_current_task_tag(df=df)
    df['location_ref'] = df['target_location'].apply(lambda x: lmd_get_location_ref(x))
    df['location_block']=df['target_location'].apply(lambda x: lmd_get_location_block(x))
    df['mission_type'] = df.apply(lambda x: lmd_get_mission_type(x['current_task_tag'], x['location_ref']), axis=1)
    # df = igv_process.get_cycle(df=df)
    # df = igv_process.get_cycle_tag(df=df)

    return df

def get_location2(df=pd.DataFrame):
    df['target_location2'] = df['target_location'].str.split(r'\， ').str[1].fillna('')
    df['target_location'] = df['target_location'].str.split(r'\， ').str[0]
    return df

def lmd_get_location_ref(target_location=str) -> str:
    '''
    Paras: 
    target_location (str)

    Returns: 
    location_ref (str): 'QCTP', 'YARD', 'TS'
    '''
    if "crane" in target_location:      return 'QCTP'
    elif "block" in target_location:    return 'YARD'
    elif 'ts' in target_location:       return 'TS'
    else: return 'None'

def lmd_get_location_block(target_location=str):
    if 'D' in target_location: return 'D'
    elif 'E' in target_location: return 'E'

def get_current_task_tag(df=pd.DataFrame):
    df['current_task_tag'] = df['mission_type'].apply(lambda x: 'LOAD' if x=='VSLD' else 'DSCH')
    return df

def lmd_get_mission_type(current_task_tag=str, location_tag=str) :
    if current_task_tag=='LOAD':
        if location_tag=='YARD':    return 'RECEIVE'
        elif (location_tag=='QCTP') | (location_tag=='TS'):  return 'DELIVER'
    elif current_task_tag=='DSCH':
        if (location_tag=='YARD') | (location_tag=='TS'):    return 'DELIVER'
        elif location_tag=='QCTP':  return 'RECEIVE'
    else: return


