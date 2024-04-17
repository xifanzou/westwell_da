import pandas as pd
from src.processer_data import igv_process

def ica_processer(df=pd.DataFrame) -> pd.DataFrame:
    df = df.sort_values('local_time').reset_index(drop=True)
    df = get_location2(df=df)
    df['location_tag'] = df['target_location'].apply(lambda x: lmd_get_location_tag(x))
    df = get_current_task_tag(df=df)
    df['mission_type'] = df.apply(lambda x: lmd_get_mission_type(x['current_task_tag'], x['location_tag']), axis=1)
    df = igv_process.get_cycle(df=df)

    return df



def get_location2(df=pd.DataFrame):
    df['target_location2'] = df['target_location'].str.split(r'\， ').str[1].fillna('')
    df['target_location'] = df['target_location'].str.split(r'\， ').str[0]
    return df

def lmd_get_location_tag(target_location=str) -> str:
        if "crane" in target_location:      return 'QCTP'
        elif "block" in target_location:    return 'YARD'
        elif 'ts' in target_location:       return 'TS'
        else: return 'None'

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
