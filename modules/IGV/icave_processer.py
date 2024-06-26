import numpy as np
import pandas as pd
from modules.IGV import igv_process

def adhoc_process(df=pd.DataFrame) -> pd.DataFrame:
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
    df['current_task_tag'] = df['current_task_tag'].ffill() # fill XRAY with previous task

    df['location_ref'] = df['target_location'].apply(lambda x: lmd_get_location_ref(x))
    df['location_ref'] = df['location_ref'].ffill()

    # location block
    df['location_block'] = df.apply(lambda x: lmd_get_location_block(x['target_location'], x['task_stage']), axis=1)

    # mission type
    df['mission_type'] = df.apply(lambda x: lmd_get_mission_type(x['current_task_tag'], x['location_ref']), axis=1)
    df['mission_type'] = df['mission_type'].ffill()
    return df

def correct_block(df=pd.DataFrame):
    '''
    Fill np.NaN in `location_block` after get `Cycle Tag` feature,
    Prepare for generating checkpoints,
    Must be executed after `get_cycle` and before `get_checkpoints`.
    
    Parameters:
    df (pd.Dataframe): cleaned and adhoc processed ICAVE dataframe, with `Cycle Tag` info

    Returns:
    df (pd.Dataframe): dataframe with correct block info
    '''
    # df = df.sort_values(by=['vehicle_id', 'Cycle Tag','local_time']).reset_index(drop=True)

    # for cycle, data in df.groupby('Cycle Tag'):
    #     try:
    #         ind = data.index.tolist()
    #         val = data['location_block'].value_counts().index.tolist()
    #         print(val)
    #         df['location_block'].iloc[ind] = df['location_block'].fillna(val[0])
    #     except IndexError:
    #         val = 'D'
    #         data.loc[data['location_block']!=val, 'location_block'] = val
    
    # return df

    exp_li = []
    if 'Cycle Tag' in df.columns:
        df = df.sort_values(by=['vehicle_id', 'Cycle Tag','local_time']).reset_index(drop=True)
        for cycle, data in df.groupby('Cycle Tag'):
            data['location_block'] = data['location_block'].ffill().bfill()
            if len(data['location_block'].value_counts().index.tolist())>1:
                val = 'D'
                data.loc[data['location_block']!=val, 'location_block'] = val
            exp_li.append(data)
        
        df = pd.concat(exp_li, axis=0)

        return df
    else:
        return df == pd.DataFrame({})

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
    else: return np.NaN

def lmd_get_location_block(target_location=str, task_stage=str):
    if ('D' in target_location) & (task_stage=='Waiting for Operation' or task_stage=='Alignment'): return 'D'
    elif ('E' in target_location) & (task_stage=='Waiting for Operation' or task_stage=='Alignment'): return 'E'

def get_current_task_tag(df=pd.DataFrame):
    df['current_task_tag'] = df['mission_type'].apply(lambda x: 'LOAD' if x=='VSLD' else 'DSCH')
    return df

def lmd_get_mission_type(current_task_tag=str, location_tag=str) :
    if current_task_tag=='LOAD':
        if (location_tag=='YARD'):    return 'RECEIVE'
        elif (location_tag=='QCTP') | (location_tag=='TS') :  return 'DELIVER'
    elif current_task_tag=='DSCH':
        if (location_tag=='YARD') | (location_tag=='TS') :    return 'DELIVER'
        elif (location_tag=='QCTP'):  return 'RECEIVE'
    else: return

