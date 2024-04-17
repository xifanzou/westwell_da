from sys import displayhook
import pandas as pd
import json
import re
from src.processer_data import igv_process_icave 

def run(projrect=str, df=pd.DataFrame):
    if projrect.upper() == 'ICA':
        df = igv_process_icave.ica_processer(df=df)
    # else:
    #     df = location_clean(df=df)

    return df

def get_cycle(df=pd.DataFrame):
    df = df.sort_values(by='local_time').reset_index(drop=True)
    df['cycle'] = ''
    index_li, data_li = [], []
    
    for task_tag, data in df.groupby(by='current_task_tag'):
        start_ind = data[data['mission_type']=='RECEIVE'].first_valid_index()
        data = data[data.index>=start_ind]
        data['mission_type_change'] = data['mission_type'] != data['mission_type'].shift(1)

        rows = data[data['mission_type_change']==True].index.tolist()
        if len(rows) %2 != 0:
            last_ind = rows[-1]
            data = data[data.index < last_ind]
            rows.remove(last_ind)

        data_li.append(data)
        index_li.append(rows)
    
    index_li = [item for sublist in index_li for item in sublist]
    cycle_ids = [i+1 if i%2!=0 else i for i in range(1, len(index_li)+1)] 
    cycle_ids = [val//2 for val in cycle_ids]
    cycle_dict = {index_li[i]: cycle_ids[i] for i in range(len(index_li))}
    
    df = pd.concat(data_li, axis=0)
    df['cycle'] = df.index.map(cycle_dict)    
    df['cycle'] = df['cycle'].ffill()
    df = df.reset_index(drop=True)

    return df


def location_clean(df=pd.DataFrame):
    '''Create and return two extra features: location_ref, location_tag.'''

    loc_li = list(df['target_location'].values)

    for i in range(1, df.shape[0]):
        current_mission, previous_mission = df['mission_type'].iloc[i], df['mission_type'].iloc[i-1]
        current_target_loc = df['target_location'].iloc[i]
        if len(current_target_loc) < 2:
            if current_mission == previous_mission:
                loc_li[i] = loc_li[i-1]

    for i in range(df.shape[0]-1,1,-1):
        previous_mission = df['mission_type'].iloc[i]
        current_mission  = df['mission_type'].iloc[i-1]
        current_target_loc = df['target_location'].iloc[i-1]
        if len(current_target_loc) < 2 and current_mission == previous_mission:
            loc_li[i-1] = loc_li[i]

    df['target_location_fillna'] = loc_li
    df['location_ref'] = df['target_location_fillna'].apply(lambda x: x.split('/')[0])
    df['location_tag'] = df['target_location_fillna'].apply(lambda x: x.split('/')[1])

    return df
