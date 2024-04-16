from sys import displayhook
import pandas as pd
import json
import re


def run(df=pd.DataFrame):


    return df



def location_clean(df=pd.DataFrame) -> pd.DataFrame:
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
