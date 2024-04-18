from sys import displayhook
import pandas as pd
import numpy as np
from pyproj import Proj
import math
import json
import re
from src.processer_data import igv_process_icave 
from src.map import get_map_config

def run(project=str, df=pd.DataFrame):
    if project.upper() == 'ICA':
        df = igv_process_icave.ica_processer(df=df)
        df = get_lon_lat(project=project, df=df)
        df = utm_rotation(df=df, angle=101.4765)

    else:
        df = get_location_tag(df=df)
        df['current_task_tag'] = df.apply(
            lambda x: lmd_get_current_task_tag(x['current_task'], x['mission_type'], x['location_ref']), 
            axis=1).ffill()
        

    return df

def get_lon_lat(project=str, df=None):
    # get map configs
    project = project.upper()
    config = get_map_config()[project]
    x_ref, y_ref, zone_ref, theta = config['ref_x'], config['ref_y'], config['zone_id'], config['ref_theta']
    
    # create utm instance
    utm = Proj(proj='utm', zone=zone_ref, ellps='WGS84') 

    rotation_matrix = np.array([[np.cos(theta),-np.sin(theta)],
                    [np.sin(theta), np.cos(theta)]])
    rotation_matrix_inv = np.linalg.inv(rotation_matrix)   

    col = ['position_lon', 'position_lat']
    if ('position_x' in df.columns) & ('position_y' in df.columns):
        df['position_lon'] = df['position_x'].apply(lambda x_pos: x_pos-x_ref)
        df['position_lat'] = df['position_y'].apply(lambda y_pos: y_pos-y_ref)
    else:
        df['position_lon'] = df['x'].apply(lambda x_pos: x_pos-x_ref)
        df['position_lat'] = df['y'].apply(lambda y_pos: y_pos-y_ref)            

    inversed_vec_res = list(map(lambda x: rotation_matrix_inv @ x, df[col].to_numpy()))
    gps_res = list(map(lambda x: utm(x[0], x[1], inverse=True), inversed_vec_res))
    lon, lat = list(map(lambda x: x[0], gps_res)), list(map(lambda x: x[1], gps_res))

    df['position_lon'], df['position_lat'] = lon, lat

    return df

def utm_rotation(df=pd.DataFrame, angle=float):
    '''
    ICAVE project only.
    Mexico angle = 101.4765
    '''
    theta = math.radians(180-float(angle))
    rotation_matrix = np.array([
        [np.cos(theta),-np.sin(theta)], [np.sin(theta), np.cos(theta)]
        ])
    
    if ('position_x' in df.columns) & ('position_y' in df.columns): col = ['position_x', 'position_y']
    else: col = ['x', 'y']

    rotated_vect = list(map(lambda x: rotation_matrix @ x, df[col].to_numpy()))
    pos_x, pos_y = list(map(lambda x: x[0], rotated_vect)), list(map(lambda x: x[1], rotated_vect))

    if ('position_x' in df.columns) & ('position_y' in df.columns):
        df['position_x'], df['position_y'] = pos_x, pos_y
    else:
        df['x'], df['y'] = pos_x, pos_y
    return df

def get_location_tag(df=pd.DataFrame):
    '''
    Extract location_ref and location_tag to prepare for current_task_tag extraction.

    Parameters:    df (pd.Dataframe)

    Returns:
    df (pd.DataFrame): Dataframe with `location_ref` and `location_tag`
    '''
    try:
        if 'target_location' in df.columns:
            _tmp = df['target_location'].str.split('/', expand=True)
            df['location_ref'], df['location_tag'] = _tmp[0], _tmp[1]
            df['location_ref'] = df['location_ref'].fillna('TS')
            df['location_tag'] = df['location_tag'].fillna('TS')
    except:
        pass

    return df

def lmd_get_current_task_tag(current_task=str, mission_type=str, location_ref=str):
    if current_task != 'DSCH' and current_task != 'LOAD':
        if ('YARD' in location_ref and mission_type=='DELIVER') or ('QC' in location_ref and mission_type=='RECEIVE'):
            return 'DSCH'
        elif('QC' in location_ref and mission_type=='DELIVER') or ('YARD' in location_ref and mission_type=='RECEIVE'):
            return 'LOAD'      
    else: return np.nan



def get_cycle(df=pd.DataFrame):
    '''
    Get cycle id,
    Input dataframe should based on vehicle entity.

    Parameters:
    df (pd.Dataframe)

    Returns:
    df (pd.Dataframe): with two extra columns `cycle`
    '''
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
    df.drop(columns=['mission_type_change'], inplace=True)
    df = df.reset_index(drop=True)
    try:
        cycle_tag_suffix = f"-{df['local_time'].iloc[1]}"
        df['Cycle Tag'] = (df['vehicle_id'].astype(str) + '-' +
                           df['vesselVisitID'].astype(str) + 'C' +
                           df['cycle'].astype(str) +
                           cycle_tag_suffix)
    except IndexError:
        print("DataFrame does not have enough rows")
        pass

    return df




def get_container_info(df=pd.DataFrame):
    
    for cycle, data in df.groupby('Cycle Tag'):
        col = ['container1_type', 'container2_type']
        data['']

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

