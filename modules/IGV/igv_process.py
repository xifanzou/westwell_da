import pandas as pd
import numpy as np
from pyproj import Proj
import math
from modules.IGV import igv_process_icave, icave_get_checkpoints
from modules.map import get_map_config

def run(project=str, df=pd.DataFrame):
    '''
    All functions should be executed in sequence.
    Processings are based on `vehicle_id` level.
    '''
    project = project.upper()

    if project == 'ICA':
        df = igv_process_icave.ica_processer(df=df)
        df = get_cycle(df=df)
        df = get_cycle_tag(df=df)
        df = get_lon_lat(project=project, df=df)
        df = utm_rotation(df=df, angle=101.4765) # icave specific
        df = get_container_type(df=df)
        df = get_power_usage(df=df)
        df = get_kpis(df=df)
        df = get_queue_mark_and_estop(df=df)
        df = get_chassis_mode(df=df)
        df = icave_get_checkpoints.get(df=df)

    elif project == 'CK':
        df = df

    else:
        df = get_location_tag(df=df)
        df['current_task_tag'] = df.apply(lambda x: lmd_get_current_task_tag(x['current_task'], x['mission_type'], x['location_ref']),axis=1)
        df['current_task_tag'] = df['current_task_tag'].ffill()
        df = get_cycle(df=df)
        df = get_cycle_tag(df=df)
        if 'Cycle Tag' in df.columns:
            df = get_lon_lat(project=project, df=df)
            df = get_container_type(df=df)
            df = get_power_usage(df=df)
            df = get_kpis(df=df)
            df = get_queue_mark_and_estop(df=df)
            df = get_chassis_mode(df=df)

    return df

def get_lon_lat(project=str, df=None):
    # get map cfgs
    project = project.upper()
    try:
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
    except KeyError:
        print(f'{project} has no map config')

    return df

def utm_rotation(df=pd.DataFrame, angle=float):
    '''
    ICAVE project only.
    ICAVE map angle = 101.4765
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

    Parameters:    
    df (pd.Dataframe)

    Returns:
    df (pd.DataFrame): Dataframe with `location_ref` and `location_tag`
    '''
    # try:
    if 'target_location' in df.columns:
        _tmp = df['target_location'].str.split('/', expand=True)
        df['location_ref'], df['location_tag'] = _tmp[0], _tmp[1]
        df['location_ref'] = df['location_ref'].fillna('TS')
        df['location_tag'] = df['location_tag'].fillna('TS')
    # except:
    #     pass
    return df.reset_index(drop=True)

def lmd_get_current_task_tag(current_task=str, mission_type=str, location_ref=str):
    if (current_task != 'DSCH') & (current_task != 'LOAD'):
        if ('YARD' in location_ref and 'DELIVER' in mission_type) or ('QC' in location_ref and 'RECEIVE' in mission_type):
            return 'DSCH'
        elif('QC' in location_ref and 'DELIVER' in mission_type) or ('YARD' in location_ref and 'RECEIVE' in mission_type):
            return 'LOAD'      
        else:
            return np.nan
    else:
        return current_task

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
    # df.drop(columns=['mission_type_change'], inplace=True)
    df = df.reset_index(drop=True)

    return df

def get_cycle_tag(df=pd.DataFrame):
    try:
        cycle_tag_suffix = f"-{df['local_time'].iloc[1]}"
        df['Cycle Tag'] = (df['vehicle_id'].astype(str) + '-' +
                            df['vesselVisitID'].astype(str) + 'C' +
                            df['cycle'].astype(str) +
                            cycle_tag_suffix)
    except IndexError:
        print(f"This file passed inital filter check but doesn't have cycles, recommend data source check. Skip processing.")
        pass
    return df

def get_container_type(df=pd.DataFrame):    
    df['box'] = ''
    df['TEU'] = ''

    for cycle, data in df.groupby('Cycle Tag'):
        data['container_tag'] = data['container1_type'].astype(str) + data['container2_type'].astype(str)

        rtg_ind = data[
            (data['task_stage']=='Waiting for Operation') & 
            (data['location_ref'].str.contains('YARD', na=False))
            ].index.to_list()
        
        rtg_data = data[data.index.isin(rtg_ind)]
        n_mission= len(rtg_data['missionID'].value_counts().index.tolist())
        cntr_tag = data['container_tag'].value_counts().index.tolist()[0]
        
        if n_mission>=2:
            container_tag = '2020'
            box, teu = 2, 2
        else:
            container_tag = cntr_tag
            box = 1
            if '2' in container_tag: teu = 1
            else: teu = 2

        data_ind = data.index
        df.loc[data_ind, "container_tag"] = container_tag
        df.loc[data_ind, 'box'] = box
        df.loc[data_ind, 'TEU'] = teu

    return df

def get_queue_mark_and_estop(df=pd.DataFrame):
    col = 'local_time'
    df[col] = pd.to_datetime(df[col], format='%Y-%m-%d %H:%M:%S')
    df["Q_mark"] = ""
    df['estop boolean'] = ""
    for cycle_id, cycle_data in df.groupby('Cycle Tag'):
        df['estop boolean'] =  (df['distance_km'] - df['distance_km'].shift(1)).apply(lambda x: True if abs(x)>=(9/1000) else False)
        queue_data = cycle_data[cycle_data["speed"] <=0.1].copy(deep=True)
        queue_data["time_diff"] = (queue_data['local_time'] - queue_data['local_time'].shift(1)).apply(lambda x: x.total_seconds()).fillna(0).astype('int64')
        queue_point = queue_data[queue_data["time_diff"] > 15]

        if len(queue_data) > 0:
            if len(queue_point) == 0:
                df.loc[queue_data.index.to_list(), "Q_mark"] = "Q1"
            else:
                queue_id = 1
                df.loc[queue_data[queue_data["local_time"] < queue_point.iloc[0]["local_time"]].index.to_list(), "Q_mark"] = "Q" + str(queue_id)
                queue_id += 1

                for i in range(0, len(queue_point) - 1):
                    df.loc[queue_data[
                        (queue_data["local_time"] >= queue_point.iloc[i]["local_time"]) &
                        ((queue_data["local_time"] < queue_point.iloc[i + 1]["local_time"]))].index.to_list(), 
                                    "Q_mark"] = "Q" + str(queue_id)
                    queue_id += 1

                df.loc[queue_data[queue_data["local_time"]
                                    >= queue_point.iloc[-1]["local_time"]].index.to_list(), "Q_mark"] = "Q" + str(queue_id)
    return df

def get_chassis_mode(df=pd.DataFrame):
    col = 'local_time'
    df[col] = pd.to_datetime(df[col], format='%Y-%m-%d %H:%M:%S')

    # Defaults
    df['target_chassis'] = ''    

    for cid, cdata in df.groupby(by='Cycle Tag', sort='local_time'):
        cdata = cdata[cdata['chassis_mode']==2].copy()
        if len(cdata) > 0:          # data with pulled records
            # chassis_mode = cdata['chassis_mode'].value_counts().index.tolist()
            cdata['time diff'] = (cdata['local_time'] - cdata['local_time'].shift(1)).apply(lambda x: x.total_seconds()).fillna(3).astype('int64')

            cdata_ind = cdata.index
            split_data = cdata[cdata['time diff'] > 90] # time difference > 90s 

            pulled_id = int(1)
            if len(split_data) == 0:
                df.loc[cdata_ind, 'target_chassis'] = 'Pulled'+str(pulled_id) + f' {cid}' # Pulled1 cid
                
            else: 
                # First pull
                df.loc[cdata[ (cdata['local_time'] <= split_data.iloc[0]['local_time'])].index, 
                    'target_chassis'] = 'Pulled' + str(pulled_id) + f' {cid}'
                pulled_id += 1
                # Pulls in between
                for i in range(0, len(split_data)-1):
                    df.loc[cdata[
                            (cdata['local_time'] >= split_data.iloc[i]['local_time']) &
                            (cdata['local_time'] <= split_data.iloc[i+1]['local_time'])
                            ].index, 
                            'target_chassis'] = 'Pulled'+str(pulled_id) + f' {cid}'
                    pulled_id += 1
                # Last pull
                df.loc[cdata[
                    (cdata['local_time'] > split_data.iloc[-1]['local_time'])].index, 
                    'target_chassis'] = 'Pulled'+str(pulled_id) + f' {cid}'
                
    return df

def get_kpis(df=pd.DataFrame):
    duration_s = [3] * df.shape[0]
    duration_min = [3/60] * df.shape[0]
    distance_m = [df['speed'].iloc[i] * 3 for i in range(df.shape[0])]
    distance_km =[float(distance_m[i])/1000 for i in range(df.shape[0])]

    df['duration_s'] = duration_s
    df['duration_min']=duration_min
    df['distance_km']= distance_km

    return df

def get_power_usage(df=pd.DataFrame):
    df['power usage'] = ''
    for cycle, data in df.groupby('Cycle Tag'):
        data['soc_diff'] = data['soc'] - data['soc'].shift(-1)
        data['power_usage'] = data['soc_diff'].apply(lambda x: lmd_power_usage(x))
        df.loc[data.index, 'power usage'] = data['power_usage']

    return df

def lmd_power_usage(soc_diff):
    if soc_diff > 0 and soc_diff<=5: return soc_diff
    elif soc_diff<=0 and soc_diff>-5: return 0
    elif soc_diff >5: return 5
    elif soc_diff<=-5: return -1

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

