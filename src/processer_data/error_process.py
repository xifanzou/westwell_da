import pandas as pd
import re

def run(df = pd.DataFrame):
    df['start_time'], df['end_time'] = pd.to_datetime(df['start_time'], format='%Y-%m-%d %H:%M:%S'), pd.to_datetime(df['end_time'], format='%Y-%m-%d %H:%M:%S')
    df['duration'] = df['end_time'] - df['start_time']
    df['duration'] = df['duration'].apply(lambda x: x.total_seconds())
    df = df[(df['duration'] > 60)]

    temp = df.groupby(by='vehicle_id')
    res = []
    for vid, data in temp:
        data = data.sort_values(by='start_time').reset_index(drop=True)
        data['break'] = data['start_time']-data['start_time'].shift(1) # start time difference between two errors
        data['break'] = data['break'].apply(lambda x: x.total_seconds()).fillna(999999)
        data = data[(data['break']>60)]
        if data.shape[0]>0:
            res.append(data)

    df = df[df['message'].apply(lambda x: __errorMsgFilter__(x))==True].reset_index(drop=True)
    df['id'] = df.index
    return

def __errorMsgFilter__(message):
    return len(re.findall(r'紧停|手柄|IMU|手动', message))==0