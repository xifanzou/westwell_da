import pandas as pd
import re

def run(df = pd.DataFrame):
    df['start_time'] = pd.to_datetime(df['start_time'], format='%Y-%m-%d %H:%M:%S')
    df['end_time'] = pd.to_datetime(df['end_time'], format='%Y-%m-%d %H:%M:%S')
    df['duration'] = (df['end_time'] - df['start_time']).dt.total_seconds()
    # df['duration'] = df['duration'].apply(lambda x: x.total_seconds())
    # print('Data types after converting duration to seconds:')
    # print(df.dtypes)  # Print data types after converting duration to seconds
    
    # duration_threshold = pd.Timedelta(seconds=60)
    # df = df[df['duration'] > duration_threshold]  # Assuming this is the line causing the error
    df = df[df['duration'] > 60] 

    temp = df.groupby(by='vehicle_id')
    res = []
    for vid, data in temp:
        data = data.sort_values(by='start_time').reset_index(drop=True)
        data['break'] = data['start_time']-data['start_time'].shift(1) # start time difference between two errors
        data['break'] = data['break'].apply(lambda x: x.total_seconds()).fillna(999999)
        data = data[(data['break']>60)]
        if data.shape[0]>0:
            res.append(data)

    df['id'] = df.index
    df = df.reset_index(drop=True)

    return df

