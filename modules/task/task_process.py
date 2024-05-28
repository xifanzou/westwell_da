import pandas as pd
import numpy as np

def run(df=pd.DataFrame):
    df = __str_strip__(df=df)
    df, time_col = __time_col_clean__(df=df)
    df = __row_calculation__(df=df)
    # output_df= __time_transfer__(df=df)
    output_df = df
    return output_df

def __str_strip__(df=pd.DataFrame) -> pd.DataFrame:
    '''remove extra space in column names and in df values'''
    df.columns = df.columns.str.strip()
    obj = df.select_dtypes(['object'])
    df[obj.columns] = obj.apply(lambda x: x.str.strip())
    df[obj.columns] = obj.replace('', np.nan)
    return df

def __time_col_clean__(df=pd.DataFrame) -> pd.DataFrame:
    '''clean time related columns so taht tableau read them as float instead of str'''
    time_col = []
    for col in df.columns:
        if ('time' in col and 'date' not in col)  or 'duration' in col:
            time_col.append(col)
    df[time_col] = df[time_col].replace(r'-', 0).astype(float)
    return df, time_col

def __time_transfer__(df=pd.DataFrame) -> pd.DataFrame:
    time_col = ['task_time', 'alignment_start_time', 'alignment_end_time', 
                'navigation_start_time', 'navigation_end_time', 
                'wairing_for_operation_start_time', 'wairing_for_operation_end_time']
    for col in time_col:
        df[col] = pd.to_datetime(df[col], unit='s', errors='coerce')
        df[col] = df[col].astype('datetime64[s]')    
    return df

def __get_navigation_duration__(df=pd.DataFrame) -> pd.DataFrame:
    navi_id = list(df['navigation_refer_id'].unique())
    navi_dur = [None] * df.shape[0]
    st = 0

    for id in navi_id:
        if len(id) > 1:
            ind = df.index[(df['navigation_refer_id']==id)]
            temp_df = df.iloc[ind]

            if 'True' in temp_df['navigation_is_final'].values and st == 0:
                st = min([float(time) for time in temp_df['navigation_start_time'].values])
                ed = max([float(time) if time != '-' else 0 for time in temp_df['navigation_end_time'].values])
                dur = ed-st
                last_ind = max(ind)
                navi_dur[last_ind] = dur
                st = 0

            elif 'True' in temp_df['navigation_is_final'].values and st != 0:
                ed = max([float(time) if time != '-' else 0 for time in temp_df['navigation_end_time'].values])
                dur = ed-st
                last_ind = max(ind)
                navi_dur[last_ind] = dur
                st = 0
                
            elif 'True' not in temp_df['navigation_is_final'].values:
                st = min([float(time) for time in temp_df['navigation_start_time'].values])

    df['navigation_duration(s)'] = navi_dur
    return df

def __get_alignment_info__(df=pd.DataFrame) -> pd.DataFrame:
    alig_id = list(df['alignment_mission_id'].unique())
    alig_dur, alig_time = [None]*df.shape[0], [None]*df.shape[0]

    for id in alig_id:
        if len(id) > 1:
            ind = df.index[df['alignment_mission_id']==id]
            last_ind = max(ind)

            temp_df = df.iloc[ind]
            st = min([float(time) for time in temp_df['alignment_start_time'].values])
            ed = max([float(time) if time != '-' else 0 for time in temp_df['alignment_end_time'].values])
            dur = ed - st

            alig_dur[last_ind] = dur
            alig_time[min(ind):max(ind)+1] = [1 if len(val)<=1 else 0 for val in temp_df['alignment_end_time']]
            # print(alig_time[min(ind):max(ind)+1])

    df['alignment_duration(s)'], df['alignment_count'] = alig_dur, alig_time
    return df

def __row_calculation__(df=pd.DataFrame) -> pd.DataFrame: # dont forget to change
    
    df['alignment_duration'] = df['alignment_end_time'] - df['alignment_start_time']
    df['navigation_duration'] = df['navigation_end_time'] - df['navigation_start_time']
    df['wairing_for_operation_duration'] = df['wairing_for_operation_end_time'] - df['wairing_for_operation_start_time']
    df['time_gap_navi-alig'] = df['alignment_start_time'] - df['navigation_end_time']
    df['time_gap_alig-ope'] = df['wairing_for_operation_start_time'] - df['navigation_end_time']

    return df

def sheet_calculation(df=pd.DataFrame):
    g = df.groupby('task_id')
    ind = list(g.groups.keys())
    # print(f'No of tasks: {len(ind)}\n')

    # navigation
    navi = df.where(df['navigation_duration']>=0).groupby('task_id')['navigation_duration'].unique()
    navi_dur = pd.DataFrame(navi.apply(lambda x: sum(x)), index=ind)['navigation_duration']

    # alignment
    align = df.where(df['alignment_duration'].ge(0.0)).groupby('task_id')['alignment_duration'].unique()
    align_dur = align.apply(lambda x: x.sum())
    align_dur = pd.DataFrame(align_dur, index=ind)['alignment_duration']
    n_align = align.apply(lambda x: 0 if (x==[0.0]).all() else len(x)-1)
    n_align = pd.DataFrame(n_align, index=ind)['alignment_duration']

    # waiting for ops
    ops = df.where(df['wairing_for_operation_duration'].ge(0.0)).groupby('task_id')['wairing_for_operation_duration'].unique()
    ops_dur = pd.DataFrame(ops.apply(lambda x: x.sum()), index=ind)['wairing_for_operation_duration']

    # time gap: navigation end to alignment starat
    # navi_align = df.where(df['time_gap_navi-alig'].ge(0.0)).groupby('task_id')['time_gap_navi-alig'].unique()
    # gap_navi_align = pd.DataFrame(navi_align.apply(lambda x: x.sum()), index=ind)['time_gap_navi-alig']
    
    min_align_st = df.where(df['alignment_start_time'].ge(1)).groupby('task_id')['alignment_start_time'].apply(lambda x: x.min())
    max_navi_end = df.where(df['navigation_end_time'].ge(1)).groupby('task_id')['navigation_end_time'].apply(lambda x: x.max())
    gap_navi_align = (min_align_st - max_navi_end).fillna(0)

    # time gap: alignment end to watiting for oper start
    # align_opera = df.where(df['time_gap_alig-ope'].ge(0.0)).groupby('task_id')['time_gap_alig-ope'].unique()
    # gap_align_opera = pd.DataFrame(align_opera.apply(lambda x: x.sum()), index=ind)['time_gap_alig-ope']

    min_navi_st  = df.where(df['wairing_for_operation_start_time'].ge(1)).groupby('task_id')['wairing_for_operation_start_time'].apply(lambda x: x.min())
    max_align_ed = df.where(df['alignment_end_time'].ge(1)).groupby('task_id')['alignment_end_time'].apply(lambda x: x.max())
    gap_align_opera = (min_navi_st - max_align_ed).fillna(0)
    

    # sheet 3
    vid  = g['vehicle_id'].value_counts().index.get_level_values(1)
    task = g['current_task'].value_counts().index.get_level_values(1)
    task_time = g['task_time'].min()
    navi_start = g['navigation_start_time'].min()
    opera_end_time = g['wairing_for_operation_end_time'].max()

    df_dict = {
        'Task Id' : ind,
        'navigation_dur(s)': navi_dur,
        'Alignment'    : n_align,
        'alignment_dur(s)' : align_dur,
        'opera_dur(s)'     : ops_dur,
        'gap_navi_to_align(s)': gap_navi_align,
        'gap_align_to_oper(s)': gap_align_opera,
        'vehicle_id'       : vid,
        'Current Task'     : task,
        'task_start_time'  : task_time,
        'navi_start_time'  : navi_start,
        'opera_end_time' : opera_end_time,
        'gap_before_navi(s)': navi_start-task_time
    }

    df_form_dict = pd.DataFrame.from_dict(df_dict).sort_values(by=['vehicle_id', 'task_start_time'])
    df_form_dict['time_idle'] = df_form_dict['task_start_time'] - df_form_dict['opera_end_time'].shift(periods=1, fill_value=0)
    df_form_dict['alignment_group'] = df_form_dict['Alignment'].apply(lambda x: '0+1次' if x <=1 else '2次及以上')
    df_form_dict['vehicle_group'] = df_form_dict['vehicle_id'].apply(lambda x: "徐工" if (x=='A002' or x=='A001' or x=='A073' or x=='A076') else "奥杰")

    sheet1 = df_form_dict[['Task Id']]
    sheet2 = df_form_dict[['Task Id', 'Alignment']]
    sheet3 = df_form_dict[['vehicle_id', 'Task Id', 
                            'Current Task', 'task_start_time', 
                            'opera_end_time', 'navi_start_time',
                            'time_idle' , 'gap_before_navi(s)']]
    
    sheet1['Time Gap'] = df_form_dict[['navigation_dur(s)', 'alignment_dur(s)','opera_dur(s)', 'gap_navi_to_align(s)', 'gap_align_to_oper(s)']].apply(list, axis=1)
    sheet1['checkpoint'] = [['navigation_duration', 'alignment_duration','waiting_for_ops_duration','gap_navi_to_align','gap_align_to_ops'] ]* sheet1.shape[0]
    sheet1 = sheet1.explode(['Time Gap','checkpoint'])
    sheet1_cols = ['checkpoint', 'Task Id', 'Time Gap']
    sheet1 = sheet1[sheet1_cols]

    return df, sheet1, sheet2, sheet3, df_form_dict