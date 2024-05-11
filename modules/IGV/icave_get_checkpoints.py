import re
import pandas as pd
from modules.map import get_ica_config

def get(df=pd.DataFrame) -> pd.DataFrame:
    # labelling service lane depending on location section
    df['location_section'] = df['target_location'].apply(lambda x: lmd_section(x))
    res = []
    for cycle_name, data in df.groupby(by='cycle'):
        data['location_section'] = data['location_section'].ffill().bfill()
        res.append(data)
    df = pd.concat(res).reset_index(drop=True)
    df['SL'] = df.apply(lambda x: lmd_if_in_sl(x['location_section'], x['position_x'], x['position_y']), axis=1)
    df['location_section'] = df.apply(lambda x: lmd_section_relabel(x['location_section'], x['position_x'], x['position_y']), axis=1)

    # get breakdown, cut in and cut outs
    df['ckp_demo'] = df.apply(lambda x: lmd_ckps(x['location_section'], x['target_location'], x['SL'], x['current_task'], x['position_x'], x['position_y']), axis=1)
    df['cut_in'] = df['ckp_demo'].apply(lambda x: True if x=='2->3' else False)
    df['cut_out']= df['ckp_demo'].apply(lambda x: True if x=='4->5' else False)

    # get path A
    df['PathA'] = df.apply(lambda df: lmd_path(df['location_block'], df['position_x'], df['ckp_demo']), axis=1)

    return df

def lmd_section(target_location):
    pat = r'block_id: (\d[A-Z])'
    if 'block' in target_location: 
        return re.findall(pattern=pat, string=target_location)[0]
    else: 
        return

def lmd_if_in_sl(location_section, x, y):
    '''
    Parameters:
    location_section (str): should be in line with `icave_points_config.json`
    x (float64)
    y (float64)

    Returns:
    SL (boolean): indicating if the point is inside service lane
    '''
    if location_section != None:
        data_dict = get_ica_config()[str(location_section)]
        if (data_dict['x_min']<= x <=data_dict['x_max']) and (data_dict['y_min']<= y <=data_dict['y_max']):
            return True
        else: return False
    else: return False

def lmd_section_relabel(location_section, x, y):
    '''
    Fill np.NaN with QC info in `location_section`.

    Parameters:
    location_section (str)
    x (float64)
    y (float64)

    Returns:
    location_section (str): added with QC info
    '''
    data_dict = get_ica_config()['QC']
    if data_dict['x_min']<=x<=data_dict['x_max'] and data_dict['y_min']<=y<=data_dict['y_max']:
        return 'QC'
    else: return location_section

def lmd_ckps(location_section, target_location, SL, current_task, x, y):
    '''
    Lambda function to encode checkpoint for each location point,
        1-2: heading to YARD and not in service lane
        2-3: heading to YARD and not in service lane, along with cut-in behaviour
        3-4: in YARD service lane
        4-5: heading to QC but still in YARD area, along with cut-out behaviour
        5-6: heading to QC and outside YARD area
        6-7: heading to QC and in left corridor
        7-8: turning to quay
        8-9: in QC area, RTG Operation
        9-1: heading to YARD but still in QC area
    
    Parameters:
    location_section (str): for configuration
    target_location (str) : to get heading direction (YARD/QC)
    SL (boolean)          
    current_task (str)    
    x (float64)
    y (float64)

    Returns:
    ckp_demo (str)
    '''
    try:
        data_dict = get_ica_config()[str(location_section)]
    except Exception as e:
        print(f"An error occurred when getting ica map config: {e}")
        pass
    else:
        corridor = get_ica_config()['CR']
        if SL == True: 
            return '3->4'
        else:
            if location_section=='QC':
                if 'block' in target_location or 'ts' in target_location: 
                     return '9->1'
                elif 'block' not in target_location:
                    if current_task!='Drive': 
                        return '8->9'
                    else: 
                        return '7->8'
            elif location_section != 'QC':
                if 'block' in target_location: 
                    if int(location_section[-2:-1]) % 2 != 0: 
                        if y < data_dict['y_max'] and x > data_dict['x_min']: 
                            return '2->3'
                        else: 
                            return '1->2'
                    elif int(location_section[-2:-1]) % 2 == 0:
                        if data_dict['y_max']+3.5> y > data_dict['y_max'] and data_dict['x_min']<=x<=data_dict['x_max']: 
                            return '2->3'
                        else: 
                            return '1->2'
                elif 'block' not in target_location:
                    if x < data_dict['x_max']: return '4->5'
                    elif y > corridor['y_max']: return '6->7'
                    else: return '5->6'

def lmd_path(location_block, x, ckp):
    # path b (good path)：不穿越堆场 （两条堆场bp） / path a (bad path)：穿越堆场
    left_corr = get_ica_config()['CL']
    right_corr= get_ica_config()['CR']

    if location_block=='D' and (ckp=='5->6'):
        if right_corr['x_min']<=x<=right_corr['x_max']: return True
    if location_block=='E' and ckp=='1->2':
        if left_corr['x_min']<=x<=left_corr['x_max']: return True