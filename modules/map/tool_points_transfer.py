import json, re
import pandas as pd

json_path  = "C:\\Users\westwell\Documents\\.WorkDocuments\IGV_Projects\For_ICA墨西哥\Map\TS_points.json"
json_export= "C:\\Users\westwell\Documents\\.WorkDocuments\\github\westwell_da\modules\map\TS_points_config.json"

# get raw json file
with open(json_path, 'r') as f:
    df = pd.DataFrame(json.load(f)['points'])

rename_mapper = {'x': 'position_x', 'y': 'position_y'}
df = df.rename(columns=rename_mapper)

### CHECK and change lambda function 
df['section'] = df['name'].apply(lambda x: re.findall(r'^([a-zA-Z]+)', x)[0].upper())
df['block']   = df['name'].apply(lambda x: re.findall(r'([A-Za-z-_]+)([A-Za-z0-9]+)', x)[0][1])
df['block']   = df['section']+df['block']

# get four points for each block
g = df.groupby(by='block')
y_max, y_min = g['position_y'].max(),  g['position_y'].min()
x_max, x_min = g['position_x'].max(), g['position_x'].min()

d = {
    'section'  : x_min.index.tolist(),
    'x_min' : x_min.values,
    'x_max' : x_max.values,
    'y_min' : y_min.values,
    'y_max' : y_max.values,
}
d = pd.DataFrame(d)

ref = d.set_index('section').to_dict(orient='index')
print(ref)

# export
with open(json_export, 'w') as f:
    json.dump(ref, f)