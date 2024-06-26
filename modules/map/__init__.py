import json
import os

def get_map_config():
    with open(os.path.join(os.path.dirname(__file__), 'map_config.json'), 'r') as f:
        map_config = json.load(f)
    return map_config

def get_ica_config():
    with open(os.path.join(os.path.dirname(__file__), 'icave_points_config.json'), 'r') as f:
        ica_points_config = json.load(f)
    return ica_points_config

def get_TS_config():
    with open(os.path.join(os.path.dirname(__file__), 'TS_points_config.json'), 'r') as f:
        TS_points_config = json.load(f)
    return TS_points_config