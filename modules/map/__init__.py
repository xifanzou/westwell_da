import json
import os

with open(os.path.join(os.path.dirname(__file__), 'map_config.json'), 'r') as f:
    map_config = json.load(f)

def get_map_config():
    return map_config

with open(os.path.join(os.path.dirname(__file__), 'icave_points_config.json'), 'r') as f:
    ica_points_config = json.load(f)

def get_ica_points_config():
    return ica_points_config