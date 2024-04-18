import json
import os

with open(os.path.join(os.path.dirname(__file__), 'map_config.json'), 'r') as f:
    map_config = json.load(f)

def get_map_config():
    return map_config

