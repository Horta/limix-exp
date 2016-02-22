import json
import os

def root_dir():
    return json.load(open(os.path.join(os.path.expanduser('~'),
                         'exp_properties.json'), 'r'))['base_dir']
