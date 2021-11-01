import brightway2 as bw
import yaml
import argparse
import os
import pickle
import logging

from project import Project

# @TODO Check that the script path in the configuration is correct

# set up arguments for command line running
parser = argparse.ArgumentParser(description='Execute automatic Brightway LCIA')
parser.add_argument('--data', help='Path to data directory.')
parser.add_argument('--config', help='Name of local config file.')
args = parser.parse_args()

# read in config (YAML) file with error handling; get variable groups
config_yaml_filename = os.path.join(args.data, args.config)
try:
    with open(config_yaml_filename, 'r') as f:
        config = yaml.load(f, Loader=yaml.FullLoader)
        flags = config.get('flags', {})
        fileIO = config.get('fileIO', {})
        project = config.get('project', {})
except IOError as err:
    print(f'Could not open {config_yaml_filename} for configuration.')
    exit(1)

# do variable assignments and filename construction as necessary
project_picklename = fileIO.get('project_picklename', 'rb')

# if a new project is being created, instantiate it
if flags.get('create_new_project',True):
    proj = Project()
    # if pickling is True, then pickle the project
    if flags.get('pickle_project', True):
        pickle.dump(
            proj,
            open(
                project_picklename,
                'wb'
            )
        )
else:
    # if a new project is not being created, then read in an existing pickle
    proj = pickle.load(
        open(
            project_picklename,
            'rb'
        )
    )




