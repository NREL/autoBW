import argparse
import os
import yaml
import logging
import time

import brightway2 as bw
import pandas as pd

from ForegroundDatabase import ForegroundDatabase

# set up arguments for command line running
parser = argparse.ArgumentParser(
    description='Execute automatic Brightway LCIA'
)
parser.add_argument('--data',
                    help='Path to data directory.')
parser.add_argument('--bwconfig',
                    help='Name of local Brightway config file.')
parser.add_argument('--caseconfig',
                    help='Name of local case study config file.')

# Set up logger
logging.basicConfig(
    filename=os.path.join(
        parser.parse_args().data,
        f'autobw-{time.time()}.log'
    ),
    level=logging.INFO
)

# read in config (YAML) file with error handling; get variable groups
bwconfig_filename = os.path.join(
    parser.parse_args().data,
    parser.parse_args().bwconfig
)
caseconfig_filename = os.path.join(
    parser.parse_args().data,
    parser.parse_args().caseconfig
)

try:
    with open(bwconfig_filename, 'r') as f:
        bwconfig = yaml.load(f, Loader=yaml.FullLoader)
        flags = bwconfig.get('flags', {})
        fileIO = bwconfig.get('fileIO', {})
except IOError as err:
    logging.error(
        msg=f'Could not open {bwconfig_filename} for configuration.'
    )
    exit(1)

try:
    with open(caseconfig_filename, 'r') as f:
        caseconfig = yaml.load(f, Loader=yaml.FullLoader)
        foreground = caseconfig.get('foreground_db', {})
        calcs = caseconfig.get('calculations', {})
        proj_params = caseconfig.get('project_parameters', {})
except IOError as err:
    logging.error(
        msg=f'Could not open {caseconfig_filename} for configuration.'
    )
    exit(1)

# Project setup
prj = proj_params.get('name')

# If the project already exists, throw an error.
if flags.get('create_new_project') and prj in list(bw.projects):
    logging.error(msg=f'Project {prj} already exists.')
    exit(1)

# Instantiate the new project
bw.projects.set_current(prj)

# Log current project name and directory
logging.info(msg=f'Current project name is {bw.projects.current}')
logging.info(msg=f'Current project directory is {bw.projects.dir}')

# Default setup step for biosphere database
# This will only execute if the project is brand new
bw.bw2setup()

# Previously imported database check
bw_db_list = [key for key, value in bw.databases.items()]
logging.info(msg=f'{prj} databases are {bw_db_list}')

# Import and format any databases that are missing
db_names = proj_params.get('include_databases')
if len(db_names) > 0:
    missing = []
    # If db_names contains database names, check that each of these is
    # imported before proceeding
    for i in range(len(db_names)):
        if db_names[i] not in bw_db_list:
            missing.append(db_names[i])

    if len(missing) > 0:
        logging.error(msg=f'{missing} must be imported before proceeding')
        exit(1)

else:
    logging.info(msg=f'No databases specified: using {bw_db_list}')

# Get path to file with custom foreground database
fg_db_file = os.path.join(
    fileIO.get('data_directory'),
    foreground.get('fg_db_import')
)

# Find and delete any foreground databases with the same name as the one being
# created.
if foreground.get('name') in bw_db_list:
    logging.warning(
        msg=f"Deleting existing foreground database {foreground.get('name')}"
    )
    del bw.databases[foreground.get('name')]

    bw_db_list = [key for key, value in bw.databases.items()]
    logging.info(msg=f'{prj} databases are {bw_db_list}')

# Initialize blank foreground database for writing
new_db = bw.Database(foreground.get('name'))

# Assemble database for import, validate, and optionally save a copy
custom_db = ForegroundDatabase(
    db_name = foreground.get('name'),
    import_template = fg_db_file,
    generate_keys = foreground.get('generate_keys'),
    logging = logging,
    project = prj,
    save_imported_db = flags.get('save_imported_db')
)




# Methods setup


# Calculation setup


# Execute LCIA and gather raw results


# Format results and generate diagnostic plots












