import argparse
import os
import yaml
import logging
import uuid

import brightway2 as bw
import pandas as pd

import pdb

# set up arguments for command line running
parser = argparse.ArgumentParser(description='Execute automatic Brightway LCIA')
parser.add_argument('--data', help='Path to data directory.')
parser.add_argument('--config', help='Name of local config file.')

# Set up logger
# @TODO Generate unique log file names for each run?
logging.basicConfig(
    filename=os.path.join(
        parser.parse_args().data,
        'autobw.log'
    ),
    level=logging.INFO
)

# read in config (YAML) file with error handling; get variable groups
config_yaml_filename = os.path.join(parser.parse_args().data,
                                    parser.parse_args().config)
try:
    with open(config_yaml_filename, 'r') as f:
        config = yaml.load(f, Loader=yaml.FullLoader)
        flags = config.get('flags', {})
        fileIO = config.get('fileIO', {})
        proj_params = config.get('project_parameters', {})
        foreground = config.get('foreground_db', {})
        calcs = config.get('calculations', {})
except IOError as err:
    logging.error(msg=f'Could not open {config_yaml_filename} for configuration.')
    exit(1)

# Project setup
proj = proj_params.get('name')

# if a new project is being created, instantiate it

# If the project already exists, throw an error.
if flags.get('create_new_project') and proj in list(bw.projects):
    logging.error(msg=f'Project {proj} already exists.')
    exit(1)

# Project setup
bw.projects.set_current(proj)

# Log current project name and directory
logging.info(msg=f'Current project name is {bw.projects.current}')
logging.info(msg=f'Current project directory is {bw.projects.dir}')

# Default setup step for biosphere database
# This will only execute if the project is brand new
bw.bw2setup()

# Imported database check

bw_db_list = [key for key, value in bw.databases.items()]
logging.info(msg=f'{proj} databases are {bw_db_list}')

# do database importing and formatting, if there are databases to import
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


# Custom foreground database setup

# Create custom database if it does not exist

# Import edit information from template
try:
    _create_activities = pd.read_excel(
        io=os.path.join(fileIO.get('data_directory'),fileIO.get('db_edits')),
        sheet_name='Create Activities'
    )
except ValueError:
    logging.warning(msg='Create Activities sheet not found')
    _create_activities = pd.DataFrame()

try:
    _delete_exchanges = pd.read_excel(
        io=os.path.join(fileIO.get('data_directory'),fileIO.get('db_edits')),
        sheet_name='Delete Exchanges'
    )
except ValueError:
    logging.warning(msg='Delete Exchanges sheet not found')
    _delete_exchanges = pd.DataFrame()

try:
    _add_exchanges = pd.read_excel(
        io=os.path.join(fileIO.get('data_directory'),fileIO.get('db_edits')),
        sheet_name='Add Exchanges'
    )
except ValueError:
    logging.warning(msg='Add Exchanges sheet not found')
    _add_exchanges = pd.DataFrame()

# Fill in new

# Create new activities if needed

if not _create_activities.empty:
    # Error handling: If the activity already exists, throw a warning and move on
    # only search the database specified in the import template
    _checkdb = _create_activities.database.unique().tolist()
    _duplicate_act = [
        act
        for db in _checkdb
        for act in bw.Database(db)
        if act['name'] in _create_activities.activity_name.tolist()
           and act['type'] != 'emission'
    ]

    if len(_duplicate_act) > 0:
        logging.warning(msg=f'Duplicate activities found: {_duplicate_act}')
        # @TODO Remove the duplicates from the _create_activities data frame

    # Generate unique activity code with uuid
    _create_activities.activity_code = [
        uuid.uuid4().hex
        for _ in range(len(_create_activities.activity_name))
    ]

    # Log the activities to be created
    logging.info(msg=f'Creating activities: {_create_activities.activity_name},'
                     f'{_create_activities.activity_code}')

# Create the import data dictionary structure, populate with activity-level
# information only. The exchange information will be added in the next step
# @TODO Is it possible to vectorize to avoid loop?
_import = {}
for i in _create_activities.index:
    _import[(_create_activities.database[i],
             _create_activities.activity_code[i])] = \
        {
            "name": _create_activities.activity_name[i],
            "unit": _create_activities.reference_product_unit[i],
            "location": _create_activities.location[i],
            "exchanges": []
        }


if not _delete_exchanges.empty:
    pass
    # Delete exchanges from existing activities
        # If the activity does not exist
        # If the exchange in the activity does not exist

pdb.set_trace()
# Add exchanges to existing activities
if not _add_exchanges.empty:

    # Fill in the activity_code column with values stored in _create_activities
    _add_exchanges = _add_exchanges.merge(_create_activities,on=['database','activity_name','location'])
    # @TODO Check if the activity does not exist in the specified database
    #for i in _add_exchanges[['database','activity_name']].drop_duplicates().index:
    #    _add_exchanges.loc[i]

    # @TODO Check if the exchange already exists for the specified activity

    # If the activity exists and the exchange does not, append the exchange
    # data to the "exchanges" list of dicts under the relevant activity
    for i in _add_exchanges.index:
        _import[
            (_add_exchanges.database[i],
             _add_exchanges.activity_code[i])
        ]['exchanges'].append(
            {
                "amount": _add_exchanges.amount[i],
                "input": _add_exchanges.exchange[i],
                "type": 'Technosphere'
            }
        )

# @TODO: Log "after" status and record changes made


# Methods setup


# Calculation setup


# Execute LCIA and gather raw results


# Format results and generate diagnostic plots












