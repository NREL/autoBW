import argparse
import os
import yaml
import logging
import uuid
import time

import brightway2 as bw
from bw2data.validate import db_validator
import pandas as pd

import pdb

# set up arguments for command line running
parser = argparse.ArgumentParser(description='Execute automatic Brightway LCIA')
parser.add_argument('--data', help='Path to data directory.')
parser.add_argument('--bwconfig', help='Name of local Brightway config file.')
parser.add_argument('--caseconfig', help='Name of local case study config file.')

# Set up logger
logging.basicConfig(
    filename=os.path.join(
        parser.parse_args().data,
        f'autobw-{time.time()}.log'
    ),
    level=logging.INFO
)

# read in config (YAML) file with error handling; get variable groups
bwconfig_filename = os.path.join(parser.parse_args().data,
                                    parser.parse_args().bwconfig)
caseconfig_filename = os.path.join(parser.parse_args().data,
                                   parser.parse_args().caseconfig)
try:
    with open(bwconfig_filename, 'r') as f:
        bwconfig = yaml.load(f, Loader=yaml.FullLoader)
        flags = bwconfig.get('flags', {})
        fileIO = bwconfig.get('fileIO', {})
except IOError as err:
    logging.error(msg=f'Could not open {bwconfig_filename} for configuration.')
    exit(1)

try:
    with open(caseconfig_filename, 'r') as f:
        caseconfig = yaml.load(f, Loader=yaml.FullLoader)
        foreground = caseconfig.get('foreground_db', {})
        calcs = caseconfig.get('calculations', {})
        proj_params = caseconfig.get('project_parameters', {})
except IOError as err:
    logging.error(msg=f'Could not open {caseconfig_filename} for configuration.')
    exit(1)

# Project setup
prj = proj_params.get('name')

# if a new project is being created, instantiate it

# If the project already exists, throw an error.
if flags.get('create_new_project') and prj in list(bw.projects):
    logging.error(msg=f'Project {prj} already exists.')
    exit(1)

# Project setup
bw.projects.set_current(prj)

# Log current project name and directory
logging.info(msg=f'Current project name is {bw.projects.current}')
logging.info(msg=f'Current project directory is {bw.projects.dir}')

# Default setup step for biosphere database
# This will only execute if the project is brand new
bw.bw2setup()

# Imported database check

bw_db_list = [key for key, value in bw.databases.items()]
logging.info(msg=f'{prj} databases are {bw_db_list}')

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


# Create foreground database from imported Excel data

# Check list of existing databases against the databases to be created
# If there is overlap, then perform checks for duplicate activities/exchanges

# Import edit information from template
fg_db_file = os.path.join(
    fileIO.get('data_directory'),
    foreground.get('fg_db_import')
)

try:
    _create_activities = pd.read_excel(
        io=fg_db_file,
        sheet_name='Create Activities'
    )
except ValueError:
    logging.warning(msg='Create Activities sheet not found')
    _create_activities = pd.DataFrame()

try:
    _delete_exchanges = pd.read_excel(
        io=fg_db_file,
        sheet_name='Delete Exchanges'
    )
except ValueError:
    logging.warning(msg='Delete Exchanges sheet not found')
    _delete_exchanges = pd.DataFrame()

try:
    _add_exchanges = pd.read_excel(
        io=fg_db_file,
        sheet_name='Add Exchanges'
    )
except ValueError:
    logging.warning(msg='Add Exchanges sheet not found')
    _add_exchanges = pd.DataFrame()


# Add activities to foreground database(s)
if not _create_activities.empty:
    # Find and delete any foreground databases with the same name as in the
    # import template.
    # Get list of foreground databases to be created.
    _fg_db = _create_activities.database.unique().tolist()

    # Identify foreground databases that already exist.
    for old in _fg_db:
        # Remove the existing (old) foreground databases
        if old in bw_db_list:
            logging.warning(msg=f"Deleting existing foreground database {old}")
            del bw.databases[old]

    bw_db_list = [key for key, value in bw.databases.items()]
    logging.info(msg=f'{prj} databases are {bw_db_list}')

    # Generate unique activity code with uuid
    _create_activities.activity_code = [
        uuid.uuid4().hex
        for _ in range(len(_create_activities.activity_name))
    ]

    # Log the activities to be created and their newly assigned codes
    logging.info(msg=f'Creating activities: {_create_activities.activity_name.values.tolist()}')
    logging.info(msg=f'Creating activity codes: {_create_activities.activity_code.values.tolist()}')
else:
    logging.info(msg='No activities to create')

# Create the import data dictionary structure, populate with activity-level
# information only. The exchange information will be added in the next step.
# @TODO Is it possible to vectorize to avoid loop?
# @TODO Will this work if _create_activities is empty? Does it matter?
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


# Add exchanges to existing activities
if not _add_exchanges.empty:
    # If activity_names listed under Add Exchanges are not also listed under
    # Create Activities, throw an error
    _missing_acts = [
        _
        for _ in _add_exchanges.activity_name.unique()
        if _ not in _create_activities.activity_name.unique()
    ]
    if _missing_acts:
        logging.error(msg=f'Add Exchanges: Error in activity_names {_missing_acts}')
        exit(1)

    # Fill in the code column with values stored in _create_activities
    _add_exchanges = _add_exchanges.merge(_create_activities,on=['database','activity_name','location'])
    pdb.set_trace()
    # Append the exchange data to the "exchanges" list of dicts under the
    # relevant activity
    for i in _add_exchanges.index:
        _import[
            (_add_exchanges.database[i],
             _add_exchanges.activity_code[i])
        ]['exchanges'].append(
            {
                "amount": _add_exchanges.amount[i],
                "input": (_add_exchanges.exchange[i], _add_exchanges.exchange_code[i]),
                "type": 'Technosphere'
            }
        )
pdb.set_trace()
if not _delete_exchanges.empty:
    pass
    # Delete exchanges from existing activities
        # If the activity does not exist
        # If the exchange in the activity does not exist

# Validate data before linking or saving
logging.debug(msg=f'Foreground database to import: {_import}')
db_validator(_import)

# @TODO: Log "after" status and record changes made


# Methods setup


# Calculation setup


# Execute LCIA and gather raw results


# Format results and generate diagnostic plots












