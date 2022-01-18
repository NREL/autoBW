import argparse
import os
import yaml
import logging
import uuid
import time
import json

import brightway2 as bw
from bw2data.validate import db_validator
import pandas as pd

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

# @TODO: Separate out activities to be created by database name. Create
# multiple custom databases to import if necessary.

# Add activities to foreground database(s)
if not _create_activities.empty:
    # Find and delete any foreground databases with the same name as in the
    # import template.
    # Get list of foreground databases to be created.
    _fg_db = _create_activities.activity_database.unique().tolist()

    # Identify foreground databases that already exist.
    for old in _fg_db:
        # Remove the existing (old) foreground databases
        if old in bw_db_list:
            logging.warning(msg=f"Deleting existing foreground database {old}")
            del bw.databases[old]

    bw_db_list = [key for key, value in bw.databases.items()]
    logging.info(msg=f'{prj} databases are {bw_db_list}')

    # Generate unique activity code with uuid.
    # Because all activities in this DataFrame are new, all of them need newly
    # created codes.
    _create_activities.code = pd.Series(
        data=[
            uuid.uuid4().hex
            for _ in range(len(_create_activities.activity))
        ],
        index=_create_activities.index
    )
    # Log the activities to be created and their newly assigned codes
    logging.info(msg=f'Creating activities: {_create_activities.activity.values.tolist()}')
    logging.info(msg=f'Creating activity codes: {_create_activities.code.values.tolist()}')
else:
    _create_activities = pd.DataFrame()
    logging.info(msg='No activities to create')

# Create the import data dictionary structure, populate with activity-level
# information only. The exchange information will be added in the next step.
# @TODO Is it possible to vectorize to avoid loop?
_import = {}
for i in _create_activities.index:
    _import[(_create_activities.activity_database[i],
             _create_activities.code[i])] = \
        {
            "name": _create_activities.activity[i],
            "unit": _create_activities.reference_product_unit[i],
            "location": _create_activities.activity_location[i],
            "exchanges": []
        }


# Add exchanges to existing activities
if not _add_exchanges.empty:
    # If activities listed under Add Exchanges are not also listed under
    # Create Activities, throw an error
    _missing_acts = [
        _
        for _ in _add_exchanges.activity.unique()
        if _ not in _create_activities.activity.unique()
    ]
    if _missing_acts:
        logging.error(msg=f'Add Exchanges: Missing new activities {_missing_acts}')
        exit(1)

    # The newly created exchanges also need codes. For exchanges that exist
    # in the database being created and imported, these codes were already
    # created and are stored in the _create_activities DataFrame. For exchanges
    # that exist in another database, like ecoinvent, the user must fill in
    # these values before beginning the import process.
    # In this step, codes stored in _create_activities are assigned to the
    # corresponding exchange. Codes already present in _add_exchanges are NOT
    # overwritten.
    _add_exchanges.activity_code = _add_exchanges.merge(
        _create_activities,
        on=['activity_database','activity','activity_location'],
        how='right'
    ).code

    # Filling the exchange codes is done in two steps. First the merge gets us
    # previously created codes for new exchanges in the custom database. Then,
    # the codes from the merge are combined with the existing exchange_code
    # column, which may have codes from other databases.
    _new_exchange_codes = _add_exchanges.merge(
        _create_activities,
        left_on=['exchange_database','exchange','activity_location'],
        right_on=['activity_database','reference_product','activity_location'],
        how='left'
    ).code
    _add_exchanges.exchange_code = _new_exchange_codes.fillna('') + \
                                   _add_exchanges.exchange_code.fillna('')

    # Append the exchange data to the "exchanges" list of dicts under the
    # relevant activity.
    for i in _add_exchanges.index:
        _import[
            (_add_exchanges.activity_database[i],
             _add_exchanges.activity_code[i])
        ]['exchanges'].append(
            {
                "amount": _add_exchanges.amount[i],
                "input": (_add_exchanges.exchange_database[i],
                          _add_exchanges.exchange_code[i]),
                "unit": _add_exchanges.unit[i],
                "type": 'Technosphere'
            }
        )

if not _delete_exchanges.empty:
    pass
    # Delete exchanges from existing activities
        # If the activity does not exist
        # If the exchange in the activity does not exist

logging.debug(msg=f'Foreground database created')

# Save a copy of the custom database for future reference
if flags.get('save_imported_db'):
    with open('imported_db', 'w') as db_dump:
        json.dump(_import, db_dump)
        db_dump.close()

# Use built-in Brightway method to validate the custom database before linking
validate = db_validator(_import)
if validate is not dict:
    logging.error(msg=f'Database to import is not valid: {validate}')
    exit(1)

db = bw.Database('new_database')

db.write(_import)

db.apply_strategies()

db.match_database("biosphere3", fields=('name', 'unit', 'location', 'categories'))

db.match_database("ecoinvent3.7.1 cut-off", fields=('name', 'unit', 'location', 'reference product'))

[i for i in db.unlinked]

# @TODO: Log "after" status and record changes made


# Methods setup


# Calculation setup


# Execute LCIA and gather raw results


# Format results and generate diagnostic plots












