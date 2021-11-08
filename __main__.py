import argparse
import os
import yaml
import logging

import brightway2 as bw
import pandas as pd



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
        project = config.get('project', {})
except IOError as err:
    logging.error(msg=f'Could not open {config_yaml_filename} for configuration.')
    exit(1)

# Project setup
_project = project.get('name')

# if a new project is being created, instantiate it

# If the project already exists, throw an error.
if flags.get('create_new_project') and _project in list(bw.projects):
    logging.error(msg=f'Project {_project} already exists.')
    exit(1)

# Project setup
bw.projects.set_current(_project)

# @TODO Log current project name and directory

# Default setup step for biosphere database
bw.bw2setup()

# Imported database setup
# Get the dictionary of database names:locations
_databases = project.get('databases')
_db_list = [key for key,value in bw.databases.items()]
logging.info(msg=f'{_project} databases are {_db_list}')

# trim the database dictionary so it contains only those databases
# that don't already exist
_db_create = {
    db: _databases[db]
    for db in [key for key, value in _databases.items()]
    if db not in [key for key, value in bw.databases.items()]
}

# if all of the databases to import already exist, then
# _db_create will be an empty dictionary. In this case, take no action.
if len(_db_create) > 0:
    # If _db_create contains elements, then these are datbases that
    # don't exist and must be imported and postprocessed.
    for _db, _loc in _db_create.items():
        logging.info(msg=f'Importing {_db} from {_loc}')
        # @TODO Add database format parameter for selecting the import method
        # Excel importer, FORWAST, others?
        try:
            _imported_db = bw.SingleOutputEcospold2Importer(
                os.path.abspath(_loc),
                _db
            )
            if _db in bw.databases:
                logging.info(msg=f'Successfully imported {_db}')
                logging.info(msg=f'Postprocessing {_db}')
                _imported_db.apply_strategies()
                _imported_db.statistics()
                _imported_db.write_database()
            else:
                logging.warning(msg=f'Failed to import {_db}')
        except AssertionError:
            # @todo log the exact assertion error
            logging.error(msg=f'Brightway AssertionError')


    _db_list = [key for key,value in bw.databases]

    logging.info(msg=f'{_project} databases updated to {_db_list}')
else:
    logging.info(msg='No new databases to add')


# Activity and exchange editing

# @TODO: Log "before" status of database?

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


# Create new activities if needed

if not _create_activities.empty:
    # Error handling: If the activity already exists, throw a warning and move on
    # @TODO List comprehension method for searching all databases?
    _checkdb = bw.Database([key for key,value in bw.databases.items()][0])
    _duplicate_act = [
        act
        for act in _checkdb
        if act["name"] in _create_activities.Name.tolist()
           and act['type'] != 'emission'
    ]
    
    if len(_duplicate_act) > 0:
        logging.warning(msg=f'Duplicate activities found: {_duplicate_act}')
        # @TODO Remove the duplicates from the _create_activities data frame


    # After removing duplicate activities,
    # Step 1: Generate unique code and store it in _create_activities.Code
    _create_activities.Code = _create_activities.Name.replace(' ', '')

    #logging.info(msg=f'Creating activities: {_create_activities}')


if not _delete_exchanges.empty:
    pass
    # Delete exchanges from existing activities
        # If the activity does not exist
        # If the exchange in the activity does not exist

if not _add_exchanges.empty:
    pass
    # Add exchanges to existing activities
        # If the activity does not exist
        # If the exchange already exists

# @TODO: Log "after" status and record changes made


# Methods setup


# Calculation setup


# Execute LCIA and gather raw results


# Format results and generate diagnostic plots












