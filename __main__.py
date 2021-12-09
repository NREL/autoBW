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
parser.add_argument('--db_name',
                    help='Names of databases for import. Only use double '
                         'quotes if names contain spaces.',
                    nargs='+',
                    action='append')
parser.add_argument('--db_loc',
                    help='Paths to local databases for import. Only use double'
                         ' quotes around path if it contains spaces.',
                    nargs='+',
                    action='append')
parser.add_argument('--db_format',
                    help='Methods to use for database import. Only use double '
                         'quotes if methods contain spaces.',
                    nargs='+',
                    action='append')

# Error checking for database import: length of databases provided must be
# equal to the length of database formats provided.
db_name = parser.parse_args().db_name[0]
db_loc = parser.parse_args().db_loc[0]
db_format = parser.parse_args().db_format[0]

if not len(db_name) == len(db_loc) == len(db_format):
    logging.warning(msg=f'{len(db_name)} database names; '
                        f'{len(db_loc)} database locations; '
                        f'{len(db_format)} database formats')
    logging.error(msg='Database names, locations, formats must be in lists of '
                      'equal lengths.')
    exit(1)

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
proj = project.get('name')

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
bw.bw2setup()

# Imported database setup

# @TODO Refactor the database import code to match the new command line args
bw_db_list = [key for key, value in bw.databases.items()]
logging.info(msg=f'{proj} databases are {bw_db_list} before import')

# @NOTE Database name - do we need? Where does Brightway get it from?

# do database importing and formatting, if there are databases to import
if len(db_name) > 0:
    # If db_create contains elements, then these are datbases that
    # don't exist and must be imported and postprocessed.
    for i in range(len(db_name)):
        if db_name[i] not in bw_db_list:
            logging.info(msg=f'Importing {db_name[i]} as {db_format[i]} from {db_loc[i]}')
            # @TODO Add database format parameter for selecting the import method?
            # Currently available: ecospold1, ecospold1-lcia, ecospold2, excel, exiobase, simapro CSV, and simapro CSV-lcia
            try:
                _imported_db = bw.SingleOutputEcospold2Importer(
                    # @TODO do string formatting to raw
                    db_loc[i],
                    db_name[i]
                )
                if db_loc[i] in bw.databases:
                    logging.info(msg=f'Successfully imported {db_name[i]}')
                    logging.info(msg=f'Postprocessing {db_name[i]}')
                    _imported_db.apply_strategies()
                    _imported_db.statistics()
                    _imported_db.write_database()
                else:
                    logging.warning(msg=f'Failed to import {db_name[i]}')
            except AssertionError as err:
                logging.error(msg=f'Brightway: {err}')


    bw_db_list = [key for key, value in bw.databases]

    logging.info(msg=f'{proj} databases are {bw_db_list} after import')
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












