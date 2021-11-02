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

# Project setup
_name = project.get('name')

# if a new project is being created, instantiate it
if flags.get('create_new_project') and _name in list(bw.projects):
    # If the project already exists, throw an error.
    print(f'Project {_name} already exists.')
    exit(1)

# Project setup
bw.projects.set_current(project.get('name'))

# @TODO Log current project name and directory

# Default setup step for biosphere database
bw.bw2setup()

# Imported database setup
# Get the dictionary of database names:locations
_databases = project.get('database_sources')

# @TODO Log list of databases in the current project

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
    for _name, _loc in _db_create.items():
        # @TODO Add database format parameter for selecting the import method
        # Excel importer, FORWAST, others?
        _imported_db = bw.SingleOutputEcospold2Importer(
            _loc,
            _name
        )
        _imported_db.apply_strategies()
        _imported_db.statistics()
        _imported_db.write_database()

    # @TODO Log "databases updated", list of databases in the current project


# Activity and exchange editing

# @TODO: Log "before" status of database

# Create new activities
    # Generate unique code and store it somewhere

    # If the exact activity already exists, throw a warning and move on

# Delete exchanges from existing activities
    # If the activity does not exist
    # If the exchange in the activity does not exist

# Add exchanges to existing activities
    # If the activity does not exist
    # If the exchange already exists

# @TODO: Log "after" status and record changes made

# Methods setup


# Calculation setup


# Execute LCIA and gather raw results


# Format results and generate diagnostic plots












