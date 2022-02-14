"""
Created on January 26 2022.

@author: rhanes
"""
import sys
import os
import yaml

import brightway2 as bw

from foreground_database import ForegroundDatabase


class LocalProject:
    """Create and set up a local Brightway project."""

    def __init__(self, parser, logging):
        """
        Initialize the project.

        Handles file IO, former database cleanup, and new database creation.

        Parameters
        ----------
        parser

        logging

        """
        # read in config (YAML) file with error handling; get variable groups
        bwconfig_filename = os.path.join(
            parser.parse_args().data, parser.parse_args().bwconfig
        )
        caseconfig_filename = os.path.join(
            parser.parse_args().data, parser.parse_args().caseconfig
        )

        try:
            with open(bwconfig_filename, "r", encoding="utf-8") as _f:
                _bwconfig = yaml.load(_f, Loader=yaml.FullLoader)
                _flags = _bwconfig.get("flags", {})
        except IOError as err:
            logging.error(msg=f"LocalProject: {bwconfig_filename} {err}")
            sys.exit(1)

        try:
            with open(caseconfig_filename, "r", encoding="utf-8") as _f:
                _caseconfig = yaml.load(_f, Loader=yaml.FullLoader)
                foreground = _caseconfig.get("foreground_db", {})
                # calcs = caseconfig.get("calculations", {})
                proj_params = _caseconfig.get("project_parameters", {})
        except IOError as err:
            logging.error(msg=f"LocalProject: {caseconfig_filename} {err}")
            sys.exit(1)

        # If the project already exists, throw an error.
        if _flags.get("create_new_project") and proj_params.get("name") in [
            i[0] for i in bw.projects.report()
        ]:
            logging.error(
                msg=f"LocalProject: Project {proj_params.get('name')} already exists."
            )
            sys.exit(1)

        # Instantiate the new project
        bw.projects.set_current(proj_params.get("name"))

        # Log current project name and directory
        logging.info(msg=f"LocalProject: Current project name is {bw.projects.current}")
        logging.info(
            msg=f"LocalProject: Current project directory is {bw.projects.dir}"
        )

        # Default setup step for biosphere database
        # This will only execute if the project is brand new
        bw.bw2setup()

        # Previously imported database check
        _bw_db_list = [key for key, value in bw.databases.items()]
        logging.info(
            msg=f"LocalProject: {proj_params.get('name')} databases are {_bw_db_list}"
        )

        # Import and format any databases that are missing
        if proj_params.get("include_databases"):
            _missing = []
            # If databases to include have been specified, check that each of these is imported
            # before proceeding
            for _i in range(len(proj_params.get("include_databases"))):
                if proj_params.get("include_databases")[_i] not in _bw_db_list:
                    _missing.append(proj_params.get("include_databases")[_i])

            if _missing:
                logging.error(
                    msg=f"LocalProject: {_missing} must be imported before proceeding"
                )
                sys.exit(1)

        else:
            logging.info(
                msg=f"LocalProject: No databases specified: using {_bw_db_list}"
            )

        # Find and delete any foreground databases with the same name as the one being
        # created.
        if foreground.get("name") in _bw_db_list:
            logging.warning(
                msg=f"LocalProject: Deleting existing foreground database {foreground.get('name')}"
            )
            del bw.databases[foreground.get("name")]

        # Create a new blank database to hold the one being created
        bw.Database(foreground.get("name")).write(data={})

        # Log the updated list of databases in this Brightway project
        _bw_db_list = [key for key, value in bw.databases.items()]
        logging.info(
            msg=f"LocalProject: {proj_params.get('name')} databases are {_bw_db_list}"
        )

        # Assemble database for import, validate the database, and optionally save a copy for
        # later use
        ForegroundDatabase(
            logging=logging,
            prj_dict=proj_params,
            fg_dict=foreground,
            file_io=_bwconfig.get("fileIO"),
        )

    @staticmethod
    def calculations():
        """Perform standard LCIA calculations."""
        return None

    @staticmethod
    def visualization():
        """Create standard impact visualizations."""
        return None
