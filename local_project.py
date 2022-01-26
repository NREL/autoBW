"""
Created on January 26 2022.

@author: rhanes
"""
import os
import yaml

import brightway2 as bw

from foreground_database import ForegroundDatabase

class LocalProject:
    """
    Create and set up a local Brightway project.
    """

    def __init__(self, parser, logging):
        """
        Initialize the project.

        Parameters
        ----------
        parser

        logging


        Returns
        -------
        """
        # read in config (YAML) file with error handling; get variable groups
        bwconfig_filename = os.path.join(parser.parse_args().data, parser.parse_args().bwconfig)
        caseconfig_filename = os.path.join(
            parser.parse_args().data, parser.parse_args().caseconfig
        )

        try:
            with open(bwconfig_filename, "r") as f:
                bwconfig = yaml.load(f, Loader=yaml.FullLoader)
                flags = bwconfig.get("flags", {})
        except IOError as err:
            logging.error(
                msg=f"LocalProject: {bwconfig_filename} {err}"
            )
            exit(1)

        try:
            with open(caseconfig_filename, "r") as f:
                caseconfig = yaml.load(f, Loader=yaml.FullLoader)
                foreground = caseconfig.get("foreground_db", {})
                #calcs = caseconfig.get("calculations", {})
                proj_params = caseconfig.get("project_parameters", {})
        except IOError as err:
            logging.error(
                msg=f"LocalProject: {caseconfig_filename} {err}"
            )
            exit(1)

        # Project setup
        prj = proj_params.get("name")

        # If the project already exists, throw an error.
        if flags.get("create_new_project") and prj in list(bw.projects):
            logging.error(msg=f"__main__.py: Project {prj} already exists.")
            exit(1)

        # Instantiate the new project
        bw.projects.set_current(prj)

        # Log current project name and directory
        logging.info(msg=f"__main__.py: Current project name is {bw.projects.current}")
        logging.info(msg=f"__main__.py: Current project directory is {bw.projects.dir}")

        # Default setup step for biosphere database
        # This will only execute if the project is brand new
        bw.bw2setup()

        # Previously imported database check
        bw_db_list = [key for key, value in bw.databases.items()]
        logging.info(msg=f"__main__.py: {prj} databases are {bw_db_list}")

        # Import and format any databases that are missing
        db_names = proj_params.get("include_databases")
        if db_names:
            missing = []
            # If db_names contains database names, check that each of these is
            # imported before proceeding
            for i in range(len(db_names)):
                if db_names[i] not in bw_db_list:
                    missing.append(db_names[i])

            if missing:
                logging.error(msg=f"__main__.py: {missing} must be imported before proceeding")
                exit(1)

        else:
            logging.info(msg=f"__main__.py: No databases specified: using {bw_db_list}")

        # Find and delete any foreground databases with the same name as the one being
        # created.
        if foreground.get("name") in bw_db_list:
            logging.warning(
                msg=f"__main__.py: Deleting existing foreground database {foreground.get('name')}"
            )
            del bw.databases[foreground.get("name")]

            bw_db_list = [key for key, value in bw.databases.items()]
            logging.info(msg=f"{prj} databases are {bw_db_list}")

        # Initialize blank foreground database for writing
        bw.Database(foreground.get("name"))

        # Assemble database for import, validate, and optionally save a copy
        ForegroundDatabase(
            logging=logging,
            prj_dict=proj_params,
            fg_dict=foreground
        )
