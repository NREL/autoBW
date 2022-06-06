"""
Created on January 20 2022.

@author: rhanes
"""
import sys
import os
import uuid
import pickle

import pandas as pd
import numpy as np
import brightway2 as bw

import bw2data
from bw2data.validate import db_validator
from peewee import DoesNotExist

from data_manager import CreateActivities, AddExchanges, CopyActivities, DeleteExchanges


class ForegroundDatabase:
    """
    Create foreground database from imported Excel data.

    Methods in this class use user-provided data in Excel format to create a foreground
    life cycle inventory database. The foreground database is linked to local databases
    in Brightway and accessible for calculations via the Brightway activity browser.
    Activities and exchanges in the database can be created from scratch or copied to
    the foreground database from ecoinvent and then edited.
    """

    def __init__(self, logging, prj_dict, fg_dict, file_io):
        """
        Assemble the foreground database as a dictionary.

        Parameters
        ----------
        logging
            logger object for writing status messages to file

        prj_dict : dict
            Dictionary of project-level parameters.

            Keys:
                name : str
                    Name of local Brightway project.
                include_databases : list
                    List of existing LCI databases that must be in the local Brightway project.

        fg_dict : dict
            Dictionary of database-level parameters.

            Keys:
                name : str
                    Name of foreground database being created.
                fg_db_import : path
                    Path to import file with database information.
                generate_keys : Boolean
                    Whether to generate new activity keys (UUIDs) or use the ones from the import
                    file.
                save_db : Boolean
                    Whether to save a copy of the database in two CSV files and one pickled object.
                link_fg_to : dict
                    Dictionary of existing database names and columns to link on.

        file_io : dict
            Dictionary defining the primary data directory.

            Keys:
                data_directory : path
                    Path to directory containing import file and other data.

        """
        # Initialize empty dictionary to hold the assembled database
        self.custom_db = {}

        # Get the path to the XLSX file with importable database information
        _import_template = fg_dict.get("fg_db_import")

        if not os.path.isfile(_import_template):
            logging.error(msg=f"{_import_template} is not a file")
            sys.exit(1)

        # Table of empty activities to add to the database. Fill in the
        # database columns with foreground database name from the config file.
        self.create_activities_data = (
            CreateActivities(fpath=_import_template)
            .backfill(column="activity_database", value=fg_dict.get("name"))
            .apply(lambda x: x.str.strip() if x.dtype == "object" else x)
        )

        # Table of activities to copy to the foreground database from an
        # existing database
        self.copy_activities_data = CopyActivities(fpath=_import_template).apply(
            lambda x: x.str.strip() if x.dtype == "object" else x
        )

        # Table of exchanges to remove from the database
        self.delete_exchanges_data = (
            DeleteExchanges(fpath=_import_template)
            .backfill(column="activity_database", value=fg_dict.get("name"))
            .apply(lambda x: x.str.strip() if x.dtype == "object" else x)
        )

        # Table of exchanges to add to the database. Fill in the database
        # columns with foreground database name from the config file.
        self.add_exchanges_data = (
            AddExchanges(fpath=_import_template)
            .backfill(
                column=["activity_database", "exchange_database"],
                value=fg_dict.get("name"),
            )
            .apply(lambda x: x.str.strip() if x.dtype == "object" else x)
        )

        self.logging = logging
        self.project = prj_dict.get("name")

        # If activities listed under Add Exchanges are not also listed under
        # Create Activities, throw an error

        _missing_acts = [
            _
            for _ in self.add_exchanges_data.activity.unique()
            if _
            not in np.concatenate(
                (
                    self.create_activities_data.activity.unique(),
                    self.copy_activities_data.activity.unique(),
                )
            )
        ]
        if _missing_acts:
            self.logging.error(
                msg=f"ForegroundDatabase.__init__: Missing created or copied activities"
                f" {_missing_acts}"
            )
            sys.exit(1)

        if fg_dict.get("generate_keys"):
            # Generate unique activity code with uuid.
            # The code is different from the "flows" value, which is a
            # separate UUID.
            # Because all activities in this DataFrame are new, all of them
            # need newly created codes. This can be done manually within the
            # file being imported, or automatically here.
            self.create_activities_data.code = pd.Series(
                data=[
                    uuid.uuid4().hex
                    for _ in range(len(self.create_activities_data.activity))
                ],
                index=self.create_activities_data.index,
            )
            # The newly created exchanges also need codes. For exchanges that exist
            # in the database being created and imported, these codes were already
            # created and are stored in the self.create_activities_data DataFrame. For
            # exchanges that exist in another database, like ecoinvent, the user
            # must fill in these values before beginning the import process.
            # In this step, codes stored in self.create_activities_data are assigned to
            # the corresponding exchange. Codes already present in
            # self.add_exchanges_data are NOT overwritten.
            self.add_exchanges_data.activity_code = self.add_exchanges_data.merge(
                self.create_activities_data,
                on=["activity_database", "activity", "activity_location"],
                how="outer",
            ).code
            # Filling the exchange codes is done in two steps. First the merge gets
            # us previously created codes for new exchanges in the foreground database.
            # Then, the codes from the merge are combined with the existing
            # exchange_code column, which may have codes from other databases.
            _new_exchange_codes = self.add_exchanges_data.merge(
                self.create_activities_data,
                left_on=["exchange_database", "exchange", "activity_location"],
                right_on=[
                    "activity_database",
                    "reference_product",
                    "activity_location",
                ],
                how="left",
            ).code

            self.add_exchanges_data.exchange_code = _new_exchange_codes.fillna(
                ""
            ).astype(str) + self.add_exchanges_data.exchange_code.astype(str).fillna("")

        # @TODO Add error handling: If any activities in create_activities_data or
        # add_exchanges_data don't have non-NAN codes at this point, throw an error
        # and stop

        # Log the activities to be created and their newly assigned codes
        self.logging.info(
            msg=f"ForegroundDatabase.__init__: Creating activities: "
            f"{self.create_activities_data.activity.values.tolist()}"
        )
        self.logging.info(
            msg=f"ForegroundDatabase.__init__: Adding activity codes: "
            f"{self.create_activities_data.code.values.tolist()}"
        )

        # Create the import data dictionary structure and populate with
        # information on newly created activities only. The exchange
        # information will be added in the next step.
        for i in self.create_activities_data.index:
            self.custom_db[
                (
                    self.create_activities_data.activity_database[i],
                    self.create_activities_data.code[i],
                )
            ] = {
                "name": self.create_activities_data.activity[i],
                "unit": self.create_activities_data.reference_product_unit[i],
                "reference product": self.create_activities_data.reference_product[i],
                "production amount": self.create_activities_data.reference_product_amount[
                    i
                ],
                "location": self.create_activities_data.activity_location[i],
                "exchanges": [],
            }

        # Copy activities from another database to the foreground database
        self.copy_activities(to_db=fg_dict.get("name"))

        # Delete exchanges from activities in the foreground database
        self.delete_exchanges()

        # Add exchanges from foreground database and existing databases
        self.add_exchanges()

        self.logging.info(msg="ForegroundDatabase.__init__: Custom database assembled")

        self.logging.info(
            msg="ForegroundDatabase.__init__: Validating foreground database"
        )

        self.validate()

        # Save a copy of the foreground database for future reference
        if fg_dict.get("save_db", True):
            with open(
                os.path.join(file_io.get("data_directory"), "imported_db.obj"), "wb"
            ) as db_dump:
                pickle.dump(self.custom_db, db_dump)
                db_dump.close()
            self.add_exchanges_data.to_csv(
                os.path.join(file_io.get("data_directory"), "add_exchanges_data.csv"),
                index=False,
            )
            self.create_activities_data.to_csv(
                os.path.join(
                    file_io.get("data_directory"), "create_activities_data.csv"
                ),
                index=False,
            )

        # Write the foreground database so it's usable by Brightway
        try:
            bw.Database(fg_dict.get("name")).write(self.custom_db)
        except KeyError as _e:
            self.logging.warning(
                msg=f"ForegroundDatabase.__init__: KeyError on database write: {_e}"
            )

    def copy_activities(self, to_db: str):
        """
        Copy activities and exchanges from an existing database to the foreground database.

        Read from the Copy Activities input dataset to locate activities and
        their exchanges in the local source database (ecoinvent format is assumed),
        format the data for inclusion in the foreground database, and copy the
        data to the foreground database. Any activities that are listed for copying
        but don't exist in the source database are skipped with a warning.

        Parameters
        ----------
        to_db : str
            Name of foreground database receiving the activity copies.
        """
        if self.copy_activities_data.empty:
            self.logging.warning(
                msg="ForegroundDatabase.copy_activities: No activities to copy"
            )
            return None

        _dblen = len(self.custom_db)

        bw.projects.set_current(self.project)

        # Check that all source_databases exist in our project
        for _sdb in self.copy_activities_data.source_database.unique():
            if _sdb not in [key for key, value in bw.databases.items()]:
                self.logging.error(
                    msg=f"ForegroundDatabase.copy_activities: Source database "
                    f"{_sdb} is not in Brightway project {self.project} "
                    f"imported databases"
                )
                sys.exit(1)

            # Use the source_database column to set the Brightway database being
            # searched
            _bwdb = bw.Database(_sdb)

            # Use the activity_code to search the database
            for _row in (
                self.copy_activities_data[["activity_code", "activity"]]
                .loc[self.copy_activities_data.source_database == _sdb]
                .iterrows()
            ):
                try:
                    _act = _bwdb.get(_row[1]["activity_code"])

                    # If the activity exists, use a separate method to
                    # format the ecoinvent information for addition to the
                    # foreground database.
                    _act_to_add = self.ecoinvent_translator(activity=_act, to_db=to_db)

                    self.custom_db[_act_to_add[0]] = _act_to_add[1]

                except DoesNotExist:
                    # Log a warning if the activity_code doesn't exist, but
                    # proceed with processing the rest of the activities to
                    # copy
                    self.logging.warning(
                        msg=f"ForegroundDatabase.copy_activities: {_row[1]['activity']} "
                        f"({_row[1]['activity_code']}) not found in {_sdb}"
                    )
                    _act = None

        self.logging.info(
            msg=f"ForegroundDatabase.copy_activities: {len(self.custom_db) - _dblen} activities "
            f"copied to foreground database"
        )

        return None

    def ecoinvent_translator(
        self, activity: bw2data.backends.peewee.proxies.Activity, to_db: str
    ):
        """
        Translate an ecoinvent activity with exchanges into the foreground database format.

        Parameters
        ----------
        activity : bw2data.backends.peewee.proxies.Activity

        to_db : str
            Name of the database to which the ecoinvent activity is being copied

        Returns
        ---------
        
        A key and value pair with activity and exchange data copied from
        ecoinvent. The pair is formatted for inclusion in the foreground database
        in dictionary (pre-import) format.
        """
        if activity is None:
            return None, None

        # Assemble the tuple that identifies this activity
        _key = (to_db, activity["code"])
        # Create the activity dictionary structure without exchange information
        _value = activity.as_dict()
        _value["exchanges"] = []
        self.logging.info(
            msg=f"ForegroundDatabase.ecoinvent_translator: Copying"
            f" {activity['name']} to database"
        )

        # Append exchanges, if there are any, to the activity dictionary
        if activity.exchanges():
            for i in activity.exchanges():
                _value["exchanges"].append(i.as_dict())

        return _key, _value

    def delete_exchanges(self):
        """Remove exchanges from the foreground database."""
        if not self.custom_db:
            self.logging.warning(
                msg="ForegroundDatabase.delete_exchanges: No exchanges in "
                "foreground database to delete"
            )

        if self.delete_exchanges_data.empty:
            self.logging.warning(
                msg="ForegroundDatabase.delete_exchanges: No delete exchange "
                "information"
            )

        # Within every activity that needs to have exchanges deleted,
        for _line in self.delete_exchanges_data.iterrows():
            try:
                # Use a tuple with the activity_database and activity_code
                # to access the activity and its
                # exchanges in self.custom_db.
                # The exchanges for the activity are a list of dictionaries.
                # Inside each dictionary is a key 'input'.
                # This key value must be matched to the exchange_database and
                # exchange_code from the data frame.
                _del_ind = [
                    _ex["input"]
                    for _ex in self.custom_db[
                        (_line[1].activity_database, _line[1].activity_code.strip())
                    ]["exchanges"]
                ].index((_line[1].exchange_database, _line[1].exchange_code.strip()))

                # Remove it from the activity's list of exchanges
                self.custom_db[
                    (_line[1].activity_database, _line[1].activity_code.strip())
                ]["exchanges"].pop(_del_ind)
                # Record the exchange that was removed
                self.logging.info(
                    msg="ForegroundDatabase.delete_exchanges: Removed "
                    f"{_line[1].exchange} from {_line[1].activity}"
                )
            except ValueError as _e:
                # If the exchanges does not exist, log a warning that includes information on
                # the missing exchange
                self.logging.warning(msg=f"ForegroundDatabase.delete_exchanges: {_e}")
                _del_ind = None

        # If any activity has its own reference product as an input, remove that exchange.
        # This crops up in activities copied directly from ecoinvent.
        # Loop through the activities in self.custom_db

        for _act in self.custom_db.items():
            # Check if the activity key (in the identifying tuple) is in the list
            # of "input" codes from that activity's exchanges list
            _gen = [_ex["input"] == _ex["output"] for _ex in _act[1]["exchanges"]]
            if any(_gen):
                # If so, get the index of the matching code
                _pop_ind = [
                    _ex["input"] == _ex["output"] for _ex in _act[1]["exchanges"]
                ].index(True)
                # Then use .pop to remove it from the activity's exchanges list
                _act[1]["exchanges"].pop(_pop_ind)

    def add_exchanges(self):
        """
        Add exchanges to existing activities in the foreground database.

        Append the exchange data to the "exchanges" list of dicts under the
        relevant activity.
        """
        for i in self.add_exchanges_data.index:
            try:
                self.custom_db[
                    (
                        self.add_exchanges_data.activity_database[i],
                        self.add_exchanges_data.activity_code[i],
                    )
                ]["exchanges"].append(
                    {
                        "amount": self.add_exchanges_data.amount[i],
                        "input": (
                            self.add_exchanges_data.exchange_database[i],
                            self.add_exchanges_data.exchange_code[i],
                        ),
                        "output": (
                            self.add_exchanges_data.activity_database[i],
                            self.add_exchanges_data.activity_code[i],
                        ),
                        "unit": self.add_exchanges_data.unit[i],
                        "type": self.add_exchanges_data.exchange_type[i],
                    }
                )
                self.logging.info(
                    msg=f"ForegroundDatabase.add_exchanges: Added "
                    f"{self.add_exchanges_data.exchange[i]} to"
                    f" {self.add_exchanges_data.activity[i]}"
                )
            except KeyError:
                self.logging.warning(
                    msg=f"ForegroundDatabase.add_exchanges: {self.add_exchanges_data.activity[i]} "
                    f"({self.add_exchanges_data.activity_database[i]}, "
                    f" {self.add_exchanges_data.activity_code[i]}) "
                    "not found in database"
                )

    def validate(self):
        """
        Use built-in Brightway method to validate the foreground database before linking.

        If the validation fails, db_validator returns an Exception. In this
        case the code fails as well and the Exception is written to the log
        file. If db_validator just returns a copy of the dictionary, then the
        database validated successfully and no value is returned.
        """
        validate = db_validator(self.custom_db)
        if not isinstance(validate, dict):
            self.logging.error(
                msg=f"ForegroundDatabase.validate: Custom database is not "
                f"valid: {validate}"
            )
            sys.exit(1)
        else:
            self.logging.info(
                msg="ForegroundDatabase.validate: Custom database is valid"
            )

    def write_foreground_db(self):
        """Use SQL backend to write the foreground database to file."""
        raise NotImplementedError
