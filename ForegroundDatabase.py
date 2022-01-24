import uuid
import pickle

import pandas as pd
import brightway2 as bw

import bw2data
from bw2data.validate import db_validator

from DataManager import CreateActivities, AddExchanges, CopyActivities,\
    DeleteExchanges

class ForegroundDatabase:
    """
    Create foreground database from imported Excel data.
    """

    def __init__(self,
                 db_name,
                 import_template,
                 generate_keys,
                 logging,
                 project,
                 save_imported_db):
        """
        Assembles a database to import into Brightway as a dictionary.

        Parameters
        ----------
        db_name : str

        import_template: str

        generate_keys : bool

        logging : logger object

        project : str

        save_imported_db : bool

        Returns
        -------
        None
        """
        # Initialize empty dictionary to hold the assembled database
        self.custom_db = {}

        # Table of empty activities to add to the database. Fill in the
        # database columns with custom database name from the config file.
        self.create_activities = CreateActivities(
            fpath=import_template
        ).backfill(
            column='activity_database',
            value=db_name
        )

        # Table of exchanges to add to the database. Fill in the database
        # columns with custom database name from the config file.
        self.add_exchanges = AddExchanges(
            fpath=import_template
        ).backfill(
            column=['activity_database','exchange_database'],
            value=db_name
        )

        # Table of activities to copy to the foreground database from an
        # existing database
        self.copy_activities = CopyActivities(
            fpath=import_template
        )

        # Table of exchanges to remove from the database
        self.delete_exchanges = DeleteExchanges(
            fpath=import_template
        )

        self.logging = logging
        self.project = project

        # If activities listed under Add Exchanges are not also listed under
        # Create Activities, throw an error
        _missing_acts = [
            _
            for _ in self.add_exchanges.activity.unique()
            if _ not in self.create_activities.activity.unique()
        ]
        if _missing_acts:
            self.logging.error(
                msg=f'Add Exchanges: Missing new activities {_missing_acts}')
            exit(1)

        if generate_keys:
            # Generate unique activity code with uuid.
            # Because all activities in this DataFrame are new, all of them
            # need newly created codes. This can be done manually within the
            # file being imported, or automatically here.
            self.create_activities.code = pd.Series(
                data=[
                    uuid.uuid4().hex
                    for _ in range(len(self.create_activities.activity))
                ],
                index=self.create_activities.index
            )

        # The newly created exchanges also need codes. For exchanges that exist
        # in the database being created and imported, these codes were already
        # created and are stored in the self.create_activities DataFrame. For
        # exchanges that exist in another database, like ecoinvent, the user
        # must fill in these values before beginning the import process.
        # In this step, codes stored in self.create_activities are assigned to
        # the corresponding exchange. Codes already present in
        # self.add_exchanges are NOT overwritten.
        self.add_exchanges.activity_code = self.add_exchanges.merge(
            self.create_activities,
            on=['activity_database', 'activity', 'activity_location']
        ).code

        # Filling the exchange codes is done in two steps. First the merge gets
        # us previously created codes for new exchanges in the custom database.
        # Then, the codes from the merge are combined with the existing
        # exchange_code column, which may have codes from other databases.
        _new_exchange_codes = self.add_exchanges.merge(
            self.create_activities,
            left_on=['exchange_database', 'exchange', 'activity_location'],
            right_on=['activity_database', 'reference_product',
                      'activity_location'],
            how='left'
        ).code
        self.add_exchanges.exchange_code = _new_exchange_codes.fillna(
            '') + self.add_exchanges.exchange_code.fillna('')

        # Log the activities to be created and their newly assigned codes
        self.logging.info(
            msg=f'Creating activities: '
                f'{self.create_activities.activity.values.tolist()}'
        )
        self.logging.info(
            msg=f'Creating activity codes: '
                f'{self.create_activities.code.values.tolist()}'
        )

        # @TODO Explicitly define the 'output' amounts to allow for
        # non-unitary activities

        # Create the import data dictionary structure and populate with
        # information on newly created activities only. The exchange
        # information will be added in the next step.
        # @NOTE Is it possible to vectorize to avoid loop?
        for i in self.create_activities.index:
            self.custom_db[(self.create_activities.activity_database[i],
                            self.create_activities.code[i])] = \
                {
                    "name": self.create_activities.activity[i],
                    "unit": self.create_activities.reference_product_unit[i],
                    "location": self.create_activities.activity_location[i],
                    "exchanges": []
                }

        # @TODO Add in functionality for emission categories, to allow for
        # connections to biosphere3 as well as ecoinvent

        # Add exchanges to newly created activities
        # Append the exchange data to the "exchanges" list of dicts under the
        # relevant activity.
        for i in self.add_exchanges.index:
            self.custom_db[
                (self.add_exchanges.activity_database[i],
                 self.add_exchanges.activity_code[i])
            ]['exchanges'].append(
                {
                    "amount": self.add_exchanges.amount[i],
                    "input" : (self.add_exchanges.exchange_database[i],
                               self.add_exchanges.exchange_code[i]),
                    "unit"  : self.add_exchanges.unit[i],
                    "type"  : self.add_exchanges.exchange_type[i],
                }
            )

        # @TODO Copy activities with their exchanges from an existing database
        # to the custom database


        self.logging.info(msg=f'Custom database assembled')

        # Save a copy of the custom database for future reference
        if save_imported_db:
            with open('imported_db.obj', 'wb') as db_dump:
                pickle.dump(self.custom_db, db_dump)
                db_dump.close()
            self.add_exchanges.to_csv('add_exchanges.csv', index=False)
            self.create_activities.to_csv('create_activities.csv', index=False)


    def copy_activities(self):
        """
        Reads from the Copy Activities input dataset to locate activities and
        their exchanges in the source database (ecoinvent format is assumed),
        format the data for inclusion in the custom database, and copy the
        data to the custom database. Any activities that are listed for copying
        but don't exist in the source database are skipped with a warning.
        """
        if not self.copy_activities:
            self.logging.warning(
                msg='ForegroundDatabase.copy_activities: No activities to copy'
            )
            pass

        _dblen = len(self.custom_db)

        bw.projects.set_current(self.project)

        # Check that all source_databases exist in our project
        for _sdb in self.copy_activities.source_database.unique():
            if _sdb not in [key for key, value in bw.databases.items()]:
                self.logging.error(
                    msg=f'ForegroundDatabase.copy_activities: Source database '
                        f'{_sdb} is not in Brightway project {self.project} '
                        f'imported databases'
                )
                pass

            # Use the source_database column to set the Brighway database being
            # searched
            _bwdb = bw.Database(_sdb)

            # Use the activity_code to search the database
            for key, act in self.copy_activities[
                'activity_code', 'activity'
            ].loc[
                self.copy_activities.source_database == _sdb
            ]:
                try:
                    _act = _bwdb.get(key)

                except:
                    # It's not good practice to have a generic except clause,
                    # but the actual exception is internal to Brightway. This
                    # does catch errors in either the database or the activity
                    # code.

                    # Log a warning if the activity_code doesn't exist, but
                    # proceed with processing the rest of the activities to
                    # copy
                    self.logging.warning(
                        msg=f'ForegroundDatabase.copy_activities: {key} '
                            f'({act}) not found in {_sdb}'
                    )
                    _act = None

                # If the activity exists, use a separate method to
                # format the ecoinvent information for addition to the
                # custom database.
                _act_to_add = self.ecoinvent_translator(_act)

                self.custom_db[_act_to_add[0]] = _act_to_add[1]

        self.logging.info(msg=f'{len(self.custom_db) - _dblen} activities '
                              f'copied to custom database')


    @staticmethod
    def ecoinvent_translator(
            activity : bw2data.backends.peewee.proxies.Activity
    ):
        """
        Parameters
        ----------
        activity : bw2data.backends.peewee.proxies.Activity

        Returns
        ---------
        A key and value pair with activity and exchange data copied from
        ecoinvent. The pair is formatted for inclusion in the custom database
        in dictionary (pre-import) format.
        """
        if activity is None:
            pass

        # Assemble the tuple that identifies this activity
        _key = (activity['database'], activity['code'])

        # Create the activity dictionary structure without exchange information
        _value = {
            "name"     : activity['name'],
            "unit"     : activity['unit'],
            "location" : activity['location'],
            "exchanges": []
        }

        # Format the exchanges
        # This will be an empty list if there are no exchanges.
        _exchanges = [i.as_dict() for i in activity.exchanges()]

        # @NOTE Leaving the more detailed formatter here in case using the
        # entire exchange dictionary breaks something downstream.
        # _exchanges = [{
        #     "amount": i.amount,
        #     "name": i.as_dict()['name'],
        #     "input" : i.as_dict['input'],
        #     "output": i.as_dict['output'],
        #     "unit"  : i.unit,
        #     "type"  : i.as_dict()['type'],
        # } for i in activity.exchanges()]

        # Append exchanges, if there are any, to the activity dictionary
        if _exchanges:
            _value['exchanges'].append(_exchanges)

        return _key, _value


    def delete_exchanges(self):
        """

        """
        if not self.custom_db:
            self.logging.warning(
                msg='ForegroundDatabase.delete_exchanges: No exchanges in '
                    'custom database to delete'
            )
            pass

        if not self.delete_exchanges:
            self.logging.warning(
                msg='ForegroundDatabase.delete_exchanges: No delete exchange '
                    'information'
            )
            pass

        # Assemble tuples with the activity_database and activity_code to
        # access the activity in self.custom_db

        # Assemble the activity_database, exchange_code tuple to identify the
        # exchange to delete

        # Remove the exchange from the activity


    def validate(self):
        """
        Use built-in Brightway method to validate the custom database before
        linking.
        If the validation fails, db_validator returns an Exception. In this
        case the code fails as well and the Exception is written to the log
        file. If db_validator just returns a copy of the dictionary, then the
        database validated successfully and no value is returned.
        """

        validate = db_validator(self.custom_db)
        if type(validate) is not dict:
            self.logging.error(
                msg=f'ForegroundDatabase.validate: Custom database is not '
                    f'valid: {validate}'
            )
            exit(1)
        else:
            self.logging.info(
                msg='ForegroundDatabase.validate: Custom database is valid'
            )


    def write_foreground_db(self):
        """

        """
        raise NotImplementedError