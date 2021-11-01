import brightway2 as bw
import yaml
import logging

class Project:
    def __init__(
            self,
            project
    ):
        """
        Parameters
        ----------
        project
            Dictionary of parameters for instantiating a new Project

        Return

        """

        # Try creating a new Project. If it already exists, throw an error
        # and request a unique project name.
        _name = project.get('name')
        if _name in list(bw.projects):
            print(f'Project {_name} already exists.')
            exit(1)
        else:
            bw.projects.set_current(project.get('name'))

        # Run the setup to get the biosphere database
        bw.bw2setup()

        # Import and do various things to any other database(s)
        # Get the dictionary of database names:locations
        _databases = project.get('database_sources')

        # trim the database dictionary so it contains only those databases
        # that don't already exist
        _db_create = {
            db : _databases[db]
            for db in [key for key, value in _databases.items()]
            if db not in [key for key, value in bw.databases.items()]
        }

        # if all of the databases to import already exist, then
        # _db_create will be an empty dictionary. In this case, take no action.
        if len(_db_create) > 0:
            # If _db_create contains elements, then these are datbases that
            # don't exist and must be imported and postprocessed.
            for _name,_loc in _db_create.items():
                # @TODO Add database format parameter for selecting the import method
                _ei371 = bw.SingleOutputEcospold2Importer(
                    _loc,
                    _name
                )
                _ei371.apply_strategies()
                _ei371.statistics()
                _ei371.write_database()