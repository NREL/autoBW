# autoBW
Automatic LCA with Brightway

## config file directory



### flags

Booleans that control how the code runs.

* create_new_project : If True, then the project to be created is assumed to be both new and unique. An error will be thrown if this flag is set to True and if the specified project name (see project parameters below) already exists in the local Brightway setup.


### fileIO

Parameters related to autoBW inputs and outputs other than LCI databases.

* data_directory : Full path to directory where the config file is saved. This location will also be where output files and graphics will be saved.
* db_edits : Excel (xlsx) file containing information for creating activities and editing exchanges in one of the current project's databases. This file can have up to three sheets with the names: Create Activities, Delete Exchanges, and Add Exchanges. If any of these sheets is missing or blank, then the corresponding type of database edits will not be performed. Sheet names other than those listed here are ignored.

### project

Parameters used in setting up the Brightway project, including the LCI databases, methods, and functional units.

* name : Project name used to either create a new, blank project or set an existing project as the current project.
* databases: A dictionary defining the databases and, if necessary, the locations from which the databases can be imported.
* methods: A dictionary of LCI method names and keys.
* functional_units: A dictionary of informal (ie not necessarily matching the exact exchange names in the database) exchange names, with the values being a list where the first element is the exchange key and the second element is the functional unit amount in kg.
