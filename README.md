# autoBW

* Generate a Brightway-compatible, custom foreground inventory database from a human-readable Excel workbook.
* Automatically validate the foreground database and connect to ecoinvent and biosphere.
* Under development, not yet implemented: automatically perform one or more impact assessment calculations.

# Setup

* Install package dependencies using either requirements.txt with pip or autobw_conda.yml with conda
* It's recommended to also install Brightway's Activity Browser in a separate environment
* Ecoinvent must be imported into your Brightway project manually. Biosphere will be imported on running autoBW, if needed.
* The name of the ecoinvent database used in your project must match between the project, the config files and the foreground database import file.
* Detailed output is saved to a log file for every autoBW run.

## Brightway config file

* By default this file is named bwconfig.yaml.
* `create_new_project` defaults to False. Because ecoinvent must be imported manually, autobw cannot currently be used to create a complete project with all background database.
* `data_directory` is the full path to directory where the config files are located. This location will also be where output files and graphics will be saved.

## Case study config file

* By default this file is named caseconfig.yaml.
* Use the provided file to specify the local Brightway projects, which (background) databases should be included in the project, and details about the foreground database to create.
* `fg_db_import` should the the name of the Excel file containing the foreground data. See the provided import_template.xlsx for guidance.
* Set `generate_keys` to False if you have provided unique activity and exchange keys for all newly created activities and exchanges in the foreground data. Otherwise UUIDs will be generated and assigned automatically.

# Run

* autoBW can be run from an IDE or from the command line. If using Visual Studio Code, the following launch.json structure can be used to set up runs:
```
{
    "version": "0.2.0",
    "configurations": [
        {
            "name": "Python: Current File",
            "type": "python",
            "request": "launch",
            "program": "${file}",
            "env": {
                "PYTHONPATH": "${workspaceFolder};[location of autobw codebase]"
            },
            "console": "integratedTerminal",
            "args": [
                "--data","[path to data diectory]",
                "--bwconfig", "bwconfig.yaml",
                "--caseconfig", "caseconfig.yaml"
            ],
        }
    ]
}
```
* Command line arguments are `--data`, the path to the data directory, `--bwconfig`, the Brightway config file name with extension, and `--caseconfig`, the case study config file name with extension.
