"""
Created on November 1 2021.

@author: rhanes
"""
import argparse
import os
import logging
import time

from local_project import LocalProject

# set up arguments for command line running
PARSER = argparse.ArgumentParser(description="Generate custom foreground database in Brightway")
PARSER.add_argument("--data", help="Path to data directory.")
PARSER.add_argument("--bwconfig", help="Name of local Brightway config file.")
PARSER.add_argument("--caseconfig", help="Name of local case study config file.")

# Set up logger
logging.basicConfig(
    filename=os.path.join(PARSER.parse_args().data, f"autobw-{time.time()}.log"),
    level=logging.INFO,
)

if __name__ == "__main__":
    LocalProject(parser=PARSER, logging=logging)
