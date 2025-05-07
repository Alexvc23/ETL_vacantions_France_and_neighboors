import os
import sys
import argparse
import glob
import json # Import the json library

# make sure we are in the right directory to import the modules
# this is a workaround for the fact that the script is run from the root of the repo
# Be cautious with hardcoding paths like this; consider making it configurable
os.chdir("/home/avalencia/ETL_vacantions_France_and_neighboors/srcs")

# import module with business logic
# Assuming this module is correctly located relative to the script's execution directory
from vacances_etl.t_vacances_etl import *

"""
Main script for the ETL process that builds the t_vacances table from CSV files.
This script is the entry point for the ETL pipeline that processes vacation data for France
and neighboring countries. It uses argparse to handle command line arguments and glob to
expand file patterns.
By default, it reads CSV file paths from a specified JSON file.
Usage:
    python main.py [--json-config path/to/config.json]
    python main.py path/to/file1.csv path/to/file2.csv # Existing functionality still works
    python main.py "path/to/*.csv" # Existing functionality still works
Arguments:
    csv: One or more CSV file paths (used if --json-config is not provided or overrides it).
         Glob patterns are supported.
    --json-config: Optional path to a JSON configuration file containing a list of CSV paths
                   under the key "csv_files". Defaults to "config.json" if not specified
                   and no 'csv' arguments are provided.
Environment:
    The script assumes it's being run from the root of the repository and
    changes directory to ensure proper module imports.
Note:
    The CSV files are expected to be semicolon-delimited.
Remarks:
    Alexander VALENCIA - 07/05/2025
    Modified to accept JSON config file - Alexander VALENCIA 
"""

if __name__ == "__main__":
    p = argparse.ArgumentParser(description="Build t_vacances from CSV(s)")

    # Add argument for the JSON configuration file
    p.add_argument(
        "--json-config",
        help="Path to a JSON file containing CSV paths under 'csv_files'",
        default="config.json" # Default JSON file name
    )

    # Keep the original 'csv' argument for flexibility (can override JSON or be used alone)
    p.add_argument(
        "csv",
        nargs="*", # Use '*' to make it optional when --json-config is used
        help="semicolon‑CSV files, glob ok (used if --json-config is not provided or overrides it)"
    )

    args = p.parse_args()

    paths = []

    # Check if CSV paths were provided directly as arguments
    if args.csv:
        print("Using CSV paths provided via command line.")
        paths = sum((glob.glob(x) for x in args.csv), [])
    else:
        # If no CSV paths were provided directly, try to load from the JSON file
        try:
            print(f"Attempting to load CSV paths from JSON config: {args.json_config}")
            with open(args.json_config, 'r') as f:
                config = json.load(f)
                if "csv_files" in config and isinstance(config["csv_files"], list):
                    paths = config["csv_files"]
                    print(f"Loaded {len(paths)} paths from {args.json_config}")
                else:
                    print(f"Error: JSON file '{args.json_config}' does not contain a 'csv_files' list.")
                    sys.exit(1) # Exit if JSON format is incorrect
        except FileNotFoundError:
            print(f"Error: JSON config file '{args.json_config}' not found.")
            print("Please provide CSV files as arguments or ensure the default/specified JSON file exists.")
            sys.exit(1) # Exit if JSON file is not found
        except json.JSONDecodeError:
            print(f"Error: Could not decode JSON from '{args.json_config}'. Please check the file format.")
            sys.exit(1) # Exit if JSON is invalid

    if not paths:
        print("No CSV files found or specified.")
        sys.exit(1) # Exit if no paths are found

    # Now pass the collected paths (either from command line or JSON) to run_etl
    run_etl(paths)