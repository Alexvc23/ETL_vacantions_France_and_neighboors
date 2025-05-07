
import os
import sys
import argparse, glob
# import module  with business logic
from vacances_etl.t_vacances_etl import * 

"""
Main script for the ETL process that builds the t_vacances table from CSV files.
This script is the entry point for the ETL pipeline that processes vacation data for France
and neighboring countries. It uses argparse to handle command line arguments and glob to 
expand file patterns.
Usage:
    python main.py path/to/file1.csv path/to/file2.csv 
    python main.py "path/to/*.csv" 
Arguments:
    csv: One or more CSV file paths. Glob patterns are supported.
    --sql-dir: Optional directory containing additional SQL scripts to execute.
Environment:
    The script assumes it's being run from the root of the repository and 
    changes directory to ensure proper module imports.
Note:
    The CSV files are expected to be semicolon-delimited.
Remarks:
    Alexander VALENCIA - 07/05/2025
"""

# make sure we are in the right directory to import the modules
# this is a workaround for the fact that the script is run from the root of the repo
os.chdir("/home/avalencia/ETL_vacantions_France_and_neighboors/srcs")  


if __name__ == "__main__":
    p = argparse.ArgumentParser(description="Build t_vacances from CSV(s)")
    p.add_argument("csv", nargs="+", help="semicolon‑CSV files, glob ok")
    args = p.parse_args()
    paths = sum((glob.glob(x) for x in args.csv), [])