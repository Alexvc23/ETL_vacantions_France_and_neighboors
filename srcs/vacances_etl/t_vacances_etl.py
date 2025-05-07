"""ETL that
1. loads the raw *_fr‑en‑calendrier‑scolaire_ CSV (semicolon‑separated)
2. creates a staging table
3. generates a *binary* day‑by‑day vacation matrix (`t_vacances`) with one
   column per region/zone, containing 0/1 integers.

The mapping rules implemented match the project brief:
  * Zone A → fr_zone_a  * Zone B → fr_zone_b  * Zone C → fr_zone_c
  * If the **Académies** field contains « Corse » the row also maps to
    fr_corse.
Additional regions (Belgium, Germany, …) can be handled later by adapting
`_extract_region_codes()`.
"""

from __future__ import annotations
import os
from datetime import timedelta
from typing import List, Sequence
import pandas as pd
from sqlalchemy import Date, create_engine
from sqlalchemy.types import Integer

# Default connection parameters (Khubeo_IA creds)
# DB_HOST = os.getenv("DB_HOST", "postgres")

# # Override with real Postgres config if provided
# DB_USER = os.getenv("POSTGRES_USER", "jvalenci")
# DB_PASS = os.getenv("POSTGRES_PASSWORD", "mysecretpassword")
# DB_NAME = os.getenv("POSTGRES_DB", "piscineds")
# DB_PORT = os.getenv("DB_PORT", '5432')
# # ──────────────────────────────────────────────────────────────────────────────

# def get_engine():
#     url = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
#     return create_engine(url, pool_pre_ping=True)


DB_HOST = os.getenv("DB_HOST", "37.61.241.45")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "Khubeo_IA")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASS = os.getenv("DB_PASS", "Xjp2yCm$G36WR4E")


def get_engine():
    """Return a SQLAlchemy engine using the Khubeo_IA credentials."""
    return create_engine(
        f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}",
        pool_pre_ping=True,
    )

# ─── Region‑code mapping ─────────────────────────────────────────────────────

# key = low‑cased value found in Zones column → value = column in t_vacances
ZONE_MAP = {
    # # France
    "zone a": "vac_fr_zone_a",
    "zone b": "vac_fr_zone_b",
    "zone c": "vac_fr_zone_c",
    # special case handled by academies: Corse → fr_corse

    # Belgique
    "be_nl": "vac_bel",

    # Allemagne
    "de_by": "vac_all",

    # Suisse
    "ch_zh": "vac_sui",

    # Italie
    "it_bz": "vac_ita",

    # Espagne
    "es_ga": "vac_esp",

    # Luxembourg (whole country)
    "lu": "vac_lux",
}

# French columns ordering
FRANCE_COLS = ["vac_fr_zone_a", "vac_fr_zone_b", "vac_fr_zone_c", "vac_fr_corse"]

# ─── Extract ─────────────────────────────────────────────────────────────────
def load_csvs(csv_paths: Sequence[str] | str) -> pd.DataFrame:
    """
    Load one or multiple CSV files into a concatenated pandas DataFrame.

    Args:
        csv_paths (Union[Sequence[str], str]): Path(s) to the CSV file(s). Can be a single path string or a sequence of path strings.

    Returns:
        pd.DataFrame: A single DataFrame containing data from all input CSV files, with rows concatenated.
                     The index will be reset to a continuous sequence.

    Notes:
        - CSV files are expected to use semicolon (;) as delimiter
        - UTF-8 with BOM encoding is assumed
        - When multiple files are provided, they are concatenated vertically (by rows)
    """
    if isinstance(csv_paths, str):
        csv_paths = [csv_paths]
    dfs = [pd.read_csv(p, sep=";", encoding="utf-8-sig") for p in csv_paths]
    return pd.concat(dfs, ignore_index=True)



# ─── Transform ────────────────────────────────────────────────────────────────
def _extract_region_codes(row) -> List[str]:
    """
    Extract region codes from a dataframe row.
    
    This function processes a row containing zone and académie information
    and returns a list of corresponding region codes.
    
    Args:
        row: A pandas Series or dict-like object containing at least 'Zones' and 'Académies' keys
    
    Returns:
        List[str]: A list of region code strings (e.g., 'fr_zone_a', 'fr_corse')
        
    Notes:
        - Uses the global ZONE_MAP dictionary to map zone names to codes
        - Special handling for Corse (Corsica) which is included regardless of zone
    """
    codes: List[str] = []
    zone_raw = str(row["Zones"]).strip().lower()
    if zone_raw in ZONE_MAP:
        codes.append(ZONE_MAP[zone_raw])
    if "corse" in str(row["Académies"]).lower():
        codes.append("vac_fr_corse")
    return codes


# ─── transform_to_binary ──────────────────────────────────────────────────────
def transform_to_binary(df: pd.DataFrame) -> pd.DataFrame:
    """
    Transforms the vacation data into a binary format where each date has flags indicating which regions have holidays.
    This function processes raw vacation data to create a table where:
    - Each row represents a unique date
    - Each region has a binary column (1 = vacation day, 0 = school day)
    - Regions are grouped and consolidated by date
    The process includes:
    1. Normalizing date formats 
    2. Expanding date ranges into individual days
    3. Mapping zone information to specific region codes
    4. Creating binary indicators for each region on each date
    5. Grouping by date and aggregating region flags
    Parameters
    ----------
    df : pd.DataFrame
        Input DataFrame containing vacation data with columns:
        - "Date de début" (start date)
        - "Date de fin" (end date)
        - "Zones" (vacation zone information)
        - "Académies" (academic region information)
        - "annee_scolaire" (school year)
    Returns
    -------
    pd.DataFrame
        Transformed DataFrame with:
        - "date" column for each individual date
        - Binary columns for each region (1=vacation, 0=school)
        - "annee_scolaire" column preserved from input
        Columns are ordered with date first, then French zones, then other countries
    """

    # normalize dates
    # We create two new columns: start and end, which are the start and end dates
    # in a better format for processing.
    # Convert the string date columns to datetime objects, then extract just the date part
    # We set utc=True to ensure consistent timezone handling during conversion
    # This creates clean date objects that can be used for the date range calculations below
    # utc means that the dates are converted to UTC timezone 
    # strip off the “T00:00:00+HH:MM” so we never convert by UTC
    df["start"] = pd.to_datetime(
        df["Date de début"].str.split("T").str[0],
        format="%Y-%m-%d"
    ).dt.date

    df["end"] = pd.to_datetime(
        df["Date de fin"].str.split("T").str[0],
        format="%Y-%m-%d"
    ).dt.date


    records = []
    # df.iterrrows() returns a tuple (index, row) for each row in the DataFrame
    # for each row, extract the start and end dates and the region codes
    for _, row in df.iterrows():
        regs = []
        zp = str(row["Zones"]).strip().lower()
        if zp in ZONE_MAP:
            regs.append(ZONE_MAP[zp])
        if "corse" in str(row["Académies"]).lower():
            regs.append("vac_fr_corse")
        if not regs:
            continue

        # create a date range from start to end date
        d = row["start"]
        # loop through the date range and create a record for each day
        while d < row["end"]:
            rec = {
                "date": d,
                "annee_scolaire": row.get("annee_scolaire")
            }
            for r in regs:
                rec[r] = 1
            records.append(rec)
            # increment date by one day
            d += timedelta(days=1)

    # create a DataFrame from the records
    # The records list contains dictionaries, each representing a row of data
    wide = pd.DataFrame.from_records(records)

    # find all flag columns
    region_cols = sorted([c for c in wide.columns
                          if c not in ("date", "annee_scolaire")])

    # Define aggregation rules for each column when grouping
    # - For region/vacation columns: use max (1 if any record has vacation, 0 otherwise)
    # - For annee_scolaire: keep the first value since it should be consistent for each date
    agg = {c: "max" for c in region_cols}
    agg["annee_scolaire"] = "first"
    
    # Group the data by date, keeping date as a regular column (not index)
    # Apply the aggregation functions defined above to consolidate multiple entries for the same date
    # This handles cases where a single date might appear in multiple records
    grouped = wide.groupby("date", as_index=False).agg(agg)

    # fill missing flags with 0
    for c in region_cols:
        grouped[c] = grouped[c].fillna(0).astype(int)

    # reorder: date, French zones, then all other countries, then annee_scolaire
    # Get all region columns that are not part of the French zones
    other = [c for c in region_cols if c not in FRANCE_COLS] 
    # Create an ordered list with date first, then French zones, then other regions, finally school year
    ordered = ["date", *FRANCE_COLS, *other, "annee_scolaire"]
    # Filter the ordered list to include only columns that actually exist in the grouped DataFrame
    final = [c for c in ordered if c in grouped.columns]

    return grouped[final]


def load_final_table(engine, df: pd.DataFrame, table_name: str = "t_vacances_scolaires"):
    """
    Loads the final DataFrame into a SQL table.
    
    This function saves the processed vacation data DataFrame into a SQL database table,
    applying appropriate data types for dates and binary flags.
    
    Parameters
    ----------
    engine : sqlalchemy.engine.Engine
        The SQLAlchemy engine connection to the database
    df : pandas.DataFrame
        The DataFrame containing the processed vacation data to be loaded
    table_name : str, default "t_vacances"
        The name of the target table in the database
        
    Returns
    -------
    None
        The function writes to the database but doesn't return any value
    
    Notes
    -----
    - Date columns are cast to SQL Date type
    - All columns except 'date' and 'annee_scolaire' are cast to Integer type
    - The function will replace the existing table if it exists
    """
       # define the table columns names and data types
    # Rename columns to add 'vac_' prefix for date and annee_scolaire
    df = df.rename(columns={
        "date": "vac_date", 
        "annee_scolaire": "vac_annee_scolaire"
    })
    
    # define column data types
    dtype = {"vac_date": Date()}
    for col in df.columns:
        if col not in ("vac_date", "vac_annee_scolaire"):
            dtype[col] = Integer()

    df.to_sql(table_name, engine, if_exists="replace", index=False, dtype=dtype)

# ─── Orchestrator ────────────────────────────────────────────────────────────
def run_etl(csv_paths: Sequence[str] | str, *, sql_dir: str | None = None):
    """
    Execute the ETL (Extract, Transform, Load) process for vacation data.
    This function loads CSV data, transforms it into binary format, and loads it into a final 
    database table. It can also execute optional SQL scripts after the main ETL process.
    Parameters:
    ----------
    csv_paths : Sequence[str] | str
        Path(s) to CSV files containing the source data. Can be a single path string
        or a sequence of path strings.
    sql_dir : str | None, optional
        Directory containing SQL scripts to execute after the ETL process. 
        If provided, the function will look for and execute 't_region_vacances.sql' 
        and 't_vacances.sql' if they exist in this directory. Default is None.
    Returns:
    -------
    None
        The function prints a confirmation message with the number of rows loaded,
        but does not return any value.
    Notes:
    -----
    The function requires a database connection which is obtained through the get_engine() function.
    """
    engine = get_engine()
    raw = load_csvs(csv_paths)

    binary = transform_to_binary(raw)
    load_final_table(engine, binary)

    # optional SQL extras
    if sql_dir:
        for fname in ("t_region_vacances.sql", "t_vacances.sql"):
            p = os.path.join(sql_dir, fname)
            if os.path.exists(p):
                with open(p, "r", encoding="utf-8") as f, engine.begin() as conn:
                    conn.exec_driver_sql(f.read())

    print(f"✅ ETL complete: {len(binary)} rows in t_vacances.")

if __name__ == "__main__":
    import argparse, glob
    p = argparse.ArgumentParser(description="Build t_vacances from CSV(s)")
    p.add_argument("csv", nargs="+", help="semicolon‑CSV files, glob ok")
    p.add_argument("--sql-dir", default=None, help="extra SQL scripts")
    args = p.parse_args()
    paths = sum((glob.glob(x) for x in args.csv), [])
    run_etl(paths, sql_dir=args.sql_dir)