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

import os
from datetime import date, timedelta
from typing import List

import pandas as pd
from sqlalchemy import Boolean, Date, create_engine, text
from sqlalchemy.types import Integer

# Default connection parameters (Khubeo_IA creds)
DB_HOST = os.getenv("DB_HOST", "postgres")

# Override with real Postgres config if provided
DB_USER = os.getenv("POSTGRES_USER", "jvalenci")
DB_PASS = os.getenv("POSTGRES_PASSWORD", "mysecretpassword")
DB_NAME = os.getenv("POSTGRES_DB", "piscineds")
DB_PORT = os.getenv("DB_PORT", '5432')

def get_engine():
    url = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    return create_engine(url, pool_pre_ping=True)


# ──────────────────────────────── E x t r a c t ──────────────────────────────

def load_csv(csv_path: str) -> pd.DataFrame:
    """Read the French school‑calendar CSV (semicolon + BOM)."""
    return pd.read_csv(csv_path, sep=";", encoding="utf-8-sig")


# ──────────────────────────────── T r a n s f o r m ──────────────────────────

def _extract_region_codes(row) -> List[str]:
    """Return region codes affected by *one* CSV row."""
    codes: List[str] = []
    zone = str(row["Zones"]).strip().lower()
    if zone.startswith("zone"):
        # "zone a" → fr_zone_a
        codes.append(f"fr_zone_{zone[-1]}")
    # Corse special‑case (does not belong to a zone)
    if "corse" in str(row["Académies"]).lower():
        codes.append("fr_corse")
    return codes


def transform_to_binary(df: pd.DataFrame) -> pd.DataFrame:
    """Expand every holiday period into daily rows and pivot to wide 0/1 table."""
    # normalise date columns
    df["start"] = pd.to_datetime(df["Date de début"], utc=True).dt.date
    df["end"] = pd.to_datetime(df["Date de fin"], utc=True).dt.date

    records: List[dict] = []
    for _, row in df.iterrows():
        codes = _extract_region_codes(row)
        if not codes:
            continue  # ignore rows we don't map yet

        current = row["start"]
        # treat *end* as exclusive so 20 Dec → 21 Dec expands one day
        while current < row["end"]:
            rec = {"jour": current}
            for c in codes:
                rec[c] = 1
            records.append(rec)
            current += timedelta(days=1)

    wide = pd.DataFrame.from_records(records)
    # roll up duplicates (same day, same zone) and fill NaNs with 0
    wide = wide.groupby("jour").max().fillna(0).astype(int).reset_index()

    # always include columns in a deterministic order
    zone_cols = sorted([c for c in wide.columns if c != "jour"])
    return wide[["jour", *zone_cols]]


# ──────────────────────────────── L o a d ─────────────────────────────────────

def create_staging_table(engine, df: pd.DataFrame, table_name: str = "staging_vacances"):
    df.to_sql(table_name, engine, if_exists="replace", index=False)


def load_final_table(engine, df: pd.DataFrame, table_name: str = "t_vacances"):
    # build dtype map: date + Boolean (stored as smallint 0/1 for portability)
    dtype = {"jour": Date()}
    for col in df.columns:
        if col == "jour":
            continue
        dtype[col] = Integer()  # 0/1 smallint → integer; could also use Boolean()
    df.to_sql(table_name, engine, if_exists="replace", index=False, dtype=dtype)


# ──────────────────────────────── D r i v e r ────────────────────────────────

def run_etl(csv_path: str, sql_dir: str | None = None):
    """End‑to‑end pipeline.

    1. Extract CSV → DataFrame
    2. Load staging table (optional αλλά useful for debugging)
    3. Transform → binary matrix
    4. Load matrix into `t_vacances`
    5. *Optionally* run extra SQL scripts in `sql_dir` (e.g. `t_region_vacances.sql`)
    """
    engine = get_engine()

    raw_df = load_csv(csv_path)
    create_staging_table(engine, raw_df)  # comment out if not needed

    binary_df = transform_to_binary(raw_df)
    load_final_table(engine, binary_df)

    if sql_dir:
        for fname in ("t_region_vacances.sql", "t_vacances.sql"):
            path = os.path.join(sql_dir, fname)
            if os.path.exists(path):
                with open(path, "r", encoding="utf-8") as f, engine.begin() as conn:
                    conn.exec_driver_sql(f.read())

    print("✅ ETL completed. Rows in t_vacances:", len(binary_df))






if __name__ == "__main__":
    import argparse

    p = argparse.ArgumentParser(description="Build t_vacances from CSV + SQL")
    p.add_argument("--csv", required=True, help="Path to calendar CSV")
    p.add_argument("--sql-dir", default=None, help="Folder containing extra SQL")
    run_etl(**vars(p.parse_args()))
