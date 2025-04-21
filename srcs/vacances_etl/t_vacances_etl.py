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
from sqlalchemy import Date, create_engine, text
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

# ─── Region‑code mapping ─────────────────────────────────────────────────────
# key = low‑cased value found in Zones column → value = column in t_vacances
ZONE_MAP = {
    # France
    "zone a": "fr_zone_a",
    "zone b": "fr_zone_b",
    "zone c": "fr_zone_c",
    # special case handled by academies: Corse → fr_corse

    # Belgique
    "be_nl": "bel",

    # Allemagne
    "de_by": "all",

    # Suisse
    "ch_zh": "sui",

    # Italie
    "it_bz": "ita",

    # Espagne
    "es_ga": "esp",

    # Luxembourg (whole country)
    "lu": "lux",
}

# ─── Extract helpers ─────────────────────────────────────────────────────────

def load_csvs(csv_paths: Sequence[str] | str) -> pd.DataFrame:
    """Read one or many semicolon‑separated CSV files and concatenate."""
    if isinstance(csv_paths, str):
        csv_paths = [csv_paths]
    frames = [pd.read_csv(p, sep=";", encoding="utf-8-sig") for p in csv_paths]
    return pd.concat(frames, ignore_index=True)

# ─── Transform helpers ───────────────────────────────────────────────────────

def _extract_region_codes(row) -> List[str]:
    codes: List[str] = []
    zone_raw = str(row["Zones"]).strip().lower()
    if zone_raw in ZONE_MAP:
        codes.append(ZONE_MAP[zone_raw])

    # special‑case: Corse is in academies field but not a separate zone
    if "corse" in str(row["Académies"]).lower():
        codes.append("fr_corse")
    return codes


def transform_to_binary(df: pd.DataFrame) -> pd.DataFrame:
    """Expand periods to daily rows and pivot to wide 0/1 table."""
    df["start"] = pd.to_datetime(df["Date de début"], utc=True).dt.date
    df["end"] = pd.to_datetime(df["Date de fin"],   utc=True).dt.date

    records = []
    for _, row in df.iterrows():
        regions = _extract_region_codes(row)
        if not regions:
            continue
        d = row["start"]
        while d < row["end"]:  # end exclusive
            rec = {"date": d}
            for r in regions:
                rec[r] = 1
            records.append(rec)
            d += timedelta(days=1)

    wide = pd.DataFrame.from_records(records)
    wide = wide.groupby("date").max().fillna(0).astype(int).reset_index()
    zone_cols = sorted(c for c in wide.columns if c != "date")
    return wide[["date", *zone_cols]]

# ─── Load helpers ────────────────────────────────────────────────────────────

def create_staging_table(engine, df: pd.DataFrame, name: str):
    df.to_sql(name, engine, if_exists="replace", index=False)


def load_final_table(engine, df: pd.DataFrame, table_name: str = "t_vacances"):
    dtype = {"date": Date()}
    for col in df.columns:
        if col != "date":
            dtype[col] = Integer()
    df.to_sql(table_name, engine, if_exists="replace", index=False, dtype=dtype)

# ─── Orchestrator ────────────────────────────────────────────────────────────

def run_etl(csv_paths: Sequence[str] | str, *, sql_dir: str | None = None):
    """Load 1‑n CSV files and build an updated `t_vacances`."""
    engine = get_engine()
    raw_df = load_csvs(csv_paths)
    create_staging_table(engine, raw_df, "staging_vacances")

    binary_df = transform_to_binary(raw_df)
    load_final_table(engine, binary_df)

    if sql_dir:
        for fname in ("t_region_vacances.sql", "t_vacances.sql"):
            path = os.path.join(sql_dir, fname)
            if os.path.exists(path):
                with open(path, "r", encoding="utf-8") as f, engine.begin() as conn:
                    conn.exec_driver_sql(f.read())

    print("✅ ETL complete – rows:", len(binary_df))


if __name__ == "__main__":
    import argparse, glob
    p = argparse.ArgumentParser(description="Build t_vacances from CSV(s)")
    p.add_argument("csv", nargs="+", help="Path(s) to source CSV files – glob allowed")
    p.add_argument("--sql-dir", default=None, help="Folder with extra SQL scripts")
    args = p.parse_args()

    # expand globs on CLI (bash usually does, but Windows doesn’t)
    csv_list = sum([glob.glob(p) for p in args.csv], [])
    run_etl(csv_list, sql_dir=args.sql_dir)