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

# French columns ordering
FRANCE_COLS = ["fr_zone_a", "fr_zone_b", "fr_zone_c", "fr_corse"]

# ─── Extract ─────────────────────────────────────────────────────────────────
def load_csvs(csv_paths: Sequence[str] | str) -> pd.DataFrame:
    if isinstance(csv_paths, str):
        csv_paths = [csv_paths]
    dfs = [pd.read_csv(p, sep=";", encoding="utf-8-sig") for p in csv_paths]
    return pd.concat(dfs, ignore_index=True)

# ─── Transform ────────────────────────────────────────────────────────────────
def _extract_region_codes(row) -> List[str]:
    codes: List[str] = []
    zone_raw = str(row["Zones"]).strip().lower()
    if zone_raw in ZONE_MAP:
        codes.append(ZONE_MAP[zone_raw])
    if "corse" in str(row["Académies"]).lower():
        codes.append("fr_corse")
    return codes


# ─── transform_to_binary ──────────────────────────────────────────────────────
def transform_to_binary(df: pd.DataFrame) -> pd.DataFrame:
    # normalize dates
    df["start"] = pd.to_datetime(df["Date de début"], utc=True).dt.date
    df["end"]   = pd.to_datetime(df["Date de fin"],   utc=True).dt.date

    records = []
    for _, row in df.iterrows():
        regs = []
        zp = str(row["Zones"]).strip().lower()
        if zp in ZONE_MAP:
            regs.append(ZONE_MAP[zp])
        if "corse" in str(row["Académies"]).lower():
            regs.append("fr_corse")
        if not regs:
            continue

        d = row["start"]
        while d < row["end"]:
            rec = {
                "date": d,
                "annee_scolaire": row.get("annee_scolaire")
            }
            for r in regs:
                rec[r] = 1
            records.append(rec)
            d += timedelta(days=1)

    wide = pd.DataFrame.from_records(records)

    # find all flag columns
    region_cols = sorted([c for c in wide.columns
                          if c not in ("date", "annee_scolaire")])

    # group & aggregate: flags=max, annee_scolaire=first
    agg = {c: "max" for c in region_cols}
    agg["annee_scolaire"] = "first"
    grouped = wide.groupby("date", as_index=False).agg(agg)

    # fill missing flags with 0
    for c in region_cols:
        grouped[c] = grouped[c].fillna(0).astype(int)

    # reorder: date, French zones, then all other countries, then annee_scolaire
    other = [c for c in region_cols if c not in FRANCE_COLS]
    ordered = ["date", *FRANCE_COLS, *other, "annee_scolaire"]
    final = [c for c in ordered if c in grouped.columns]

    return grouped[final]

# ─── Load ────────────────────────────────────────────────────────────────────
def create_staging_table(engine, df: pd.DataFrame, name: str = "staging_vacances"):
    df.to_sql(name, engine, if_exists="replace", index=False)


def load_final_table(engine, df: pd.DataFrame, table_name: str = "t_vacances"):
    # define dtypes: date and 0/1 flags
    dtype = {"date": Date()}
    for col in df.columns:
        if col not in ("date", "annee_scolaire"):
            dtype[col] = Integer()
    df.to_sql(table_name, engine, if_exists="replace", index=False, dtype=dtype)

# ─── Orchestrator ────────────────────────────────────────────────────────────
def run_etl(csv_paths: Sequence[str] | str, *, sql_dir: str | None = None):
    engine = get_engine()
    raw = load_csvs(csv_paths)
    create_staging_table(engine, raw)

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