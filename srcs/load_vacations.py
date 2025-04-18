"""Populate t_vacances table on your **Khubeo_IA** Postgres server from the official
MENJ CSV (and future neighbouring‑country feeds).

The script follows the same connection pattern as your existing helper
functions **getDataPred** and **deliverPredictions** so it drops straight
into your codebase.

Usage (examples)
----------------
# import only 2025 dates
python load_vacations.py --year 2025

# full import, overriding connection parameters via env‑vars
DB_HOST=127.0.0.1 DB_PASS=secret python load_vacations.py

Idempotent: rows are *upserted* with `ON CONFLICT (vc_date)`.
"""

from __future__ import annotations

import argparse
import os
from datetime import date, timedelta
from typing import Iterable

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
from tqdm import tqdm

# ---------------------------------------------------------------------------
# Database connection (same style as your examples)                     🐘
# ---------------------------------------------------------------------------

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


# ---------------------------------------------------------------------------
# CSV source & mapping                                                  📑
# ---------------------------------------------------------------------------

CSV_PATH = os.getenv(
    "FR_CSV_PATH",
    "calendrier-scolaire-toutes-academies (1).csv",
)

# Map labels from the MENJ CSV to boolean column names in t_vacances
ZONE_COLUMN_MAP: dict[str, str] = {
    "Zone A": "fr_zone_a",
    "Zone B": "fr_zone_b",
    "Zone C": "fr_zone_c",
    "Corse": "fr_zone_corse",
    # ---- add neighbouring countries here when feeds are ready ----
    # "Belgique": "belgique",
    # "Allemagne": "allemagne",
    # ...
}

ALL_COLUMNS: list[str] = [
    "vc_date",
    "fr_zone_a",
    "fr_zone_b",
    "fr_zone_c",
    "fr_zone_corse",
    "allemagne",
    "belgique",
    "espagne",
    "italie",
    "suisse",
    "andorre",
    "monaco",
    "luxembourg",
]

# ---------------------------------------------------------------------------
# Helpers                                                               🔧
# ---------------------------------------------------------------------------

def each_day(start: date, end: date) -> Iterable[date]:
    """Yield every day **inclusive** between *start* and *end*."""
    d = start
    while d <= end:
        yield d
        d += timedelta(days=1)


def load_french_csv(csv_path: str, school_year: int | None = None) -> pd.DataFrame:
    """Return a DataFrame (*vc_date*, *region_col*) from the MENJ CSV."""
    df_src = pd.read_csv(csv_path, dayfirst=True)
    records: list[tuple[date, str]] = []

    for _, row in df_src.iterrows():
        desc = str(row.get("Description", "")).strip()
        loc = str(row.get("Location", "")).strip()
        label = next((c for c in (desc, loc) if c in ZONE_COLUMN_MAP), None)
        if label is None:
            continue  # not a zone we care about

        start = pd.to_datetime(row["Start Date"], dayfirst=True, errors="coerce").date()
        end = pd.to_datetime(row["End Date"], dayfirst=True, errors="coerce").date()
        if pd.isna(start) or pd.isna(end):
            continue

        if school_year and start.year != school_year and end.year != school_year:
            continue  # ignore other years

        for d in each_day(start, end):
            records.append((d, ZONE_COLUMN_MAP[label]))

    return pd.DataFrame(records, columns=["vc_date", "region_col"])


def pivot_flags(df_long: pd.DataFrame) -> pd.DataFrame:
    """Convert long format to wide boolean‑flag format suitable for INSERT."""
    wide = (
        df_long.assign(flag=True)
        .pivot_table(index="vc_date", columns="region_col", values="flag", fill_value=False)
        .reset_index()
    )
    # add missing columns (pivot drops zones not present in the subset)
    for col in ALL_COLUMNS[1:]:
        if col not in wide.columns:
            wide[col] = False
    return wide[ALL_COLUMNS].sort_values("vc_date")


def upsert_t_vacances(df_flags: pd.DataFrame):
    """Insert/Update each date row into **t_vacances** (idempotent)."""
    engine = get_engine()
    placeholders = ", ".join(f"{c} = EXCLUDED.{c}" for c in ALL_COLUMNS[1:])
    insert_sql = (
        f"INSERT INTO t_vacances ({', '.join(ALL_COLUMNS)})\n"
        f"VALUES ({', '.join(':'+c for c in ALL_COLUMNS)})\n"
        f"ON CONFLICT (vc_date) DO UPDATE SET {placeholders};"
    )

    with engine.begin() as conn:
        for _, row in tqdm(df_flags.iterrows(), total=len(df_flags), desc="Upserting"):
            conn.execute(text(insert_sql), row.to_dict())


# ---------------------------------------------------------------------------
# Main                                                                   🚀
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Load school vacation days into Khubeo_IA.t_vacances")
    parser.add_argument("--year", type=int, default=None, help="Only import a given calendar year (e.g. 2025)")
    args = parser.parse_args()

    try:
        print("Parsing French CSV…")
        if not os.path.exists(CSV_PATH):
            raise FileNotFoundError(f"CSV file not found: {CSV_PATH}")
            
        df_long = load_french_csv(CSV_PATH, school_year=args.year)
        if df_long.empty:
            print("Warning: No vacation data found in the CSV file.")
            return
            
        df_flags = pivot_flags(df_long)
        print(f"{len(df_flags):,} distinct dates ready for upsert → {DB_HOST}:{DB_NAME}")

        try:
            upsert_t_vacances(df_flags)
            print("✅ Import completed (ok)")
        except SQLAlchemyError as err:
            print(f"❌ Database error: {err}")
            raise SystemExit(1)
            
    except FileNotFoundError as err:
        print(f"❌ File error: {err}")
        raise SystemExit(2)
    except pd.errors.ParserError as err:
        print(f"❌ CSV parsing error: {err}")
        raise SystemExit(3)
    except Exception as err:
        print(f"❌ Unexpected error: {type(err).__name__}: {err}")
        raise SystemExit(4)


if __name__ == "__main__":
    main()
