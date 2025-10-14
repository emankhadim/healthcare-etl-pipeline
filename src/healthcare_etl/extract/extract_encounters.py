"""
Extract encounters from a messy CSV:
- Handles extra semicolons inside cells
- Skips repeated header rows
- Returns a DataFrame 
"""

from __future__ import annotations
import csv
from pathlib import Path
import pandas as pd
import numpy as np
from healthcare_etl.core.config import ENCOUNTERS_FILE

HEADERS = ["encounter_id", "patient_id", "admit_dt", "discharge_dt", "encounter_type", "source_file"]

def _clean(s):
    return str(s).strip() if pd.notna(s) else ""

def _expand_semicolons(row):
    out = []
    for cell in row:
        c = _clean(cell)
        if ";" in c:
            out.extend([x.strip() for x in c.split(";")])
        else:
            out.append(c)
    return out

def _looks_like_header(row):
    cells = [_clean(c).lower().lstrip(",") for c in row]
    return sum(c in HEADERS for c in cells) >= 3

def read_encounters() -> pd.DataFrame:
    src_name = Path(ENCOUNTERS_FILE).name
    rows = []

    with open(ENCOUNTERS_FILE, "r", newline="", encoding="utf-8") as f:
        for line in csv.reader(f):
            if line and any(_clean(c) for c in line):
                rows.append(_expand_semicolons(line))

    if not rows:
        raise ValueError("No rows found in encounters source.")

    header_idx = next((i for i, r in enumerate(rows) if _looks_like_header(r)), None)
    if header_idx is None:
        raise ValueError("Header not found in encounters file.")

    raw_header = rows[header_idx]
    cols = {h: i for i, h in enumerate(raw_header) if _clean(h).lower().lstrip(",") in HEADERS}

    # Build DF
    df = pd.DataFrame(
        [
            [r[cols[h]] if len(r) > cols[h] else "" for h in HEADERS]
            for r in rows[header_idx + 1:]
            if not _looks_like_header(r)
        ],
        columns=HEADERS,
    )

    # Basic empty cleanup
    df = df.replace({"": np.nan, "nan": np.nan})
    df = df.dropna(subset=["encounter_id", "patient_id"], how="all")

    # Ensure source_file is set from path if missing/blank
    if "source_file" in df.columns:
        df["source_file"] = df["source_file"].astype(str)
        df.loc[df["source_file"].str.strip().eq("") | df["source_file"].isna(), "source_file"] = src_name
    else:
        df["source_file"] = src_name

    return df
